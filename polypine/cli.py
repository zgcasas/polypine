import argparse
import asyncio
import logging
import signal
import time

import duckdb

from .config import ASSETS, TIMEFRAMES, all_series
from .storage import SCHEMAS, ParquetSink, compact


def _collect(args) -> None:
    from .collector import Collector

    series = all_series(args.assets, args.timeframes)
    c = Collector(ParquetSink(args.data), series, flush_every=args.flush_every)

    async def main():
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, c.stop)
        await c.run(duration_s=args.duration)

    asyncio.run(main())


def _backfill_underlying(args) -> None:
    from .underlying import UnderlyingFetcher

    async def main():
        sink = ParquetSink(args.data)
        end = int(time.time() * 1000) - 2000
        start = end - int(args.hours * 3600 * 1000)
        fetcher = UnderlyingFetcher(sink)
        try:
            for a in args.assets:
                await fetcher.fetch(a, start, end)
                print(f"{a}: {sink.flush()} rows")
        finally:
            await fetcher.aclose()

    asyncio.run(main())


def _import_kacho(args) -> None:
    from .importers import import_kacho

    for asset, st in import_kacho(args.raw, args.data, args.assets).items():
        print(asset, st)


def _import_binance(args) -> None:
    from datetime import date

    from .importers import import_binance_archive

    st = import_binance_archive(args.data, date.fromisoformat(args.start), date.fromisoformat(args.end), args.assets)
    for asset, n in st.items():
        print(asset, n)


def _backtest(args) -> None:
    import json
    from datetime import datetime, timezone

    from .datafeed import Feed
    from .engine import BacktestConfig, run_backtest

    def ms(d):
        return int(datetime.fromisoformat(d).replace(tzinfo=timezone.utc).timestamp() * 1000)

    inputs = dict(kv.split("=", 1) for kv in args.input)
    inputs = {k: json.loads(v) if v.replace(".", "", 1).lstrip("-").isdigit() else v for k, v in inputs.items()}
    cfg = BacktestConfig(script=args.script, asset=args.asset, tf=args.tf, bar=args.bar,
                         start_ms=ms(args.start), end_ms=ms(args.end), inputs=inputs, stake=args.stake,
                         latency_ms=args.latency_ms, min_secs_left=args.min_secs_left, roll=args.roll)
    res = run_backtest(cfg, Feed(args.data), keep_series=bool(args.out))
    print(json.dumps(res["stats"], indent=2, default=str))
    reasons = {}
    for s in res["skipped"]:
        reasons[s["reason"]] = reasons.get(s["reason"], 0) + 1
    if reasons:
        print("skipped:", reasons)
    if args.out:
        with open(args.out, "w") as f:
            json.dump(res, f, default=str)
        print("wrote", args.out)


def _serve(args) -> None:
    import uvicorn

    from .server import create_app

    print(f"PolyPine on http://{args.host}:{args.port}")
    uvicorn.run(create_app(args.data, args.strategies), host=args.host, port=args.port, log_level="warning")


def _compact(args) -> None:
    for table in SCHEMAS:
        n = compact(args.data, table)
        if n:
            print(f"{table}: compacted {n} partitions")


def _status(args) -> None:
    for table in SCHEMAS:
        glob = f"{args.data}/{table}/**/*.parquet"
        try:
            has_tf = "tf" in SCHEMAS[table].names
            q = f"select asset, {'tf, ' if has_tf else ''}count(*) n " \
                f"from read_parquet('{glob}', hive_partitioning=true, union_by_name=true) group by all order by all"
            rows = duckdb.sql(q).fetchall()
        except duckdb.IOException:
            continue
        print(f"\n{table}")
        for r in rows:
            print("  ", *r)


def main() -> None:
    p = argparse.ArgumentParser(prog="polypine")
    p.add_argument("--data", default="data", help="data root directory")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("collect", help="record live markets to Parquet")
    c.add_argument("--assets", nargs="+", default=list(ASSETS), choices=ASSETS)
    c.add_argument("--timeframes", nargs="+", default=list(TIMEFRAMES), choices=TIMEFRAMES)
    c.add_argument("--flush-every", type=int, default=60, help="seconds between Parquet flushes")
    c.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    c.set_defaults(func=_collect)

    b = sub.add_parser("backfill-underlying", help="download Binance 1s klines")
    b.add_argument("--assets", nargs="+", default=list(ASSETS), choices=ASSETS)
    b.add_argument("--hours", type=float, default=24)
    b.set_defaults(func=_backfill_underlying)

    k = sub.add_parser("import-kacho", help="import the public 5m dataset (+ Gamma resolutions)")
    k.add_argument("--raw", default="data/raw/kacho")
    k.add_argument("--assets", nargs="+", default=list(ASSETS), choices=ASSETS)
    k.set_defaults(func=_import_kacho)

    ib = sub.add_parser("import-binance", help="import Binance daily 1s kline archives")
    ib.add_argument("--start", required=True, help="YYYY-MM-DD")
    ib.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    ib.add_argument("--assets", nargs="+", default=list(ASSETS), choices=ASSETS)
    ib.set_defaults(func=_import_binance)

    bt = sub.add_parser("backtest", help="backtest a Pyne/Pine strategy on Polymarket contracts")
    bt.add_argument("script")
    bt.add_argument("--asset", default="BTC", choices=ASSETS)
    bt.add_argument("--tf", default="5m", choices=TIMEFRAMES, help="Polymarket market timeframe")
    bt.add_argument("--bar", default="1m", help="chart bar the script runs on (1s..1d)")
    bt.add_argument("--start", required=True, help="YYYY-MM-DD[THH:MM]")
    bt.add_argument("--end", required=True)
    bt.add_argument("--input", action="append", default=[], help="script input override: 'Title=value'")
    bt.add_argument("--stake", type=float, default=100.0)
    bt.add_argument("--latency-ms", type=int, default=1000)
    bt.add_argument("--min-secs-left", type=int, default=0)
    bt.add_argument("--roll", action="store_true", help="re-enter each window while Pine stays in position")
    bt.add_argument("--out", help="write full JSON result")
    bt.set_defaults(func=_backtest)

    sv = sub.add_parser("serve", help="run the web app")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8765)
    sv.add_argument("--strategies", default="strategies")
    sv.set_defaults(func=_serve)

    sub.add_parser("compact", help="merge small part files").set_defaults(func=_compact)
    sub.add_parser("status", help="row counts per table").set_defaults(func=_status)

    args = p.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args.func(args)


if __name__ == "__main__":
    main()
