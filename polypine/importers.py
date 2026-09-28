"""Bulk importers for historical data:

- the public CC0 Kacho dataset of 5m Up/Down markets (per-second top of book), with market metadata and
  true oracle resolutions re-fetched from Gamma in batches, and
- Binance daily 1s kline archives (data.binance.vision) for the underlying.
"""

import asyncio
import io
import logging
import zipfile
from datetime import date, timedelta
from pathlib import Path

import duckdb
import httpx

from .config import ASSETS, BINANCE_SYMBOL, GAMMA_URL, Series
from .gamma import parse_market, parse_resolution
from .storage import ParquetSink

log = logging.getLogger("polypine.importers")

GAMMA_BATCH = 100
BINANCE_ARCHIVE = "https://data.binance.vision/data/spot/daily/klines/{sym}/1s/{sym}-1s-{d}.zip"


def _copy_partitioned(con: duckdb.DuckDBPyConnection, query: str, dest: Path, prefix: str) -> None:
    con.execute(f"""
        COPY ({query}) TO '{dest}' (FORMAT parquet, COMPRESSION zstd,
            PARTITION_BY (asset, tf, date), WRITE_PARTITION_COLUMNS true,
            FILENAME_PATTERN '{prefix}_{{i}}', OVERWRITE_OR_IGNORE true)""")


async def _gamma_events(slugs: list[str], concurrency: int = 4) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)
    out: list[dict] = []
    async with httpx.AsyncClient(base_url=GAMMA_URL, timeout=60) as client:
        async def one(batch):
            async with sem:
                for attempt in range(5):
                    try:
                        r = await client.get("/events", params=[("slug", s) for s in batch] + [("limit", len(batch))])
                        r.raise_for_status()
                        out.extend(r.json())
                        return
                    except httpx.HTTPError as e:
                        log.warning("gamma batch failed (%s), retry %d", e, attempt + 1)
                        await asyncio.sleep(2 ** attempt)
                log.error("gamma batch gave up: %s..%s", batch[0], batch[-1])

        await asyncio.gather(*(one(slugs[i:i + GAMMA_BATCH]) for i in range(0, len(slugs), GAMMA_BATCH)))
    return out


def import_kacho(raw_dir: str | Path, data_root: str | Path, assets=ASSETS) -> dict:
    """Import Kacho 5m ticks into book_ticks, plus Gamma metadata/resolutions for the same markets."""
    raw_dir, data_root = Path(raw_dir), Path(data_root)
    con = duckdb.connect()
    stats = {}
    for asset in assets:
        coin = asset.lower()
        mk, tk = raw_dir / f"{coin}_markets.parquet", raw_dir / f"{coin}_ticks.parquet"
        if not mk.exists() or not tk.exists():
            log.warning("skipping %s: %s not found", asset, mk.parent)
            continue
        series = Series(asset, "5m")

        # Market metadata + oracle outcomes from Gamma (the dataset's outcome is inferred from the last tick).
        slugs = [r[0] for r in con.execute(f"select slug from '{mk}' where slug is not null").fetchall()]
        events = asyncio.run(_gamma_events(slugs))
        sink = ParquetSink(data_root)
        n_res = 0
        for e in events:
            if not e.get("markets"):
                continue
            sink.write("markets", parse_market(e, series).to_row())
            r = parse_resolution(e, series)
            if r:
                sink.write("resolutions", r.to_row())
                n_res += 1
        sink.flush()

        # Ticks -> two book_ticks rows per second (UP and DOWN). The books mirror each other, so the
        # Down bid depth within 5c is the Up ask depth within 5c and vice versa.
        q = f"""
            with t as (select * from '{tk}' where condition_id in (select condition_id from '{mk}'))
            select t*1000 as ts_ms, condition_id, '{asset}' as asset, '5m' as tf, 'UP' as side,
                   bu as best_bid, au as best_ask, su as bid_size, sau as ask_size,
                   du as bid_depth_5c, dd as ask_depth_5c,
                   strftime(to_timestamp(t), '%Y-%m-%d') as date
            from t
            union all
            select t*1000, condition_id, '{asset}', '5m', 'DOWN',
                   bd, ad, sd, sad, dd, du, strftime(to_timestamp(t), '%Y-%m-%d')
            from t"""
        _copy_partitioned(con, q, data_root / "book_ticks", "kacho")
        n_ticks = con.execute(f"select count(*) * 2 from '{tk}'").fetchone()[0]
        stats[asset] = {"markets": len(events), "resolved": n_res, "book_ticks": n_ticks}
        log.info("%s: %s", asset, stats[asset])
    return stats


async def _download_day(client: httpx.AsyncClient, sym: str, d: date, sem: asyncio.Semaphore) -> bytes | None:
    async with sem:
        url = BINANCE_ARCHIVE.format(sym=sym, d=d.isoformat())
        for attempt in range(4):
            try:
                r = await client.get(url)
                if r.status_code == 404:
                    log.warning("no archive for %s %s (not published yet?)", sym, d)
                    return None
                r.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    return z.read(z.namelist()[0])
            except httpx.HTTPError as e:
                log.warning("%s %s failed (%s), retry", sym, d, e)
                await asyncio.sleep(2 ** attempt)
        return None


def import_binance_archive(data_root: str | Path, start: date, end: date, assets=ASSETS,
                           workdir: str | Path | None = None) -> dict:
    """Import Binance 1s klines for each day in [start, end] into underlying_1s."""
    data_root = Path(data_root)
    workdir = Path(workdir or data_root / "raw" / "binance")
    workdir.mkdir(parents=True, exist_ok=True)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]

    async def fetch_all():
        sem = asyncio.Semaphore(8)
        async with httpx.AsyncClient(timeout=120) as client:
            for asset in assets:
                sym = BINANCE_SYMBOL[asset]
                todo = [d for d in days if not (workdir / f"{sym}-1s-{d}.csv").exists()]
                blobs = await asyncio.gather(*(_download_day(client, sym, d, sem) for d in todo))
                for d, blob in zip(todo, blobs):
                    if blob:
                        (workdir / f"{sym}-1s-{d}.csv").write_bytes(blob)

    asyncio.run(fetch_all())

    con = duckdb.connect()
    stats = {}
    for asset in assets:
        sym = BINANCE_SYMBOL[asset]
        files = [str(workdir / f"{sym}-1s-{d}.csv") for d in days if (workdir / f"{sym}-1s-{d}.csv").exists()]
        if not files:
            continue
        # Binance spot archives switched open_time from ms to µs in 2025; normalise to ms.
        q = f"""
            select case when c0 > 1e14 then c0 // 1000 else c0 end as ts_ms, '{asset}' as asset,
                   c1 as open, c2 as high, c3 as low, c4 as close, c5 as volume
            from read_csv({files}, header=false, columns={{
                'c0':'BIGINT','c1':'DOUBLE','c2':'DOUBLE','c3':'DOUBLE','c4':'DOUBLE','c5':'DOUBLE',
                'c6':'BIGINT','c7':'DOUBLE','c8':'BIGINT','c9':'DOUBLE','c10':'DOUBLE','c11':'VARCHAR'}})"""
        con.execute(f"""
            COPY (select *, strftime(to_timestamp(ts_ms / 1000), '%Y-%m-%d') as date from ({q}))
            TO '{data_root / "underlying_1s"}' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (asset, date),
                WRITE_PARTITION_COLUMNS true, FILENAME_PATTERN 'binance_{{i}}', OVERWRITE_OR_IGNORE true)""")
        stats[asset] = con.execute(f"select count(*) from ({q})").fetchone()[0]
        log.info("%s: %d underlying rows", asset, stats[asset])
        for f in files:  # re-downloadable; the CSVs are ~20x the size of the Parquet output
            Path(f).unlink()
    return stats
