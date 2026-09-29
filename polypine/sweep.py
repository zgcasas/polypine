"""Parameter sweep: run one strategy over a grid of input values in parallel and rank the results.

Bars (underlying + Polymarket quotes) are built once and shared with the workers; each combination only
re-runs the script and the contract simulator (~0.1s for 24h of 1m bars).

A combination only counts as profitable if it is robust enough to be worth a second look: positive net
PnL, at least `min_trades` trades, and still positive without its single best trade (one cheap longshot
paying 20x can make a losing strategy look good). With thousands of combinations on one day some will
pass by chance, so the top results are also split into two halves of the period: parameters with a real
edge should make money in both.
"""

import csv
import itertools
import multiprocessing as mp
import os
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from .datafeed import Feed
from .engine import BacktestConfig, build_bars, run_backtest, script_inputs

_W: dict = {}  # per-worker state: feed, bars, base config


def parse_grid(spec: str) -> tuple[str, list]:
    """'name=start:stop:step' (inclusive, step may be negative) or 'name=a,b,c' -> (name, values)."""
    name, _, rng = spec.partition("=")
    if not name or not rng:
        raise ValueError(f"grid spec {spec!r}: use name=start:stop:step or name=a,b,c")
    if ":" not in rng:
        return name, [_num(v) for v in rng.split(",")]
    start, stop, step = (Decimal(x) for x in rng.split(":"))
    if step == 0 or (stop - start) * step < 0:
        raise ValueError(f"grid spec {spec!r}: step {step} never reaches {stop} from {start}")
    vals, v = [], start
    while (v <= stop) if step > 0 else (v >= stop):
        vals.append(v)
        v += step
    is_int = all(x == x.to_integral_value() for x in (start, stop, step))
    return name, [int(x) if is_int else float(x) for x in vals]


def _num(v: str):
    v = v.strip()
    try:
        return int(v)
    except ValueError:
        return float(v)


def robust(st: dict, min_trades: int) -> bool:
    return (st["trades"] >= min_trades and st["net_pnl"] > 0
            and st["net_pnl_ex_best"] is not None and st["net_pnl_ex_best"] > 0)


def _init(data_root: str, cfg: BacktestConfig, bars: list) -> None:
    _W.update(feed=Feed(data_root), cfg=cfg, bars=bars)


def _run(inputs: dict) -> dict:
    cfg = replace(_W["cfg"], inputs=inputs)
    st = run_backtest(cfg, _W["feed"], keep_series=False, bars=_W["bars"])["stats"]
    return {**inputs, **{k: st[k] for k in ("trades", "win_rate", "avg_entry_price", "net_pnl", "roi", "fees",
                                           "max_drawdown", "largest_win", "net_pnl_ex_best", "pnl_per_trade_t")}}


def _halves(data_root: str, cfg: BacktestConfig, inputs: dict) -> tuple[dict, dict]:
    mid = (cfg.start_ms + cfg.end_ms) // 2
    feed = Feed(data_root)
    out = []
    for a, b in ((cfg.start_ms, mid), (mid, cfg.end_ms)):
        out.append(run_backtest(replace(cfg, inputs=inputs, start_ms=a, end_ms=b), feed, keep_series=False)["stats"])
    return out[0], out[1]


def sweep(cfg: BacktestConfig, grid: dict[str, list], data_root: str = "data", workers: int | None = None,
          min_trades: int = 20, until_profitable: bool = False, out_csv: str | Path | None = None,
          progress=print) -> list[dict]:
    """Run every combination in `grid` (input name -> values). Returns result rows, best net PnL first."""
    known = script_inputs(Path(cfg.script))
    unknown = [k for k in grid if k not in known]
    if unknown:
        raise ValueError(f"unknown inputs {unknown}; the script has: {', '.join(known) or 'none'}")
    names = list(grid)
    combos = [dict(zip(names, vals)) for vals in itertools.product(*(grid[n] for n in names))]

    feed = Feed(data_root)
    t0 = time.time()
    bars = build_bars(feed, cfg)
    if not bars:
        raise ValueError(f"no data for {cfg.asset} in the requested range")
    progress(f"{len(combos)} combinations x {len(bars)} bars; bars built in {time.time() - t0:.1f}s")

    workers = workers or max(1, (os.cpu_count() or 2) - 1)
    rows, found = [], None
    t0 = time.time()
    ctx = mp.get_context("spawn")
    with ctx.Pool(workers, initializer=_init, initargs=(data_root, cfg, bars)) as pool:
        for i, row in enumerate(pool.imap_unordered(_run, combos, chunksize=4), 1):
            rows.append(row)
            if robust(row, min_trades) and (found is None or row["net_pnl"] > found["net_pnl"]):
                found = row
                if until_profitable:
                    progress(f"[{i}/{len(combos)}] first robust profitable combination found")
                    pool.terminate()
                    break
            if i % 200 == 0 or i == len(combos):
                rate = i / (time.time() - t0)
                best = f"best so far {_fmt_inputs(found, names)} net ${found['net_pnl']:.2f}" if found else "none robust yet"
                progress(f"[{i}/{len(combos)}] {rate:.0f}/s, eta {(len(combos) - i) / rate:.0f}s, {best}")

    rows.sort(key=lambda r: r["net_pnl"], reverse=True)
    if out_csv:
        Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
        with open(out_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    return rows


def _fmt_inputs(row: dict | None, names: list[str]) -> str:
    return "" if row is None else " ".join(f"{n}={row[n]}" for n in names)
