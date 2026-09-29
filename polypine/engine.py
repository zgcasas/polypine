"""Backtest engine: run a Pine/Pyne strategy on underlying bars, then trade its signals as Polymarket
Up/Down binary contracts.

Pipeline
  1. Underlying bars (Binance 1s aggregated to `bar`) are fed to PyneCore, with per-bar extra fields
     describing the live Polymarket market at the bar's close (`up_ask`, `secs_left`, `price_to_beat`...).
  2. Every Pine trade (entry time, exit time, direction) becomes a signal. Pine fills orders at the next
     bar's open, so signals never see the bar they were computed on.
  3. The simulator buys the Up token for long signals and the Down token for short signals, at the
     recorded best ask `latency_ms` after the signal, walking the book with the depth we recorded. The
     position is sold at the best bid if Pine exits before the window ends, otherwise it settles at
     $1/$0 on the oracle outcome. Polymarket's taker fee applies to every fill.
"""

import ast
import importlib
import importlib.util
import math
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .config import TF_SECONDS
from .datafeed import BAR_SECONDS, Feed

# PyneCore writes input values into a <script>.toml next to the script (and reads them back, overriding the
# script's defaults). Per-run overrides would then stick forever, so don't let backtests write it.
os.environ.setdefault("PYNE_SAVE_SCRIPT_TOML", "0")

EXTRA_FIELDS = ("up_bid", "up_ask", "down_bid", "down_ask", "secs_left", "secs_in", "price_to_beat",
                "window_start", "oracle_price", "oracle_twap60")


@dataclass
class BacktestConfig:
    script: str                  # path to a Pyne .py script, or a .pine file (needs PYNESYS_API_KEY)
    asset: str = "BTC"
    tf: str = "5m"               # Polymarket market timeframe to trade
    bar: str = "1m"              # chart bar the script runs on
    start_ms: int | None = None
    end_ms: int | None = None
    inputs: dict = field(default_factory=dict)
    stake: float = 100.0         # USDC per entry
    latency_ms: int = 1000       # signal -> fill delay
    min_secs_left: int = 0       # skip entries with less time left in the window
    max_entry_price: float = 0.99
    roll: bool = False           # while the Pine position stays open, re-enter each new window
    tick: float = 0.01


@dataclass
class Signal:
    entry_ms: int
    exit_ms: int | None          # None: still open at the end of the data
    direction: int               # +1 long -> buy UP, -1 short -> buy DOWN
    entry_id: str = ""


@dataclass
class Trade:
    signal_ms: int
    market: str
    slug_start_ms: int
    end_ms: int
    token: str                   # UP / DOWN
    entry_ms: int
    entry_price: float
    shares: float
    cost: float
    fees: float
    exit_ms: int
    exit_price: float | None     # sale price, 1/0 at settlement, None while the market is unresolved
    exit_kind: str               # "sold" | "settled" | "open"
    outcome: str | None
    pnl: float


def fee(shares: float, price: float, rate: float, exponent: float = 1.0) -> float:
    """Polymarket taker fee in USDC for one match: shares * rate * (p * (1 - p)) ** exponent, rounded to 5
    decimals (docs.polymarket.com/trading/fees). rate/exponent come from each market's Gamma feeSchedule
    (crypto: 0.07 and 1). Makers pay nothing; their 20% rebate is paid daily from a shared pool and is not
    modelled (this simulator only takes liquidity)."""
    return round(shares * rate * (price * (1 - price)) ** exponent, 5)


DEPTH_TICKS = 5  # the recorded depth covers the 5c above the best ask; with a 1c tick that's 5 levels


def fill_buy(stake: float, ask: float, ask_size: float | None, depth_5c: float | None,
             tick: float) -> list[tuple[float, float]]:
    """Buy `stake` USDC of shares. Returns the fills as [(shares, price), ...].

    The 1s book record gives the best level's size and the total resting within 5c. The depth beyond the
    best level is assumed to be spread evenly over the next DEPTH_TICKS ticks and is walked level by level;
    anything beyond the 5c depth is left unfilled. (Putting it all one tick worse was far too optimistic for
    cheap contracts, where a tick is 20-25% of the price.)"""
    if ask_size is None:
        return [(stake / ask, ask)]
    fills = []
    left = stake
    take = min(ask_size, left / ask)
    if take > 0:
        fills.append((take, ask))
        left -= take * ask
    rest = 0.0 if depth_5c is None else max(depth_5c - ask_size, 0.0)
    for k in range(1, DEPTH_TICKS + 1):
        px = round(ask + k * tick, 6)
        if left <= 1e-9 or rest <= 0 or px > 0.99:
            break
        take = min(rest / DEPTH_TICKS, left / px)
        fills.append((take, px))
        left -= take * px
    return fills


class Simulator:
    def __init__(self, feed: Feed, cfg: BacktestConfig):
        self.feed, self.cfg = feed, cfg

    def _legs(self, sig: Signal, markets: list[dict]) -> list[tuple[dict, int, int | None]]:
        """(market, entry_ms, exit_ms or None for settle) for each window this signal trades."""
        t_in = sig.entry_ms + self.cfg.latency_ms
        t_out = sig.exit_ms + self.cfg.latency_ms if sig.exit_ms is not None else None
        legs = []
        for m in markets:
            if m["end_ms"] <= t_in or (t_out is not None and m["start_ms"] >= t_out):
                continue
            live = m["start_ms"] <= t_in < m["end_ms"]
            if not live and not (self.cfg.roll and m["start_ms"] > t_in):
                continue
            enter = t_in if live else m["start_ms"] + self.cfg.latency_ms
            leave = t_out if t_out is not None and t_out < m["end_ms"] else None
            legs.append((m, enter, leave))
            if not self.cfg.roll:
                break
        return legs

    def run(self, signals: list[Signal]) -> tuple[list[Trade], list[dict]]:
        cfg = self.cfg
        if not signals:
            return [], []
        lo = min(s.entry_ms for s in signals) - TF_SECONDS[cfg.tf] * 1000
        hi = max((s.exit_ms or s.entry_ms) for s in signals) + TF_SECONDS[cfg.tf] * 1000 * 2
        markets = self.feed.markets(cfg.asset, cfg.tf, lo, hi)

        plan = [(s, leg) for s in signals for leg in self._legs(s, markets)]
        times = sorted({t for _, (_, a, b) in plan for t in (a, b) if t is not None})
        quotes = self.feed.quotes_at(cfg.asset, cfg.tf, times)

        trades, skipped = [], []
        for sig, (m, t_in, t_out) in plan:
            side = "up" if sig.direction > 0 else "down"
            q = quotes.get(t_in)
            reason = None
            if q is None or q["condition_id"] != m["condition_id"]:
                reason = "no live market"
            elif q[f"{side}_ask"] is None:
                reason = "no quote"
            elif (m["end_ms"] - t_in) / 1000 < cfg.min_secs_left:
                reason = "too late in window"
            elif q[f"{side}_ask"] > cfg.max_entry_price:
                reason = "price above max"
            if reason:
                skipped.append({"signal_ms": sig.entry_ms, "market": m["slug"], "reason": reason})
                continue

            ask = q[f"{side}_ask"]
            fills = fill_buy(cfg.stake, ask, q[f"{side}_ask_size"], q[f"{side}_ask_depth"], cfg.tick)
            shares, cost = sum(n for n, _ in fills), sum(n * px for n, px in fills)
            if shares <= 0:
                skipped.append({"signal_ms": sig.entry_ms, "market": m["slug"], "reason": "no depth"})
                continue
            rate = m.get("fee_rate") or 0.0  # 0 when the market has fees disabled
            expo = m.get("fee_exponent") or 1.0
            avg_px = cost / shares
            fees = sum(fee(n, px, rate, expo) for n, px in fills)  # charged per match, at its own price

            exit_kind, exit_ms, exit_px = None, m["end_ms"], None
            qo = quotes.get(t_out) if t_out is not None else None
            if qo is not None and qo["condition_id"] == m["condition_id"] and qo[f"{side}_bid"] is not None:
                exit_kind, exit_ms, exit_px = "sold", t_out, qo[f"{side}_bid"]
                fees += fee(shares, exit_px, rate, expo)
            elif m.get("outcome"):
                exit_kind, exit_px = "settled", 1.0 if m["outcome"] == side.upper() else 0.0
            else:
                exit_kind, exit_px = "open", None  # market not resolved yet

            pnl = shares * exit_px - cost - fees if exit_kind != "open" else 0.0
            trades.append(Trade(signal_ms=sig.entry_ms, market=m["slug"], slug_start_ms=m["start_ms"],
                                end_ms=m["end_ms"], token=side.upper(), entry_ms=t_in, entry_price=avg_px,
                                shares=shares, cost=cost, fees=fees, exit_ms=exit_ms, exit_price=exit_px,
                                exit_kind=exit_kind, outcome=m.get("outcome"), pnl=pnl))
        return trades, skipped


def stats(trades: list[Trade], skipped: list[dict]) -> dict:
    done = [t for t in trades if t.exit_kind != "open"]
    pnl = [t.pnl for t in sorted(done, key=lambda t: t.exit_ms)]
    equity, peak, mdd = 0.0, 0.0, 0.0
    for p in pnl:
        equity += p
        peak = max(peak, equity)
        mdd = max(mdd, peak - equity)
    staked = sum(t.cost for t in done)
    settled = [t for t in done if t.exit_kind == "settled"]
    wins = [t for t in done if t.pnl > 0]
    mean = sum(pnl) / len(pnl) if pnl else 0.0
    sd = math.sqrt(sum((p - mean) ** 2 for p in pnl) / (len(pnl) - 1)) if len(pnl) > 1 else 0.0
    return {
        "trades": len(done), "open": len(trades) - len(done), "skipped": len(skipped),
        "win_rate": len(wins) / len(done) if done else None,
        "net_pnl": sum(pnl), "fees": sum(t.fees for t in done), "staked": staked,
        "roi": sum(pnl) / staked if staked else None,
        "avg_entry_price": sum(t.entry_price for t in done) / len(done) if done else None,
        # Settled trades: realised hit rate minus the price paid = edge over the market's implied probability.
        "edge_vs_implied": (sum((t.exit_price - t.entry_price) for t in settled) / len(settled)) if settled else None,
        "max_drawdown": mdd,
        "pnl_per_trade_t": mean / (sd / math.sqrt(len(pnl))) if sd else None,
        "sold": sum(1 for t in done if t.exit_kind == "sold"), "settled": len(settled),
        # Robustness: one cheap longshot paying 20x can dominate a short backtest.
        "largest_win": max(pnl) if pnl else None,
        "net_pnl_ex_best": (sum(pnl) - max(pnl)) if pnl else None,
    }


def _syminfo(asset: str, bar: str):
    from pynecore.core.syminfo import SymInfo

    # Pine timeframe strings: minutes as digits, otherwise S/D suffixes.
    s = BAR_SECONDS[bar]
    period = f"{s}S" if s < 60 else (str(s // 60) if s < 86400 else "1D")
    return SymInfo(prefix="BINANCE", ticker=f"{asset}USDT", currency="USDT", basecurrency=asset,
                   description=f"{asset} / USDT", period=period, type="crypto", mintick=0.00001,
                   pricescale=100000, minmove=1, pointvalue=1.0, mincontract=1e-8, timezone="UTC",
                   volumetype="base", opening_hours=[], session_starts=[], session_ends=[])


def resolve_script(path: str) -> Path:
    p = Path(path)
    if p.suffix == ".pine":
        key = os.environ.get("PYNESYS_API_KEY")
        if not key:
            raise RuntimeError("Compiling .pine needs PYNESYS_API_KEY (pynesys.io); or pass a Pyne .py script")
        from pynecore.pynesys.compiler import PyneComp

        return PyneComp(key).compile(p)
    return p


def script_defaults(script: Path) -> dict:
    """{argument name: default value} for `main(x = input.*(default, ...))` parameters (literal defaults)."""
    tree = ast.parse(script.read_text())
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            args = node.args.args[len(node.args.args) - len(node.args.defaults):]
            for arg, default in zip(args, node.args.defaults):
                if isinstance(default, ast.Call) and default.args and isinstance(default.args[0], ast.Constant):
                    out[arg.arg] = default.args[0].value
                elif isinstance(default, ast.Call):
                    for kw in default.keywords:
                        if kw.arg == "defval" and isinstance(kw.value, ast.Constant):
                            out[arg.arg] = kw.value.value
    return out


def script_inputs(script: Path) -> dict[str, str | None]:
    """{argument name: title} for the script's `main(x = input.*(..., title=...))` parameters."""
    tree = ast.parse(script.read_text())
    out: dict[str, str | None] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            args = node.args.args[len(node.args.args) - len(node.args.defaults):]
            for arg, default in zip(args, node.args.defaults):
                title = None
                if isinstance(default, ast.Call):
                    for kw in default.keywords:
                        if kw.arg == "title" and isinstance(kw.value, ast.Constant):
                            title = kw.value.value
                out[arg.arg] = title
    return out


def resolve_inputs(script: Path, inputs: dict) -> dict:
    """Map override keys to PyneCore's argument names. Keys may be the argument name (`min_bps`) or the
    input title ("Min distance from price to beat (bps)"). Unknown keys raise instead of being ignored."""
    if not inputs:
        return {}
    known = script_inputs(script)
    by_title = {t: a for a, t in known.items() if t}
    out = {}
    for k, v in inputs.items():
        if k in known:
            out[k] = v
        elif k in by_title:
            out[by_title[k]] = v
        else:
            names = ", ".join(f"{a} ({t})" if t else a for a, t in known.items()) or "none"
            raise ValueError(f"unknown input {k!r}; this script's inputs are: {names}")
    return out


def build_bars(feed: Feed, cfg: BacktestConfig):
    """Underlying OHLCV bars with Polymarket extra fields as of each bar's close."""
    from pynecore.types.na import NA
    from pynecore.types.ohlcv import OHLCV

    raw = feed.underlying_bars(cfg.asset, cfg.start_ms, cfg.end_ms, cfg.bar)
    bar_ms = BAR_SECONDS[cfg.bar] * 1000
    closes = [b[0] + bar_ms - 1 for b in raw]
    quotes = feed.quotes_at(cfg.asset, cfg.tf, closes)
    oracle = feed.oracle_at(cfg.asset, closes)
    bars = []
    for ts, o, h, l, c, v in raw:
        q = quotes.get(ts + bar_ms - 1)
        ef = {k: NA(float) for k in EXTRA_FIELDS}
        if q:
            close_ms = ts + bar_ms
            for k in ("up_bid", "up_ask", "down_bid", "down_ask", "price_to_beat"):
                if q[k] is not None:
                    ef[k] = float(q[k])
            ef["secs_left"] = (q["end_ms"] - close_ms) / 1000
            ef["secs_in"] = (close_ms - q["start_ms"]) / 1000
            ef["window_start"] = float(q["start_ms"])
        price, twap = oracle.get(ts + bar_ms - 1, (None, None))
        if price is not None:
            ef["oracle_price"] = float(price)
        if twap is not None:
            ef["oracle_twap60"] = float(twap)
        bars.append(OHLCV(ts, o, h, l, c, v, ef))
    return bars


def run_backtest(cfg: BacktestConfig, feed: Feed | None = None, keep_series: bool = True,
                 bars: list | None = None) -> dict:
    """Run one backtest. `bars` (from build_bars for the same asset/tf/bar/range) can be passed to reuse them
    across runs that only change script inputs or simulator settings, e.g. a parameter sweep."""
    from pynecore.core.script_runner import ScriptRunner

    feed = feed or Feed()
    script = resolve_script(cfg.script)
    inputs = resolve_inputs(script, cfg.inputs)
    # PyneCore imports the script as a module and evaluates the input() defaults and overrides at import.
    # Python caches imports, so in a long-running process (the web server) every run after the first would
    # reuse the first run's code and inputs. Drop the cached module so each run re-imports the script.
    # Also drop its cached bytecode: CPython only revalidates a .pyc on source mtime (whole seconds) + size,
    # so an edit like 100 -> 200 saved and run within the same second would run the old code.
    sys.modules.pop(script.stem, None)
    Path(importlib.util.cache_from_source(str(script.resolve()))).unlink(missing_ok=True)
    importlib.invalidate_caches()
    if bars is None:
        bars = build_bars(feed, cfg)
    if not bars:
        raise ValueError(f"no underlying data for {cfg.asset} in the requested range")

    runner = ScriptRunner(script, bars, _syminfo(cfg.asset, cfg.bar), last_bar_index=len(bars) - 1,
                          last_bar_time=bars[-1].timestamp, inputs=inputs or None)
    signals, plots, pine_trades = [], [], []
    for candle, plot, *rest in runner.run_iter():
        if keep_series:
            plots.append({"t": candle.timestamp, **{k: _num(v) for k, v in (plot or {}).items()}})
        for tr in (rest[0] if rest else []):
            signals.append(Signal(tr.entry_time, tr.exit_time, 1 if tr.sign > 0 else -1, tr.entry_id))
            pine_trades.append({"entry_ms": tr.entry_time, "exit_ms": tr.exit_time, "dir": int(tr.sign),
                                "entry_px": tr.entry_price, "exit_px": tr.exit_price, "profit": tr.profit})
    position = getattr(runner.script, "position", None)
    for tr in getattr(position, "open_trades", []) or []:
        signals.append(Signal(tr.entry_time, None, 1 if tr.sign > 0 else -1, tr.entry_id))

    trades, skipped = Simulator(feed, cfg).run(signals)
    effective = {**script_defaults(script), **inputs}  # what this run actually used
    result = {"config": asdict(cfg), "inputs": effective, "signals": len(signals), "stats": stats(trades, skipped),
              "trades": [asdict(t) for t in trades], "skipped": skipped, "pine_trades": pine_trades}
    if keep_series:
        result["bars"] = [{"t": b.timestamp, "o": b.open, "h": b.high, "l": b.low, "c": b.close, "v": b.volume}
                          for b in bars]
        result["plots"] = plots
    return result


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f
