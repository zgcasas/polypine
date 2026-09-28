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

import math
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .config import TF_SECONDS
from .datafeed import BAR_SECONDS, Feed

EXTRA_FIELDS = ("up_bid", "up_ask", "down_bid", "down_ask", "secs_left", "secs_in", "price_to_beat",
                "window_start")


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
    exit_price: float            # sale price, or 1/0 at settlement
    exit_kind: str               # "sold" | "settled" | "open"
    outcome: str | None
    pnl: float


def fee(shares: float, price: float, rate: float) -> float:
    """Polymarket taker fee in USDC: shares * rate * p * (1 - p)."""
    return shares * rate * price * (1 - price)


def fill_buy(stake: float, ask: float, ask_size: float | None, depth_5c: float | None,
             tick: float) -> tuple[float, float]:
    """Buy `stake` USDC of shares. Returns (shares, cost).

    The recorded book gives the best level size and the total within 5c; the part beyond the best level is
    filled one tick worse, and anything beyond the 5c depth is left unfilled."""
    want = stake / ask
    lvl1 = want if ask_size is None else min(want, ask_size)
    rest_cap = 0.0 if ask_size is None or depth_5c is None else max(depth_5c - ask_size, 0.0)
    worse = min(ask + tick, 0.99)
    lvl2 = min((stake - lvl1 * ask) / worse, rest_cap) if want > lvl1 else 0.0
    return lvl1 + lvl2, lvl1 * ask + lvl2 * worse


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
            shares, cost = fill_buy(cfg.stake, ask, q[f"{side}_ask_size"], q[f"{side}_ask_depth"], cfg.tick)
            if shares <= 0:
                skipped.append({"signal_ms": sig.entry_ms, "market": m["slug"], "reason": "no depth"})
                continue
            rate = m.get("fee_rate") or 0.0
            avg_px = cost / shares
            fees = fee(shares, avg_px, rate)

            exit_kind, exit_ms, exit_px = None, m["end_ms"], None
            qo = quotes.get(t_out) if t_out is not None else None
            if qo is not None and qo["condition_id"] == m["condition_id"] and qo[f"{side}_bid"] is not None:
                exit_kind, exit_ms, exit_px = "sold", t_out, qo[f"{side}_bid"]
                fees += fee(shares, exit_px, rate)
            elif m.get("outcome"):
                exit_kind, exit_px = "settled", 1.0 if m["outcome"] == side.upper() else 0.0
            else:
                exit_kind, exit_px = "open", float("nan")

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


def build_bars(feed: Feed, cfg: BacktestConfig):
    """Underlying OHLCV bars with Polymarket extra fields as of each bar's close."""
    from pynecore.types.na import NA
    from pynecore.types.ohlcv import OHLCV

    raw = feed.underlying_bars(cfg.asset, cfg.start_ms, cfg.end_ms, cfg.bar)
    bar_ms = BAR_SECONDS[cfg.bar] * 1000
    quotes = feed.quotes_at(cfg.asset, cfg.tf, [b[0] + bar_ms - 1 for b in raw])
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
        bars.append(OHLCV(ts, o, h, l, c, v, ef))
    return bars


def run_backtest(cfg: BacktestConfig, feed: Feed | None = None, keep_series: bool = True) -> dict:
    from pynecore.core.script_runner import ScriptRunner

    feed = feed or Feed()
    script = resolve_script(cfg.script)
    bars = build_bars(feed, cfg)
    if not bars:
        raise ValueError(f"no underlying data for {cfg.asset} in the requested range")

    runner = ScriptRunner(script, bars, _syminfo(cfg.asset, cfg.bar), last_bar_index=len(bars) - 1,
                          last_bar_time=bars[-1].timestamp, inputs=cfg.inputs or None)
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
    result = {"config": asdict(cfg), "signals": len(signals), "stats": stats(trades, skipped),
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
