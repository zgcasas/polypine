"""End-to-end: synthetic Parquet store -> Feed -> PyneCore script -> binary contract trades."""
import math

import pytest

from polypine.datafeed import Feed
from polypine.engine import BacktestConfig, fee, run_backtest
from polypine.storage import ParquetSink

T0 = 1_776_000_000_000  # 2026-04-12 13:20 UTC, a 5m boundary
W = 300_000

SCRIPT = '''"""
@pyne
"""
from pynecore import Series
from pynecore.lib import script, strategy, na, extra_fields


@script.strategy("Buy Up once per window")
def main():
    secs_left: Series[float] = extra_fields["secs_left"]
    window: Series[float] = extra_fields["window_start"]
    if strategy.position_size != 0 and window != window[1]:
        strategy.close_all()
    if strategy.position_size == 0 and not na(secs_left) and secs_left == 120:
        strategy.entry("Up", strategy.long)
'''


@pytest.fixture
def store(tmp_path):
    s = ParquetSink(tmp_path)
    for i in range(4):
        start, end = T0 + i * W, T0 + (i + 1) * W
        cid = f"c{i}"
        s.write("markets", {"condition_id": cid, "slug": f"btc-updown-5m-{start // 1000}", "asset": "BTC", "tf": "5m",
                            "start_ms": start, "end_ms": end, "token_up": "u", "token_down": "d", "tick_size": 0.01,
                            "min_order_size": 5, "fee_rate": 0.07, "fee_exponent": 1, "fee_taker_only": True,
                            "twap_lookback_s": 60, "resolution_source": "", "price_to_beat": None})
        s.write("resolutions", {"condition_id": cid, "slug": f"m{i}", "asset": "BTC", "tf": "5m", "end_ms": end,
                                "outcome": "UP" if i % 2 == 0 else "DOWN", "price_to_beat": 100.0, "final_price": 101.0})
        for t in range(start, end, 1000):
            for side, bid, ask in (("UP", 0.59, 0.60), ("DOWN", 0.40, 0.41)):
                s.write("book_ticks", {"ts_ms": t, "condition_id": cid, "asset": "BTC", "tf": "5m", "side": side,
                                       "best_bid": bid, "best_ask": ask, "bid_size": 1e6, "ask_size": 1e6,
                                       "bid_depth_5c": 1e6, "ask_depth_5c": 1e6})
    for t in range(T0 - 600_000, T0 + 4 * W, 1000):
        px = 100 + math.sin(t / 60_000)
        s.write("underlying_1s", {"ts_ms": t, "asset": "BTC", "open": px, "high": px, "low": px, "close": px, "volume": 1.0})
    s.flush()
    (tmp_path / "s.py").write_text(SCRIPT)
    return tmp_path


def test_script_sees_extra_fields_and_trades_each_window(store):
    cfg = BacktestConfig(script=str(store / "s.py"), asset="BTC", tf="5m", bar="1m",
                         start_ms=T0 - 600_000, end_ms=T0 + 4 * W, stake=60.0)
    res = run_backtest(cfg, Feed(store))
    trades = res["trades"]
    # secs_left == 120 at the close of the 3rd minute bar of each window -> fill at the next bar open + 1s.
    assert [t["entry_ms"] for t in trades] == [T0 + i * W + 180_000 + 1000 for i in range(4)]
    assert all(t["token"] == "UP" and t["entry_price"] == pytest.approx(0.60) for t in trades)
    assert [t["exit_price"] for t in trades] == [1.0, 0.0, 1.0, 0.0]
    per = fee(100, 0.60, 0.07)
    assert res["stats"]["net_pnl"] == pytest.approx(2 * (100 - 60) - 2 * 60 - 4 * per)
    assert res["stats"]["settled"] == 4
