import pytest

from polypine.engine import BacktestConfig, Signal, Simulator, fee, fill_buy, stats

W = 300_000  # 5m window
T0 = 1_776_000_000_000


class FakeFeed:
    """Three consecutive 5m windows; quotes are flat per window."""

    def __init__(self, outcomes=("UP", "DOWN", "UP"), up_ask=0.60, up_bid=0.58):
        self.ms = [{"condition_id": f"c{i}", "slug": f"m{i}", "start_ms": T0 + i * W, "end_ms": T0 + (i + 1) * W,
                    "outcome": o, "fee_rate": 0.07} for i, o in enumerate(outcomes)]
        self.up_ask, self.up_bid = up_ask, up_bid

    def markets(self, asset, tf, lo, hi):
        return [m for m in self.ms if m["end_ms"] > lo and m["start_ms"] < hi]

    def quotes_at(self, asset, tf, times):
        out = {}
        for t in times:
            m = next((m for m in self.ms if m["start_ms"] <= t < m["end_ms"]), None)
            if m:
                out[t] = {"t": t, "condition_id": m["condition_id"], "start_ms": m["start_ms"],
                          "end_ms": m["end_ms"], "up_bid": self.up_bid, "up_ask": self.up_ask,
                          "up_ask_size": 1e9, "up_ask_depth": 1e9,
                          "down_bid": round(1 - self.up_ask, 2), "down_ask": round(1 - self.up_bid, 2),
                          "down_ask_size": 1e9, "down_ask_depth": 1e9}
        return out


def sim(feed=None, **kw):
    return Simulator(feed or FakeFeed(), BacktestConfig(script="x.py", stake=60.0, **kw))


def test_fee_formula_matches_docs():
    assert fee(100, 0.5, 0.07) == pytest.approx(1.75)
    assert fee(100, 0.3, 0.07) == pytest.approx(fee(100, 0.7, 0.07))


def test_fee_is_rounded_to_5_decimals_like_polymarket():
    assert fee(1, 0.999, 0.07) == 0.00007
    assert fee(0.01, 0.999, 0.07) == 0.0  # below the 0.00001 USDC minimum -> no fee


def test_fill_buy_walks_one_tick_beyond_best_level():
    fills = fill_buy(100, 0.50, ask_size=100, depth_5c=1000, tick=0.01)
    assert fills[0] == (100, 0.50)
    assert fills[1][0] == pytest.approx(50 / 0.51) and fills[1][1] == 0.51
    # Depth caps the fill.
    fills = fill_buy(100, 0.50, ask_size=10, depth_5c=20, tick=0.01)
    assert fills == [(10, 0.50), (pytest.approx(10), 0.51)]


def test_multi_level_fill_pays_fee_per_level():
    feed = FakeFeed()
    orig = feed.quotes_at

    def thin(asset, tf, times):  # only 10 shares at the best ask
        out = orig(asset, tf, times)
        for q in out.values():
            q["up_ask_size"], q["up_ask_depth"] = 10, 1e9
        return out

    feed.quotes_at = thin
    (t,), _ = sim(feed).run([Signal(T0 + 10_000, None, +1)])
    rest = (60 - 10 * 0.60) / 0.61
    assert t.shares == pytest.approx(10 + rest)
    assert t.fees == pytest.approx(fee(10, 0.60, 0.07) + fee(rest, 0.61, 0.07))


def test_long_held_to_expiry_settles_at_one():
    trades, skipped = sim().run([Signal(T0 + 10_000, None, +1)])
    (t,) = trades
    assert not skipped
    assert t.token == "UP" and t.exit_kind == "settled" and t.exit_price == 1.0
    assert t.shares == pytest.approx(100)  # 60 USDC / 0.60
    assert t.pnl == pytest.approx(100 - 60 - fee(100, 0.60, 0.07))


def test_short_buys_down_and_loses_on_up_outcome():
    (t,), _ = sim().run([Signal(T0 + 10_000, T0 + W + 5_000, -1)])  # Pine exits after expiry -> settle
    assert t.token == "DOWN" and t.entry_price == pytest.approx(0.42)
    assert t.exit_kind == "settled" and t.exit_price == 0.0
    assert t.pnl == pytest.approx(-60 - t.fees)


def test_exit_before_expiry_sells_at_bid_and_pays_fee_twice():
    (t,), _ = sim().run([Signal(T0 + 10_000, T0 + 100_000, +1)])
    assert t.exit_kind == "sold" and t.exit_price == 0.58
    assert t.fees == pytest.approx(fee(100, 0.60, 0.07) + fee(100, 0.58, 0.07))
    assert t.pnl == pytest.approx(100 * 0.58 - 60 - t.fees)


def test_roll_reenters_each_window_while_position_open():
    trades, _ = sim(roll=True).run([Signal(T0 + 10_000, T0 + 2 * W + 50_000, +1)])
    assert [t.market for t in trades] == ["m0", "m1", "m2"]
    assert [t.exit_kind for t in trades] == ["settled", "settled", "sold"]
    assert trades[1].entry_ms == T0 + W + 1000  # window start + latency


def test_no_roll_trades_only_the_live_window():
    trades, _ = sim().run([Signal(T0 + 10_000, T0 + 2 * W + 50_000, +1)])
    assert [t.market for t in trades] == ["m0"]


def test_filters_skip_late_and_expensive_entries():
    _, skipped = sim(min_secs_left=60).run([Signal(T0 + W - 30_000, None, +1)])
    assert skipped[0]["reason"] == "too late in window"
    _, skipped = sim(FakeFeed(up_ask=0.97), max_entry_price=0.95).run([Signal(T0 + 10_000, None, +1)])
    assert skipped[0]["reason"] == "price above max"


def test_stats_edge_and_roi():
    trades, sk = sim().run([Signal(T0 + 10_000, None, +1), Signal(T0 + W + 10_000, None, +1)])
    s = stats(trades, sk)
    assert s["trades"] == 2 and s["win_rate"] == 0.5
    assert s["edge_vs_implied"] == pytest.approx(0.5 - 0.60)
    assert s["roi"] == pytest.approx(s["net_pnl"] / 120)


# docs.polymarket.com/trading/fees: taker fee on 100 crypto shares at each price (rounded to cents).
FEE_TABLE = [(0.01, 0.07), (0.05, 0.33), (0.10, 0.63), (0.15, 0.89), (0.20, 1.12), (0.25, 1.31), (0.30, 1.47),
             (0.35, 1.59), (0.40, 1.68), (0.45, 1.73), (0.50, 1.75), (0.55, 1.73), (0.60, 1.68), (0.65, 1.59),
             (0.70, 1.47), (0.75, 1.31), (0.80, 1.12), (0.85, 0.89), (0.90, 0.63), (0.95, 0.33), (0.99, 0.07)]


@pytest.mark.parametrize("price,expected", FEE_TABLE)
def test_fee_matches_polymarket_published_table(price, expected):
    assert round(fee(100, price, 0.07), 2) == expected


def test_fee_exponent_applies_to_price_term():
    # exponent 2 -> (p(1-p))^2: at p=0.5 that is 0.0625 instead of 0.25
    assert fee(100, 0.5, 0.07, exponent=2) == pytest.approx(100 * 0.07 * 0.0625)
    assert fee(100, 0.5, 0.07, exponent=1) == fee(100, 0.5, 0.07)
