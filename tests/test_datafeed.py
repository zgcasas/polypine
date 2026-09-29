from polypine.datafeed import Feed
from polypine.storage import ParquetSink

T0 = 1_790_609_700_000  # a 5m boundary


def market(cid, start, ptb=None):
    return {"condition_id": cid, "slug": cid, "asset": "BTC", "tf": "5m", "start_ms": start, "end_ms": start + 300_000,
            "token_up": "u", "token_down": "d", "tick_size": 0.01, "min_order_size": 5, "fee_rate": 0.07,
            "fee_exponent": 1, "fee_taker_only": True, "twap_lookback_s": 60, "resolution_source": "",
            "price_to_beat": ptb}


def test_live_price_to_beat_is_chainlink_twap_before_start(tmp_path):
    s = ParquetSink(tmp_path)
    s.write("markets", market("live", T0))
    s.write("markets", market("gappy", T0 + 300_000))
    s.write("markets", market("official", T0 + 600_000, ptb=123.0))
    # 60 prices before T0 (the TWAP window), and the spot at T0 which must NOT be used.
    for i in range(60):
        s.write("chainlink_1s", {"ts_ms": T0 - 60_000 + i * 1000, "asset": "BTC", "price": 100.0 + i})
    s.write("chainlink_1s", {"ts_ms": T0, "asset": "BTC", "price": 999.0})
    # Only 10 seconds before the second window: too sparse to estimate.
    for i in range(10):
        s.write("chainlink_1s", {"ts_ms": T0 + 290_000 + i * 1000, "asset": "BTC", "price": 50.0})
    s.flush()
    ms = {m["condition_id"]: m for m in Feed(tmp_path).markets("BTC", "5m", T0 - 1, T0 + 900_000)}
    assert ms["live"]["price_to_beat"] == 129.5 and ms["live"]["price_to_beat_source"] == "chainlink"
    assert ms["gappy"]["price_to_beat"] is None
    assert ms["official"]["price_to_beat"] == 123.0 and "price_to_beat_source" not in ms["official"]


def test_oracle_at_latest_price_and_60s_twap(tmp_path):
    s = ParquetSink(tmp_path)
    for i in range(120):  # prices 0..119, one per second
        s.write("chainlink_1s", {"ts_ms": T0 + i * 1000, "asset": "BTC", "price": float(i)})
    s.flush()
    f = Feed(tmp_path)
    t = T0 + 119_000
    got = f.oracle_at("BTC", [t, T0 + 30_000, T0 + 130_000, T0 - 1])
    assert got[t] == (119.0, sum(range(60, 120)) / 60)   # mean over (t-60s, t]
    assert got[T0 + 30_000] == (30.0, None)              # only 31s of history: no TWAP yet
    assert got[T0 + 130_000] == (None, None)             # last price 11s old: stale
    assert got[T0 - 1] == (None, None)                   # before any data
