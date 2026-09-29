import json
from pathlib import Path

from polypine.collector import ActiveMarket, Collector
from polypine.config import Series
from polypine.gamma import parse_market
from polypine.storage import ParquetSink

FIX = Path(__file__).parent / "fixtures"


def setup(tmp_path):
    m = parse_market(json.loads((FIX / "event_5m_open.json").read_text())[0], Series("BTC", "5m"))
    sink = ParquetSink(tmp_path)
    return Collector(sink, [Series("BTC", "5m")]), ActiveMarket(m), sink


def test_handle_book_change_and_trade(tmp_path):
    c, am, sink = setup(tmp_path)
    up, down = am.m.token_up, am.m.token_down
    # Shapes captured from the live market channel.
    c._handle(am, [{"event_type": "book", "asset_id": up, "timestamp": "1",
                    "bids": [{"price": "0.01", "size": "100"}, {"price": "0.55", "size": "10"}],
                    "asks": [{"price": "0.99", "size": "100"}, {"price": "0.56", "size": "20"}]}])
    c._handle(am, {"event_type": "price_change", "timestamp": "2", "price_changes": [
        {"asset_id": up, "price": "0.555", "size": "4", "side": "BUY"},
        {"asset_id": "someone-else", "price": "0.3", "size": "1", "side": "BUY"}]})
    c._handle(am, {"event_type": "last_trade_price", "asset_id": down, "price": "0.45", "size": "10.5",
                   "side": "BUY", "fee_rate_bps": "0", "timestamp": "1790601634420", "transaction_hash": "0xabc"})
    top = am.books[up].top()
    assert top["best_bid"] == 0.555 and top["best_ask"] == 0.56
    assert not am.books[down].ready
    assert c.stats["trades"] == 1 and sink.pending() == 1


def test_restart_requeues_recently_ended_unresolved_markets(tmp_path):
    import time

    from polypine.gamma import Resolution

    c, am, sink = setup(tmp_path)
    now = int(time.time() * 1000)
    base = am.m.to_row()
    ended = {**base, "condition_id": "ended", "start_ms": now - 600_000, "end_ms": now - 300_000}
    resolved = {**base, "condition_id": "resolved", "start_ms": now - 600_000, "end_ms": now - 300_000}
    ancient = {**base, "condition_id": "ancient", "start_ms": now - 10 * 3600_000, "end_ms": now - 9 * 3600_000}
    future = {**base, "condition_id": "future", "start_ms": now + 60_000, "end_ms": now + 360_000}
    for r in (ended, resolved, ancient, future):
        sink.write("markets", r)
    sink.write("resolutions", Resolution("resolved", "s", "BTC", "5m", resolved["end_ms"], "UP", 1.0, 2.0).to_row())
    sink.flush()
    assert c.reload_unresolved() == 1
    assert list(c.unresolved) == ["ended"]
    assert c.unresolved["ended"].slug == base["slug"]


def test_flush_logs_and_resets_per_interval_loop_lag(tmp_path, caplog):
    import logging

    c, _, _ = setup(tmp_path)
    c.lag = {"max_ms": 2112, "stalls_250ms": 3}
    with caplog.at_level(logging.INFO, logger="polypine.collector"):
        c.flush()
        c.flush()
    first, second = [r.getMessage() for r in caplog.records if "flushed" in r.getMessage()]
    assert "max 2112ms, 3 stalls >250ms" in first
    assert "max 0ms, 0 stalls >250ms" in second  # a one-off stall no longer sticks forever
