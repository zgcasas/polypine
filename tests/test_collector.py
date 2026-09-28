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
