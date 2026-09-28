import json
from pathlib import Path

from polypine.config import Series
from polypine.gamma import parse_market, parse_resolution

FIX = Path(__file__).parent / "fixtures"


def load(name):
    return json.loads((FIX / name).read_text())[0]


def test_parse_5m_market():
    e = load("event_5m_open.json")
    m = parse_market(e, Series("BTC", "5m"))
    assert m.slug.startswith("btc-updown-5m-")
    assert m.end_ms - m.start_ms == 300_000
    assert m.token_up != m.token_down
    assert m.fee_rate == 0.07 and m.fee_exponent == 1 and m.fee_taker_only
    assert m.twap_lookback_s == 60
    assert m.tick_size == 0.01


def test_parse_hourly_and_daily_windows():
    h = parse_market(load("event_1h_open.json"), Series("BTC", "1h"))
    d = parse_market(load("event_1d_open.json"), Series("DOGE", "1d"))
    assert h.end_ms - h.start_ms == 3_600_000
    assert d.end_ms - d.start_ms == 86_400_000
    assert h.price_to_beat is not None
    assert h.twap_lookback_s is None


def test_up_token_matches_outcome_order():
    e = load("event_5m_open.json")
    m = parse_market(e, Series("BTC", "5m"))
    raw = e["markets"][0]
    outcomes = json.loads(raw["outcomes"])
    tokens = json.loads(raw["clobTokenIds"])
    assert m.token_up == tokens[outcomes.index("Up")]


def test_resolution_of_closed_market():
    r = parse_resolution(load("event_5m_closed.json"), Series("BTC", "5m"))
    assert r is not None
    # finalPrice 83311.6 < priceToBeat 83377.7 -> Down
    assert r.outcome == "DOWN"
    assert r.final_price < r.price_to_beat


def test_open_market_has_no_resolution():
    assert parse_resolution(load("event_5m_open.json"), Series("BTC", "5m")) is None
