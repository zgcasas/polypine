from polypine.book import OrderBook


def snap():
    b = OrderBook()
    # Polymarket sends bids ascending and asks descending (best last); order must not matter.
    b.apply_snapshot(
        bids=[{"price": "0.40", "size": "50"}, {"price": "0.45", "size": "10"}, {"price": "0.50", "size": "5"}],
        asks=[{"price": "0.60", "size": "30"}, {"price": "0.55", "size": "20"}, {"price": "0.52", "size": "8"}],
    )
    return b


def test_not_ready_before_snapshot():
    assert OrderBook().top() is None


def test_top_of_book_and_depth():
    t = snap().top()
    assert t["best_bid"] == 0.50 and t["bid_size"] == 5
    assert t["best_ask"] == 0.52 and t["ask_size"] == 8
    assert t["bid_depth_5c"] == 15  # 0.50 + 0.45 (0.40 is outside 5c)
    assert t["ask_depth_5c"] == 28  # 0.52 + 0.55


def test_change_updates_and_removes_levels():
    b = snap()
    b.apply_change("0.51", "7", "BUY")
    assert b.top()["best_bid"] == 0.51
    b.apply_change("0.51", "0", "BUY")
    assert b.top()["best_bid"] == 0.50
    b.apply_change("0.52", "0", "SELL")
    assert b.top()["best_ask"] == 0.55


def test_levels_sorted_best_first():
    bids, asks = snap().levels(2)
    assert bids == [(0.50, 5), (0.45, 10)]
    assert asks == [(0.52, 8), (0.55, 20)]
