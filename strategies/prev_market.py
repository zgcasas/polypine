"""
@pyne
"""
from pynecore import Series, Persistent
from pynecore.lib import script, strategy, input, na, extra_fields


@script.strategy("Copy Previous Market", overlay=True)
def main(
        min_bps: float = input.float(0.0, title="Previous market won by at least (bps)"),
        entry_after_secs: int = input.int(5, title="Enter N seconds into the window"),
        max_price: float = input.float(0.99, title="Max contract price"),
):
    """Copy the previous market's result when it won by at least `min_bps`: previous market resolved Up by
    >= min_bps -> buy Up; resolved Down by >= min_bps -> buy Down; a smaller move -> no bet. One bet per
    market, held to settlement. With min_bps = 0 it bets every market (a move of exactly 0 resolves Up).

    The previous market's move is known at the window start: a 5m/15m market's price to beat is the previous
    market's final price, so move = price_to_beat now / price_to_beat of the previous window - 1. Only
    consecutive windows count (no bet after a gap in the data)."""
    ptb: Series[float] = extra_fields["price_to_beat"]
    window: Series[float] = extra_fields["window_start"]
    secs_in: Series[float] = extra_fields["secs_in"]
    secs_left: Series[float] = extra_fields["secs_left"]
    up_ask: Series[float] = extra_fields["up_ask"]
    down_ask: Series[float] = extra_fields["down_ask"]

    cur_window: Persistent[float] = na(float)
    cur_ptb: Persistent[float] = na(float)
    prev_window: Persistent[float] = na(float)
    prev_ptb: Persistent[float] = na(float)
    bet_window: Persistent[float] = na(float)

    # Comparisons with na are never true in Pine, so check na explicitly.
    if not na(window) and (na(cur_window) or window != cur_window):
        # A new market started: the previous bet has settled, flatten so the next one can open.
        if strategy.position_size != 0:
            strategy.close_all()
        prev_window = cur_window
        prev_ptb = cur_ptb
        cur_window = window
        cur_ptb = na(float)
    if not na(ptb):
        cur_ptb = ptb

    prev_bps = na(float)
    window_len_ms = (secs_in + secs_left) * 1000 if not na(secs_in) and not na(secs_left) else na(float)
    consecutive = not na(prev_window) and not na(window_len_ms) and cur_window - prev_window == window_len_ms
    if consecutive and not na(prev_ptb) and not na(cur_ptb):
        prev_bps = (cur_ptb / prev_ptb - 1) * 10000

    if not na(prev_bps) and (na(bet_window) or bet_window != cur_window) and not na(secs_in) \
            and secs_in >= entry_after_secs:
        if prev_bps >= 0 and prev_bps >= min_bps:  # previous market won Up (final >= price to beat)
            if not na(up_ask) and up_ask <= max_price:
                strategy.entry("Up", strategy.long)
                bet_window = cur_window
        elif prev_bps < 0 and -prev_bps >= min_bps:  # previous market won Down
            if not na(down_ask) and down_ask <= max_price:
                strategy.entry("Down", strategy.short)
                bet_window = cur_window
    return {"prev_bps": prev_bps}
