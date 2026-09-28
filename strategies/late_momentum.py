"""
@pyne
"""
from pynecore import Series
from pynecore.lib import script, close, strategy, input, na, extra_fields


@script.strategy("Late Momentum", overlay=True)
def main(
        window_secs: int = input.int(90, title="Enter within last N seconds"),
        min_bps: float = input.float(5.0, title="Min distance from price to beat (bps)"),
        max_price: float = input.float(0.80, title="Max contract price"),
):
    """Near the end of a window, if the underlying is clearly above/below the price to beat but the
    contract is still cheap, buy that side and hold to settlement. Closes at every new window."""
    up_ask: Series[float] = extra_fields["up_ask"]
    down_ask: Series[float] = extra_fields["down_ask"]
    ptb: Series[float] = extra_fields["price_to_beat"]
    secs_left: Series[float] = extra_fields["secs_left"]
    window: Series[float] = extra_fields["window_start"]

    # A new window started: the previous bet has settled, flatten so the next one can open.
    if strategy.position_size != 0 and window != window[1]:
        strategy.close_all()

    dist_bps = (close / ptb - 1) * 10000 if not na(ptb) else na(float)
    if strategy.position_size == 0 and not na(dist_bps) and not na(secs_left) and 0 < secs_left <= window_secs:
        if dist_bps >= min_bps and not na(up_ask) and up_ask <= max_price:
            strategy.entry("Up", strategy.long)
        elif dist_bps <= -min_bps and not na(down_ask) and down_ask <= max_price:
            strategy.entry("Down", strategy.short)
    return {"dist_bps": dist_bps, "up_ask": up_ask}
