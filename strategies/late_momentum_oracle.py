"""
@pyne
"""
from pynecore import Series
from pynecore.lib import script, close, strategy, input, na, extra_fields


@script.strategy("Late Momentum (oracle)", overlay=True)
def main(
        window_secs: int = input.int(100, title="Enter within last N seconds"),
        min_bps: float = input.float(2.0, title="Min distance from price to beat (bps)"),
        max_price: float = input.float(0.90, title="Max contract price"),
):
    """Late Momentum measured against the settlement oracle. 5m/15m markets settle on Chainlink, and Binance
    can sit several bps away from it, so a Binance-based distance can call the wrong side. Uses the recorded
    Chainlink price (oracle_price) and falls back to the Binance close where none was recorded."""
    up_ask: Series[float] = extra_fields["up_ask"]
    down_ask: Series[float] = extra_fields["down_ask"]
    ptb: Series[float] = extra_fields["price_to_beat"]
    secs_left: Series[float] = extra_fields["secs_left"]
    window: Series[float] = extra_fields["window_start"]
    oracle: Series[float] = extra_fields["oracle_price"]

    if strategy.position_size != 0 and window != window[1]:
        strategy.close_all()

    px = oracle if not na(oracle) else close
    dist_bps = (px / ptb - 1) * 10000 if not na(ptb) else na(float)
    if strategy.position_size == 0 and not na(dist_bps) and not na(secs_left) and 0 < secs_left <= window_secs:
        if dist_bps >= min_bps and not na(up_ask) and up_ask <= max_price:
            strategy.entry("Up", strategy.long)
        elif dist_bps <= -min_bps and not na(down_ask) and down_ask <= max_price:
            strategy.entry("Down", strategy.short)
    return {"dist_bps": dist_bps, "up_ask": up_ask}
