"""
@pyne
"""
from pynecore import Series
from pynecore.lib import script, close, ta, strategy, input


@script.strategy("EMA Cross", overlay=True)
def main(fast: int = input.int(9, title="Fast"), slow: int = input.int(21, title="Slow")):
    """Classic momentum: long on a fast/slow EMA cross up, short on a cross down.
    On Polymarket this buys Up (long) or Down (short) in the live window and holds it until
    Pine flips direction or the window settles."""
    f: Series[float] = ta.ema(close, fast)
    s: Series[float] = ta.ema(close, slow)
    if ta.crossover(f, s):
        strategy.entry("Long", strategy.long)
    elif ta.crossunder(f, s):
        strategy.entry("Short", strategy.short)
    return {"EMA fast": f, "EMA slow": s}
