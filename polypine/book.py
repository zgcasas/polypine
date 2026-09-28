"""Local L2 order book for one CLOB token, rebuilt from WebSocket events."""

DEPTH_BAND = 0.05  # aggregate resting size within 5 cents of the best price


class OrderBook:
    def __init__(self):
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.ready = False  # set once a full snapshot has arrived

    def apply_snapshot(self, bids: list[dict], asks: list[dict]) -> None:
        self.bids = {float(l["price"]): float(l["size"]) for l in bids if float(l["size"]) > 0}
        self.asks = {float(l["price"]): float(l["size"]) for l in asks if float(l["size"]) > 0}
        self.ready = True

    def apply_change(self, price: str, size: str, side: str) -> None:
        """`size` is the new absolute size at `price`; 0 removes the level."""
        levels = self.bids if side.upper() == "BUY" else self.asks
        p, s = float(price), float(size)
        if s > 0:
            levels[p] = s
        else:
            levels.pop(p, None)

    def top(self) -> dict | None:
        if not self.ready:
            return None
        bb = max(self.bids) if self.bids else None
        ba = min(self.asks) if self.asks else None
        return {
            "best_bid": bb,
            "best_ask": ba,
            "bid_size": self.bids[bb] if bb is not None else None,
            "ask_size": self.asks[ba] if ba is not None else None,
            "bid_depth_5c": sum(s for p, s in self.bids.items() if bb is not None and p >= bb - DEPTH_BAND - 1e-9),
            "ask_depth_5c": sum(s for p, s in self.asks.items() if ba is not None and p <= ba + DEPTH_BAND + 1e-9),
        }

    def levels(self, n: int = 10) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        """Top-n levels per side, best first (for L2 snapshots / fill simulation)."""
        bids = sorted(self.bids.items(), reverse=True)[:n]
        asks = sorted(self.asks.items())[:n]
        return bids, asks
