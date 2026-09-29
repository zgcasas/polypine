"""Underlying 1s bars from Binance: spot klines, or USD-M futures trades for coins whose markets settle on
the perpetual (HYPE). Futures has no 1s klines, so 1s bars are built from aggregated trades."""

import httpx

from .config import BINANCE_FUTURES_URL, BINANCE_URL, UNDERLYING
from .storage import ParquetSink

MAX_TRADE_WINDOW_MS = 3_600_000  # aggTrades rejects startTime/endTime ranges longer than 1h


def trades_to_1s(trades: list[tuple[int, float, float]], start_ms: int, end_ms: int,
                 prev_close: float | None = None) -> list[tuple[int, float, float, float, float, float]]:
    """(ts_ms, price, qty) trades -> 1s OHLCV bars for every second in [start_ms, end_ms).

    Seconds without trades become flat bars at the previous close with zero volume (like Binance's 1s
    klines). Seconds before the first known price are skipped."""
    by_sec: dict[int, list[tuple[float, float]]] = {}
    for ts, px, qty in trades:
        if start_ms <= ts < end_ms:
            by_sec.setdefault(ts // 1000 * 1000, []).append((px, qty))
    bars = []
    close = prev_close
    for sec in range(start_ms // 1000 * 1000, end_ms, 1000):
        ticks = by_sec.get(sec)
        if ticks:
            prices = [p for p, _ in ticks]
            bars.append((sec, prices[0], max(prices), min(prices), prices[-1], sum(q for _, q in ticks)))
            close = prices[-1]
        elif close is not None:
            bars.append((sec, close, close, close, close, 0.0))
    return bars


class UnderlyingFetcher:
    """Incremental fetcher; remembers the last close per asset to fill quiet seconds across calls."""

    def __init__(self, sink: ParquetSink, timeout: float = 30):
        self.sink = sink
        self.spot = httpx.AsyncClient(base_url=BINANCE_URL, timeout=timeout)
        self.futures = httpx.AsyncClient(base_url=BINANCE_FUTURES_URL, timeout=timeout)
        self.last_close: dict[str, float] = {}

    async def fetch(self, asset: str, start_ms: int, end_ms: int) -> int:
        """Write 1s bars for [start_ms, end_ms) and return the next start (a whole second)."""
        market, symbol = UNDERLYING[asset]
        start_ms = start_ms // 1000 * 1000
        end_ms = end_ms // 1000 * 1000
        if end_ms <= start_ms:
            return start_ms
        if market == "spot":
            return await self._spot(asset, symbol, start_ms, end_ms)
        return await self._futures(asset, symbol, start_ms, end_ms)

    async def _spot(self, asset: str, symbol: str, start_ms: int, end_ms: int) -> int:
        cursor = start_ms
        while cursor < end_ms:
            r = await self.spot.get("/api/v3/klines", params={
                "symbol": symbol, "interval": "1s", "startTime": cursor, "endTime": end_ms - 1, "limit": 1000})
            r.raise_for_status()
            rows = r.json()
            if not rows:
                break
            for k in rows:
                self._write(asset, (int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])))
            cursor = int(rows[-1][0]) + 1000
            if len(rows) < 1000:
                break
        return cursor

    async def _futures(self, asset: str, symbol: str, start_ms: int, end_ms: int) -> int:
        cursor = start_ms
        while cursor < end_ms:
            stop = min(end_ms, cursor + MAX_TRADE_WINDOW_MS)
            trades: list[tuple[int, float, float]] = []
            params = {"symbol": symbol, "startTime": cursor, "endTime": stop - 1, "limit": 1000}
            while True:
                r = await self.futures.get("/fapi/v1/aggTrades", params=params)
                r.raise_for_status()
                page = r.json()
                trades += [(int(t["T"]), float(t["p"]), float(t["q"])) for t in page]
                if len(page) < 1000 or int(page[-1]["T"]) >= stop - 1:
                    break
                params = {"symbol": symbol, "fromId": int(page[-1]["a"]) + 1, "limit": 1000}
            bars = trades_to_1s(trades, cursor, stop, self.last_close.get(asset))
            for b in bars:
                self._write(asset, b)
            cursor = stop
        return cursor

    def _write(self, asset: str, bar: tuple) -> None:
        ts, o, h, l, c, v = bar
        self.last_close[asset] = c
        self.sink.write("underlying_1s", {"ts_ms": ts, "asset": asset, "open": o, "high": h, "low": l,
                                          "close": c, "volume": v})

    async def aclose(self) -> None:
        await self.spot.aclose()
        await self.futures.aclose()
