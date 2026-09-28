"""Live recorder: discovers Up/Down markets, streams their order books and trades, polls
resolutions and underlying 1s klines, and writes everything to partitioned Parquet."""

import asyncio
import json
import logging
import time

import httpx
import orjson
import websockets

from .book import OrderBook
from .config import (BINANCE_SYMBOL, BINANCE_URL, CHAINLINK_SYMBOL, CLOB_WS_URL, RTDS_URL, TF_SECONDS, Series,
                     all_series)
from .gamma import GammaClient, Market
from .storage import ParquetSink

log = logging.getLogger("polypine.collector")

DISCOVERY_EVERY = 30
SAMPLE_EVERY = 1
L2_EVERY = 10
RESOLVE_EVERY = 60
PTB_EVERY = 5
UNDERLYING_EVERY = 60
POST_END_GRACE_S = 30
RESOLVE_GIVE_UP_S = 3 * 3600
FINAL_PRICE_WAIT_S = 30 * 60


def now_ms() -> int:
    return int(time.time() * 1000)


class ActiveMarket:
    def __init__(self, market: Market):
        self.m = market
        self.books = {market.token_up: OrderBook(), market.token_down: OrderBook()}
        self.side = {market.token_up: "UP", market.token_down: "DOWN"}
        self.task: asyncio.Task | None = None

    @property
    def record_from_ms(self) -> int:
        # Also record pre-window trading, up to one window length before start.
        return self.m.start_ms - TF_SECONDS[self.m.tf] * 1000

    @property
    def record_until_ms(self) -> int:
        return self.m.end_ms + POST_END_GRACE_S * 1000


class Collector:
    def __init__(self, sink: ParquetSink, series: list[Series] | None = None, flush_every: int = 60):
        self.sink = sink
        self.series = series or all_series()
        self.series_by_key = {(s.asset, s.tf): s for s in self.series}
        self.flush_every = flush_every
        self.gamma = GammaClient()
        self.markets: dict[str, ActiveMarket] = {}  # condition_id -> market
        self.unresolved: dict[str, Market] = {}
        self.stats = {"ws_msgs": 0, "ticks": 0, "trades": 0, "reconnects": 0, "max_loop_lag_ms": 0}
        self._stop = asyncio.Event()

    # ---------- discovery ----------
    async def discover_once(self) -> None:
        results = await asyncio.gather(*(self.gamma.upcoming(s) for s in self.series), return_exceptions=True)
        for s, res in zip(self.series, results):
            if isinstance(res, Exception):
                log.warning("discovery failed for %s: %s", s.slug, res)
                continue
            for m in res:
                if m.condition_id not in self.markets:
                    self.markets[m.condition_id] = ActiveMarket(m)
                    self.unresolved[m.condition_id] = m
                    self.sink.write("markets", m.to_row())
                    log.info("discovered %s", m.slug)

    async def discovery_loop(self) -> None:
        while not self._stop.is_set():
            await self.discover_once()
            self._schedule_streams()
            await self._sleep(DISCOVERY_EVERY)

    def _schedule_streams(self) -> None:
        t = now_ms()
        for cid, am in list(self.markets.items()):
            if t > am.record_until_ms:
                if am.task is None or am.task.done():
                    del self.markets[cid]
                continue
            if am.task is None and t >= am.record_from_ms:
                am.task = asyncio.create_task(self.stream_market(am), name=f"ws:{am.m.slug}")

    # ---------- websocket ----------
    async def stream_market(self, am: ActiveMarket) -> None:
        backoff = 1
        while not self._stop.is_set() and now_ms() < am.record_until_ms:
            try:
                # Large receive queue: a busy 5m market bursts to thousands of msgs/s near expiry and
                # the default (16 frames) back-pressures the socket until the server drops us (1013).
                async with websockets.connect(CLOB_WS_URL, ping_interval=None, max_size=None,
                                              max_queue=8192) as ws:
                    await ws.send(json.dumps({"assets_ids": list(am.books), "type": "market"}))
                    pinger = asyncio.create_task(self._ping(ws))
                    try:
                        backoff = 1
                        async with asyncio.timeout((am.record_until_ms - now_ms()) / 1000):
                            async for raw in ws:
                                if raw != "PONG":
                                    self._handle(am, orjson.loads(raw))
                    except TimeoutError:
                        return  # recording window over
                    finally:
                        pinger.cancel()
            except (OSError, websockets.WebSocketException) as e:
                self.stats["reconnects"] += 1
                for b in am.books.values():
                    b.ready = False  # wait for a fresh snapshot after reconnecting
                log.warning("ws %s dropped (%s); retry in %ss", am.m.slug, e, backoff)
                if isinstance(e, websockets.ConnectionClosed) and e.rcvd and e.rcvd.code == 1013:
                    self.stats["slow_consumer"] = self.stats.get("slow_consumer", 0) + 1
                await self._sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _ping(self, ws) -> None:
        while True:
            await asyncio.sleep(10)
            await ws.send("PING")

    def _handle(self, am: ActiveMarket, payload) -> None:
        for msg in payload if isinstance(payload, list) else [payload]:
            self.stats["ws_msgs"] += 1
            et = msg.get("event_type")
            if et == "book":
                book = am.books.get(msg["asset_id"])
                if book:
                    book.apply_snapshot(msg["bids"], msg["asks"])
            elif et == "price_change":
                for ch in msg["price_changes"]:
                    book = am.books.get(ch["asset_id"])
                    if book:
                        book.apply_change(ch["price"], ch["size"], ch["side"])
            elif et == "last_trade_price":
                tok = msg["asset_id"]
                if tok in am.side:
                    self.stats["trades"] += 1
                    self.sink.write("trades", {
                        "ts_ms": int(msg["timestamp"]), "condition_id": am.m.condition_id,
                        "asset": am.m.asset, "tf": am.m.tf, "side": am.side[tok],
                        "price": float(msg["price"]), "size": float(msg["size"]),
                        "taker_side": msg.get("side"), "fee_rate_bps": int(msg.get("fee_rate_bps") or 0),
                        "tx_hash": msg.get("transaction_hash"),
                    })

    # ---------- sampling ----------
    async def sample_loop(self) -> None:
        tick = 0
        ts = (now_ms() // 1000) * 1000
        while not self._stop.is_set():
            # Target the next whole second explicitly: sleeping on the monotonic clock and flooring
            # wall-clock time can wake a hair early, producing a duplicate second and then a gap.
            ts += SAMPLE_EVERY * 1000
            delay = (ts - now_ms()) / 1000
            if delay > 0:
                await asyncio.sleep(delay)
                while now_ms() < ts:  # woke early by a few ms
                    await asyncio.sleep(0.001)
            elif delay < -SAMPLE_EVERY:  # fell behind (e.g. laptop sleep): skip ahead
                ts = (now_ms() // 1000) * 1000
            tick += 1
            for am in list(self.markets.values()):
                if am.task is None or am.task.done() or ts > am.m.end_ms + POST_END_GRACE_S * 1000:
                    continue
                for tok, book in am.books.items():
                    top = book.top()
                    if top is None:
                        continue
                    base = {"ts_ms": ts, "condition_id": am.m.condition_id, "asset": am.m.asset,
                            "tf": am.m.tf, "side": am.side[tok]}
                    self.sink.write("book_ticks", {**base, **top})
                    self.stats["ticks"] += 1
                    if tick % L2_EVERY == 0:
                        bids, asks = book.levels(10)
                        self.sink.write("book_l2", {**base,
                                                    "bid_px": [p for p, _ in bids], "bid_sz": [s for _, s in bids],
                                                    "ask_px": [p for p, _ in asks], "ask_sz": [s for _, s in asks]})

    # ---------- price to beat ----------
    async def price_to_beat_loop(self) -> None:
        """Gamma publishes eventMetadata.priceToBeat only once a window opens, after we discovered the
        market. Re-fetch live markets until it appears and write an updated markets row (the datafeed
        keeps the row that has it)."""
        while not self._stop.is_set():
            await self._sleep(PTB_EVERY)
            t = now_ms()
            for am in list(self.markets.values()):
                m = am.m
                # 5m/15m: Gamma only publishes priceToBeat after resolution; it comes from Chainlink instead.
                if m.tf not in ("1h", "1d") or m.price_to_beat is not None or not (m.start_ms + 2000 <= t < m.end_ms):
                    continue
                try:
                    fresh = await self.gamma.market(m.slug, self.series_by_key[(m.asset, m.tf)])
                except httpx.HTTPError as e:
                    log.warning("price to beat fetch failed for %s: %s", m.slug, e)
                    continue
                if fresh and fresh.price_to_beat is not None:
                    m.price_to_beat = fresh.price_to_beat
                    self.sink.write("markets", m.to_row())
                    log.info("price to beat %s = %s", m.slug, m.price_to_beat)

    # ---------- chainlink ----------
    async def chainlink_loop(self) -> None:
        """Record Chainlink oracle prices from Polymarket's real-time data socket (~1/s per asset)."""
        by_symbol = {CHAINLINK_SYMBOL[a]: a for a in {s.asset for s in self.series}}
        backoff = 1
        while not self._stop.is_set():
            try:
                async with websockets.connect(RTDS_URL, ping_interval=None, max_size=None) as ws:
                    await ws.send(json.dumps({"action": "subscribe", "subscriptions": [
                        {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]}))
                    pinger = asyncio.create_task(self._ping(ws))
                    try:
                        backoff = 1
                        async for raw in ws:
                            if not raw or raw[0] != "{":
                                continue
                            msg = orjson.loads(raw)
                            p = msg.get("payload") or {}
                            asset = by_symbol.get(p.get("symbol"))
                            if asset and msg.get("topic") == "crypto_prices_chainlink" and p.get("value") is not None:
                                self.sink.write("chainlink_1s", {"ts_ms": int(p["timestamp"]), "asset": asset,
                                                                 "price": float(p["value"])})
                                self.stats["chainlink"] = self.stats.get("chainlink", 0) + 1
                    finally:
                        pinger.cancel()
            except (OSError, websockets.WebSocketException) as e:
                log.warning("chainlink socket dropped (%s); retry in %ss", e, backoff)
                await self._sleep(backoff)
                backoff = min(backoff * 2, 30)

    # ---------- resolutions ----------
    async def resolve_loop(self) -> None:
        while not self._stop.is_set():
            await self._sleep(RESOLVE_EVERY)
            t = now_ms()
            for cid, m in list(self.unresolved.items()):
                if t < m.end_ms + 20_000:
                    continue
                if t > m.end_ms + RESOLVE_GIVE_UP_S * 1000:
                    log.warning("giving up on resolution for %s", m.slug)
                    del self.unresolved[cid]
                    continue
                try:
                    r = await self.gamma.resolution(m.slug, self.series_by_key[(m.asset, m.tf)])
                except httpx.HTTPError as e:
                    log.warning("resolution fetch failed for %s: %s", m.slug, e)
                    continue
                # Gamma publishes the outcome before eventMetadata.finalPrice; wait for it a while.
                if r and (r.final_price is not None or t > m.end_ms + FINAL_PRICE_WAIT_S * 1000):
                    self.sink.write("resolutions", r.to_row())
                    del self.unresolved[cid]
                    log.info("resolved %s -> %s", m.slug, r.outcome)

    # ---------- underlying ----------
    async def underlying_loop(self, lookback_s: int = 600) -> None:
        assets = sorted({s.asset for s in self.series})
        since = {a: now_ms() - lookback_s * 1000 for a in assets}
        async with httpx.AsyncClient(base_url=BINANCE_URL, timeout=20) as client:
            while not self._stop.is_set():
                for a in assets:
                    try:
                        since[a] = await fetch_klines_1s(client, self.sink, a, since[a], now_ms() - 1000)
                    except httpx.HTTPError as e:
                        log.warning("binance klines failed for %s: %s", a, e)
                await self._sleep(UNDERLYING_EVERY)

    # ---------- plumbing ----------
    async def flush_loop(self) -> None:
        while not self._stop.is_set():
            await self._sleep(self.flush_every)
            await asyncio.to_thread(self.flush)

    async def lag_loop(self) -> None:
        """Track worst event-loop stall; stalls this long mean the WS buffers are backing up."""
        while not self._stop.is_set():
            t = time.perf_counter()
            await asyncio.sleep(0.1)
            self.stats["max_loop_lag_ms"] = max(self.stats["max_loop_lag_ms"],
                                                int((time.perf_counter() - t - 0.1) * 1000))

    def flush(self) -> None:
        n = self.sink.flush()
        live = sum(1 for am in self.markets.values() if am.task and not am.task.done())
        log.info("flushed %d rows | live markets=%d tracked=%d unresolved=%d | %s",
                 n, live, len(self.markets), len(self.unresolved), self.stats)

    async def _sleep(self, s: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=s)
        except asyncio.TimeoutError:
            pass

    def stop(self) -> None:
        self._stop.set()

    async def run(self, duration_s: float | None = None) -> None:
        loops = [self.discovery_loop(), self.sample_loop(), self.resolve_loop(), self.price_to_beat_loop(),
                 self.chainlink_loop(),
                 self.underlying_loop(), self.flush_loop(), self.lag_loop()]
        tasks = [asyncio.create_task(c) for c in loops]
        try:
            if duration_s:
                await self._sleep(duration_s)
                self.stop()
            else:
                await self._stop.wait()
        finally:
            self.stop()
            for t in tasks:
                t.cancel()
            streams = [am.task for am in self.markets.values() if am.task]
            for t in streams:
                t.cancel()
            await asyncio.gather(*tasks, *streams, return_exceptions=True)
            await self.gamma.aclose()
            self.flush()


async def fetch_klines_1s(client: httpx.AsyncClient, sink: ParquetSink, asset: str,
                          start_ms: int, end_ms: int) -> int:
    """Write Binance 1s klines in [start_ms, end_ms] to the sink. Returns the next start_ms."""
    cursor = start_ms
    while cursor <= end_ms:
        r = await client.get("/api/v3/klines", params={
            "symbol": BINANCE_SYMBOL[asset], "interval": "1s",
            "startTime": cursor, "endTime": end_ms, "limit": 1000})
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        for k in rows:
            sink.write("underlying_1s", {"ts_ms": int(k[0]), "asset": asset, "open": float(k[1]),
                                         "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
                                         "volume": float(k[5])})
        cursor = int(rows[-1][0]) + 1000
        if len(rows) < 1000:
            break
    return cursor
