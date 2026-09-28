"""Gamma API: discover Up/Down markets and fetch their resolutions."""

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import httpx

from .config import GAMMA_URL, Series


def _ts_ms(iso: str | None) -> int | None:
    if not iso:
        return None
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


@dataclass
class Market:
    condition_id: str
    slug: str
    asset: str
    tf: str
    start_ms: int
    end_ms: int
    token_up: str
    token_down: str
    tick_size: float
    min_order_size: float
    fee_rate: float
    fee_exponent: float
    fee_taker_only: bool
    twap_lookback_s: int | None
    resolution_source: str
    price_to_beat: float | None

    def to_row(self) -> dict:
        return asdict(self)


@dataclass
class Resolution:
    condition_id: str
    slug: str
    asset: str
    tf: str
    end_ms: int
    outcome: str  # "UP" or "DOWN"
    price_to_beat: float | None
    final_price: float | None

    def to_row(self) -> dict:
        return asdict(self)


def parse_market(event: dict, series: Series) -> Market:
    m = event["markets"][0]
    outcomes = json.loads(m["outcomes"])
    tokens = json.loads(m["clobTokenIds"])
    by_outcome = dict(zip([o.lower() for o in outcomes], tokens))
    fees = m.get("feeSchedule") or {}
    cfg = m.get("cryptoMarketConfig") or {}
    meta = event.get("eventMetadata") or {}
    return Market(
        condition_id=m["conditionId"],
        slug=event["slug"],
        asset=series.asset,
        tf=series.tf,
        start_ms=_ts_ms(m.get("eventStartTime") or event.get("startTime")),
        end_ms=_ts_ms(m["endDate"]),
        token_up=by_outcome["up"],
        token_down=by_outcome["down"],
        tick_size=float(m.get("orderPriceMinTickSize") or 0.01),
        min_order_size=float(m.get("orderMinSize") or 0),
        fee_rate=float(fees.get("rate", 0.0)) if m.get("feesEnabled") else 0.0,
        fee_exponent=float(fees.get("exponent", 1)),
        fee_taker_only=bool(fees.get("takerOnly", True)),
        twap_lookback_s=cfg.get("twapLookbackSeconds") if cfg.get("twapEnabled") else None,
        resolution_source=m.get("resolutionSource") or event.get("resolutionSource") or "",
        price_to_beat=meta.get("priceToBeat"),
    )


def parse_resolution(event: dict, series: Series) -> Resolution | None:
    """Return the resolution if the market has settled, else None."""
    m = event["markets"][0]
    if not m.get("closed") or m.get("umaResolutionStatus") != "resolved":
        return None
    prices = [float(p) for p in json.loads(m["outcomePrices"])]
    outcomes = [o.lower() for o in json.loads(m["outcomes"])]
    winner = outcomes[prices.index(max(prices))]
    meta = event.get("eventMetadata") or {}
    return Resolution(
        condition_id=m["conditionId"],
        slug=event["slug"],
        asset=series.asset,
        tf=series.tf,
        end_ms=_ts_ms(m["endDate"]),
        outcome=winner.upper(),
        price_to_beat=meta.get("priceToBeat"),
        final_price=meta.get("finalPrice"),
    )


class GammaClient:
    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client or httpx.AsyncClient(base_url=GAMMA_URL, timeout=20)

    async def upcoming(self, series: Series, limit: int = 3) -> list[Market]:
        """Open markets in the series, soonest-ending first."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        r = await self._client.get(
            "/events",
            params={"series_slug": series.slug, "closed": "false", "end_date_min": now,
                    "order": "endDate", "ascending": "true", "limit": limit},
        )
        r.raise_for_status()
        return [parse_market(e, series) for e in r.json() if e.get("markets")]

    async def resolution(self, slug: str, series: Series) -> Resolution | None:
        r = await self._client.get("/events", params={"slug": slug})
        r.raise_for_status()
        events = r.json()
        return parse_resolution(events[0], series) if events else None

    async def aclose(self):
        await self._client.aclose()
