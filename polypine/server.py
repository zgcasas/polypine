"""Local web app: chart underlying + contract prices, edit strategies, run backtests."""

import math
import re
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .config import ASSETS, TIMEFRAMES
from .datafeed import BAR_SECONDS, Feed
from .engine import BacktestConfig, run_backtest

WEB = Path(__file__).parent / "web"
MAX_CHART_BARS = 20_000

_lock = threading.Lock()  # DuckDB connection and PyneCore's module state are single-threaded


def _ms(s: str | None, default_ms: int | None = None) -> int:
    """Parse an ISO date/datetime (UTC unless it has an offset). Empty -> default_ms."""
    if not s:
        if default_ms is None:
            raise HTTPException(400, "start and end are required (YYYY-MM-DD or YYYY-MM-DDTHH:MM, UTC)")
        return default_ms
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        raise HTTPException(400, f"invalid date {s!r}: use YYYY-MM-DD or YYYY-MM-DDTHH:MM (UTC)")
    return int((d if d.tzinfo else d.replace(tzinfo=timezone.utc)).timestamp() * 1000)


def _range(start: str | None, end: str | None) -> tuple[int, int]:
    """Chart range; defaults to the last 24h."""
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    e = _ms(end, now)
    s = _ms(start, e - 86_400_000)
    if s >= e:
        raise HTTPException(400, "start must be before end")
    return s, e


def _downsample(bars: list[dict], plots: list[dict], trades_t: set[int]) -> tuple[list[dict], list[dict], int]:
    """Aggregate to at most MAX_CHART_BARS for display. Returns (bars, plots, factor)."""
    k = -(-len(bars) // MAX_CHART_BARS)
    if k <= 1:
        return bars, plots, 1
    out_b, out_p = [], []
    for i in range(0, len(bars), k):
        chunk = bars[i:i + k]
        out_b.append({"t": chunk[0]["t"], "o": chunk[0]["o"], "h": max(b["h"] for b in chunk),
                      "l": min(b["l"] for b in chunk), "c": chunk[-1]["c"], "v": sum(b["v"] for b in chunk)})
        out_p.append({**plots[min(i + k, len(plots)) - 1], "t": chunk[0]["t"]})
    return out_b, out_p, k


def _json_safe(v):
    """NaN/inf are not valid JSON; replace them with null anywhere in a result."""
    if isinstance(v, float):
        return None if math.isnan(v) or math.isinf(v) else v
    if isinstance(v, dict):
        return {k: _json_safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
    return v


def _check(asset: str | None = None, tf: str | None = None, bar: str | None = None) -> None:
    if asset is not None and asset not in ASSETS:
        raise HTTPException(400, f"asset must be one of {list(ASSETS)}")
    if tf is not None and tf not in TIMEFRAMES:
        raise HTTPException(400, f"tf must be one of {list(TIMEFRAMES)}")
    if bar is not None and bar not in BAR_SECONDS:
        raise HTTPException(400, f"bar must be one of {list(BAR_SECONDS)}")


class BacktestRequest(BaseModel):
    script: str
    asset: str = "BTC"
    tf: str = "5m"
    bar: str = "1m"
    start: str
    end: str
    inputs: dict = Field(default_factory=dict)
    stake: float = 100.0
    latency_ms: int = 1000
    min_secs_left: int = 0
    max_entry_price: float = 0.99
    roll: bool = False


class ScriptBody(BaseModel):
    source: str


def create_app(data_root: str = "data", strategies_dir: str = "strategies") -> FastAPI:
    app = FastAPI(title="polypine")
    feed = Feed(data_root)
    sdir = Path(strategies_dir)

    def script_path(name: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9_\-]+\.(py|pine)", name):
            raise HTTPException(400, "script names are letters, digits, _ or -, ending in .py or .pine")
        return sdir / name

    @app.get("/")
    def index():
        # Revalidate on every load (cheap: 304 when unchanged), so a deploy is visible on the next reload
        # instead of whenever the browser's heuristic cache expires.
        return FileResponse(WEB / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/api/meta")
    def meta():
        with _lock:
            feed._views()  # pick up markets written since startup
            cov = feed.coverage()
        return {"assets": ASSETS, "timeframes": TIMEFRAMES, "bars": list(BAR_SECONDS), "coverage": cov}

    @app.get("/api/strategies")
    def strategies():
        return sorted(p.name for p in sdir.glob("*") if p.suffix in (".py", ".pine"))

    @app.get("/api/strategies/{name}")
    def get_strategy(name: str):
        p = script_path(name)
        if not p.exists():
            raise HTTPException(404, "not found")
        return {"name": name, "source": p.read_text()}

    @app.put("/api/strategies/{name}")
    def put_strategy(name: str, body: ScriptBody):
        p = script_path(name)
        sdir.mkdir(parents=True, exist_ok=True)
        p.write_text(body.source)
        return {"ok": True}

    @app.get("/api/bars")
    def bars(asset: str, start: str = "", end: str = "", bar: str = "1m"):
        _check(asset=asset, bar=bar)
        s, e = _range(start, end)
        with _lock:
            rows = feed.underlying_bars(asset, s, e, bar)
        return [{"t": r[0], "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]} for r in rows[-MAX_CHART_BARS:]]

    @app.get("/api/contract")
    def contract(asset: str, tf: str, start: str = "", end: str = "", bar: str = "1m"):
        _check(asset=asset, tf=tf, bar=bar)
        s, e = _range(start, end)
        with _lock:
            rows = feed.contract_bars(asset, tf, s, e, bar)
            windows = feed.markets(asset, tf, s, e)
        return {"bars": [{"t": r[0], "o": r[1], "h": r[2], "l": r[3], "c": r[4]} for r in rows[-MAX_CHART_BARS:]],
                "windows": [{**{k: w[k] for k in ("slug", "start_ms", "end_ms", "outcome", "price_to_beat",
                                                   "final_price")},
                             "price_to_beat_source": w.get("price_to_beat_source", "polymarket")}
                            for w in windows[-2000:]]}

    @app.post("/api/backtest")
    def backtest(req: BacktestRequest):
        _check(asset=req.asset, tf=req.tf, bar=req.bar)
        cfg = BacktestConfig(script=str(script_path(req.script)), asset=req.asset, tf=req.tf, bar=req.bar,
                             start_ms=_ms(req.start), end_ms=_ms(req.end), inputs=req.inputs, stake=req.stake,
                             latency_ms=req.latency_ms, min_secs_left=req.min_secs_left,
                             max_entry_price=req.max_entry_price, roll=req.roll)
        try:
            with _lock:
                res = run_backtest(cfg, feed)
        except Exception as e:  # script errors are user-facing
            raise HTTPException(400, f"{type(e).__name__}: {e}")
        res["bars"], res["plots"], res["bar_factor"] = _downsample(res["bars"], res["plots"], set())
        return _json_safe(res)

    return app
