"""DuckDB query layer over the Parquet store: underlying bars, market windows, and point-in-time quotes."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

BAR_SECONDS = {"1s": 1, "5s": 5, "15s": 15, "30s": 30, "1m": 60, "3m": 180, "5m": 300, "15m": 900,
               "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}
STALE_MS = 5_000  # a book tick older than this is treated as "no quote"
# 5m/15m price to beat = Chainlink 60s TWAP ending at the window start (the same value Gamma later
# publishes as the previous window's finalPrice). Gamma only exposes it after resolution, so for live
# windows we compute it from the recorded Chainlink stream.
PTB_TWAP_MS = 60_000


def _dates(start_ms: int, end_ms: int) -> list[str]:
    d0 = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date()
    d1 = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).date()
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


class Feed:
    def __init__(self, root: str | Path = "data"):
        self.root = Path(root)
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone = 'UTC'")
        self._markets_view = False
        self._views()

    def _src(self, table: str) -> str:
        return (f"read_parquet('{self.root}/{table}/**/*.parquet', hive_partitioning=true, "
                f"union_by_name=true)")

    def _has(self, table: str) -> bool:
        d = self.root / table
        found = d.exists() and next(d.rglob("*.parquet"), None) is not None
        if found and table == "markets" and not self._markets_view:
            self._views()
        return found

    def _views(self) -> None:
        d = self.root / "markets"
        if not (d.exists() and next(d.rglob("*.parquet"), None) is not None):
            return
        self._markets_view = True
        res = (f"select * from {self._src('resolutions')} qualify row_number() over "
               f"(partition by condition_id order by final_price is null) = 1") if self._has("resolutions") else \
            "select null::varchar condition_id, null::varchar outcome, null::double price_to_beat, " \
            "null::double final_price where false"
        # One row per market with its resolution; price_to_beat from either source.
        self.con.execute(f"""
            create or replace view markets as
            with m as (select * exclude (date) from {self._src('markets')}
                       qualify row_number() over (partition by condition_id order by price_to_beat is null) = 1),
                 r as ({res})
            select m.* exclude (price_to_beat), coalesce(r.price_to_beat, m.price_to_beat) as price_to_beat,
                   r.outcome, r.final_price
            from m left join r using (condition_id)""")

    # ---------- catalog ----------
    def coverage(self) -> list[dict]:
        """Per asset/timeframe: market count and the span that has book data."""
        if not self._has("markets"):
            return []
        q = """select asset, tf, count(*) markets, count(outcome) resolved,
                      min(start_ms) first_ms, max(end_ms) last_ms from markets group by all order by all"""
        return self._dicts(q)

    def markets(self, asset: str, tf: str, start_ms: int, end_ms: int) -> list[dict]:
        if not self._has("markets"):
            return []
        rows = self._dicts(
            "select * from markets where asset=? and tf=? and end_ms > ? and start_ms < ? order by start_ms",
            [asset, tf, start_ms, end_ms])
        missing = [r["start_ms"] for r in rows if r["price_to_beat"] is None and r["tf"] in ("5m", "15m")]
        if missing:
            ptb = self.chainlink_price_to_beat(asset, missing)
            for r in rows:
                if r["price_to_beat"] is None and r["start_ms"] in ptb:
                    r["price_to_beat"], r["price_to_beat_source"] = ptb[r["start_ms"]], "chainlink"
        return rows

    def chainlink_price_to_beat(self, asset: str, starts_ms: list[int]) -> dict[int, float]:
        """TWAP of recorded Chainlink prices over [start - 60s, start) for each window start. A window
        needs at least 45 of the 60 seconds recorded, otherwise it is left out."""
        if not starts_ms or not self._has("chainlink_1s"):
            return {}
        lo, hi = min(starts_ms) - PTB_TWAP_MS, max(starts_ms)
        q = f"""
            with c as (select distinct on (ts_ms) ts_ms, price from {self._src('chainlink_1s')}
                       where asset = ? and date in (select unnest(?::date[])) and ts_ms >= ? and ts_ms < ?),
                 w as (select unnest(?::bigint[]) as start_ms)
            select w.start_ms, avg(c.price), count(*) from w
            join c on c.ts_ms >= w.start_ms - {PTB_TWAP_MS} and c.ts_ms < w.start_ms
            group by 1 having count(*) >= 45"""
        rows = self.con.execute(q, [asset, _dates(lo, hi), lo, hi, starts_ms]).fetchall()
        return {r[0]: r[1] for r in rows}

    def market(self, condition_id: str) -> dict | None:
        if not self._has("markets"):
            return None
        rows = self._dicts("select * from markets where condition_id = ?", [condition_id])
        return rows[0] if rows else None

    # ---------- underlying ----------
    def underlying_bars(self, asset: str, start_ms: int, end_ms: int, bar: str = "1m") -> list[tuple]:
        """(ts_ms, open, high, low, close, volume) bars aggregated from 1s klines, bar-open timestamps."""
        if not self._has("underlying_1s"):
            return []
        ms = BAR_SECONDS[bar] * 1000
        q = f"""
            with s as (select distinct on (ts_ms) ts_ms, open, high, low, close, volume
                       from {self._src('underlying_1s')}
                       where asset = ? and date in (select unnest(?::date[])) and ts_ms >= ? and ts_ms < ?)
            select (ts_ms // {ms}) * {ms} as t, arg_min(open, ts_ms), max(high), min(low), arg_max(close, ts_ms),
                   sum(volume)
            from s group by 1 order by 1"""
        return self.con.execute(q, [asset, _dates(start_ms, end_ms), start_ms, end_ms]).fetchall()

    # ---------- contract quotes ----------
    def quotes_at(self, asset: str, tf: str, times_ms: list[int]) -> dict[int, dict]:
        """For each timestamp: the market live at that moment (start <= t < end) and the latest top-of-book
        at or before t for its Up and Down tokens. Quotes older than STALE_MS are dropped."""
        if not times_ms or not self._has("markets") or not self._has("book_ticks"):
            return {}
        lo, hi = min(times_ms), max(times_ms)
        self.con.execute("create or replace temp table _grid as select unnest(?::bigint[]) as t", [times_ms])
        ticks = (f"select ts_ms, condition_id, side, best_bid, best_ask, ask_size, ask_depth_5c, bid_depth_5c "
                 f"from {self._src('book_ticks')} where asset = ? and tf = ? "
                 f"and date in (select unnest(?::date[])) and ts_ms between ? and ?")
        q = f"""
            with g as (select g.t, m.condition_id, m.start_ms, m.end_ms, m.price_to_beat, m.fee_rate
                       from _grid g join markets m on m.asset = ? and m.tf = ? and g.t >= m.start_ms and g.t < m.end_ms),
                 bt as ({ticks})
            select g.*, u.best_bid up_bid, u.best_ask up_ask, u.ask_size up_ask_size, u.ask_depth_5c up_ask_depth,
                   u.ts_ms up_ts, d.best_bid down_bid, d.best_ask down_ask, d.ask_size down_ask_size,
                   d.ask_depth_5c down_ask_depth, d.ts_ms down_ts
            from g
            asof left join (select * from bt where side = 'UP') u
                 on u.condition_id = g.condition_id and u.ts_ms <= g.t
            asof left join (select * from bt where side = 'DOWN') d
                 on d.condition_id = g.condition_id and d.ts_ms <= g.t"""
        params = [asset, tf, asset, tf, _dates(lo - 86_400_000, hi), lo - 86_400_000, hi]
        out = {}
        cols = None
        cur = self.con.execute(q, params)
        cols = [c[0] for c in cur.description]
        for row in cur.fetchall():
            r = dict(zip(cols, row))
            for side in ("up", "down"):
                ts = r.pop(f"{side}_ts")
                if ts is None or r["t"] - ts > STALE_MS:
                    for k in ("bid", "ask", "ask_size", "ask_depth"):
                        r[f"{side}_{k}"] = None
            out[r["t"]] = r
        missing = sorted({r["start_ms"] for r in out.values() if r["price_to_beat"] is None})
        if missing and tf in ("5m", "15m"):
            ptb = self.chainlink_price_to_beat(asset, missing)
            for r in out.values():
                if r["price_to_beat"] is None:
                    r["price_to_beat"] = ptb.get(r["start_ms"])
        return out

    # ---------- chart helpers ----------
    def market_ticks(self, condition_id: str) -> list[dict]:
        m = self.market(condition_id)
        if not m or not self._has("book_ticks"):
            return []
        q = f"""select ts_ms, side, best_bid, best_ask, bid_size, ask_size from {self._src('book_ticks')}
                where asset = ? and tf = ? and date in (select unnest(?::date[])) and condition_id = ?
                order by ts_ms, side"""
        return self._dicts(q, [m["asset"], m["tf"], _dates(m["start_ms"] - 86_400_000, m["end_ms"]), condition_id])

    def contract_bars(self, asset: str, tf: str, start_ms: int, end_ms: int, bar: str = "1m") -> list[tuple]:
        """OHLC of the Up-token mid for whichever market is live, stitched across windows.
        The series jumps at each window boundary (a new market starts near 0.5)."""
        if not self._has("markets") or not self._has("book_ticks"):
            return []
        ms = BAR_SECONDS[bar] * 1000
        q = f"""
            with t as (select b.ts_ms, (b.best_bid + b.best_ask) / 2 mid from {self._src('book_ticks')} b
                       join markets m using (condition_id)
                       where b.asset = ? and b.tf = ? and b.side = 'UP' and b.date in (select unnest(?::date[]))
                         and b.ts_ms >= ? and b.ts_ms < ? and b.ts_ms >= m.start_ms and b.ts_ms < m.end_ms
                         and b.best_bid is not null and b.best_ask is not null)
            select (ts_ms // {ms}) * {ms}, arg_min(mid, ts_ms), max(mid), min(mid), arg_max(mid, ts_ms)
            from t group by 1 order by 1"""
        return self.con.execute(q, [asset, tf, _dates(start_ms, end_ms), start_ms, end_ms]).fetchall()

    def _dicts(self, q: str, params=None) -> list[dict]:
        cur = self.con.execute(q, params or [])
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
