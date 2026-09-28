"""Buffered, partitioned Parquet sink: data/<table>/asset=X/tf=Y/date=YYYY-MM-DD/part-*.parquet"""

import os
import threading
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMAS: dict[str, pa.Schema] = {
    "markets": pa.schema([
        ("condition_id", pa.string()), ("slug", pa.string()), ("asset", pa.string()), ("tf", pa.string()),
        ("start_ms", pa.int64()), ("end_ms", pa.int64()),
        ("token_up", pa.string()), ("token_down", pa.string()),
        ("tick_size", pa.float64()), ("min_order_size", pa.float64()),
        ("fee_rate", pa.float64()), ("fee_exponent", pa.float64()), ("fee_taker_only", pa.bool_()),
        ("twap_lookback_s", pa.int32()), ("resolution_source", pa.string()), ("price_to_beat", pa.float64()),
    ]),
    "resolutions": pa.schema([
        ("condition_id", pa.string()), ("slug", pa.string()), ("asset", pa.string()), ("tf", pa.string()),
        ("end_ms", pa.int64()), ("outcome", pa.string()),
        ("price_to_beat", pa.float64()), ("final_price", pa.float64()),
    ]),
    # Top of book sampled once per second per token.
    "book_ticks": pa.schema([
        ("ts_ms", pa.int64()), ("condition_id", pa.string()), ("asset", pa.string()), ("tf", pa.string()),
        ("side", pa.string()),  # "UP" / "DOWN" token
        ("best_bid", pa.float64()), ("best_ask", pa.float64()),
        ("bid_size", pa.float64()), ("ask_size", pa.float64()),
        ("bid_depth_5c", pa.float64()), ("ask_depth_5c", pa.float64()),
    ]),
    # Every trade print from the market channel.
    "trades": pa.schema([
        ("ts_ms", pa.int64()), ("condition_id", pa.string()), ("asset", pa.string()), ("tf", pa.string()),
        ("side", pa.string()), ("price", pa.float64()), ("size", pa.float64()),
        ("taker_side", pa.string()), ("fee_rate_bps", pa.int32()), ("tx_hash", pa.string()),
    ]),
    # Top-10 L2 levels every N seconds, for depth-aware fill simulation.
    "book_l2": pa.schema([
        ("ts_ms", pa.int64()), ("condition_id", pa.string()), ("asset", pa.string()), ("tf", pa.string()),
        ("side", pa.string()),
        ("bid_px", pa.list_(pa.float64())), ("bid_sz", pa.list_(pa.float64())),
        ("ask_px", pa.list_(pa.float64())), ("ask_sz", pa.list_(pa.float64())),
    ]),
    "underlying_1s": pa.schema([
        ("ts_ms", pa.int64()), ("asset", pa.string()),
        ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
        ("volume", pa.float64()),
    ]),
}

# Column whose timestamp decides the date partition.
_TIME_COL = {"markets": "end_ms", "resolutions": "end_ms"}


def _date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


class ParquetSink:
    def __init__(self, root: str | os.PathLike = "data"):
        self.root = Path(root)
        self._buf: dict[tuple, list[dict]] = defaultdict(list)
        self._lock = threading.Lock()  # flush() may run in a worker thread

    def write(self, table: str, row: dict) -> None:
        ms = row[_TIME_COL.get(table, "ts_ms")]
        key = (table, row["asset"], row.get("tf"), _date(ms))
        with self._lock:
            self._buf[key].append(row)

    def pending(self) -> int:
        return sum(len(v) for v in self._buf.values())

    def flush(self) -> int:
        with self._lock:
            buf, self._buf = self._buf, defaultdict(list)
        n = 0
        for (table, asset, tf, date), rows in buf.items():
            parts = [table, f"asset={asset}"] + ([f"tf={tf}"] if tf else []) + [f"date={date}"]
            d = self.root.joinpath(*parts)
            d.mkdir(parents=True, exist_ok=True)
            tbl = pa.Table.from_pylist(rows, schema=SCHEMAS[table])
            name = f"part-{int(datetime.now().timestamp() * 1000)}-{uuid.uuid4().hex[:6]}.parquet"
            tmp = d / (name + ".tmp")
            pq.write_table(tbl, tmp, compression="zstd")
            tmp.rename(d / name)  # atomic: readers never see partial files
            n += len(rows)
        return n


def compact(root: str | os.PathLike, table: str) -> int:
    """Merge the many part files in each partition into one file. Returns partitions compacted."""
    count = 0
    for d in sorted(Path(root, table).rglob("date=*")):
        parts = sorted(d.glob("part-*.parquet"))
        if len(parts) < 2:
            continue
        tbl = pa.concat_tables([pq.read_table(p, schema=SCHEMAS[table]) for p in parts])
        sort_keys = [(c, "ascending") for c in ("condition_id", "side", "ts_ms") if c in tbl.column_names]
        if sort_keys:
            tbl = tbl.sort_by(sort_keys)
        out = d / f"part-compacted-{uuid.uuid4().hex[:6]}.parquet"
        pq.write_table(tbl, out.with_suffix(".tmp"), compression="zstd")
        out.with_suffix(".tmp").rename(out)
        for p in parts:
            p.unlink()
        count += 1
    return count
