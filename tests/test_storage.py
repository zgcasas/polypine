import duckdb

from polypine.storage import ParquetSink, compact


def tick(ts, cid="c1", asset="BTC", tf="5m"):
    return {"ts_ms": ts, "condition_id": cid, "asset": asset, "tf": tf, "side": "UP",
            "best_bid": 0.5, "best_ask": 0.51, "bid_size": 1.0, "ask_size": 2.0,
            "bid_depth_5c": 10.0, "ask_depth_5c": 12.0}


def test_flush_partitions_and_reads_back(tmp_path):
    s = ParquetSink(tmp_path)
    day1, day2 = 1790601300000, 1790601300000 + 86_400_000
    s.write("book_ticks", tick(day1))
    s.write("book_ticks", tick(day1 + 1000))
    s.write("book_ticks", tick(day2, asset="ETH"))
    assert s.flush() == 3 and s.pending() == 0
    assert (tmp_path / "book_ticks/asset=BTC/tf=5m/date=2026-09-28").is_dir()
    assert (tmp_path / "book_ticks/asset=ETH/tf=5m/date=2026-09-29").is_dir()
    n = duckdb.sql(f"select count(*) from read_parquet('{tmp_path}/book_ticks/**/*.parquet', hive_partitioning=true)").fetchone()[0]
    assert n == 3


def test_compact_merges_parts(tmp_path):
    s = ParquetSink(tmp_path)
    for i in range(3):
        s.write("book_ticks", tick(1790601300000 + i * 1000))
        s.flush()
    part_dir = tmp_path / "book_ticks/asset=BTC/tf=5m/date=2026-09-28"
    assert len(list(part_dir.glob("*.parquet"))) == 3
    assert compact(tmp_path, "book_ticks") == 1
    files = list(part_dir.glob("*.parquet"))
    assert len(files) == 1
    assert duckdb.sql(f"select count(*) from '{files[0]}'").fetchone()[0] == 3


def test_underlying_has_no_tf_partition(tmp_path):
    s = ParquetSink(tmp_path)
    s.write("underlying_1s", {"ts_ms": 1790601300000, "asset": "BTC", "open": 1.0, "high": 1.0,
                              "low": 1.0, "close": 1.0, "volume": 0.0})
    s.flush()
    assert (tmp_path / "underlying_1s/asset=BTC/date=2026-09-28").is_dir()
