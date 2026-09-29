import duckdb

from polypine.config import UNDERLYING, Series
from polypine.importers import _futures_query
from polypine.underlying import trades_to_1s

T = 1_775_779_200_000  # 2026-04-10 00:00 UTC


def test_trades_to_1s_ohlcv_and_fills_quiet_seconds():
    trades = [(T + 100, 10.0, 1.0), (T + 500, 12.0, 2.0), (T + 900, 11.0, 1.0), (T + 3_200, 9.0, 5.0)]
    bars = trades_to_1s(trades, T, T + 4_000)
    assert bars == [
        (T, 10.0, 12.0, 10.0, 11.0, 4.0),
        (T + 1000, 11.0, 11.0, 11.0, 11.0, 0.0),  # no trades: flat at previous close
        (T + 2000, 11.0, 11.0, 11.0, 11.0, 0.0),
        (T + 3000, 9.0, 9.0, 9.0, 9.0, 5.0),
    ]


def test_trades_to_1s_uses_previous_close_and_skips_unknown_start():
    assert trades_to_1s([], T, T + 2000) == []
    assert trades_to_1s([], T, T + 2000, prev_close=5.0) == [(T, 5, 5, 5, 5, 0.0), (T + 1000, 5, 5, 5, 5, 0.0)]
    # trades outside the window are ignored
    assert trades_to_1s([(T - 1, 1.0, 1.0), (T + 5000, 2.0, 1.0)], T, T + 1000) == []


def test_futures_archive_query_builds_continuous_grid(tmp_path):
    f = tmp_path / "HYPEUSDT-futures-2026-04-10.csv"
    f.write_text("agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n"
                 f"1,10.0,1.0,1,1,{T + 100},false\n2,12.0,2.0,2,2,{T + 600},true\n3,9.0,1.0,3,3,{T + 2_500},false\n")
    rows = duckdb.sql(_futures_query([str(f)], "HYPE", T, T + 4_000) + " order by ts_ms").fetchall()
    assert [r[0] for r in rows] == [T, T + 1000, T + 2000, T + 3000]
    assert rows[0][2:] == (10.0, 12.0, 10.0, 12.0, 3.0)
    assert rows[1][2:] == (12.0, 12.0, 12.0, 12.0, 0)
    assert rows[3][2:] == (9.0, 9.0, 9.0, 9.0, 0)


def test_hype_config():
    assert [Series("HYPE", tf).slug for tf in ("5m", "15m", "1h", "1d")] == [
        "hype-up-or-down-5m", "hype-up-or-down-15m", "hype-up-or-down-hourly", "hype-up-or-down-daily"]
    assert UNDERLYING["HYPE"] == ("futures", "HYPEUSDT")
    assert UNDERLYING["BTC"] == ("spot", "BTCUSDT")
