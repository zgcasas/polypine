import pytest

from polypine.engine import BacktestConfig
from polypine.sweep import parse_grid, robust, sweep
from tests.test_integration import INPUT_SCRIPT, T0, W, store  # noqa: F401  (fixture)


def test_parse_grid_inclusive_ranges_and_lists():
    assert parse_grid("window_secs=200:30:-10")[1] == list(range(200, 29, -10))
    name, vals = parse_grid("max_price=0.50:0.96:0.02")
    assert name == "max_price" and len(vals) == 24 and vals[0] == 0.5 and vals[-1] == 0.96  # no float drift
    assert parse_grid("min_bps=0:10:1")[1] == list(range(11))
    assert parse_grid("x=1,2.5,3")[1] == [1, 2.5, 3]
    with pytest.raises(ValueError):
        parse_grid("x=0:10:-1")
    with pytest.raises(ValueError):
        parse_grid("nonsense")


def test_robust_needs_trades_and_profit_without_best_trade():
    base = {"trades": 30, "net_pnl": 50.0, "net_pnl_ex_best": 10.0}
    assert robust(base, 20)
    assert not robust({**base, "trades": 10}, 20)
    assert not robust({**base, "net_pnl_ex_best": -5.0}, 20)  # one longshot carries it
    assert not robust({**base, "net_pnl": -1.0}, 20)


def test_sweep_runs_every_combination_in_parallel(store, tmp_path):  # noqa: F811
    script = store / "tunable.py"
    script.write_text(INPUT_SCRIPT.replace("DEFAULT", "120"))
    cfg = BacktestConfig(script=str(script), asset="BTC", tf="5m", bar="1m", start_ms=T0 - 600_000,
                         end_ms=T0 + 4 * W, stake=60.0)
    rows = sweep(cfg, {"secs": [60, 120, 180]}, data_root=str(store), workers=2, min_trades=1,
                 out_csv=tmp_path / "out.csv", progress=lambda *_: None)
    assert sorted(r["secs"] for r in rows) == [60, 120, 180]
    assert all(r["trades"] == 3 for r in rows)  # one entry per window; the 4th window is unresolved (open)
    assert rows[0]["net_pnl"] >= rows[-1]["net_pnl"]       # ranked best first
    assert (tmp_path / "out.csv").read_text().splitlines()[0].startswith("secs,trades")
    with pytest.raises(ValueError, match="unknown inputs"):
        sweep(cfg, {"nope": [1]}, data_root=str(store), progress=lambda *_: None)
