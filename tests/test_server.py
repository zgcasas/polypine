from fastapi.testclient import TestClient

from polypine.server import MAX_CHART_BARS, _downsample, create_app


def client(tmp_path):
    (tmp_path / "strategies").mkdir()
    (tmp_path / "strategies" / "a.py").write_text("x = 1\n")
    return TestClient(create_app(str(tmp_path / "data"), str(tmp_path / "strategies")))


def test_strategy_crud_and_name_validation(tmp_path):
    c = client(tmp_path)
    assert c.get("/api/strategies").json() == ["a.py"]
    assert c.put("/api/strategies/b.py", json={"source": "y = 2\n"}).status_code == 200
    assert c.get("/api/strategies/b.py").json()["source"] == "y = 2\n"
    for bad in ("..%2Fx.py", "x.sh", "a b.py"):
        assert c.put(f"/api/strategies/{bad}", json={"source": ""}).status_code in (400, 404)
    assert not (tmp_path / "x.py").exists()


def test_backtest_rejects_bad_script_name(tmp_path):
    r = client(tmp_path).post("/api/backtest", json={"script": "../etc.py", "start": "2026-04-06", "end": "2026-04-07"})
    assert r.status_code == 400


def test_downsample_keeps_ohlc_semantics():
    n = MAX_CHART_BARS * 3
    bars = [{"t": i, "o": i, "h": i + 1, "l": i - 1, "c": i + 0.5, "v": 1} for i in range(n)]
    plots = [{"t": i, "x": i} for i in range(n)]
    b, p, k = _downsample(bars, plots, set())
    assert k == 3 and len(b) == MAX_CHART_BARS
    assert b[0] == {"t": 0, "o": 0, "h": 3, "l": -1, "c": 2.5, "v": 3}
    assert p[0]["x"] == 2  # last value of the bucket
