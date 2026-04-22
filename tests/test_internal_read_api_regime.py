"""Tests for GET /api/regime/{ticker}."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
import pytest
from starlette.testclient import TestClient

pytest.importorskip("httpx")


def _bar(ts: str, close: float) -> SimpleNamespace:
    return SimpleNamespace(
        timestamp=ts, open=close, high=close, low=close, close=close, volume=1_000_000.0
    )


def _seed_spy_trending_up(db_path: Path) -> None:
    from app.db.repository import AlphaRepository

    repo = AlphaRepository(db_path=str(db_path))
    start = date(2020, 1, 1)
    daily: list[SimpleNamespace] = []
    for i in range(210):
        d = start + timedelta(days=i)
        ts = f"{d.isoformat()}T00:00:00+00:00"
        c = 100.0 + i * 0.5
        daily.append(_bar(ts, c))
    repo.save_price_bars("SPY", "1d", daily, tenant_id="default")
    repo.conn.commit()
    repo.conn.close()


def _seed_tst_short(db_path: Path) -> None:
    from app.db.repository import AlphaRepository

    repo = AlphaRepository(db_path=str(db_path))
    daily = [
        _bar("2025-06-01T00:00:00+00:00", 100.0),
        _bar("2025-06-02T00:00:00+00:00", 110.0),
    ]
    repo.save_price_bars("TST", "1d", daily, tenant_id="default")
    repo.conn.commit()
    repo.conn.close()


@pytest.fixture
def spy_regime_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    db = tmp_path / "spy_regime.db"
    monkeypatch.setenv("ALPHA_DB_PATH", str(db))
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    _seed_spy_trending_up(db)
    from app.internal_read_v1.app import app

    with TestClient(app) as client:
        yield client


@pytest.fixture
def short_history_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    db = tmp_path / "short.db"
    monkeypatch.setenv("ALPHA_DB_PATH", str(db))
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    _seed_tst_short(db)
    from app.internal_read_v1.app import app

    with TestClient(app) as client:
        yield client


def test_api_regime_spy_valid_regime(spy_regime_client: TestClient) -> None:
    res = spy_regime_client.get("/api/regime/SPY")
    assert res.status_code == 200
    body = res.json()
    assert body["ticker"] == "SPY"
    assert body["regime"] in ("risk_on", "risk_off")
    assert body["close"] > 0
    assert body["sma20"] > 0
    assert body["sma200"] > 0
    assert isinstance(body["score"], (int, float))
    assert 0 <= float(body["score"]) <= 1
    assert isinstance(body["confirmedBars"], int)
    assert 1 <= body["confirmedBars"] <= 5
    assert len(body["asOf"]) >= 10


def test_api_regime_insufficient_history(short_history_client: TestClient) -> None:
    res = short_history_client.get("/api/regime/TST")
    assert res.status_code == 422
    assert res.json() == {"error": "insufficient_history"}
