"""Regression tests for /api market routes (seeded SQLite file DB)."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from starlette.testclient import TestClient

pytest.importorskip("httpx")


def _bar(ts: str, close: float, **kw: float) -> SimpleNamespace:
    o = kw.get("open", close)
    h = kw.get("high", close)
    low = kw.get("low", close)
    v = kw.get("volume", 1_000_000.0)
    return SimpleNamespace(timestamp=ts, open=o, high=h, low=low, close=close, volume=v)


def _seed_price_bars(db_path: Path) -> None:
    from app.db.repository import AlphaRepository

    repo = AlphaRepository(db_path=str(db_path))
    daily = [
        _bar("2022-06-01T00:00:00+00:00", 100.0, high=105.0, low=95.0, volume=1e6),
        _bar("2022-06-02T00:00:00+00:00", 110.0, high=112.0, low=108.0, volume=2e6),
    ]
    cheap_daily = [
        _bar("2022-06-01T00:00:00+00:00", 1.40, high=1.60, low=1.20, volume=3e6),
        _bar("2022-06-02T00:00:00+00:00", 1.70, high=1.90, low=1.35, volume=4e6),
    ]
    repo.save_price_bars("TST", "1d", daily, tenant_id="default")
    repo.save_price_bars("CHEAP", "1d", cheap_daily, tenant_id="default")
    repo.conn.execute(
        """
        INSERT OR REPLACE INTO candidate_queue
          (tenant_id, ticker, status, first_seen_at, last_seen_at, signal_count, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("default", "TST", "admitted", "2022-06-01T00:00:00+00:00", "2022-06-02T00:00:00+00:00", 2, "{}"),
    )
    repo.save_consensus_signal(
        {
            "ticker": "TST",
            "regime": "NORMAL",
            "sentiment_strategy_id": "test_s",
            "quant_strategy_id": "test_q",
            "sentiment_score": 0.8,
            "quant_score": 0.7,
            "ws": 0.5,
            "wq": 0.5,
            "agreement_bonus": 0.0,
            "p_final": 0.72,
            "stability_score": 0.7,
        },
        tenant_id="default",
    )
    repo.conn.execute(
        """
        INSERT OR REPLACE INTO candidate_queue
          (tenant_id, ticker, status, first_seen_at, last_seen_at, signal_count, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("default", "CHEAP", "admitted", "2022-06-01T00:00:00+00:00", "2022-06-02T00:00:00+00:00", 2, "{}"),
    )
    repo.conn.execute(
        """
        INSERT INTO ranking_snapshots
          (id, tenant_id, ticker, score, conviction, attribution_json, regime, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid4()),
            "default",
            "TST",
            0.55,
            0.55,
            "{}",
            "NORMAL",
            "2022-06-02T00:00:00+00:00",
        ),
    )
    repo.conn.execute(
        """
        INSERT INTO ranking_snapshots
          (id, tenant_id, ticker, score, conviction, attribution_json, regime, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid4()),
            "default",
            "CHEAP",
            0.68,
            0.68,
            "{}",
            "NORMAL",
            "2022-06-02T00:00:00+00:00",
        ),
    )
    repo.save_consensus_signal(
        {
            "ticker": "CHEAP",
            "regime": "NORMAL",
            "sentiment_strategy_id": "test_s2",
            "quant_strategy_id": "test_q2",
            "sentiment_score": 0.75,
            "quant_score": 0.7,
            "ws": 0.5,
            "wq": 0.5,
            "agreement_bonus": 0.0,
            "p_final": 0.73,
            "stability_score": 0.72,
        },
        tenant_id="default",
    )
    repo.conn.commit()
    repo.conn.close()


@pytest.fixture
def market_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    db = tmp_path / "market.db"
    profiles_dir = tmp_path / "company_profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "CHEAP.json").write_text(
        """
{"longName":"Cheap Co","sector":"Technology","industry":"Software","website":"https://cheap.example","country":"US","fullTimeEmployees":2500}
""".strip(),
        encoding="utf-8",
    )
    (profiles_dir / "TST.json").write_text(
        """
{"longName":"Test Holdings","sector":"Finance","industry":"Services","website":"https://tst.example","country":"US","fullTimeEmployees":1200}
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("ALPHA_DB_PATH", str(db))
    monkeypatch.setenv("COMPANY_PROFILES_DIR", str(profiles_dir))
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    _seed_price_bars(db)
    from app.internal_read_v1.app import app

    with TestClient(app) as client:
        yield client


def test_api_stats_seeded(market_client: TestClient) -> None:
    res = market_client.get("/api/stats/TST")
    assert res.status_code == 200
    j = res.json()
    assert j["ticker"] == "TST"
    assert j["price"] == 110.0
    assert j["dayChangePct"] is not None
    assert abs(j["dayChangePct"] - 10.0) < 0.01
    assert j["high52"] >= j["price"]
    assert j["avgVolume"] is not None


def test_api_quote_seeded(market_client: TestClient) -> None:
    res = market_client.get("/api/quote/TST")
    assert res.status_code == 200
    assert res.json()["price"] == 110.0


def test_api_quote_unknown_404(market_client: TestClient) -> None:
    res = market_client.get("/api/quote/ZZZNOTFOUND")
    assert res.status_code == 404


def test_api_history_seeded(market_client: TestClient) -> None:
    # MAX ensures seeded past bars stay in-window regardless of test run date
    res = market_client.get("/api/history/TST", params={"range": "MAX", "interval": "1D"})
    assert res.status_code == 200
    j = res.json()
    assert j["ticker"] == "TST"
    assert j["range"] == "MAX"
    assert j["interval"] == "1D"
    assert len(j["points"]) >= 1


def test_api_history_invalid_range_400(market_client: TestClient) -> None:
    res = market_client.get("/api/history/TST", params={"range": "bogus"})
    assert res.status_code == 400


def test_api_candles_invalid_range_400(market_client: TestClient) -> None:
    res = market_client.get("/api/candles/TST", params={"range": "nope"})
    assert res.status_code == 400


def test_api_candles_seeded(market_client: TestClient) -> None:
    res = market_client.get("/api/candles/TST", params={"range": "MAX", "interval": "1D"})
    assert res.status_code == 200
    j = res.json()
    assert "candles" in j
    assert len(j["candles"]) >= 1


def test_api_tickers_search_msft(market_client: TestClient) -> None:
    res = market_client.get("/api/tickers", params={"q": "ms"})
    assert res.status_code == 200
    tickers = res.json()["tickers"]
    assert "MSFT" in tickers


def test_api_recommendations_latest(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/latest", params={"limit": 5, "mode": "balanced"})
    assert res.status_code == 200
    body = res.json()
    assert body["selectionPreference"] == "absolute"
    rows = body["recommendations"]
    assert len(rows) >= 1
    assert rows[0]["ticker"] == "TST"
    assert "action" in rows[0]
    assert "confidence" in rows[0]
    assert rows[0]["selectionPreference"] == "absolute"


def test_api_recommendations_latest_long_only(market_client: TestClient) -> None:
    res = market_client.get(
        "/api/recommendations/latest",
        params={"limit": 5, "mode": "balanced", "preference": "long_only"},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["selectionPreference"] == "long_only"
    rows = body["recommendations"]
    assert len(rows) >= 1
    assert rows[0]["selectionPreference"] == "long_only"


def test_api_recommendations_best(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/best")
    assert res.status_code == 200
    row = res.json()
    assert row["ticker"] == "TST"
    assert row["mode"] == "balanced"
    assert row["selectionPreference"] == "absolute"


def test_api_recommendations_best_long_only(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/best", params={"preference": "long_only"})
    assert res.status_code == 200
    row = res.json()
    assert row["selectionPreference"] == "long_only"
    assert row["action"] in ("BUY", "SELL", "HOLD")


def test_api_recommendations_ticker(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/TST", params={"mode": "conservative"})
    assert res.status_code == 200
    row = res.json()
    assert row["ticker"] == "TST"
    assert row["mode"] == "conservative"


def test_api_recommendations_invalid_mode_400(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/latest", params={"mode": "x"})
    assert res.status_code == 400


def test_api_recommendations_best_invalid_preference_400(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/best", params={"preference": "x"})
    assert res.status_code == 400


def test_api_recommendations_latest_invalid_preference_400(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/latest", params={"preference": "x"})
    assert res.status_code == 400


def test_api_recommendations_under_price(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/under/2", params={"mode": "balanced", "limit": 10})
    assert res.status_code == 200
    body = res.json()
    assert body["selectionPreference"] == "long_only"
    assert body["priceCap"] == 2.0
    rows = body["recommendations"]
    assert len(rows) >= 1
    assert rows[0]["ticker"] == "CHEAP"
    assert rows[0]["entryZone"][1] <= 2.0
    assert rows[0]["band"] == "under_2"
    assert rows[0]["eligibilityPassed"] is True
    assert "compositeScore" in rows[0]
    assert "fiftyDollarPotential" in rows[0]
    assert rows[0]["disqualifiers"] == []


def test_api_recommendations_under_price_invalid_400(market_client: TestClient) -> None:
    res = market_client.get("/api/recommendations/under/0")
    assert res.status_code == 400


def test_api_recommendations_under_price_skips_null_latest_close(market_client: TestClient) -> None:
    from app.internal_read_v1.chart_market import build_quote_payload

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE price_bars (
            tenant_id TEXT,
            ticker TEXT,
            timeframe TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
        """,
    )
    conn.executemany(
        """
        INSERT INTO price_bars
          (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("default", "CHEAP", "1d", "2022-06-02T00:00:00+00:00", 1.6, 1.9, 1.4, 1.7, 4_000_000.0),
            ("default", "CHEAP", "1d", "2022-06-03T00:00:00+00:00", 1.8, 2.0, 1.7, None, 4_200_000.0),
        ],
    )
    out = build_quote_payload(conn, tenant_id="default", ticker="CHEAP")
    assert out is not None
    assert out["price"] == 1.7
