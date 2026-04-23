"""Integration tests for advanced internal-read intelligence routes."""

from __future__ import annotations

from pathlib import Path

import pytest
from starlette.testclient import TestClient

pytest.importorskip("httpx")


def _seed_intelligence_data(db_path: Path) -> None:
    from app.db.repository import AlphaRepository

    repo = AlphaRepository(db_path=str(db_path))
    c = repo.conn
    tenant = "default"

    c.execute(
        """
        INSERT INTO strategies (
          id, tenant_id, track, name, version, strategy_type, mode, active, config_json,
          status, is_champion, backtest_score, forward_score, live_score, stability_score,
          sample_size, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "strat_1",
            tenant,
            "quant",
            "Q Momentum",
            "v1",
            "baseline_momentum",
            "balanced",
            1,
            "{}",
            "ACTIVE",
            1,
            0.61,
            0.58,
            0.57,
            0.95,
            120,
            "2026-04-20T00:00:00+00:00",
        ),
    )
    c.execute(
        """
        INSERT INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("stab_1", tenant, "strat_1", 0.64, 0.6, 0.94, "2026-04-21T12:00:00+00:00"),
    )
    c.execute(
        """
        INSERT INTO regime_performance (id, tenant_id, regime, prediction_count, accuracy, avg_return, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("rp_1", tenant, "risk_on", 140, 0.62, 0.013, "2026-04-21T12:00:00+00:00"),
    )
    c.execute(
        """
        INSERT INTO consensus_signals (
          id, tenant_id, ticker, regime, sentiment_strategy_id, quant_strategy_id,
          sentiment_score, quant_score, ws, wq, agreement_bonus, p_final, stability_score, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "cs_1",
            tenant,
            "SPY",
            "risk_on",
            "sent_1",
            "strat_1",
            0.72,
            0.76,
            0.5,
            0.5,
            0.05,
            0.79,
            0.93,
            "2026-04-21T13:00:00+00:00",
        ),
    )
    c.execute(
        """
        INSERT INTO raw_events (id, tenant_id, timestamp, source, text, tickers_json, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "raw_1",
            tenant,
            "2026-04-21T10:00:00+00:00",
            "news",
            "SPY rally on macro relief",
            '["SPY"]',
            "{}",
        ),
    )
    c.execute(
        """
        INSERT INTO scored_events (
          id, tenant_id, raw_event_id, primary_ticker, category, materiality, direction, confidence,
          company_relevance, concept_tags_json, explanation_terms_json, scorer_version, taxonomy_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "se_1",
            tenant,
            "raw_1",
            "SPY",
            "macro",
            0.82,
            "up",
            0.78,
            0.9,
            '["rates","inflation"]',
            '["soft landing","risk appetite"]',
            "s1",
            "t1",
        ),
    )
    c.execute(
        """
        INSERT INTO predictions (
          id, tenant_id, strategy_id, scored_event_id, ticker, timestamp, prediction, confidence,
          horizon, entry_price, mode, feature_snapshot_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "pred_1",
            tenant,
            "strat_1",
            "se_1",
            "SPY",
            "2026-04-21T10:05:00+00:00",
            "BUY",
            0.81,
            "5d",
            550.0,
            "balanced",
            "{}",
        ),
    )
    c.execute(
        """
        INSERT INTO prediction_outcomes (
          id, tenant_id, prediction_id, exit_price, return_pct, direction_correct, max_runup,
          max_drawdown, evaluated_at, exit_reason, residual_alpha
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "po_1",
            tenant,
            "pred_1",
            555.0,
            0.009,
            1,
            0.014,
            -0.004,
            "2026-04-22T10:05:00+00:00",
            "horizon",
            0.006,
        ),
    )
    c.execute(
        """
        INSERT INTO prediction_runs (
          id, tenant_id, ingress_start, ingress_end, prediction_start, prediction_end, timeframe, regime, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "run_1",
            tenant,
            "2026-04-21T09:00:00+00:00",
            "2026-04-21T09:10:00+00:00",
            "2026-04-21T09:10:00+00:00",
            "2026-04-21T09:11:00+00:00",
            "1d",
            "risk_on",
            "2026-04-21T09:11:00+00:00",
        ),
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS loop_heartbeats (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          run_id TEXT,
          idempotency_key TEXT,
          loop_type TEXT NOT NULL,
          status TEXT NOT NULL,
          notes TEXT,
          created_at TEXT NOT NULL,
          timestamp TEXT
        )
        """
    )
    c.execute(
        """
        INSERT INTO loop_heartbeats (id, tenant_id, run_id, idempotency_key, loop_type, status, notes, created_at, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "hb_1",
            tenant,
            "run_1",
            "idem_1",
            "live",
            "ok",
            "alive",
            "2026-04-21T09:12:00+00:00",
            "2026-04-21T09:12:00+00:00",
        ),
    )
    c.commit()
    c.close()


@pytest.fixture
def intel_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    db = tmp_path / "intel.db"
    monkeypatch.setenv("ALPHA_DB_PATH", str(db))
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    _seed_intelligence_data(db)
    from app.internal_read_v1.app import app

    with TestClient(app) as client:
        yield client


def test_api_strategies_catalog(intel_client: TestClient) -> None:
    res = intel_client.get("/api/strategies/catalog")
    assert res.status_code == 200
    body = res.json()
    assert body["count"] >= 1
    assert body["strategies"][0]["status"] == "ACTIVE"


def test_api_strategy_stability(intel_client: TestClient) -> None:
    res = intel_client.get("/api/strategies/strat_1/stability")
    assert res.status_code == 200
    body = res.json()
    assert body["strategyId"] == "strat_1"
    assert "stabilityScore" in body


def test_api_performance_regime(intel_client: TestClient) -> None:
    res = intel_client.get("/api/performance/regime")
    assert res.status_code == 200
    body = res.json()
    assert body["regimes"][0]["regime"] == "risk_on"


def test_api_consensus_signals(intel_client: TestClient) -> None:
    res = intel_client.get("/api/consensus/signals")
    assert res.status_code == 200
    body = res.json()
    assert body["count"] >= 1
    assert "agreementBonus" in body["signals"][0]
    assert "pFinal" in body["signals"][0]


def test_api_ticker_attribution(intel_client: TestClient) -> None:
    res = intel_client.get("/api/ticker/SPY/attribution")
    assert res.status_code == 200
    body = res.json()
    assert body["ticker"] == "SPY"
    assert body["count"] >= 1


def test_api_ticker_accuracy(intel_client: TestClient) -> None:
    res = intel_client.get("/api/ticker/SPY/accuracy")
    assert res.status_code == 200
    body = res.json()
    assert body["sampleCount"] == 1
    assert body["hitRate"] is not None


def test_api_system_heartbeat(intel_client: TestClient) -> None:
    res = intel_client.get("/api/system/heartbeat")
    assert res.status_code == 200
    body = res.json()
    assert len(body["loops"]) >= 1
    assert body["loops"][0]["loopType"] == "live"


def test_api_prediction_runs_latest(intel_client: TestClient) -> None:
    res = intel_client.get("/api/predictions/runs/latest")
    assert res.status_code == 200
    body = res.json()
    assert body["id"] == "run_1"
    assert "ingressEnd" in body
    assert "predictionEnd" in body
    assert body["runStatus"] in ("HEALTHY", "DEGRADED", "FAILED")
    assert isinstance(body["degradedReasons"], list)
    assert "runQuality" in body
    assert "coverageRatio" in body
