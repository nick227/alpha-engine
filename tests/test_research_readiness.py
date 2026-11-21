"""Tests for research readiness, dataset freshness reporting, and contract v2 fail-closed behavior."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.pipeline_gates import recommendation_quality
from app.internal_read_v1.chart_market import build_company_payload
from app.internal_read_v1.data_health_read import build_data_health_compact
from app.internal_read_v1.intelligence_read import get_consensus_signals
from app.internal_read_v1.recommendations import _row_to_payload


@pytest.fixture
def test_db(tmp_path: Path) -> sqlite3.Connection:
    db_file = tmp_path / "test_research.db"
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE price_bars (
            tenant_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY (tenant_id, ticker, timeframe, timestamp)
        );
        CREATE TABLE predictions (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            horizon TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            score REAL NOT NULL,
            direction TEXT NOT NULL,
            confidence REAL NOT NULL
        );
        CREATE TABLE consensus_signals (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            regime TEXT,
            sentiment_strategy_id TEXT,
            quant_strategy_id TEXT,
            sentiment_score REAL,
            quant_score REAL,
            ws REAL,
            wq REAL,
            agreement_bonus REAL,
            p_final REAL,
            stability_score REAL,
            score REAL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE fundamentals_snapshot (
            tenant_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            sector TEXT,
            industry TEXT,
            PRIMARY KEY (tenant_id, ticker, as_of_date)
        );
        CREATE TABLE strategies (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE ml_learning_rows (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE raw_events (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            timestamp TEXT NOT NULL
        );
        CREATE TABLE ranking_snapshots (
            tenant_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            score REAL NOT NULL,
            timestamp TEXT NOT NULL
        );
        CREATE TABLE house_recommendations (
            tenant_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            ticker TEXT NOT NULL,
            action TEXT NOT NULL,
            score REAL NOT NULL,
            horizon TEXT NOT NULL,
            as_of TEXT NOT NULL,
            source_json TEXT NOT NULL,
            PRIMARY KEY (tenant_id, mode, ticker)
        );
        """
    )
    return conn


def test_missing_company_profile_is_explicitly_unavailable(test_db: sqlite3.Connection) -> None:
    res = build_company_payload(test_db, tenant_id="default", ticker="MISSING")
    assert res["status"] == "unavailable"
    assert res["profile_loaded"] is False
    assert res["fundamentals_loaded"] is False
    assert res["sector"] is None
    assert res["industry"] is None


def test_available_fundamentals_marks_company_available(test_db: sqlite3.Connection) -> None:
    test_db.execute(
        "INSERT INTO fundamentals_snapshot (tenant_id, ticker, as_of_date, sector, industry) VALUES ('default', 'AAPL', '2026-11-20', 'Technology', 'Consumer Electronics')"
    )
    test_db.commit()
    res = build_company_payload(test_db, tenant_id="default", ticker="AAPL")
    assert res["status"] == "available"
    assert res["fundamentals_loaded"] is True
    assert res["sector"] == "Technology"
    assert res["industry"] == "Consumer Electronics"


def test_missing_consensus_signals_returns_unavailable(test_db: sqlite3.Connection) -> None:
    res = get_consensus_signals(test_db, tenant_id="default", limit=10, min_p_final=None, ticker="NVDA")
    assert res["status"] == "unavailable"
    assert res["count"] == 0
    assert res["signals"] == []


def test_available_consensus_signals_returns_available(test_db: sqlite3.Connection) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    test_db.execute(
        "INSERT INTO consensus_signals (id, tenant_id, ticker, p_final, score, created_at) VALUES ('cs-1', 'default', 'NVDA', 0.85, 1.2, ?)",
        (now_iso,),
    )
    test_db.commit()
    res = get_consensus_signals(test_db, tenant_id="default", limit=10, min_p_final=None, ticker="NVDA")
    assert res["status"] == "available"
    assert res["count"] == 1
    assert res["signals"][0]["ticker"] == "NVDA"
    assert res["signals"][0]["pFinal"] == 0.85


def test_data_health_compact_reports_all_datasets(test_db: sqlite3.Connection) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    test_db.execute(
        "INSERT INTO consensus_signals (id, tenant_id, ticker, p_final, score, created_at) VALUES ('cs-1', 'default', 'AAPL', 0.8, 1.0, ?)",
        (now_iso,),
    )
    test_db.execute("INSERT INTO strategies (id, tenant_id, name, active) VALUES ('s1', 'default', 'Trend', 1)")
    test_db.execute("INSERT INTO ml_learning_rows (id, tenant_id, created_at) VALUES ('m1', 'default', ?)", (now_iso,))
    test_db.execute("INSERT INTO raw_events (id, tenant_id, timestamp) VALUES ('e1', 'default', ?)", (now_iso,))
    test_db.commit()

    health = build_data_health_compact(test_db, tenant_id="default")
    assert "prices" in health
    assert "fundamentals" in health
    assert "profiles" in health
    assert "predictions" in health
    assert "consensus" in health
    assert "ml_features" in health
    assert "strategy_stats" in health
    assert "events" in health

    assert health["strategy_stats"]["active"] == 1
    assert health["strategy_stats"]["status"] == "OK"
    assert health["ml_features"]["fresh"] == 1
    assert health["ml_features"]["status"] == "OK"
    assert health["events"]["fresh"] == 1
    assert health["events"]["status"] == "OK"


def test_fail_closed_contract_v2_suppresses_stale_or_missing_signals(test_db: sqlite3.Connection) -> None:
    """When any required dataset is stale or missing, recommendation contract v2 must fail-closed."""
    quality = recommendation_quality(test_db, tenant_id="default", ticker="XYZ")
    assert quality["actionable"] is False
    assert len(quality["suppressionReasons"]) > 0

    source = {
        "quality": quality,
        "research_action": "BUY",
        "preliminary_score": 0.85,
    }
    test_db.execute(
        """
        INSERT INTO house_recommendations (tenant_id, mode, ticker, action, score, horizon, as_of, source_json)
        VALUES ('default', 'swing', 'XYZ', 'BUY', 0.85, '5d', '2026-11-20T00:00:00Z', ?)
        """,
        (json.dumps(source),),
    )
    test_db.commit()

    row = test_db.execute("SELECT * FROM house_recommendations WHERE ticker = 'XYZ'").fetchone()
    item = _row_to_payload(row, mode="swing")

    assert item["actionable"] is False
    assert item["action"] == "WATCH"
    assert item["confidence"] is None
    assert item["entryZone"] is None
    assert item["risk"] is None
    assert item["expectedReturn"] is None
    assert item["guidanceAvailability"]["entryZone"]["status"] == "unavailable"
    assert "SYMBOL_PRICE_STALE_OR_MISSING" in item["suppressionReasons"] or "REFERENCE_QUOTE_STALE_OR_MISSING" in item["suppressionReasons"]
