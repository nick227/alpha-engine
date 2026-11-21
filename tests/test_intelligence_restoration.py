"""Tests for intelligence restoration: fallback fundamentals, consensus refresh,
strategy stats recomputation, and idempotent daily scheduler."""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.discovery.fundamentals_fmp import FundamentalsSnapshot, fetch_fallback_fundamentals_batch
from app.jobs.daily_pipeline_runner import record_stage_status
from app.jobs.pipeline_control import read_status
from app.jobs.scheduler import DailyScheduler
from dev_scripts.scripts.recompute_strategy_stats import recompute_strategy_statistics
from dev_scripts.scripts.refresh_consensus import refresh_consensus_for_universe


@pytest.fixture
def isolated_db(tmp_path: Path) -> Path:
    db = tmp_path / "test_intel.db"
    conn = sqlite3.connect(str(db))
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
        CREATE TABLE candidate_queue (
            tenant_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            status TEXT NOT NULL,
            first_seen_at TEXT,
            last_seen_at TEXT,
            signal_count INTEGER,
            metadata_json TEXT,
            PRIMARY KEY (tenant_id, ticker)
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
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE predictions (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            strategy_id TEXT,
            regime TEXT,
            horizon TEXT,
            timestamp TEXT
        );
        CREATE TABLE prediction_outcomes (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            prediction_id TEXT,
            return_pct REAL,
            direction_correct INTEGER,
            residual_alpha REAL,
            horizon TEXT,
            is_quarantined INTEGER DEFAULT 0
        );
        CREATE TABLE strategy_performance (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            horizon TEXT NOT NULL,
            prediction_count INTEGER NOT NULL,
            accuracy REAL NOT NULL,
            avg_return REAL NOT NULL,
            avg_residual_alpha REAL NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(tenant_id, strategy_id, horizon)
        );
        CREATE TABLE regime_performance (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            regime TEXT NOT NULL,
            prediction_count INTEGER NOT NULL,
            accuracy REAL NOT NULL,
            avg_return REAL NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(tenant_id, regime)
        );
        """
    )
    conn.commit()
    conn.close()
    return db


def test_fallback_fundamentals_batch_from_profile_cache(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir(parents=True)
    aapl_profile = profiles_dir / "AAPL.json"
    aapl_profile.write_text(
        json.dumps({
            "ticker": "AAPL",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "sharesOutstanding": 15000000000,
            "totalRevenue": 390000000000,
        }),
        encoding="utf-8",
    )

    snaps = fetch_fallback_fundamentals_batch(["AAPL"], profiles_dir=profiles_dir)
    assert len(snaps) == 1
    assert snaps[0].ticker == "AAPL"
    assert snaps[0].sector == "Technology"
    assert snaps[0].industry == "Consumer Electronics"
    assert snaps[0].shares_outstanding == 15000000000
    assert snaps[0].revenue_ttm == 390000000000


def test_recompute_strategy_statistics_ignores_quarantined_outcomes(isolated_db: Path) -> None:
    conn = sqlite3.connect(str(isolated_db))
    # Seed 2 valid outcomes and 1 quarantined outcome
    conn.execute(
        "INSERT INTO predictions (id, tenant_id, strategy_id, regime, horizon) VALUES ('p1', 'default', 'quant_v1', 'NORMAL', '1d')"
    )
    conn.execute(
        "INSERT INTO predictions (id, tenant_id, strategy_id, regime, horizon) VALUES ('p2', 'default', 'quant_v1', 'NORMAL', '1d')"
    )
    conn.execute(
        "INSERT INTO predictions (id, tenant_id, strategy_id, regime, horizon) VALUES ('p3', 'default', 'quant_v1', 'NORMAL', '1d')"
    )

    conn.execute(
        "INSERT INTO prediction_outcomes (id, tenant_id, prediction_id, return_pct, direction_correct, residual_alpha, horizon, is_quarantined) "
        "VALUES ('o1', 'default', 'p1', 0.05, 1, 0.02, '1d', 0)"
    )
    conn.execute(
        "INSERT INTO prediction_outcomes (id, tenant_id, prediction_id, return_pct, direction_correct, residual_alpha, horizon, is_quarantined) "
        "VALUES ('o2', 'default', 'p2', 0.03, 1, 0.01, '1d', 0)"
    )
    # Quarantined mock outcome: should be excluded from stats!
    conn.execute(
        "INSERT INTO prediction_outcomes (id, tenant_id, prediction_id, return_pct, direction_correct, residual_alpha, horizon, is_quarantined) "
        "VALUES ('o3', 'default', 'p3', -0.50, 0, -0.50, '1d', 1)"
    )
    conn.commit()
    conn.close()

    res = recompute_strategy_statistics(db_path=str(isolated_db), tenant_id="default")
    assert res["valid_outcomes_evaluated"] == 2

    conn = sqlite3.connect(str(isolated_db))
    conn.row_factory = sqlite3.Row
    perf = conn.execute(
        "SELECT * FROM strategy_performance WHERE strategy_id = 'quant_v1' AND horizon = '1d'"
    ).fetchone()
    assert perf is not None
    assert perf["prediction_count"] == 2
    assert perf["accuracy"] == 1.0
    assert round(perf["avg_return"], 2) == 0.04
    conn.close()


def test_refresh_consensus_signals(isolated_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Seed bars for AAPL
    conn = sqlite3.connect(str(isolated_db))
    now = datetime.now(timezone.utc)
    for i in range(25):
        ts = (now - timedelta(days=25 - i)).isoformat()
        close_price = 150.0 + (i * 0.5)
        conn.execute(
            "INSERT INTO price_bars (tenant_id, ticker, timeframe, timestamp, close) VALUES ('default', 'AAPL', '1d', ?, ?)",
            (ts, close_price),
        )
    conn.commit()
    conn.close()

    with patch("dev_scripts.scripts.refresh_consensus.get_active_universe_tickers", return_value=["AAPL"]):
        res = refresh_consensus_for_universe(db_path=str(isolated_db), tenant_id="default")
        assert res["synced"] == 1

    conn = sqlite3.connect(str(isolated_db))
    conn.row_factory = sqlite3.Row
    cs = conn.execute("SELECT * FROM consensus_signals WHERE ticker = 'AAPL'").fetchone()
    assert cs is not None
    assert cs["p_final"] > 0
    assert cs["regime"] in ("NORMAL", "LOW_VOLATILITY", "HIGH_VOLATILITY")
    conn.close()


def test_scheduler_respects_disabled_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "sched.db"
    monkeypatch.setenv("ALPHA_SCHEDULER_ENABLED", "false")
    sched = DailyScheduler(db_path=str(db))
    assert sched.is_enabled() is False
    st = sched.status()
    assert st["enabled"] is False


def test_scheduler_idempotent_skip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "sched2.db"
    status_file = tmp_path / "pipeline-status.json"
    today = date.today().isoformat()
    status_file.write_text(
        json.dumps({
            "state": "succeeded",
            "finished_at": f"{today}T21:05:00+00:00",
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("ALPHA_PIPELINE_STATUS_PATH", str(status_file))

    sched = DailyScheduler(db_path=str(db), utc_target_hour=21)
    now_evening = datetime(date.today().year, date.today().month, date.today().day, 22, 0, 0, tzinfo=timezone.utc)
    # Should not trigger again because today already succeeded!
    assert sched._should_trigger_today(now_evening) is False


def test_pipeline_runner_records_failed_stage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "pipeline_fail.db"
    status_file = tmp_path / "pipeline-status.json"
    monkeypatch.setenv("ALPHA_PIPELINE_STATUS_PATH", str(status_file))

    record_stage_status(str(db), "2_discovery_nightly", "failed", "Discovery nightly memory error")
    st = read_status(str(db))
    assert st is not None
    assert st.get("last_failed_stage") == "2_discovery_nightly"
    assert "memory error" in st.get("last_error", "").lower()
