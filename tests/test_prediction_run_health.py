"""Run health uses recorded facts: prediction_runs stores data windows, not processing times."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from app.db.repository import AlphaRepository
from app.internal_read_v1.intelligence_read import get_prediction_run_latest


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _run(tmp_path, *, created: datetime, ingress_days: int = 30, horizon_days: int = 30, cutoff_lag_min: int = 0):
    repo = AlphaRepository(db_path=str(tmp_path / "a.db"))
    ingress_end = created - timedelta(minutes=cutoff_lag_min)
    repo.conn.execute(
        "INSERT INTO prediction_runs (id, tenant_id, ingress_start, ingress_end, prediction_start, prediction_end, timeframe, regime, created_at)"
        " VALUES ('r1', 'default', ?, ?, ?, ?, '1d', NULL, ?)",
        (_iso(ingress_end - timedelta(days=ingress_days)), _iso(ingress_end), _iso(created), _iso(created + timedelta(days=horizon_days)),
         created.strftime("%Y-%m-%d %H:%M:%S")),
    )
    repo.conn.commit()
    repo.conn.row_factory = sqlite3.Row
    return get_prediction_run_latest(repo.conn, tenant_id="default", timeframe=None)


def test_long_lookback_and_horizon_are_not_latency(tmp_path) -> None:
    out = _run(tmp_path, created=datetime.now(timezone.utc) - timedelta(minutes=5))
    assert out["ingestLatencySec"] == 0
    assert out["predictLatencySec"] is None
    assert "INGRESS_LATENCY_HIGH" not in out["degradedReasons"]
    assert "PREDICTION_LATENCY_HIGH" not in out["degradedReasons"]
    assert out["stalenessMinutes"] <= 6
    assert "STALE_PREDICTION_RUN" not in out["degradedReasons"]


def test_old_run_is_stale_even_though_its_forecast_horizon_is_in_the_future(tmp_path) -> None:
    out = _run(tmp_path, created=datetime.now(timezone.utc) - timedelta(days=2, hours=8))
    assert out["stalenessMinutes"] >= 2 * 1440
    assert "STALE_PREDICTION_RUN" in out["degradedReasons"]
    assert out["runStatus"] != "HEALTHY"


def test_slow_publish_after_data_cutoff_is_ingest_latency(tmp_path) -> None:
    out = _run(tmp_path, created=datetime.now(timezone.utc) - timedelta(minutes=1), cutoff_lag_min=45)
    assert out["ingestLatencySec"] == 45 * 60
    assert "INGRESS_LATENCY_HIGH" in out["degradedReasons"]
