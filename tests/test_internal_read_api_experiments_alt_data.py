from __future__ import annotations

import json

import pytest
from app.services.dashboard_service import DashboardService


def test_meta_ranker_alt_data_coverage_service() -> None:
    svc = DashboardService(db_path=":memory:")
    tenant = "default"
    c = svc.store.conn
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS alt_data_daily (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          as_of_date TEXT NOT NULL,
          symbol TEXT NOT NULL,
          source TEXT NOT NULL,
          feature_json TEXT NOT NULL,
          quality_score REAL NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS experiment_results (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          class_key TEXT NOT NULL,
          experiment_key TEXT NOT NULL,
          metadata_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    c.executemany(
        """
        INSERT INTO alt_data_daily
          (id, tenant_id, as_of_date, symbol, source, feature_json, quality_score, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("alt_1", tenant, "2026-04-24", "AAPL", "proxy_free", "{}", 1.0, "2026-04-24T00:00:00+00:00", "2026-04-24T00:00:00+00:00"),
            ("alt_2", tenant, "2026-04-24", "MSFT", "proxy_free", "{}", 0.5, "2026-04-24T00:00:00+00:00", "2026-04-24T00:00:00+00:00"),
            ("alt_3", tenant, "2026-04-23", "NVDA", "proxy_free", "{}", 0.75, "2026-04-23T00:00:00+00:00", "2026-04-23T00:00:00+00:00"),
        ],
    )
    run_meta = {
        "cohort": {"as_of_date": "2026-04-24"},
        "alt_data_ingest": {"enabled": True, "mode": "news+search", "source": "proxy_free", "written": 2, "requested_symbols": 3, "coverage": 2 / 3},
        "alt_data": {"features_with_alt_data": 2},
    }
    c.execute(
        """
        INSERT INTO experiment_results (
          id, tenant_id, run_id, class_key, experiment_key, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("res_1", tenant, "run_meta_1", "ml_model", "ml_meta_ranker_v1", json.dumps(run_meta, sort_keys=True), "2026-04-24T04:00:00+00:00"),
    )
    c.commit()

    body = svc.get_meta_ranker_alt_data_coverage()
    assert body["tenant_id"] == "default"
    assert len(body["rows"]) >= 2
    first = body["rows"][0]
    assert first["as_of_date"] == "2026-04-24"
    assert first["source"] == "proxy_free"
    assert first["symbols"] == 2
    assert first["avgQuality"] == pytest.approx(0.75, abs=1e-9)
    assert body["latest_challenger_run"]["run_id"] == "run_meta_1"
    assert body["runs"][0]["run_id"] == "run_meta_1"
    assert body["runs"][0]["as_of_date"] == "2026-04-24"
    svc.close()

