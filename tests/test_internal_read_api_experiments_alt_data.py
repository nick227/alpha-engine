from __future__ import annotations

import json

import pytest
from app.services.dashboard_service import DashboardService


@pytest.fixture
def svc() -> DashboardService:
    svc = DashboardService(db_path=":memory:")
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
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_queue (
          tenant_id TEXT NOT NULL,
          as_of_date TEXT NOT NULL,
          symbol TEXT NOT NULL,
          source TEXT NOT NULL,
          priority INTEGER NOT NULL,
          status TEXT NOT NULL,
          metadata_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY (tenant_id, as_of_date, symbol, source)
        )
        """
    )
    c.commit()
    try:
        yield svc
    finally:
        svc.close()


def test_meta_ranker_alt_data_coverage_service(svc: DashboardService) -> None:
    tenant = "default"
    c = svc.store.conn
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


def test_meta_ranker_alt_data_coverage_filters(svc: DashboardService) -> None:
    c = svc.store.conn
    c.executemany(
        """
        INSERT INTO alt_data_daily
          (id, tenant_id, as_of_date, symbol, source, feature_json, quality_score, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("alt_f1", "default", "2026-04-22", "AMD", "proxy_free", "{}", 0.9, "2026-04-22T00:00:00+00:00", "2026-04-22T00:00:00+00:00"),
            ("alt_f2", "default", "2026-04-22", "TSLA", "news_api", "{}", 0.6, "2026-04-22T00:00:00+00:00", "2026-04-22T00:00:00+00:00"),
        ],
    )
    c.commit()

    filtered = svc.get_meta_ranker_alt_data_coverage(as_of_date="2026-04-22", source="news_api")
    assert filtered["as_of_date"] == "2026-04-22"
    assert filtered["source"] == "news_api"
    assert len(filtered["rows"]) == 1
    assert filtered["rows"][0]["source"] == "news_api"
    assert filtered["rows"][0]["symbols"] == 1


def test_meta_ranker_strategy_queue_share_service(svc: DashboardService) -> None:
    c = svc.store.conn
    c.executemany(
        """
        INSERT INTO prediction_queue
          (tenant_id, as_of_date, symbol, source, priority, status, metadata_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("default", "2026-04-24", "AAPL", "discovery", 100, "pending", '{"strategy":"silent_compounder","queue_path":"watchlist"}', "2026-04-24T00:00:00+00:00"),
            ("default", "2026-04-24", "MSFT", "discovery", 90, "pending", '{"primary_strategy":"silent_compounder","queue_path":"diversity_topup"}', "2026-04-24T00:00:00+00:00"),
            ("default", "2026-04-24", "NVDA", "discovery", 80, "pending", '{"strategy":"narrative_lag","queue_path":"watchlist"}', "2026-04-24T00:00:00+00:00"),
            ("default", "2026-04-24", "AMZN", "discovery", 70, "done", '{"strategy":"narrative_lag","queue_path":"watchlist"}', "2026-04-24T00:00:00+00:00"),
        ],
    )
    c.commit()

    body = svc.get_meta_ranker_strategy_queue_share(as_of_date="2026-04-24", status="pending")
    assert body["tenant_id"] == "default"
    assert body["as_of_date"] == "2026-04-24"
    assert body["status"] == "pending"
    assert body["total"] == 3
    assert body["rows"][0]["strategy"] == "silent_compounder"
    assert body["rows"][0]["count"] == 2
    assert body["rows"][0]["share"] == pytest.approx(2 / 3, abs=1e-9)
    assert body["rows"][0]["queue_paths"]["watchlist"] == 1
    assert body["rows"][0]["queue_paths"]["diversity_topup"] == 1


def test_meta_ranker_strategy_queue_share_defaults_to_latest_date(svc: DashboardService) -> None:
    c = svc.store.conn
    c.executemany(
        """
        INSERT INTO prediction_queue
          (tenant_id, as_of_date, symbol, source, priority, status, metadata_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("default", "2026-04-23", "IBM", "discovery", 20, "pending", "{}", "2026-04-23T00:00:00+00:00"),
            ("default", "2026-04-24", "META", "discovery", 30, "pending", '{"primary_strategy":"realness_repricer","queue_path":"watchlist"}', "2026-04-24T00:00:00+00:00"),
        ],
    )
    c.commit()

    body = svc.get_meta_ranker_strategy_queue_share()
    assert body["as_of_date"] == "2026-04-24"
    assert body["total"] == 1
    assert body["rows"][0]["strategy"] == "realness_repricer"

