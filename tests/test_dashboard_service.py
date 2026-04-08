from __future__ import annotations

import os
from pathlib import Path

from app.ui.middle.dashboard_service import DashboardService


def test_dashboard_service_smoke() -> None:
    svc = DashboardService(db_path=":memory:")
    try:
        # Isolate Target Stocks config for this test.
        prev = os.environ.get("TARGET_STOCKS_CONFIG")
        tmp = Path.cwd() / ".pytest_cache" / "target_stocks_test.yaml"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text("- AAPL\n", encoding="utf-8")
        os.environ["TARGET_STOCKS_CONFIG"] = str(tmp)
        from app.core.target_stocks import get_target_stocks_registry
        get_target_stocks_registry.cache_clear()

        tenant_id = "default"

        # Minimal schema required by the read store
        svc.store.conn.execute(
            "CREATE TABLE strategies (id TEXT, tenant_id TEXT, strategy_type TEXT, version TEXT, mode TEXT, active INTEGER)"
        )
        svc.store.conn.execute(
            "CREATE TABLE strategy_performance (tenant_id TEXT, strategy_id TEXT, horizon TEXT, prediction_count INTEGER, accuracy REAL, avg_return REAL)"
        )
        svc.store.conn.execute(
            "CREATE TABLE strategy_stability (tenant_id TEXT, strategy_id TEXT, stability_score REAL)"
        )
        svc.store.conn.execute(
            "CREATE TABLE predictions (tenant_id TEXT, strategy_id TEXT, ticker TEXT, timestamp TEXT, prediction TEXT, confidence REAL, regime TEXT)"
        )
        svc.store.conn.execute(
            "CREATE TABLE loop_heartbeats (tenant_id TEXT, loop_type TEXT, status TEXT, notes TEXT, created_at TEXT)"
        )
        svc.store.conn.execute(
            "CREATE TABLE regime_performance (tenant_id TEXT, regime TEXT, accuracy REAL)"
        )

        # Strategies
        svc.store.conn.execute(
            "INSERT INTO strategies VALUES (?,?,?,?,?,?)",
            ("text-mra-v1", tenant_id, "text_mra", "v1", "backtest", 1),
        )
        svc.store.conn.execute(
            "INSERT INTO strategies VALUES (?,?,?,?,?,?)",
            ("technical-vwap-v1", tenant_id, "technical_vwap_reclaim", "v1", "backtest", 1),
        )
        svc.store.conn.execute(
            "INSERT INTO strategies VALUES (?,?,?,?,?,?)",
            ("consensus-v1", tenant_id, "consensus", "v1", "backtest", 1),
        )

        # Performance + stability
        svc.store.conn.execute(
            "INSERT INTO strategy_performance VALUES (?,?,?,?,?,?)",
            (tenant_id, "text-mra-v1", "ALL", 10, 0.7, 0.01),
        )
        svc.store.conn.execute(
            "INSERT INTO strategy_performance VALUES (?,?,?,?,?,?)",
            (tenant_id, "technical-vwap-v1", "ALL", 10, 0.6, 0.005),
        )
        svc.store.conn.execute(
            "INSERT INTO strategy_stability VALUES (?,?,?)",
            (tenant_id, "text-mra-v1", 0.8),
        )
        svc.store.conn.execute(
            "INSERT INTO strategy_stability VALUES (?,?,?)",
            (tenant_id, "technical-vwap-v1", 0.75),
        )

        # Regime strengths
        svc.store.conn.execute("INSERT INTO regime_performance VALUES (?,?,?)", (tenant_id, "HIGH", 0.9))
        svc.store.conn.execute("INSERT INTO regime_performance VALUES (?,?,?)", (tenant_id, "LOW", 0.4))

        # Consensus prediction (also drives tickers)
        svc.store.conn.execute(
            "INSERT INTO predictions VALUES (?,?,?,?,?,?,?)",
            (tenant_id, "consensus-v1", "AAPL", "2026-04-08T00:00:00Z", "up", 0.91, "HIGH"),
        )

        assert svc.list_tenants() == ["default"]
        assert svc.list_tickers(tenant_id=tenant_id) == ["AAPL"]

        champs = svc.get_champions(tenant_id=tenant_id, min_predictions=5)
        assert champs["sentiment"].track == "sentiment"
        assert champs["quant"].track == "quant"

        consensus = svc.get_latest_consensus(tenant_id=tenant_id, ticker="AAPL")
        assert consensus is not None
        assert consensus.confidence == 0.91
        assert consensus.active_regime == "HIGH"
        assert consensus.high_vol_strength == 0.9
        assert consensus.low_vol_strength == 0.4
    finally:
        if "prev" in locals():
            if prev is None:
                os.environ.pop("TARGET_STOCKS_CONFIG", None)
            else:
                os.environ["TARGET_STOCKS_CONFIG"] = prev
            from app.core.target_stocks import get_target_stocks_registry
            get_target_stocks_registry.cache_clear()
        svc.close()
