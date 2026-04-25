from __future__ import annotations

from app.engine.prediction_sync import score_sync
from app.services.dashboard_service import DashboardService


def test_score_sync_basic_alignment() -> None:
    # Perfectly aligned up-up sequence.
    predicted = [100.0, 101.0, 102.0, 103.0]
    actual = [50.0, 50.5, 51.0, 51.5]
    s = score_sync(predicted, actual)
    assert s.forecast_days == 3
    assert 0.99 <= s.direction_hit_rate <= 1.0
    assert 0.99 <= s.sync_rate <= 1.0
    assert s.efficiency_rating > 0.5


def test_score_sync_misalignment_can_go_negative() -> None:
    # Opposite directions; magnitude errors should push the composite down.
    predicted = [100.0, 102.0, 104.0, 106.0]
    actual = [100.0, 98.0, 96.0, 94.0]
    s = score_sync(predicted, actual)
    assert s.forecast_days == 3
    assert s.direction_hit_rate <= 0.34
    assert s.efficiency_rating < 0.5


def test_dashboard_efficiency_rankings_smoke() -> None:
    svc = DashboardService(db_path=":memory:")
    try:
        tenant_id = "default"
        svc.store.conn.execute("DELETE FROM prediction_scores")
        svc.store.conn.execute(
            """
            INSERT INTO prediction_scores
              (id, tenant_id, run_id, strategy_id, strategy_version, ticker, timeframe, forecast_days,
               direction_hit_rate, sync_rate, total_return_actual, total_return_pred, total_return_error,
               magnitude_error, horizon_weight, efficiency_rating, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            ("s1", tenant_id, "run1", "stratA", "v1", "AAPL", "1d", 20, 0.6, 0.7, 0.1, 0.12, 0.02, 0.01, 0.8, 0.75, "2026-04-08T00:00:00Z"),
        )
        svc.store.conn.execute(
            """
            INSERT INTO prediction_scores
              (id, tenant_id, run_id, strategy_id, strategy_version, ticker, timeframe, forecast_days,
               direction_hit_rate, sync_rate, total_return_actual, total_return_pred, total_return_error,
               magnitude_error, horizon_weight, efficiency_rating, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            ("s2", tenant_id, "run1", "stratB", "v1", "AAPL", "1d", 20, 0.5, 0.55, 0.1, 0.09, 0.01, 0.02, 0.8, 0.62, "2026-04-08T00:00:00Z"),
        )

        ranked = svc.get_efficiency_rankings(tenant_id=tenant_id, ticker="AAPL", timeframe="1d", limit=10)
        assert ranked
        assert ranked[0].strategy_id == "stratA"
        assert ranked[0].avg_efficiency_rating >= ranked[1].avg_efficiency_rating

        champ = svc.get_efficiency_champion(
            tenant_id=tenant_id,
            ticker="AAPL",
            timeframe="1d",
            min_samples=1,
            min_total_forecast_days=1,
        )
        assert champ is not None
        assert champ.strategy_id == "stratA"
    finally:
        svc.close()
