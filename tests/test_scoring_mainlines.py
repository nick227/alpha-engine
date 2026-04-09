from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.db.repository import AlphaRepository
from app.engine.prediction_scoring_runner import PredictionScoringRunner
from app.engine.prediction_sync import EfficiencyConfig, score_sync


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def test_score_sync_efficiency_matches_components() -> None:
    cfg = EfficiencyConfig()
    predicted = [100.0, 101.0, 100.0, 102.0]
    actual = [100.0, 100.5, 100.0, 101.5]
    s = score_sync(predicted, actual, config=cfg)

    w = cfg.weights
    daily_scale = float(cfg.scales.daily_return_scale)
    total_scale = daily_scale * (max(1, s.forecast_days) ** 0.5)
    magnitude_score = 1.0 - (s.magnitude_error / daily_scale) if daily_scale > 0 else 0.0
    total_return_score = 1.0 - (s.total_return_error / total_scale) if total_scale > 0 else 0.0

    expected = (
        w.sync * s.sync_rate
        + w.direction * s.direction_hit_rate
        + w.horizon * s.horizon_weight
        + w.magnitude * magnitude_score
        + w.total_return * total_return_score
    )
    assert abs(s.efficiency_rating - expected) < 1e-9


def test_scoring_runner_persists_and_ranks() -> None:
    repo = AlphaRepository(":memory:")
    try:
        tenant_id = "default"
        ticker = "AAPL"
        timeframe = "1d"

        ingress_start = datetime(2025, 12, 1, tzinfo=timezone.utc)
        ingress_end = datetime(2025, 12, 31, tzinfo=timezone.utc)
        prediction_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        prediction_end = datetime(2026, 1, 5, tzinfo=timezone.utc)

        run_id = repo.create_prediction_run(
            tenant_id=tenant_id,
            timeframe=timeframe,
            ingress_start=_iso(ingress_start),
            ingress_end=_iso(ingress_end),
            prediction_start=_iso(prediction_start),
            prediction_end=_iso(prediction_end),
        )

        # Predicted curve (consensus-v1) – simple increasing levels across 5 days.
        pred_points = []
        for i in range(0, 5):
            ts = prediction_start + timedelta(days=i)
            pred_points.append((_iso(ts), 100.0 + i))
        repo.upsert_predicted_series_points(
            run_id=run_id,
            strategy_id="consensus-v1",
            ticker=ticker,
            timeframe=timeframe,
            points=pred_points,
            tenant_id=tenant_id,
        )

        # Actual bars for same days.
        for i in range(0, 5):
            ts = prediction_start + timedelta(days=i)
            repo.conn.execute(
                """
                INSERT INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (tenant_id, ticker, timeframe, _iso(ts), 100.0, 101.0, 99.0, 100.0 + (i * 0.5), 1.0),
            )

        runner = PredictionScoringRunner(repository=repo)
        rows = runner.score_run(run_id=run_id, tenant_id=tenant_id, timeframe=timeframe, materialize_actual=True)
        assert len(rows) == 1
        assert rows[0]["strategy_id"] == "consensus-v1"

        ranked = repo.rank_strategies(tenant_id=tenant_id, ticker=ticker, timeframe=timeframe, limit=10)
        assert ranked
        assert ranked[0]["strategy_id"] == "consensus-v1"
    finally:
        repo.close()


def test_materialize_actual_series_from_intraday_for_daily() -> None:
    repo = AlphaRepository(":memory:")
    try:
        tenant_id = "default"
        ticker = "AAPL"

        ingress_start = datetime(2025, 12, 1, tzinfo=timezone.utc)
        ingress_end = datetime(2025, 12, 31, tzinfo=timezone.utc)
        prediction_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        prediction_end = datetime(2026, 1, 3, tzinfo=timezone.utc)

        run_id = repo.create_prediction_run(
            tenant_id=tenant_id,
            timeframe="1d",
            ingress_start=_iso(ingress_start),
            ingress_end=_iso(ingress_end),
            prediction_start=_iso(prediction_start),
            prediction_end=_iso(prediction_end),
        )

        # Seed intraday bars (1m) – 2 bars per day; last bar should be selected per day.
        for day in range(0, 3):
            d0 = prediction_start + timedelta(days=day)
            for minute in (0, 10):
                ts = d0 + timedelta(minutes=minute)
                repo.conn.execute(
                    """
                    INSERT INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (tenant_id, ticker, "1m", _iso(ts), 100.0, 101.0, 99.0, 100.0 + day + (minute / 100.0), 1.0),
                )

        runner = PredictionScoringRunner(repository=repo)
        inserted = runner.materialize_actual_series_from_price_bars(run_id=run_id, ticker=ticker, timeframe="1d", tenant_id=tenant_id)
        assert inserted == 3

        series = repo.fetch_actual_series(run_id=run_id, ticker=ticker, timeframe="1d", tenant_id=tenant_id)
        assert len(series) == 3
        # Each day should be the last bar of that day (minute 10).
        for (ts, val), day in zip(series, range(0, 3)):
            assert ts.endswith("+00:00") or ts.endswith("Z")
            assert abs(val - (100.0 + day + 0.10)) < 1e-9
    finally:
        repo.close()

