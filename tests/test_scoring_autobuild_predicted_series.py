from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.db.repository import AlphaRepository
from app.engine.prediction_scoring_runner import PredictionScoringRunner


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def test_score_run_autobuild_predicted_series_smoke() -> None:
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

        repo.conn.execute(
            """
            INSERT INTO consensus_signals
              (id, tenant_id, ticker, regime, sentiment_strategy_id, quant_strategy_id,
               sentiment_score, quant_score, ws, wq, agreement_bonus, p_final, stability_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "cs1",
                tenant_id,
                ticker,
                "NORMAL",
                "sent",
                "quant",
                0.7,
                0.6,
                0.5,
                0.5,
                0.0,
                0.8,
                0.9,
                _iso(ingress_end - timedelta(hours=1)),
            ),
        )

        # Seed bars.
        repo.conn.execute(
            """
            INSERT INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (tenant_id, ticker, timeframe, _iso(prediction_start - timedelta(days=1)), 99.0, 101.0, 98.0, 100.0, 1.0),
        )
        for i in range(0, 5):
            ts = prediction_start + timedelta(days=i)
            repo.conn.execute(
                """
                INSERT INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (tenant_id, ticker, timeframe, _iso(ts), 100.0, 101.0, 99.0, 100.0 + i, 1.0),
            )

        runner = PredictionScoringRunner(repository=repo)
        rows = runner.score_run(
            run_id=run_id,
            tenant_id=tenant_id,
            timeframe=timeframe,
            materialize_actual=True,
            autobuild_predicted_series=True,
        )
        assert rows
        assert rows[0]["ticker"] == ticker
    finally:
        repo.close()

