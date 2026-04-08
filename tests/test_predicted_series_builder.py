from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.db.repository import AlphaRepository
from app.engine.predicted_series_builder import (
    BuildConfig,
    DirectionalDriftModel,
    FlatHoldModel,
    PredictedSeriesBuilder,
    Signal,
)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def test_flat_hold_model() -> None:
    model = FlatHoldModel()
    start = 123.45
    stamps = ["2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00"]
    out = model.build_curve(
        start_level=start,
        timestamps=stamps,
        signal=Signal(direction="up", confidence=0.9),
        features={"vol_scale": 0.02},
        config=BuildConfig(),
    )
    assert out == [start, start]


def test_directional_drift_monotonic() -> None:
    model = DirectionalDriftModel()
    start = 100.0
    stamps = ["t1", "t2", "t3", "t4"]
    out = model.build_curve(
        start_level=start,
        timestamps=stamps,
        signal=Signal(direction="up", confidence=0.8),
        features={"vol_scale": 0.02},
        config=BuildConfig(cap_daily_return=0.05),
    )
    assert len(out) == len(stamps)
    for a, b in zip(out, out[1:]):
        assert b >= a


def test_build_end_to_end_idempotent() -> None:
    repo = AlphaRepository(":memory:")
    try:
        builder = PredictedSeriesBuilder(repository=repo)

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

        # Seed consensus signal within ingress window.
        # (Use predictions fallback for signal resolution in this test.)
        repo.conn.execute(
            """
            INSERT INTO predictions
              (id, tenant_id, strategy_id, scored_event_id, ticker, timestamp, prediction, confidence, horizon,
               entry_price, mode, feature_snapshot_json, regime, trend_strength)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "p1",
                tenant_id,
                "technical-rsi-v1",
                "se1",
                ticker,
                _iso(ingress_end - timedelta(hours=1)),
                "up",
                0.8,
                "1d",
                100.0,
                "backfill",
                "{}",
                "NORMAL",
                None,
            ),
        )

        # Seed intraday bars only; builder should derive daily timestamps/vol/start_level from intraday when needed.
        repo.conn.execute(
            """
            INSERT INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (tenant_id, ticker, "1m", _iso(prediction_start - timedelta(days=1)), 99.0, 101.0, 98.0, 100.0, 1.0),
        )
        for i in range(0, 5):
            ts = prediction_start + timedelta(days=i)
            repo.conn.execute(
                """
                INSERT INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (tenant_id, ticker, "1m", _iso(ts), 100.0, 101.0, 99.0, 100.0 + i, 1.0),
            )

        cfg = BuildConfig(tenant_id=tenant_id, skip_if_exists=True)
        r1 = builder.build(run_id=run_id, ticker=ticker, config=cfg)
        assert not r1.skipped
        assert r1.points_written == 5

        r2 = builder.build(run_id=run_id, ticker=ticker, config=cfg)
        assert r2.skipped
        assert r2.skip_reason == "already_exists"
    finally:
        repo.close()
