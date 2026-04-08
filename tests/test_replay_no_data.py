from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.repository import Repository
from app.core.types import Prediction, StrategyConfig
from app.engine.replay_sqlite import SQLiteMetricsUpdater, SQLiteOutcomeWriter, SQLitePredictionRepository, SQLitePriceRepository
from app.engine.replay_worker import ReplayWorker


def test_replay_marks_no_data_after_grace_and_does_not_affect_performance(tmp_path) -> None:
    db_path = tmp_path / "alpha.db"
    repo = Repository(db_path=db_path)

    strat_cfg = StrategyConfig(
        id="strat-test",
        name="test",
        version="v1",
        strategy_type="baseline_momentum",
        mode="paper",
        active=True,
        config={"horizon": "1m"},
    )

    with repo.transaction():
        repo.persist_strategy(strat_cfg)

        pred_ts = (datetime.now(timezone.utc) - timedelta(hours=3)).replace(second=0, microsecond=0)
        pred = Prediction(
            id="pred-1",
            strategy_id=strat_cfg.id,
            scored_event_id="scored-1",
            ticker="NVDA",
            timestamp=pred_ts,
            prediction="up",
            confidence=0.6,
            horizon="1m",
            entry_price=100.0,
            mode="paper",
            feature_snapshot={"regime": "HIGH", "trend_strength": "UNKNOWN"},
        )
        repo.persist_prediction(pred)

        # Only a bar before expiry, none at/after expiry.
        before_expiry = (pred_ts + timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
        repo.upsert_price_bar(
            ticker="NVDA",
            timestamp=before_expiry,
            open_price=100.0,
            high=100.2,
            low=99.8,
            close=100.1,
            volume=1000.0,
        )

    predictions = SQLitePredictionRepository(repo)
    prices = SQLitePriceRepository(repo)
    outcomes = SQLiteOutcomeWriter(repo)
    metrics = SQLiteMetricsUpdater(repo)
    worker = ReplayWorker(predictions=predictions, prices=prices, outcomes=outcomes, metrics=metrics)

    # Now is far past expiry + grace (grace is >= 30m for 1m horizon).
    now = datetime.now(timezone.utc).replace(microsecond=0)
    scored = worker.run_once(now)
    assert scored == 0  # no scored horizon outcomes

    row = repo.conn.execute(
        "SELECT exit_reason, return_pct FROM prediction_outcomes WHERE tenant_id='default' AND prediction_id='pred-1'"
    ).fetchone()
    assert row is not None
    assert str(row["exit_reason"]) == "no_data"
    assert float(row["return_pct"]) == 0.0

    # Performance should not be created/updated because no_data outcomes are excluded.
    perf = repo.conn.execute(
        "SELECT prediction_count FROM strategy_performance WHERE tenant_id='default' AND strategy_id=? AND horizon='ALL'",
        (strat_cfg.id,),
    ).fetchone()
    assert perf is None
    repo.close()
