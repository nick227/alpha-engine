from __future__ import annotations

from datetime import datetime, timezone

from app.core.types import RawEvent, StrategyConfig
from app.core.repository import Repository
from app.engine.runner import run_pipeline


def test_run_pipeline_retry_does_not_create_duplicate_predictions(tmp_path) -> None:
    db_path = tmp_path / "alpha.db"

    strategy = StrategyConfig(
        id="quant-1",
        name="quant",
        version="v1",
        strategy_type="baseline_momentum",
        mode="paper",
        active=True,
        config={"min_short_trend": 0.0001, "horizon": "15m"},
    )

    evt = RawEvent(
        id="evt-1",
        timestamp=datetime.now(timezone.utc).replace(microsecond=0),
        source="test",
        text="Nvidia raises guidance after record datacenter demand",
        tickers=["NVDA"],
    )

    price_contexts = {
        evt.id: {
            "entry_price": 100.0,
            "short_trend": 0.02,
            "volume_ratio": 2.0,
            "return_5m": 0.01,
            "return_15m": 0.02,
            "future_return_15m": 0.02,
        }
    }

    # Run twice for the same raw_event_id to simulate a retry.
    run_pipeline([evt], price_contexts, persist=True, db_path=str(db_path), strategy_configs=[strategy], mode_override="live", evaluate_outcomes=False)
    run_pipeline([evt], price_contexts, persist=True, db_path=str(db_path), strategy_configs=[strategy], mode_override="live", evaluate_outcomes=False)

    repo = Repository(db_path=db_path)
    row = repo.conn.execute(
        """
        SELECT COUNT(*) as c
        FROM predictions p
        JOIN scored_events s
          ON s.tenant_id = p.tenant_id
         AND s.id = p.scored_event_id
        WHERE p.tenant_id = 'default'
          AND s.raw_event_id = ?
          AND p.strategy_id = ?
          AND p.horizon = '15m'
          AND p.mode = 'live'
        """,
        (evt.id, strategy.id),
    ).fetchone()
    assert row is not None
    assert int(row["c"]) == 1
    repo.close()

