from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

from app.core.repository import Repository
from app.core.types import StrategyConfig
from app.engine.champion_state import get_active_champion_id, refresh_active_champions_from_ranked, set_active_champion


def _seed_scored_outcomes(repo: Repository, n: int) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for _ in range(n):
        repo.execute(
            """
            INSERT INTO prediction_outcomes
              (id, tenant_id, prediction_id, exit_price, return_pct, direction_correct, max_runup, max_drawdown, evaluated_at, exit_reason, residual_alpha)
            VALUES (?, 'default', ?, 100.0, 0.0, 1, 0.0, 0.0, ?, 'horizon', 0.0)
            """,
            (str(uuid4()), f"pred-{uuid4()}", now),
        )


def test_champion_refresh_respects_cooldown_and_switch_threshold(tmp_path) -> None:
    db_path = tmp_path / "alpha.db"
    repo = Repository(db_path=db_path)

    incumbent = StrategyConfig(
        id="quant-inc",
        name="inc",
        version="v1",
        strategy_type="baseline_momentum",
        mode="paper",
        active=True,
        config={},
    )
    challenger = StrategyConfig(
        id="quant-chal",
        name="chal",
        version="v1",
        strategy_type="technical_vwap_reclaim",
        mode="paper",
        active=True,
        config={},
    )

    with repo.transaction():
        repo.persist_strategy(incumbent)
        repo.persist_strategy(challenger)

        # Make challenger win the ranked selection: similar stability, better avg_return.
        repo.execute(
            "INSERT OR REPLACE INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at) "
            "VALUES (?, 'default', ?, 0.0, 0.0, ?, ?)",
            ("stab_inc", incumbent.id, 0.80, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_stability (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy, stability_score, updated_at) "
            "VALUES (?, 'default', ?, 0.0, 0.0, ?, ?)",
            ("stab_chal", challenger.id, 0.81, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_performance "
            "(id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha, updated_at) "
            "VALUES (?, 'default', ?, 'ALL', 20, 0.0, ?, 0.0, ?)",
            ("perf_inc", incumbent.id, 0.001, repo.now_iso().replace("+00:00", "Z")),
        )
        repo.execute(
            "INSERT OR REPLACE INTO strategy_performance "
            "(id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha, updated_at) "
            "VALUES (?, 'default', ?, 'ALL', 20, 0.0, ?, 0.0, ?)",
            ("perf_chal", challenger.id, 0.005, repo.now_iso().replace("+00:00", "Z")),
        )

        # Set incumbent as active champion at scored_total=0.
        set_active_champion(repo, track="quant", strategy_id=incumbent.id, scored_total_at_switch=0, reason="test")

        # Not enough scored outcomes for cooldown (requires 50).
        _seed_scored_outcomes(repo, 10)

    refresh_active_champions_from_ranked(repo, min_predictions=5)
    assert get_active_champion_id(repo, track="quant") == incumbent.id

    # After enough additional outcomes, refresh can switch (challenger better on return, stability comparable).
    with repo.transaction():
        _seed_scored_outcomes(repo, 60)

    refresh_active_champions_from_ranked(repo, min_predictions=5)
    assert get_active_champion_id(repo, track="quant") == challenger.id

    payload = json.loads(repo.get_kv("champions:active:quant") or "{}")
    assert int(payload.get("scored_total_at_switch") or 0) >= 50
    repo.close()

