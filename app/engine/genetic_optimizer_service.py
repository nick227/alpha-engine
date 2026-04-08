from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import mean
from typing import Any

import pandas as pd

from app.core.price_context import build_price_contexts_from_bars
from app.core.repository import Repository
from app.core.types import RawEvent, StrategyConfig
from app.engine.mutation_engine import mutate_strategy_config
from app.engine.promotion_gate import passes_forward_gate
from app.engine.runner import run_pipeline, _strategy_track


@dataclass(frozen=True)
class StrategyWindowMetrics:
    strategy_id: str
    strategy_name: str
    strategy_type: str
    horizon: str
    total_predictions: int
    accuracy: float
    avg_return: float


@dataclass(frozen=True)
class CandidateScore:
    strategy_id: str
    forward_alpha: float
    stability_score: float
    sample_size: int


def _split_events(raw_events: list[RawEvent], forward_ratio: float = 0.3) -> tuple[list[RawEvent], list[RawEvent]]:
    rows = sorted(raw_events, key=lambda e: e.timestamp)
    if not rows:
        return [], []
    cut = max(1, int(len(rows) * (1.0 - forward_ratio)))
    return rows[:cut], rows[cut:]


def _metrics_from_summary(summary: list[dict[str, Any]]) -> dict[str, StrategyWindowMetrics]:
    out: dict[str, StrategyWindowMetrics] = {}
    for row in summary:
        strategy = str(row.get("strategy", ""))
        strategy_type = str(row.get("strategy_type", "unknown"))
        # StrategyConfig ids aren't in this table; we treat "strategy" as unique label for ranking.
        out[strategy] = StrategyWindowMetrics(
            strategy_id=strategy,
            strategy_name=strategy,
            strategy_type=strategy_type,
            horizon=str(row.get("horizon", "")),
            total_predictions=int(row.get("total_predictions", 0)),
            accuracy=float(row.get("accuracy", 0.0)),
            avg_return=float(row.get("avg_return", 0.0)),
        )
    return out


def _stability(train: StrategyWindowMetrics, forward: StrategyWindowMetrics) -> float:
    # Simple drift score in [0, 1] (higher is better).
    acc_drift = forward.accuracy - train.accuracy
    ret_drift = forward.avg_return - train.avg_return
    score = max(0.0, 1.0 - ((abs(acc_drift) + abs(ret_drift)) / 2.0))
    return float(round(score, 4))


class GeneticOptimizerService:
    """
    Minimal genetic optimizer:
    - choose a parent StrategyConfig
    - mutate config to create candidate StrategyConfigs
    - forward-slice gate compares candidate vs parent
    - persist lineage + lifecycle status into SQLite
    """

    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    def propose_candidates(self, parent: StrategyConfig, max_children: int = 10) -> list[StrategyConfig]:
        return mutate_strategy_config(parent, max_children=max_children)

    def evaluate_forward_gate(
        self,
        *,
        raw_events: list[RawEvent],
        bars: pd.DataFrame,
        parent: StrategyConfig,
        candidate: StrategyConfig,
        forward_ratio: float = 0.3,
        min_stability_required: float = 0.6,
        min_sample_size: int = 5,
    ) -> tuple[bool, dict]:
        train_events, forward_events = _split_events(raw_events, forward_ratio=forward_ratio)

        # Build price contexts from bars once per window.
        train_ctx = build_price_contexts_from_bars(raw_events=train_events, bars=bars)
        fwd_ctx = build_price_contexts_from_bars(raw_events=forward_events, bars=bars)

        parent_train = run_pipeline(train_events, train_ctx, persist=False, strategy_configs=[parent])
        parent_fwd = run_pipeline(forward_events, fwd_ctx, persist=False, strategy_configs=[parent])

        cand_train = run_pipeline(train_events, train_ctx, persist=False, strategy_configs=[candidate])
        cand_fwd = run_pipeline(forward_events, fwd_ctx, persist=False, strategy_configs=[candidate])

        p_train = _metrics_from_summary(parent_train["summary"])
        p_fwd = _metrics_from_summary(parent_fwd["summary"])
        c_train = _metrics_from_summary(cand_train["summary"])
        c_fwd = _metrics_from_summary(cand_fwd["summary"])

        # Each evaluation only has one strategy label.
        p_train_m = next(iter(p_train.values())) if p_train else StrategyWindowMetrics("parent", "parent", parent.strategy_type, "ALL", 0, 0.0, 0.0)
        p_fwd_m = next(iter(p_fwd.values())) if p_fwd else StrategyWindowMetrics("parent", "parent", parent.strategy_type, "ALL", 0, 0.0, 0.0)
        c_train_m = next(iter(c_train.values())) if c_train else StrategyWindowMetrics("candidate", "candidate", candidate.strategy_type, "ALL", 0, 0.0, 0.0)
        c_fwd_m = next(iter(c_fwd.values())) if c_fwd else StrategyWindowMetrics("candidate", "candidate", candidate.strategy_type, "ALL", 0, 0.0, 0.0)

        candidate_snapshot = {
            "id": candidate.id,
            "forward_alpha": c_fwd_m.avg_return,
            "stability_score": _stability(c_train_m, c_fwd_m),
            "sample_size": c_fwd_m.total_predictions,
        }
        parent_snapshot = {
            "id": parent.id,
            "forward_alpha": p_fwd_m.avg_return,
            "stability_score": _stability(p_train_m, p_fwd_m),
            "sample_size": p_fwd_m.total_predictions,
        }

        passed, gate_logs = passes_forward_gate(
            candidate_snapshot,
            parent_snapshot,
            min_stability_required=min_stability_required,
            min_sample_size=min_sample_size,
        )
        gate_logs.update(
            {
                "candidate_train_accuracy": c_train_m.accuracy,
                "candidate_forward_accuracy": c_fwd_m.accuracy,
                "candidate_forward_avg_return": c_fwd_m.avg_return,
                "parent_forward_avg_return": p_fwd_m.avg_return,
                "candidate_stability_score": candidate_snapshot["stability_score"],
                "parent_stability_score": parent_snapshot["stability_score"],
            }
        )
        return passed, gate_logs

    def persist_candidate(
        self,
        *,
        parent: StrategyConfig,
        candidate: StrategyConfig,
        status: str = "CANDIDATE",
    ) -> None:
        track = _strategy_track(candidate.strategy_type)
        self.repo.persist_strategy(candidate)
        self.repo.upsert_strategy_state(
            strategy_id=candidate.id,
            track=track,
            status=status,
            parent_id=parent.id,
            version=candidate.version,
            notes=candidate.name,
        )

    def record_gate_result(
        self,
        *,
        parent: StrategyConfig,
        candidate: StrategyConfig,
        passed: bool,
        gate_logs: dict,
    ) -> None:
        track = _strategy_track(candidate.strategy_type)
        action = "nominated" if passed else "archived"
        status = "PROBATION" if passed else "ARCHIVED"
        self.repo.upsert_strategy_state(
            strategy_id=candidate.id,
            track=track,
            status=status,
            parent_id=parent.id,
            version=candidate.version,
            notes=candidate.name,
        )
        self.repo.add_promotion_event(
            strategy_id=candidate.id,
            parent_id=parent.id,
            track=track,
            action=action,
            reason="forward_gate",
            gate_logs=gate_logs,
        )

