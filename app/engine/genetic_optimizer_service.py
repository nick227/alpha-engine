from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from app.core.price_context import build_price_contexts_from_bars
from app.core.repository import Repository
from app.core.types import RawEvent, StrategyConfig
from app.engine.mutation_engine import mutate_strategy_config
from app.engine.promotion_gate import passes_forward_gate
from app.engine.runner import _strategy_track
from app.engine.strategy_factory import build_strategy_instance
from app.core.scoring import score_event
from app.core.mra import compute_mra
from app.engine.evaluate import evaluate_prediction


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

    @dataclass(frozen=True)
    class PrecomputedEvent:
        raw: RawEvent
        scored: Any
        mra: Any
        price_context: dict

    @dataclass(frozen=True)
    class WindowStats:
        total_predictions: int
        accuracy: float
        avg_return: float

    def precompute_windows(
        self,
        *,
        raw_events: list[RawEvent],
        bars: pd.DataFrame,
        forward_ratio: float = 0.3,
    ) -> tuple[list["GeneticOptimizerService.PrecomputedEvent"], list["GeneticOptimizerService.PrecomputedEvent"]]:
        train_events, forward_events = _split_events(raw_events, forward_ratio=forward_ratio)

        # Prepare bars timestamp once; contexts assume sorted bars.
        bars_prepared = bars
        if "timestamp" in bars_prepared.columns:
            ts = bars_prepared["timestamp"]
            if not pd.api.types.is_datetime64_any_dtype(ts):
                bars_prepared = bars_prepared.copy(deep=False)
                bars_prepared["timestamp"] = pd.to_datetime(ts, utc=True)
            else:
                tz = getattr(ts.dtype, "tz", None)
                if tz is None or str(tz) != "UTC":
                    bars_prepared = bars_prepared.copy(deep=False)
                    bars_prepared["timestamp"] = pd.to_datetime(ts, utc=True)

        all_events = list(train_events) + list(forward_events)
        ctx_all = build_price_contexts_from_bars(raw_events=all_events, bars=bars_prepared, bars_already_utc=True)

        def precompute(events: list[RawEvent]) -> list[GeneticOptimizerService.PrecomputedEvent]:
            out: list[GeneticOptimizerService.PrecomputedEvent] = []
            for evt in events:
                ctx = ctx_all.get(evt.id)
                if not ctx:
                    continue
                scored = score_event(evt)
                mra = compute_mra(scored, ctx)
                out.append(GeneticOptimizerService.PrecomputedEvent(raw=evt, scored=scored, mra=mra, price_context=ctx))
            return out

        return precompute(train_events), precompute(forward_events)

    def evaluate_strategy_on_window(
        self,
        *,
        strategy: StrategyConfig,
        window: list["GeneticOptimizerService.PrecomputedEvent"],
    ) -> "GeneticOptimizerService.WindowStats":
        instance = build_strategy_instance(strategy)
        if instance is None or not window:
            return GeneticOptimizerService.WindowStats(total_predictions=0, accuracy=0.0, avg_return=0.0)

        total = 0
        correct = 0
        ret_sum = 0.0
        for item in window:
            pred = instance.maybe_predict(item.scored, item.mra, item.price_context, item.raw.timestamp)
            if pred is None:
                continue
            total += 1
            out = evaluate_prediction(pred, item.price_context)
            if out.direction_correct:
                correct += 1
            ret_sum += float(out.return_pct)

        accuracy = (correct / total) if total else 0.0
        avg_return = (ret_sum / total) if total else 0.0
        return GeneticOptimizerService.WindowStats(total_predictions=total, accuracy=accuracy, avg_return=avg_return)

    def gate_decision(
        self,
        *,
        parent: StrategyConfig,
        candidate: StrategyConfig,
        parent_train: "GeneticOptimizerService.WindowStats",
        parent_forward: "GeneticOptimizerService.WindowStats",
        candidate_train: "GeneticOptimizerService.WindowStats",
        candidate_forward: "GeneticOptimizerService.WindowStats",
        min_stability_required: float = 0.6,
        min_sample_size: int = 5,
    ) -> tuple[bool, dict]:
        candidate_snapshot = {
            "id": candidate.id,
            "forward_alpha": candidate_forward.avg_return,
            "stability_score": _stability(
                StrategyWindowMetrics("c", "c", candidate.strategy_type, "ALL", candidate_train.total_predictions, candidate_train.accuracy, candidate_train.avg_return),
                StrategyWindowMetrics("c", "c", candidate.strategy_type, "ALL", candidate_forward.total_predictions, candidate_forward.accuracy, candidate_forward.avg_return),
            ),
            "sample_size": candidate_forward.total_predictions,
        }
        parent_snapshot = {
            "id": parent.id,
            "forward_alpha": parent_forward.avg_return,
            "stability_score": _stability(
                StrategyWindowMetrics("p", "p", parent.strategy_type, "ALL", parent_train.total_predictions, parent_train.accuracy, parent_train.avg_return),
                StrategyWindowMetrics("p", "p", parent.strategy_type, "ALL", parent_forward.total_predictions, parent_forward.accuracy, parent_forward.avg_return),
            ),
            "sample_size": parent_forward.total_predictions,
        }

        passed, gate_logs = passes_forward_gate(
            candidate_snapshot,
            parent_snapshot,
            min_stability_required=min_stability_required,
            min_sample_size=min_sample_size,
        )
        gate_logs.update(
            {
                "candidate_train_accuracy": candidate_train.accuracy,
                "candidate_forward_accuracy": candidate_forward.accuracy,
                "candidate_forward_avg_return": candidate_forward.avg_return,
                "parent_forward_avg_return": parent_forward.avg_return,
                "candidate_stability_score": candidate_snapshot["stability_score"],
                "parent_stability_score": parent_snapshot["stability_score"],
                "candidate_forward_sample_size": candidate_forward.total_predictions,
                "parent_forward_sample_size": parent_forward.total_predictions,
            }
        )
        return passed, gate_logs

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
        train_pre, fwd_pre = self.precompute_windows(raw_events=raw_events, bars=bars, forward_ratio=forward_ratio)

        parent_train = self.evaluate_strategy_on_window(strategy=parent, window=train_pre)
        parent_fwd = self.evaluate_strategy_on_window(strategy=parent, window=fwd_pre)
        cand_train = self.evaluate_strategy_on_window(strategy=candidate, window=train_pre)
        cand_fwd = self.evaluate_strategy_on_window(strategy=candidate, window=fwd_pre)

        return self.gate_decision(
            parent=parent,
            candidate=candidate,
            parent_train=parent_train,
            parent_forward=parent_fwd,
            candidate_train=cand_train,
            candidate_forward=cand_fwd,
            min_stability_required=min_stability_required,
            min_sample_size=min_sample_size,
        )

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
