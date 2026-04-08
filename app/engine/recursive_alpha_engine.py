from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.engine.weight_engine import derive_track_weights
from app.engine.promotion_gate import passes_forward_gate
from app.engine.reaper_engine import should_reap


@dataclass
class TrackChampion:
    strategy_id: str
    track: str
    score: float
    stability: float
    regime_focus: str | None = None


class RecursiveAlphaEngine:
    """Top-level orchestration scaffold for the v3.0 Recursive Alpha Engine."""

    def build_consensus_signal(
        self,
        sentiment_score: float,
        quant_score: float,
        sentiment_accuracy: float,
        quant_accuracy: float,
        sentiment_stability: float,
        quant_stability: float,
        agreement_bonus: float = 0.0,
    ) -> dict[str, float]:
        weights = derive_track_weights(
            sentiment_accuracy=sentiment_accuracy,
            quant_accuracy=quant_accuracy,
            sentiment_stability=sentiment_stability,
            quant_stability=quant_stability,
        )
        p_final = (
            weights["ws"] * sentiment_score
            + weights["wq"] * quant_score
            + agreement_bonus
        )
        return {
            "ws": weights["ws"],
            "wq": weights["wq"],
            "sentiment_score": round(sentiment_score, 4),
            "quant_score": round(quant_score, 4),
            "agreement_bonus": round(agreement_bonus, 4),
            "p_final": round(p_final, 4),
        }

    def choose_track_champions(
        self,
        sentiment_candidates: list[dict[str, Any]],
        quant_candidates: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any] | None]:
        def winner(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
            if not rows:
                return None
            return sorted(
                rows,
                key=lambda r: (r.get("stability_score", 0.0), r.get("live_score", 0.0)),
                reverse=True,
            )[0]

        return {
            "sentiment": winner(sentiment_candidates),
            "quant": winner(quant_candidates),
        }

    def evaluate_candidate_promotion(self, candidate: dict[str, Any], parent: dict[str, Any]) -> dict[str, Any]:
        passed, gate_logs = passes_forward_gate(candidate, parent)
        return {
            "candidate_id": candidate.get("id"),
            "parent_id": parent.get("id"),
            "passed": passed,
            "gate_logs": gate_logs,
            "next_status": "PROBATION" if passed else "ARCHIVED",
        }

    def evaluate_rollback(self, strategy_snapshot: dict[str, Any]) -> dict[str, Any]:
        reap, reason = should_reap(strategy_snapshot)
        return {
            "should_rollback": reap,
            "reason": reason,
            "next_status": "ROLLED_BACK" if reap else strategy_snapshot.get("status", "ACTIVE"),
        }
