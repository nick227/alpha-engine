from __future__ import annotations

from .mutation_engine import MutationEngine
from .promotion_gate import passes_forward_gate
from .tournament_engine import choose_winner


class OptimizerService:
    """Mutation -> tournament -> gate -> candidate lifecycle scaffold."""

    def __init__(self) -> None:
        self.mutator = MutationEngine()

    def propose_candidates(self, parent: dict, max_children: int = 10) -> list[dict]:
        return self.mutator.mutate(parent, max_children=max_children)

    def run_tournament(self, candidate_results: list[dict], metric: str = "stability_score") -> dict | None:
        return choose_winner(candidate_results, metric=metric)

    def evaluate_for_promotion(self, candidate: dict, parent: dict) -> dict:
        passed, gate_logs = passes_forward_gate(candidate, parent)
        return {
            "candidate_id": candidate.get("id"),
            "parent_id": parent.get("id"),
            "passed": passed,
            "gate_logs": gate_logs,
            "next_status": "PROBATION" if passed else "ARCHIVED",
        }
