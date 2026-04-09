from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class EfficiencyChampionDecision:
    action: Literal["promote", "keep", "skip"]
    reason: str
    delta: float | None = None


def decide_efficiency_champion(
    *,
    incumbent: dict[str, Any] | None,
    challenger: dict[str, Any] | None,
    min_efficiency: float = 0.60,
    min_delta_vs_incumbent: float = 0.05,
) -> EfficiencyChampionDecision:
    """
    Decide whether to switch a context's efficiency champion.

    - If there is no incumbent, promote the challenger.
    - If the challenger is not better than incumbent, keep incumbent.
    - If the incumbent is already "good enough" (>= min_efficiency), require a
      meaningful delta to switch to avoid churn.
    """
    if not challenger:
        return EfficiencyChampionDecision(action="skip", reason="no_challenger")

    if not incumbent:
        return EfficiencyChampionDecision(action="promote", reason="no_incumbent", delta=None)

    inc_id = str(incumbent.get("strategy_id") or "")
    ch_id = str(challenger.get("strategy_id") or "")
    if inc_id and ch_id and inc_id == ch_id:
        return EfficiencyChampionDecision(action="keep", reason="same_strategy", delta=0.0)

    inc_eff = float(incumbent.get("avg_efficiency_rating") or 0.0)
    ch_eff = float(challenger.get("avg_efficiency_rating") or 0.0)
    delta = ch_eff - inc_eff

    if delta <= 0:
        return EfficiencyChampionDecision(action="keep", reason="not_better", delta=delta)

    if inc_eff >= float(min_efficiency) and delta < float(min_delta_vs_incumbent):
        return EfficiencyChampionDecision(action="keep", reason="delta_below_threshold", delta=delta)

    return EfficiencyChampionDecision(action="promote", reason="better_challenger", delta=delta)

