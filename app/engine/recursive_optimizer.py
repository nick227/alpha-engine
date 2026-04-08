from __future__ import annotations

from dataclasses import dataclass, replace
from itertools import count
from typing import Iterable
import copy


@dataclass
class StrategyVariant:
    strategy_id: str
    parent_strategy_id: str | None
    version: str
    config: dict


class RecursiveOptimizer:
    def __init__(self) -> None:
        self._counter = count(1)

    def mutate(self, base_variant: StrategyVariant) -> list[StrategyVariant]:
        children: list[StrategyVariant] = []

        mutation_sets = [
            {"min_confidence": 0.55},
            {"min_confidence": 0.60},
            {"min_confidence": 0.65},
            {"agreement_bonus": 0.08},
            {"agreement_bonus": 0.10},
            {"agreement_bonus": 0.12},
            {"sentiment_weight_bias": 0.55},
            {"sentiment_weight_bias": 0.50},
            {"sentiment_weight_bias": 0.45},
            {"min_stability": 0.60},
        ]

        for patch in mutation_sets:
            child_config = copy.deepcopy(base_variant.config)
            child_config.update(patch)
            child_no = next(self._counter)
            children.append(
                StrategyVariant(
                    strategy_id=f"{base_variant.strategy_id}-child-{child_no}",
                    parent_strategy_id=base_variant.strategy_id,
                    version=f"{base_variant.version}.child{child_no}",
                    config=child_config,
                )
            )

        return children

    def tournament_rank_key(self, metrics: dict) -> tuple[float, float, float]:
        # higher is better
        return (
            metrics.get("sharpe_proxy", 0.0),
            metrics.get("accuracy", 0.0),
            metrics.get("avg_return", 0.0),
        )
