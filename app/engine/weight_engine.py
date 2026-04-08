from __future__ import annotations
from typing import Dict
from app.engine.continuous_learning import StrategyPerformance

class WeightEngine:
    """
    Computes dynamic weights per strategy based on the ContinuousLearning output.
    Formula: W = confidence_weight * regime_match * recency_decay
    """
    def __init__(self, default_decay: float = 1.0):
        # In a fully stateful app, decay decreases strategy weights if they haven't traded recently
        self.default_decay = default_decay
        self.current_weights: Dict[str, float] = {}

    def compute_weight(self, perf: StrategyPerformance, current_regime: str) -> float:
        """Calculate the instantaneous weight of a strategy in the current regime."""
        regime_match = perf.regime_strength.get(current_regime, 0.0)
        recency_decay = self.default_decay 
        final_weight = perf.confidence_weight * regime_match * recency_decay
        return max(0.0, final_weight)

    def update_all(self, performances: Dict[str, StrategyPerformance], current_regime: str) -> Dict[str, float]:
        """Bulk update and return new normalized routing weights."""
        new_weights = {}
        for s_id, perf in performances.items():
            new_weights[s_id] = self.compute_weight(perf, current_regime)
        self.current_weights = new_weights
        return self.current_weights
