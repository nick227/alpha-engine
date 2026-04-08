from __future__ import annotations
from typing import Dict, List
from app.engine.strategy_registry import StrategyRegistry
from app.engine.continuous_learning import StrategyPerformance

class PromotionEngine:
    """
    Gates strategy promotions between Challenger and Champion status.
    Uses conservative thresholds to prevent thrashing.
    """
    def __init__(self, registry: StrategyRegistry):
        self.registry = registry
        
        # Invariants & Minimum Thresholds
        self.min_win_rate = 0.51
        self.min_alpha = 0.5
        self.min_stability = 0.40

    def evaluate_candidates(self, challenger_perfs: Dict[str, StrategyPerformance]):
        """Review challengers for promotion."""
        for s_id, perf in challenger_perfs.items():
            if self._meets_promotion_criteria(perf):
                self.registry.promote_to_champion(s_id)
                print(f"[PromotionEngine] Strategy {s_id} promoted to CHAMPION.")

    def review_champions(self, champion_perfs: Dict[str, StrategyPerformance]):
        """Review active champions for demotion."""
        for s_id, perf in champion_perfs.items():
            if self._fails_champion_criteria(perf):
                self.registry.demote_champion(s_id, kill=False)
                print(f"[PromotionEngine] Strategy {s_id} demoted to CHALLENGER.")

    def _meets_promotion_criteria(self, perf: StrategyPerformance) -> bool:
        if perf.win_rate < self.min_win_rate:
            return False
        if perf.alpha < self.min_alpha:
            return False
        if perf.stability < self.min_stability:
            return False
            
        # Example to ensure it works across multiple regimes reasonably well or heavily dominates one
        regime_scores = list(perf.regime_strength.values())
        if sum(regime_scores) == 0.0:
            return False
            
        return True

    def _fails_champion_criteria(self, perf: StrategyPerformance) -> bool:
        """
        Criteria for demotion should be MORE forgiving than promotion to prevent thrashing.
        E.g. A strategy must perform significantly worse than the promotion bar to be ousted.
        """
        if perf.win_rate < (self.min_win_rate - 0.05): # Drop below 46% win rate
            return True
        if perf.alpha < -1.0: # Significant negative alpha
            return True
        if perf.stability < (self.min_stability - 0.20): 
            return True
        return False
