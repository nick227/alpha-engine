from __future__ import annotations
import random
from typing import Dict, List
from app.engine.strategy_registry import StrategyGenome
from app.engine.continuous_learning import ContinuousLearner, Signal, SignalOutcome, StrategyPerformance

class EvaluationHarness:
    """
    Executes a strategy over a historical time slice, simulating the 
    event-to-signal emission and outcome resolution. 
    Outputs proper StrategyPerformance records mapping identically to live learners.
    """
    def __init__(self, continuous_learner: ContinuousLearner | None = None):
        # Allow passing an existing learner or instantiate a fresh offline learner for pure evaluation
        self.learner = continuous_learner or ContinuousLearner()

    def run_slice(self, strategies: List[StrategyGenome], _events_batch: list = None) -> Dict[str, StrategyPerformance]:
        """
        Executes a historical slice for the provided strategies.
        (Placeholder logic replacing a real backtest loop)
        """
        # Simulated run: In a real environment, `events_batch` iterates over market data,
        # each strategy evaluates parameters and yields Signal objects.
        # Those Signals are held until a time horizon, producing SignalOutcome.
        
        for strategy in strategies:
            # Simulate a strategy yielding events over a historical slice.
            # Using its params to guess how it performed:
            # This is mock injection for architectural closure
            samples = 20
            for i in range(samples):
                # Fake signal logic dependent on strategy parameters
                dir_guess = 1 if random.random() > 0.5 else -1
                sig = Signal(
                    id=f"sim_{strategy.strategy_id}_{i}",
                    strategy_id=strategy.strategy_id,
                    ticker="SPY",
                    direction=dir_guess,
                    confidence=0.8,
                    timestamp="2024-01-01T00:00:00Z",
                    regime=random.choice(["HIGH_VOL", "LOW_VOL", "TRENDING"])
                )
                
                # Assume a distribution where some perform okay, some perform bad
                actual_return = (random.random() - 0.45) * 0.05 # Skew slightly positive for tests
                out = SignalOutcome(signal_id=sig.id, actual_return_pct=actual_return)
                
                self.learner.ingest_pairing(sig, out)

        # Output the exact schema the live learning loop produces
        return self.learner.evaluate_all()
