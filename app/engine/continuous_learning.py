from __future__ import annotations
from typing import Dict, List
from pydantic import BaseModel
import numpy as np

# --- Data Models ---
class Signal(BaseModel):
    id: str  # Added ID to link outcome
    strategy_id: str
    ticker: str
    direction: int   # 1 (up) or -1 (down), could also be 0 (flat) 
    confidence: float
    timestamp: str
    regime: str = "UNKNOWN"

class SignalOutcome(BaseModel):
    signal_id: str
    actual_return_pct: float

class StrategyPerformance(BaseModel):
    strategy_id: str
    win_rate: float
    alpha: float
    stability: float
    regime_strength: Dict[str, float]
    confidence_weight: float


# --- Continuous Learning Engine ---
class ContinuousLearner:
    def __init__(self):
        # State tracking: strategy_id -> list of (Signal, SignalOutcome)
        self.history: Dict[str, List[tuple[Signal, SignalOutcome]]] = {}

    def ingest_pairing(self, signal: Signal, outcome: SignalOutcome):
        """Register a resolved signal outcome."""
        if signal.strategy_id not in self.history:
            self.history[signal.strategy_id] = []
        self.history[signal.strategy_id].append((signal, outcome))

    def evaluate_strategy(self, strategy_id: str) -> StrategyPerformance | None:
        """Computes correctness, alpha, stability, and regime performance for a strategy."""
        records = self.history.get(strategy_id, [])
        if not records:
            return None

        correct_count = 0
        cumulative_alpha = 0.0
        
        # Stability tracking variables
        rolling_returns = []
        
        # Regime tracking variables
        regime_wins = {}
        regime_counts = {}

        for sig, out in records:
            # 1. Correctness
            is_correct = False
            # direction matches return sign (ignoring exactly 0 return for simplicity or deciding flat cases)
            if (sig.direction > 0 and out.actual_return_pct > 0) or \
               (sig.direction < 0 and out.actual_return_pct < 0):
                is_correct = True
                correct_count += 1
                
            # Treat exact 0 return as incorrect for standard long/short alpha hunting unless direction was flat (0).
            if sig.direction == 0 and out.actual_return_pct == 0.0:
                is_correct = True
                correct_count += 1

            # 2. Alpha Magnitude
            trade_alpha = sig.direction * (out.actual_return_pct * 100) # Percentage points
            cumulative_alpha += trade_alpha
            rolling_returns.append(trade_alpha)

            # 4. Regime Performance Tracking
            reg = sig.regime
            if reg not in regime_counts:
                regime_counts[reg] = 0
                regime_wins[reg] = 0
            
            regime_counts[reg] += 1
            if is_correct:
                regime_wins[reg] += 1

        total_trials = len(records)
        win_rate = correct_count / total_trials

        # 3. Stability
        if len(rolling_returns) > 1:
            std_dev = float(np.std(rolling_returns))
            mean_ret = float(np.mean(rolling_returns))
            stability = (mean_ret / std_dev) if std_dev > 0.0001 else 0.0
        else:
            stability = 0.0

        # Construct Regime Strength Profile
        regime_strength = {}
        for r, c in regime_counts.items():
            regime_strength[r] = regime_wins[r] / c if c > 0 else 0.0

        return StrategyPerformance(
            strategy_id=strategy_id,
            win_rate=win_rate,
            alpha=cumulative_alpha,
            stability=stability,
            regime_strength=regime_strength,
            confidence_weight=win_rate * max(0.1, min(1.0, stability))
        )

    def evaluate_all(self) -> Dict[str, StrategyPerformance]:
        results = {}
        for s_id in self.history:
            perf = self.evaluate_strategy(s_id)
            if perf:
                results[s_id] = perf
        return results
