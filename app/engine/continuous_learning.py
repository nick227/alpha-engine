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
    mode: str = "backtest"  # backtest | paper | live - keeps corpus clean

class SignalOutcome(BaseModel):
    signal_id: str
    actual_return_pct: float
    mode: str = "backtest"  # backtest | paper | live - source of outcome

class StrategyPerformance(BaseModel):
    strategy_id: str
    win_rate: float
    alpha: float
    stability: float
    regime_strength: Dict[str, float]
    confidence_weight: float
    mode: str = "backtest"  # backtest | paper | live - which corpus this belongs to
    sample_count: int = 0   # Number of outcomes in this performance calculation


# --- Continuous Learning Engine ---
class ContinuousLearner:
    """
    Mode-aware continuous learning engine.
    
    Separates learning by mode to keep corpus clean:
    - BACKTEST: learns from replay outcomes
    - PAPER: learns from executed trade outcomes  
    - LIVE: learns from broker-confirmed outcomes
    """
    
    def __init__(self):
        # State tracking: mode -> strategy_id -> list of (Signal, SignalOutcome)
        # This separation prevents cross-contamination between modes
        self.history: Dict[str, Dict[str, List[tuple[Signal, SignalOutcome]]]] = {
            "backtest": {},
            "paper": {},
            "live": {}
        }
        self.current_mode: str = "backtest"

    def ingest_pairing(self, signal: Signal, outcome: SignalOutcome, mode: str = None):
        """
        Register a resolved signal outcome.
        
        Args:
            signal: The signal that generated the prediction
            outcome: The actual outcome (return, correctness)
            mode: backtest | paper | live (defaults to signal.mode or current_mode)
        """
        # Determine mode - explicit > signal > current > default
        effective_mode = mode or signal.mode or self.current_mode or "backtest"
        
        # Validate mode
        if effective_mode not in self.history:
            raise ValueError(f"Invalid mode: {effective_mode}. Must be backtest|paper|live")
        
        # Ensure outcome has matching mode
        outcome.mode = effective_mode
        
        # Store in mode-specific history
        mode_history = self.history[effective_mode]
        if signal.strategy_id not in mode_history:
            mode_history[signal.strategy_id] = []
        mode_history[signal.strategy_id].append((signal, outcome))
        
        return effective_mode

    def evaluate_strategy(self, strategy_id: str, mode: str = None) -> StrategyPerformance | None:
        """
        Computes correctness, alpha, stability, and regime performance for a strategy.
        
        Args:
            strategy_id: Strategy to evaluate
            mode: backtest | paper | live (defaults to current_mode)
            
        Returns:
            StrategyPerformance for the specified mode
        """
        effective_mode = mode or self.current_mode or "backtest"
        
        if effective_mode not in self.history:
            return None
            
        records = self.history[effective_mode].get(strategy_id, [])
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
            confidence_weight=win_rate * max(0.1, min(1.0, stability)),
            mode=effective_mode,
            sample_count=total_trials
        )

    def evaluate_all(self, mode: str = None) -> Dict[str, StrategyPerformance]:
        """
        Evaluate all strategies for a given mode.
        
        Args:
            mode: backtest | paper | live (defaults to current_mode)
            
        Returns:
            Dict of strategy_id -> StrategyPerformance for specified mode
        """
        effective_mode = mode or self.current_mode or "backtest"
        
        if effective_mode not in self.history:
            return {}
            
        results = {}
        for s_id in self.history[effective_mode]:
            perf = self.evaluate_strategy(s_id, effective_mode)
            if perf:
                results[s_id] = perf
        return results
    
    def evaluate_all_by_horizon(self, mode: str = None) -> Dict[tuple[str, str, str], StrategyPerformance]:
        """
        Evaluate strategies grouped by (strategy_id, ticker, horizon) for horizon-scoped analytics.
        
        Args:
            mode: backtest | paper | live (defaults to current_mode)
        """
        effective_mode = mode or self.current_mode or "backtest"
        
        if effective_mode not in self.history:
            return {}
            
        results = {}
        for s_id in self.history[effective_mode]:
            perf = self.evaluate_strategy(s_id, effective_mode)
            if perf:
                # For now, use default ticker and horizon since ContinuousLearning doesn't track them
                # In a full implementation, this would extract from the signal data
                results[(s_id, "DEFAULT", "1d", effective_mode)] = perf
        return results
    
    def set_mode(self, mode: str):
        """Set the current learning mode (backtest | paper | live)."""
        if mode not in ("backtest", "paper", "live"):
            raise ValueError(f"Invalid mode: {mode}. Must be backtest|paper|live")
        self.current_mode = mode
    
    def get_mode_stats(self) -> Dict[str, Dict[str, int]]:
        """
        Get sample counts per mode and strategy.
        
        Returns:
            {mode: {strategy_id: sample_count}}
        """
        stats = {}
        for mode, mode_history in self.history.items():
            stats[mode] = {
                s_id: len(records) 
                for s_id, records in mode_history.items()
            }
        return stats
    
    def clear_mode(self, mode: str):
        """Clear all history for a specific mode."""
        if mode in self.history:
            self.history[mode] = {}
