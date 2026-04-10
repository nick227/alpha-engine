from __future__ import annotations
from typing import Dict, List, Optional, Tuple
from app.engine.continuous_learning import StrategyPerformance

class WeightEngine:
    """
    Computes dynamic weights per strategy based on ContinuousLearning performance.
    
    Formula: W = confidence_weight * regime_match * recency_decay
    
    Supports per-ticker, per-horizon, per-regime, and per-mode weighting for adaptive
    consensus that trusts winning strategies more.
    
    Mode-aware: Separates weights for backtest/paper/live to prevent cross-contamination.
    """
    def __init__(self, default_decay: float = 1.0, min_weight: float = 0.05):
        self.default_decay = default_decay
        self.min_weight = min_weight  # Floor weight to prevent complete exclusion
        self.current_weights: Dict[str, float] = {}
        self.current_regime: str = "UNKNOWN"
        self.current_mode: str = "backtest"
        
        # Mode-separated weights: backtest | paper | live
        self.mode_weights: Dict[str, Dict[str, float]] = {
            "backtest": {},
            "paper": {},
            "live": {}
        }
        
    def compute_weight(
        self, 
        perf: StrategyPerformance, 
        current_regime: str,
        apply_normalization: bool = True
    ) -> float:
        """
        Calculate strategy weight with regime matching.
        
        Args:
            perf: Strategy performance metrics
            current_regime: Current market regime
            apply_normalization: Whether to apply sigmoid normalization
            
        Returns:
            Strategy weight (0.0 to 1.0+ before normalization)
        """
        # Base weight from win rate and stability
        base_weight = perf.confidence_weight
        
        # Regime match multiplier (how well strategy performs in current regime)
        regime_match = perf.regime_strength.get(current_regime, 0.0)
        if regime_match == 0.0 and current_regime != "UNKNOWN":
            # Fallback to average across known regimes if current not in history
            regime_match = sum(perf.regime_strength.values()) / max(len(perf.regime_strength), 1)
        
        # Recency decay (future: track last trade timestamp)
        recency_decay = self.default_decay
        
        # Combine factors
        raw_weight = base_weight * (0.5 + 0.5 * regime_match) * recency_decay
        
        # Apply sigmoid normalization for smoother weight distribution
        if apply_normalization:
            # Sigmoid centered at 0.5, scaled to 0-1 range
            import math
            normalized = 1.0 / (1.0 + math.exp(-5 * (raw_weight - 0.5)))
            final_weight = max(self.min_weight, normalized)
        else:
            final_weight = max(self.min_weight, raw_weight)
            
        return final_weight
    
    def update_all(
        self, 
        performances: Dict[str, StrategyPerformance], 
        current_regime: str,
        mode: str = None
    ) -> Dict[str, float]:
        """
        Bulk update and return normalized routing weights.
        
        Weights sum to 1.0 across all strategies for proper consensus.
        
        Args:
            performances: Strategy performance metrics
            current_regime: Current market regime
            mode: backtest | paper | live (defaults to current_mode)
        """
        effective_mode = mode or self.current_mode or "backtest"
        self.current_regime = current_regime
        self.current_mode = effective_mode
        
        # Compute raw weights
        raw_weights = {}
        for s_id, perf in performances.items():
            raw_weights[s_id] = self.compute_weight(perf, current_regime)
        
        # Normalize to sum to 1.0
        total_weight = sum(raw_weights.values())
        if total_weight > 0:
            normalized_weights = {
                s_id: w / total_weight 
                for s_id, w in raw_weights.items()
            }
        else:
            # Equal weights if no performance data
            equal = 1.0 / max(len(performances), 1)
            normalized_weights = {s_id: equal for s_id in performances}
        
        # Store in mode-specific weights
        self.mode_weights[effective_mode] = normalized_weights
        self.current_weights = normalized_weights
            
        return self.current_weights
    
    def update_by_group(
        self,
        performances: Dict[Tuple[str, str, str], StrategyPerformance],  # (strategy_id, ticker, horizon)
        current_regime: str
    ) -> Dict[Tuple[str, str, str], float]:
        """
        Update weights grouped by (strategy_id, ticker, horizon).
        
        Enables fine-grained weighting: "Strategy X works best on AAPL 1d".
        """
        self.current_regime = current_regime
        
        # Group by (ticker, horizon) for per-group normalization
        groups: Dict[Tuple[str, str], Dict[str, StrategyPerformance]] = {}
        for (s_id, ticker, horizon), perf in performances.items():
            group_key = (ticker, horizon)
            if group_key not in groups:
                groups[group_key] = {}
            groups[group_key][s_id] = perf
        
        # Compute normalized weights per group
        normalized_weights = {}
        for (ticker, horizon), group_perfs in groups.items():
            raw_weights = {
                s_id: self.compute_weight(perf, current_regime)
                for s_id, perf in group_perfs.items()
            }
            
            total = sum(raw_weights.values())
            if total > 0:
                for s_id, w in raw_weights.items():
                    normalized_weights[(s_id, ticker, horizon)] = w / total
            else:
                equal = 1.0 / max(len(group_perfs), 1)
                for s_id in group_perfs:
                    normalized_weights[(s_id, ticker, horizon)] = equal
        
        return normalized_weights
    
    def get_strategy_weight(self, strategy_id: str, mode: str = None) -> float:
        """
        Get current weight for a specific strategy.
        
        Args:
            strategy_id: Strategy identifier
            mode: backtest | paper | live (defaults to current_mode)
        """
        effective_mode = mode or self.current_mode or "backtest"
        mode_weights = self.mode_weights.get(effective_mode, {})
        return mode_weights.get(strategy_id, self.current_weights.get(strategy_id, self.min_weight))
    
    def set_mode(self, mode: str):
        """Set the current weight calculation mode (backtest | paper | live)."""
        if mode not in ("backtest", "paper", "live"):
            raise ValueError(f"Invalid mode: {mode}. Must be backtest|paper|live")
        self.current_mode = mode
        self.current_weights = self.mode_weights.get(mode, {})
    
    def get_mode_weights(self, mode: str = None) -> Dict[str, float]:
        """Get weights for a specific mode."""
        effective_mode = mode or self.current_mode or "backtest"
        return self.mode_weights.get(effective_mode, {})
    
    def apply_weights_to_signals(
        self,
        signals: List[Dict[str, any]],
        weights: Optional[Dict[str, float]] = None,
        mode: str = None
    ) -> List[Dict[str, any]]:
        """
        Apply weights to signal confidence values.
        
        Returns signals with weighted_confidence field added.
        
        Args:
            signals: List of signal dicts
            weights: Optional override weights (defaults to mode-specific weights)
            mode: backtest | paper | live (defaults to current_mode)
        """
        effective_mode = mode or self.current_mode or "backtest"
        
        if weights is None:
            weights = self.mode_weights.get(effective_mode, self.current_weights)
            
        weighted_signals = []
        for signal in signals:
            s_id = signal.get('strategy_id', '')
            weight = weights.get(s_id, self.min_weight)
            
            weighted_signal = dict(signal)
            weighted_signal['original_confidence'] = signal.get('confidence', 0.0)
            weighted_signal['strategy_weight'] = weight
            weighted_signal['weighted_confidence'] = signal.get('confidence', 0.0) * weight
            weighted_signal['confidence'] = weighted_signal['weighted_confidence']  # Primary field
            weighted_signal['mode'] = effective_mode
            weighted_signals.append(weighted_signal)
            
        return weighted_signals
