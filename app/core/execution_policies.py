import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Union


class ExecutionPolicy:
    """Base class for execution policies that modify signal behavior."""
    
    def __init__(self, base_signals: pd.DataFrame):
        """
        Initialize policy with base signals.
        
        Args:
            base_signals: DataFrame of base signals (ts, symbol, direction)
        """
        self.base_signals = base_signals
        
    def apply(self) -> pd.DataFrame:
        """Apply policy to base signals."""
        raise NotImplementedError("Subclasses must implement apply()")


class PositionClusteringConstraint(ExecutionPolicy):
    """Execution policy to limit position clustering."""
    
    def __init__(self, base_signals: pd.DataFrame, max_positions: int = 2):
        """
        Initialize constraint with position limit.
        
        Args:
            base_signals: DataFrame of base signals
            max_positions: Maximum number of concurrent positions
        """
        super().__init__(base_signals)
        self.max_positions = max(1, max_positions)  # Minimum of 1 position allowed
        
    def apply(self) -> pd.DataFrame:
        """
        Apply position clustering constraint.
        
        Returns:
            DataFrame with constrained signals
        """
        if self.max_positions >= len(self.base_signals.columns):
            return self.base_signals  # No constraint needed
            
        constrained_signals = self.base_signals.copy()
        
        # For each day, if positions exceed max, remove weakest signals
        for date in constrained_signals.index:
            # Count active positions
            active_positions = (constrained_signals.loc[date] != 0).sum()
            
            if active_positions > self.max_positions:
                # Get confidence scores for this date
                signals = []
                for symbol in constrained_signals.columns:
                    if constrained_signals.loc[date, symbol] != 0:
                        signals.append({
                            "symbol": symbol,
                            "direction": constrained_signals.loc[date, symbol]
                        })
                        
                # Sort by strength of signal (absolute value)
                signals.sort(key=lambda x: abs(x["direction"]), reverse=True)
                
                # Clear weakest signals to reach max_positions
                for signal in signals[self.max_positions:]:
                    constrained_signals.loc[date, signal["symbol"]] = 0
                    
        return constrained_signals


class PositionSizingConstraint(ExecutionPolicy):
    """Execution policy for dynamic position sizing."""
    
    def __init__(self, base_signals: pd.DataFrame, base_size: float = 1.0):
        """
        Initialize position sizing policy.
        
        Args:
            base_signals: DataFrame of base signals
            base_size: Base size per position (1.0 = 100% of capital)
        """
        super().__init__(base_signals)
        self.base_size = max(0.01, base_size)
        
    def apply(self, sizing_method: str = "equal") -> pd.DataFrame:
        """
        Apply position sizing to signals.
        
        Args:
            sizing_method: Method to use ('equal', 'confidence', 'volatility_inverse', 'risk_fixed')
            
        Returns:
            DataFrame with size-adjusted signals
        """
        num_positions = self.base_signals.shape[1]
        total_signals = (self.base_signals != 0).sum().sum()
        
        if total_signals == 0:
            return self.base_signals
            
        if sizing_method == "equal":
            # Equal-weighted size
            def _scale(v: float) -> float:
                if v == 0:
                    return 0.0
                return float(self.base_size) * (float(v) / abs(float(v)))

            return self.base_signals.map(_scale)
            
        elif sizing_method == "confidence":
            # Confidence-weighted size (using absolute signal strength as proxy)
            signal_strength = self.base_signals.abs()
            total_strength = signal_strength.sum().sum()
            
            # Create size-adjusted signals
            size_adjusted = pd.DataFrame(index=self.base_signals.index)
            
            for date in self.base_signals.index:
                active_strength = signal_strength.loc[date].sum()
                if active_strength > 0:
                    for symbol in self.base_signals.columns:
                        if self.base_signals.loc[date, symbol] != 0:
                            size = (signal_strength.loc[date, symbol] / active_strength) * self.base_size
                            size_sign = self.base_signals.loc[date, symbol] / abs(self.base_signals.loc[date, symbol])
                            size_adjusted.loc[date, symbol] = size * size_sign
                        else:
                            size_adjusted.loc[date, symbol] = 0
            
            return size_adjusted
            
        elif sizing_method == "volatility_inverse":
            # Inverse volatility weighting
            # Calculate rolling volatility (20-day)
            volatility = self.base_signals.pct_change().rolling(20).std() * np.sqrt(126)  # Annualized
            
            # Avoid division by zero by filling with average volatility
            avg_volatility = volatility.mean().mean()
            volatility = volatility.replace(0, avg_volatility)
            
            # Calculate inverse volatility weights
            inv_volatility = 1 / volatility
            for date in self.base_signals.index:
                active_volatility = inv_volatility.loc[date]
                total_volatility = active_volatility.sum()
                
                for symbol in self.base_signals.columns:
                    if self.base_signals.loc[date, symbol] != 0:
                        weight = active_volatility[symbol] / total_volatility
                        constrained_signals.loc[date, symbol] = self.base_size * weight
                        
            return constrained_signals
            
        elif sizing_method == "risk_fixed":
            # Fixed risk per position with confidence weighting
            risk_per_position = 0.02  # 2% risk per position
            risk_multiplier = 1.5  # Multiplier for high conviction signals
            
            # This would integrate with volatility for risk-based sizing
            def _risk_scale(v: float) -> float:
                if v == 0:
                    return 0.0
                return float(risk_per_position) * (float(v) / abs(float(v)))

            return self.base_signals.map(_risk_scale)
            
        else:
            raise ValueError(f"Unknown sizing method: {sizing_method}")
    
    def get_risk_allocation(self) -> Dict[str, float]:
        """Get risk allocation metrics for current policy."""
        active_signals = self.base_signals[self.base_signals != 0]
        
        # Count of positions by instrument
        position_counts = active_signals.notna().sum()
        
        # Signal strength distribution
        strength_distribution = {
            "mean": active_signals.abs().mean(),
            "std": active_signals.abs().std(),
            "histogram": np.histogram(active_signals, bins=5)
        }
        
        # Signal frequency
        signal_frequency = active_signals.notna().mean() * 100
        
        return {
            "position_limit": self.max_positions,
            "average_positions": (self.base_signals != 0).sum().mean(),
            "strength_distribution": strength_distribution,
            "signal_frequency": signal_frequency
        }


class CapitalConstraint(ExecutionPolicy):
    """Execution policy for overall capital risk control."""
    
    def __init__(self, base_signals: pd.DataFrame, max_capital: float = 1.0):
        """
        Initialize with position limit.
        
        Args:
            base_signals: DataFrame of base signals
            max_capital: Maximum total capital to be used (1.0 = full capital)
        """
        super().__init__(base_signals)
        self.max_capital = max(0.05, max_capital)  # Minimum 5% of capital
        
    def apply(self, max_sector_exposure: float = 1.0) -> pd.DataFrame:
        """
        Apply capital constraints to signals.
        
        Args:
            max_sector_exposure: Maximum capital per sector
        """
        if self.max_capital >= 1.0 and max_sector_exposure >= 1.0:
            return self.base_signals  # No constraint needed
            
        # This would integrate with sector information to apply sector limits
        return self.base_signals  # Currently just returns base signals
    
    def get_risk_parameters(self) -> Dict[str, float]:
        """Get risk parameters for this policy."""
        return {
            "max_capital": self.max_capital,
            "position_size": 0,  # Would calculate based on max_capital
            "sector_exposure": 0  # Would calculate based on sector limits
        }

def apply_execution_policy(
    signals: pd.DataFrame,
    policy_type: str = "clustering",
    **kwargs
) -> pd.DataFrame:
    """
    Apply execution policy to signals.
    
    Args:
        signals: Base signals in matrix format
        policy_type: Type of policy ('clustering', 'sizing', 'capital')
    
    Returns:
        DataFrame with policy-applied signals
    """
    if policy_type == "clustering":
        policy = PositionClusteringConstraint(signals, kwargs.get("max_positions", 2))
        return policy.apply()
    elif policy_type == "sizing":
        policy = PositionSizingConstraint(signals, kwargs.get("base_size", 1.0))
        return policy.apply(sizing_method=kwargs.get("sizing_method", "equal"))
    elif policy_type == "capital":
        policy = CapitalConstraint(signals, kwargs.get("max_capital", 1.0))
        return policy.apply(max_sector_exposure=kwargs.get("max_sector_exposure", 1.0))
    else:
        raise ValueError(f"Unknown policy type: {policy_type}")

# Example:
# signals = strategy.get_signals(pricing)
# constrained_signals = apply_execution_policy(signals, "clustering", max_positions=2)
