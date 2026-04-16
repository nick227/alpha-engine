import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from app.core.types import SignalDirection
from app.core.feature_engine import FeatureEngine
from experiments.strategies.baseline_momentum import BaselineMomentum


class BaselineMomentumV3A(BaselineMomentum):
    """Baseline Momentum Strategy with Delayed Entry (v3_a).
    
    Adds entry delay constraint to reduce signal noise and correlation stacking.
    Inherits core signal logic from v2 while improving execution robustness.
    """
    
    def __init__(self, lookback: int = 20, threshold: float = 0.02, entry_delay: int = 1):
        """
        Initialize strategy with delayed entry capability.
        
        Args:
            lookback: Lookback period for momentum calculation
            threshold: Minimum momentum threshold to generate signals
            entry_delay: Number of days to delay entry (1 = T+1)
        """
        super().__init__(lookback=lookback, threshold=threshold)
        self.entry_delay = max(0, entry_delay)  # Ensure non-negative
        self.feature_engine = FeatureEngine()
    
    def generate_raw_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate raw momentum signals without entry delay.
        
        Args:
            price_data: DataFrame with price data
            
        Returns:
            DataFrame containing raw momentum signals
        """
        # Call parent class's generate_signals method
        return super().generate_signals(price_data)
    
    def generate_signals(self, pricing: pd.DataFrame) -> pd.DataFrame:
        """
        Generate final signals with entry delay applied.
        
        Args:
            pricing: DataFrame with price data
            
        Returns:
            DataFrame containing final signals with delay
        """
        # Get raw signals from parent class
        raw_signals = self.generate_raw_signals(pricing)
        
        # Apply entry delay to all signals
        delayed_signals = raw_signals.shift(self.entry_delay)
        
        # Fill initial period with 0 (no position)
        for i in range(min(self.entry_delay, len(delayed_signals))):
            delayed_signals.iloc[i] = 0
            
        return delayed_signals
    
    def calculate_confidence(self, pricing: pd.DataFrame, symbol: str, direction: int) -> float:
        """
        Calculate confidence score for a signal with delay adjustment.
        
        Args:
            pricing: DataFrame with price data up to current timestamp
            symbol: Symbol to calculate confidence for
            direction: Signal direction
            
        Returns:
            Confidence score (0-1)
        """
        # Get parent confidence
        confidence = super().calculate_confidence(pricing, symbol, direction)
        
        # Adjust confidence based on entry delay
        if self.entry_delay > 1:
            # Reduce confidence linearly with delay
            delay_penalty = 1 / self.entry_delay
            confidence = max(0.1, confidence - delay_penalty)
            
        return confidence


class BaselineMomentumV3B(BaselineMomentum):
    """Baseline Momentum Strategy with Break Confirmation (v3_b).
    
    Adds break confirmation requirement to reduce false signals.
    """
    
    def __init__(self, lookback: int = 20, threshold: float = 0.02, confirm_window: int = 3):
        """
        Initialize strategy with break confirmation.
        
        Args:
            lookback: Lookback period for momentum calculation
            threshold: Minimum momentum threshold to generate signals
            confirm_window: Window for price confirmation
        """
        super().__init__(lookback=lookback, threshold=threshold)
        self.confirm_window = max(1, confirm_window)
        self.feature_engine = FeatureEngine()
    
    def generate_signals(self, pricing: pd.DataFrame) -> pd.DataFrame:
        """Generate signals with break confirmation filter."""
        # Generate base signals
        base_signals = super().generate_signals(pricing)
        
        # Initialize confirmed signals
        confirmed_signals = pd.DataFrame(index=base_signals.index)
        
        # Add break confirmation logic
        for column in pricing.columns:
            # Calculate rolling max/min for confirmation
            price_series = pricing[column]
            base_series = base_signals[column]
            
            for i in range(len(price_series)):
                if i < self.confirm_window or base_series.iloc[i] == SignalDirection.NEUTRAL:
                    continue
                    
                # Check if price confirmed direction
                confirmed = False
                
                if base_series.iloc[i] == SignalDirection.LONG:
                    # Check for continuation in lookback period
                    prev_high = price_series.iloc[i-self.confirm_window:i].max()
                    if price_series.iloc[i] > prev_high:
                        confirmed = True
                        
                elif base_series.iloc[i] == SignalDirection.SHORT:
                    # Check for continuation in lookback period
                    prev_low = price_series.iloc[i-self.confirm_window:i].min()
                    if price_series.iloc[i] < prev_low:
                        confirmed = True
                        
                # Set confirmed signal
                confirmed_signals.loc[confirmed_signals.index[i], column] = (
                    base_series.iloc[i] if confirmed else SignalDirection.NEUTRAL
                )
                
        return confirmed_signals
    

class BaselineMomentumV3C(BaselineMomentum):
    """Baseline Momentum Strategy with ATR filter (v3_c).
    
    Uses ATR to filter signals in high volatility environments.
    """
    
    def __init__(self, lookback: int = 20, threshold: float = 0.02, atr_window: int = 14):
        """
        Initialize strategy with ATR filtering.
        
        Args:
            lookback: Lookback period for momentum calculation
            threshold: Minimum momentum threshold to generate signals
            atr_window: Window for ATR calculation
        """
        super().__init__(lookback=lookback, threshold=threshold)
        self.atr_window = max(1, atr_window)
        self.feature_engine = FeatureEngine()
    
    def _calculate_atr(self, prices: pd.Series) -> pd.Series:
        """Calculate Average True Range for price series."""
        # Calculate ATR - simplified implementation
        high = pricing[column].rolling(self.atr_window).max()
        low = pricing[column].rolling(self.atr_window).min()
        
        tr = pd.DataFrame(index=prices.index)
        tr['tr1'] = high - low
        tr['tr2'] = abs(high - prices.shift())
        tr['tr3'] = abs(low - prices.shift())
        tr['tr'] = tr.max(axis=1)
        
        return tr['tr'].rolling(self.atr_window).mean()
    
    def generate_signals(self, pricing: pd.DataFrame) -> pd.DataFrame:
        """Generate signals with ATR volatility filtering."""
        # Generate base signals
        base_signals = super().generate_signals(pricing)
        
        # Initialize filtered signals
        filtered_signals = pd.DataFrame(index=base_signals.index)
        
        # Calculate ATR for each instrument
        for column in pricing.columns:
            price_series = pricing[column]
            base_series = base_signals[column]
            
            # Calculate ATR
            base_period = pricing.rolling(self.atr_window)
            high = base_period.max()
            low = base_period.min()
            
            tr = pd.DataFrame(index=pricing.index)
            tr['tr_high'] = high - low
            tr['tr_high_prev'] = abs(high - price_series.shift())
            tr['tr_low_prev'] = abs(low - price_series.shift())
            tr['tr'] = tr.max(['tr_high', 'tr_high_prev', 'tr_low_prev'])
            
            # Simple ATR approximation
            atr = tr['tr'].rolling(self.atr_window).mean()
            
            # Find high volatility days
            volatility_rank = atr.rank(pct=True)  # Normalized between 0-1
            high_volatility = volatility_rank >= 0.75  # Top quartile
            
            # Filter signals in high volatility
            for i in range(len(price_series)):
                if high_volatility.iloc[i]:
                    # Skip signals in high volatility
                    filtered_signals.loc[base_signals.index[i], column] = SignalDirection.NEUTRAL
                else:
                    # Keep signals in normal volatility
                    filtered_signals.loc[base_signals.index[i], column] = base_series.iloc[i]
                    
        return filtered_signals


# Create a versioned alias for deployment tracking
baseline_momentum_v3_a = BaselineMomentumV3A
baseline_momentum_v3_b = BaselineMomentumV3B
baseline_momentum_v3_c = BaselineMomentumV3C