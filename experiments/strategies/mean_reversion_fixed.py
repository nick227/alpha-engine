import pandas as pd
import numpy as np
from datetime import timedelta
from app.core.types import Signal, SignalDirection
from app.core.feature_engine import FeatureEngine

class MeanReversionStrategy:
    """Short-term mean reversion strategy for portfolio diversification."""
    
    def __init__(self, lookback: int = 5, threshold: float = 0.015):
        """Initialize strategy.
        
        Args:
            lookback: Lookback period for mean calculation
            threshold: Minimum deviation to generate signals
        """
        self.lookback = lookback
        self.threshold = threshold
        self.feature_engine = FeatureEngine()
    
    def generate_signals(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """Generate mean reversion signals.
        
        Args:
            price_data: DataFrame with price data
            
        Returns:
            DataFrame containing generated signals
        """
        # Calculate simple moving average
        sma = price_data.rolling(self.lookback).mean()
        
        # Calculate price deviation from average
        deviation = (price_data - sma) / sma
        
        # Initialize signal data
        signal_data = pd.DataFrame(index=price_data.index)
        
        # Generate signals for all columns in price data
        for column in price_data.columns:
            signal_data[column] = np.where(
                deviation[column] > self.threshold,
                SignalDirection.SHORT,
                np.where(deviation[column] < -self.threshold, 
                    SignalDirection.LONG, 
                    SignalDirection.NEUTRAL)
            )
        
        # Add strategy metadata
        signal_data['strategy'] = 'short_term_mean_reversion'
        
        return signal_data
    
    def get_signals(self, pricing: pd.DataFrame) -> list:
        """Get signals in standardized format.
        
        Args:
            pricing: DataFrame with price data
            
        Returns:
            List of Signal objects
        """
        # Generate raw signals
        signal_data = self.generate_signals(pricing)
        
        # Convert DataFrame to list of Signal objects
        signals = []
        for idx, row in signal_data.iterrows():
            for column in pricing.columns:
                if column in row and row[column] != SignalDirection.NEUTRAL:
                    signals.append(Signal(
                        ts=str(row.name),
                        symbol=column,
                        direction=int(row[column]),
                        strategy=row['strategy'],
                        confidence=self.calculate_confidence(
                            pricing[:(row.name + 1)],
                            column,
                            row[column]
                        )
                    ))
                    
        return signals
    
    def calculate_confidence(self, pricing: pd.DataFrame, symbol: str, direction: int) -> float:
        """Calculate confidence score for a signal.
        
        Args:
            pricing: DataFrame with price data up to current timestamp
            symbol: Symbol to calculate confidence for
            direction: Signal direction
            
        Returns:
            Confidence score (0-1)
        """
        # Get most recent deviation
        current_price = pricing[symbol].iloc[-1]
        price_window = pricing[symbol].iloc[-self.lookback-5:]
        
        # Calculate rolling mean and z-score
        rolling_mean = price_window.mean()
        rolling_std = price_window.std()
        z_score = abs((current_price - rolling_mean) / rolling_std)
        
        # Confidence increases with z-score
        confidence_base = min(1.0, z_score / 2)  # Cap at z-score 2
        
        return confidence_base