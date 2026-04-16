from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol


class SignalRepository(Protocol):
    """Interface for signal data repository."""
    
    def get_signals(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get signals in the specified date range.
        
        Args:
            start_date: Start date for signal window
            end_date: End date for signal window
            
        Returns:
            List of signal dictionaries with ts, symbol, direction, and metadata
        """
        ...

    def get_signal_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Get raw signal data for analysis.
        
        Args:
            start_date: Start date for signal window
            end_date: End date for signal window
            
        Returns:
            Dictionary of signal data for advanced analysis
        """
        ...


class PriceRepository(Protocol):
    """Interface for price data repository."""
    
    def get_price_data(self, start_date: datetime, end_date: datetime, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get price data for the specified date range and symbols.
        
        Args:
            start_date: Start date for price window
            end_date: End date for price window
            symbols: Optional list of symbols to get prices for
            
        Returns:
            List of price data dictionaries with ts, symbol, open, high, low, close, volume
        """
        ...

    def get_volatility_stats(self, window: int = 20) -> Dict[str, Any]:
        """Get volatility statistics for the portfolio universe.
        
        Args:
            window: Lookback window in days
            
        Returns:
            Dictionary with volatility stats for each symbol
        """
        ...

    def get_volume_profile(self) -> Dict[str, Any]:
        """Get volume profile across symbols.
        
        Returns:
            Dictionary with volume data for each symbol
        """
        ...


@dataclass(slots=True)
class Signal:
    """Standardized signal representation."""
    ts: datetime
    symbol: str
    direction: int  # -1, 0, or 1
    strategy: str
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert signal to dictionary representation."""
        return {
            "ts": self.ts,
            "symbol": self.symbol,
            "direction": self.direction,
            "strategy": self.strategy,
            "confidence": self.confidence,
            "metadata": self.metadata
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Signal:
        """Create signal from dictionary."""
        return cls(
            ts=datetime.fromisoformat(data["ts"]),
            symbol=data["symbol"],
            direction=data["direction"],
            strategy=data["strategy"],
            confidence=data.get("confidence", 1.0),
            metadata=data.get("metadata", {})
        )
        
    def __post_init__(self):
        """Validate signal after initialization."""
        if not isinstance(self.direction, int) or self.direction not in [-1, 0, 1]:
            raise ValueError("Direction must be -1, 0, or 1")
        
        if not 0 <= self.confidence <= 1:
            raise ValueError("Confidence must be between 0 and 1")
            
    def __str__(self) -> str:
        """String representation of signal."""
        return f"Signal(ts={self.ts}, symbol={self.symbol}, direction={self.direction}, strategy={self.strategy}, confidence={self.confidence})"