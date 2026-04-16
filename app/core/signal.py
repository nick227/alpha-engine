from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

SignalDirection = Literal[-1, 0, 1]
"""Type for signal directions:
- -1: Short/sell signal
-  0: Neutral/no signal
-  1: Long/buy signal
"""


@dataclass(slots=True)
class Signal:
    ts: datetime
    symbol: str
    direction: SignalDirection
    strategy: str
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def model_dump(self) -> Dict[str, Any]:
        """Convert signal to dictionary representation"""
        return {
            "ts": self.ts.isoformat(),
            "symbol": self.symbol,
            "direction": self.direction,
            "strategy": self.strategy,
            "confidence": self.confidence,
            "metadata": self.metadata
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Signal:
        """Create signal from dictionary"""
        return cls(
            ts=datetime.fromisoformat(data["ts"]),
            symbol=data["symbol"],
            direction=data["direction"],
            strategy=data["strategy"],
            confidence=data.get("confidence", 1.0),
            metadata=data.get("metadata", {})
        )
        
    def __post_init__(self):
        """Validate signal after initialization"""
        if not isinstance(self.direction, int) or self.direction not in [-1, 0, 1]:
            raise ValueError("Direction must be -1, 0, or 1")
        
        if not 0 <= self.confidence <= 1:
            raise ValueError("Confidence must be between 0 and 1")
            
    def __str__(self) -> str:
        """String representation of signal"""
        return f"Signal(ts={self.ts}, symbol={self.symbol}, direction={self.direction}, strategy={self.strategy}, confidence={self.confidence})"