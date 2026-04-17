"""
Minimal base types for discovery strategies that wrap or extend a core analyzer.

`TemporalCorrelationStrategy` depends on these; keep this module small and stable.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List


class SignalType(Enum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass
class Signal:
    symbol: str
    signal_type: SignalType
    strength: float
    confidence: float
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseStrategy(ABC):
    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    def analyze(self, market_data: Dict[str, Any]) -> List[Signal]:
        """Return zero or more signals for the given market snapshot."""
