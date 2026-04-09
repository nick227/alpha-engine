"""
Intelligence Hub State Management

Defines state structure and events for the Intelligence Hub interface.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class StateEvent(Enum):
    """State change events for Intelligence Hub"""
    ASSET_CHANGE = 'asset_change'
    TIMEFRAME_CHANGE = 'timeframe_change'
    HORIZON_CHANGE = 'horizon_change'
    STRATEGY_TOGGLE = 'strategy_toggle'
    RUN_CHANGE = 'run_change'
    FILTER_CHANGE = 'filter_change'
    STRATEGY_SELECT = 'strategy_select'


@dataclass
class IntelligenceHubState:
    """State container for Intelligence Hub interface"""
    tenant_id: str = "default"
    ticker: str = 'NVDA'
    timeframe: str = '3M'  # 1M, 3M, 6M, 1Y
    horizon: int = 7  # 1, 7, 30
    run_id: Optional[str] = None
    strategy_ids: List[str] = field(default_factory=list)
    selected_strategy: Optional[str] = None
    filter_mode: str = 'All predictions'  # All, Correct only, Incorrect only
    
    def copy(self, **changes) -> 'IntelligenceHubState':
        """Create a copy with specified changes"""
        import copy
        new_state = copy.deepcopy(self)
        for key, value in changes.items():
            setattr(new_state, key, value)
        return new_state
