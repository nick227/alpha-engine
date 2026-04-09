"""
Intelligence Hub Data Transfer Objects

Defines DTOs for clean data flow between service layer and UI components.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class IntelligenceHubDTO:
    """Complete intelligence hub state for UI rendering - real data only"""
    state: 'IntelligenceHubState'
    tickers: List[str]
    runs: List['PredictionRunView']
    matrix_rows: Optional[List['ChampionMatrixView']] = None
    strategy_rankings: Optional[List['StrategyEfficiencyView']] = None
    overlay_series: Optional['ComparisonData'] = None
    timeline: Optional['StrategyTimelineView'] = None
    consensus: Optional['ConsensusView'] = None
    champions: Optional[List[dict]] = None  # Derived from real matrix data
    window_date_range: Optional[str] = None  # UI formatting helper


@dataclass
class ChampionRow:
    """Champion performance data per horizon"""
    horizon: int
    champion_strategy: str
    alpha: float
    mae: float
    samples: int
    recent_alpha: float


@dataclass
class StrategyRanking:
    """Strategy efficiency ranking data"""
    strategy_id: str
    efficiency_score: float
    alpha: float
    mae: float
    win_rate: float


@dataclass
class ComparisonData:
    """Multi-strategy comparison data for overlays"""
    strategies: List['StrategySeries']
    actual_series: 'PriceSeries'
    prediction_points: List['PredictionPoint']


@dataclass
class TimelineData:
    """Detailed timeline for single strategy"""
    strategy_id: str
    predictions: List['PredictionDetail']
    actual_prices: 'PriceSeries'
    news_events: List['NewsEvent']
    performance_metrics: 'PerformanceMetrics'


@dataclass
class StrategySeries:
    """Single strategy series data"""
    strategy_id: str
    predicted: List['PredictionPoint']
    actual: List['PricePoint']


@dataclass
class PredictionPoint:
    """Individual prediction data point"""
    timestamp: str
    value: float
    strategy_id: str


@dataclass
class PricePoint:
    """Price data point"""
    timestamp: str
    value: float


@dataclass
class PredictionDetail:
    """Detailed prediction with metadata"""
    timestamp: str
    strategy_id: str
    predicted_return: float
    actual_return: float
    direction_correct: bool
    entry_price: float
    target_price: float


@dataclass
class NewsEvent:
    """News event aligned to prediction window"""
    timestamp: str
    headline: str
    polarity: float
    category: str


@dataclass
class PerformanceMetrics:
    """Strategy performance metrics"""
    alpha: float
    mae: float
    win_rate: float
    total_return: float
    samples: int
