"""
Comparison Chart Schema - Phase 1-3 Implementation
Enhanced card schema for backtest prediction overlays and evaluation metrics
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import pandas as pd


# ============================================================================
# PHASE 1: COMPARISON VIEW DEFINITION
# ============================================================================

@dataclass
class SeriesPoint:
    """Universal series point for all chart types"""
    x: Any  # Timestamp
    y: Any  # Price/value
    kind: str = "actual"  # "actual", "prediction", "confidence_upper", "confidence_lower"
    label: str = None  # Optional label for series identification


@dataclass
class TradePoint:
    """Specific trade execution point"""
    timestamp: datetime
    price: float
    reason: str = None  # Entry/exit reason
    confidence: float = None  # Confidence at execution


# ============================================================================
# PHASE 2: EXISTING FIELD INVENTORY & MAPPING
# ============================================================================

@dataclass
class PredictionMetrics:
    """Pre-outcome prediction metrics from existing fields"""
    confidence: float           # Prediction.confidence
    direction: str             # Prediction.direction  
    timestamp: datetime        # Prediction.timestamp
    horizon: str              # Prediction.horizon
    strategy: str             # Strategy context
    ticker: str               # Asset identifier


@dataclass
class OutcomeMetrics:
    """Post-outcome evaluation metrics from existing fields"""
    direction_correct: bool    # PredictionOutcome.direction_correct
    return_pct: float          # PredictionOutcome.return_pct
    max_runup: float           # PredictionOutcome.max_runup
    max_drawdown: float        # PredictionOutcome.max_drawdown
    exit_reason: str           # PredictionOutcome.exit_reason
    mra_score: float = None    # MRAOutcome.mra_score (optional)


@dataclass
class DerivedQualityScore:
    """Derived composite quality metrics (Phase 2 requirement)"""
    win_rate: float = 0.0
    backtest_score: float = 0.0
    composite_quality: float = 0.0
    
    @classmethod
    def calculate(cls, prediction: PredictionMetrics, outcome: OutcomeMetrics) -> 'DerivedQualityScore':
        """Calculate derived scores from existing fields"""
        # Composite quality: weighted combination of return, confidence, and risk
        return_weight = 0.5
        confidence_weight = 0.3
        risk_weight = 0.2
        
        # Normalize return to -1 to 1 scale (assuming typical returns -20% to +20%)
        normalized_return = max(-1, min(1, outcome.return_pct / 0.2))
        
        # Calculate composite quality
        composite = (
            normalized_return * return_weight +
            prediction.confidence * confidence_weight -
            (outcome.max_drawdown / 0.1) * risk_weight  # Normalize drawdown
        )
        
        # Simple backtest score (could be enhanced)
        backtest_score = outcome.return_pct * (1.0 if outcome.direction_correct else -0.5)
        
        return cls(
            backtest_score=backtest_score,
            composite_quality=composite
        )


# ============================================================================
# PHASE 3: COMPARISON CARD DTO
# ============================================================================

@dataclass
class ComparisonCardData:
    """Lean API shape for chart overlay cards"""
    # Core series data
    actual_series: List[SeriesPoint]
    prediction_series: List[SeriesPoint] = None  # Optional full prediction path
    prediction_direction: str = None  # Direction-only fallback
    
    # Trade execution points
    entry_point: TradePoint = None
    exit_point: TradePoint = None
    
    # Prediction metrics (pre-outcome)
    prediction_metrics: PredictionMetrics = None
    
    # Outcome metrics (post-outcome)  
    outcome_metrics: OutcomeMetrics = None
    
    # Derived quality scores
    quality_scores: DerivedQualityScore = None
    
    # Contextual data
    ticker: str = None
    strategy: str = None
    horizon: str = None
    
    def __post_init__(self):
        """Calculate derived scores if not provided"""
        if self.prediction_metrics and self.outcome_metrics and not self.quality_scores:
            self.quality_scores = DerivedQualityScore.calculate(
                self.prediction_metrics, 
                self.outcome_metrics
            )
    
    @property
    def is_direction_only(self) -> bool:
        """Check if only direction is available (no full prediction path)"""
        return not self.prediction_series and self.prediction_direction
    
    @property
    def has_outcome(self) -> bool:
        """Check if outcome data is available"""
        return self.outcome_metrics is not None
    
    @property
    def is_winner(self) -> bool:
        """Check if prediction was correct"""
        return self.outcome_metrics.direction_correct if self.has_outcome else None
    
    @property
    def primary_sort_key(self) -> float:
        """Primary sorting key based on outcome availability"""
        if self.has_outcome:
            return self.outcome_metrics.return_pct
        elif self.prediction_metrics:
            return self.prediction_metrics.confidence
        return 0.0
    
    @property
    def secondary_sort_key(self) -> float:
        """Secondary sorting key for tie-breaking"""
        if self.quality_scores:
            return self.quality_scores.composite_quality
        return 0.0


class ComparisonCard:
    """Enhanced card schema for comparison charts"""
    
    def __init__(self, title: str, data: ComparisonCardData, card_id: str = None):
        self.card_type = "comparison_chart"
        self.title = title
        self.data = data
        self.card_id = card_id or title.lower().replace(" ", "_")
    
    @classmethod
    def from_prediction_outcome(cls, prediction: PredictionMetrics, 
                                outcome: OutcomeMetrics = None,
                                actual_prices: List[tuple] = None,
                                prediction_path: List[tuple] = None) -> 'ComparisonCard':
        """Factory method to create comparison card from existing data"""
        
        # Convert price data to SeriesPoint format
        actual_series = []
        if actual_prices:
            for timestamp, price in actual_prices:
                actual_series.append(SeriesPoint(timestamp, price, "actual"))
        
        # Convert prediction path to SeriesPoint format
        pred_series = []
        if prediction_path:
            for timestamp, price in prediction_path:
                pred_series.append(SeriesPoint(timestamp, price, "prediction"))
        
        # Create trade points
        entry_point = TradePoint(
            timestamp=prediction.timestamp,
            price=actual_prices[0][1] if actual_prices else 100.0,
            confidence=prediction.confidence
        )
        
        exit_point = None
        if outcome and actual_prices and len(actual_prices) > 1:
            exit_point = TradePoint(
                timestamp=actual_prices[-1][0],
                price=actual_prices[-1][1],
                reason=outcome.exit_reason
            )
        
        # Create comparison data
        comparison_data = ComparisonCardData(
            actual_series=actual_series,
            prediction_series=pred_series if pred_series else None,
            prediction_direction=prediction.direction if not pred_series else None,
            entry_point=entry_point,
            exit_point=exit_point,
            prediction_metrics=prediction,
            outcome_metrics=outcome,
            ticker=prediction.ticker,
            strategy=prediction.strategy,
            horizon=prediction.horizon
        )
        
        # Generate title
        title = f"{prediction.ticker} - {prediction.strategy.title()} Trade"
        if outcome:
            result = "WIN" if outcome.direction_correct else "LOSS"
            title += f" ({result})"
        
        return cls(title, comparison_data)


# ============================================================================
# MOCK DATA GENERATOR FOR TESTING
# ============================================================================

def create_mock_comparison_data() -> List[ComparisonCard]:
    """Create mock comparison cards for testing phases"""
    
    # Mock actual price series
    base_date = datetime(2024, 1, 1)
    actual_prices = []
    for i in range(30):
        date = base_date + pd.Timedelta(days=i)
        price = 100 + i * 0.5 + (i % 3) * 2  # Simulate price movement
        actual_prices.append((date, price))
    
    # Mock prediction path
    prediction_path = []
    for i in range(10):
        date = base_date + pd.Timedelta(days=i)
        base_price = 100 + i * 0.3  # Different trajectory
        prediction_path.append((date, base_price))
    
    # Create sample predictions and outcomes
    comparison_cards = []
    
    # Example 1: Winning trade
    pred1 = PredictionMetrics(
        confidence=0.87,
        direction="bullish",
        timestamp=base_date,
        horizon="1W",
        strategy="semantic",
        ticker="NVDA"
    )
    
    outcome1 = OutcomeMetrics(
        direction_correct=True,
        return_pct=12.4,
        max_runup=15.2,
        max_drawdown=3.1,
        exit_reason="target_reached"
    )
    
    card1 = ComparisonCard.from_prediction_outcome(
        pred1, outcome1, actual_prices, prediction_path
    )
    comparison_cards.append(card1)
    
    # Example 2: Losing trade
    pred2 = PredictionMetrics(
        confidence=0.72,
        direction="bearish", 
        timestamp=base_date + pd.Timedelta(days=10),
        horizon="1W",
        strategy="quant",
        ticker="AAPL"
    )
    
    outcome2 = OutcomeMetrics(
        direction_correct=False,
        return_pct=-8.7,
        max_runup=2.1,
        max_drawdown=11.3,
        exit_reason="stop_loss"
    )
    
    # Different price series for second trade
    actual_prices2 = [(base_date + pd.Timedelta(days=10+i), 120 - i*0.3) for i in range(15)]
    
    card2 = ComparisonCard.from_prediction_outcome(
        pred2, outcome2, actual_prices2
    )
    comparison_cards.append(card2)
    
    # Example 3: Direction-only prediction (no full path)
    pred3 = PredictionMetrics(
        confidence=0.65,
        direction="neutral",
        timestamp=base_date + pd.Timedelta(days=20),
        horizon="3D",
        strategy="house",
        ticker="MSFT"
    )
    
    outcome3 = OutcomeMetrics(
        direction_correct=True,
        return_pct=2.1,
        max_runup=4.2,
        max_drawdown=1.8,
        exit_reason="time_exit"
    )
    
    actual_prices3 = [(base_date + pd.Timedelta(days=20+i), 110) for i in range(8)]
    
    card3 = ComparisonCard.from_prediction_outcome(
        pred3, outcome3, actual_prices3
    )
    comparison_cards.append(card3)
    
    return comparison_cards


# ============================================================================
# SORTING AND FILTERING UTILITIES
# ============================================================================

class ComparisonSorter:
    """Sorting utilities for comparison cards"""
    
    @staticmethod
    def sort_by_primary(cards: List[ComparisonCard], reverse: bool = True) -> List[ComparisonCard]:
        """Sort by primary key (return_pct or confidence)"""
        return sorted(cards, key=lambda c: c.data.primary_sort_key, reverse=reverse)
    
    @staticmethod
    def sort_by_confidence(cards: List[ComparisonCard], reverse: bool = True) -> List[ComparisonCard]:
        """Sort by prediction confidence"""
        return sorted(
            [c for c in cards if c.data.prediction_metrics],
            key=lambda c: c.data.prediction_metrics.confidence,
            reverse=reverse
        )
    
    @staticmethod
    def sort_by_return(cards: List[ComparisonCard], reverse: bool = True) -> List[ComparisonCard]:
        """Sort by realized return"""
        return sorted(
            [c for c in cards if c.data.has_outcome],
            key=lambda c: c.data.outcome_metrics.return_pct,
            reverse=reverse
        )
    
    @staticmethod
    def sort_by_quality(cards: List[ComparisonCard], reverse: bool = True) -> List[ComparisonCard]:
        """Sort by composite quality score"""
        return sorted(
            [c for c in cards if c.data.quality_scores],
            key=lambda c: c.data.quality_scores.composite_quality,
            reverse=reverse
        )


class ComparisonFilter:
    """Filtering utilities for comparison cards"""
    
    @staticmethod
    def filter_wins(cards: List[ComparisonCard]) -> List[ComparisonCard]:
        """Filter for winning trades only"""
        return [c for c in cards if c.data.is_winner is True]
    
    @staticmethod
    def filter_losses(cards: List[ComparisonCard]) -> List[ComparisonCard]:
        """Filter for losing trades only"""
        return [c for c in cards if c.data.is_winner is False]
    
    @staticmethod
    def filter_by_strategy(cards: List[ComparisonCard], strategy: str) -> List[ComparisonCard]:
        """Filter by strategy type"""
        return [c for c in cards if c.data.strategy == strategy]
    
    @staticmethod
    def filter_by_min_return(cards: List[ComparisonCard], min_return: float) -> List[ComparisonCard]:
        """Filter by minimum return percentage"""
        return [
            c for c in cards 
            if c.data.has_outcome and c.data.outcome_metrics.return_pct >= min_return
        ]
    
    @staticmethod
    def filter_by_min_confidence(cards: List[ComparisonCard], min_confidence: float) -> List[ComparisonCard]:
        """Filter by minimum confidence level"""
        return [
            c for c in cards 
            if c.data.prediction_metrics and c.data.prediction_metrics.confidence >= min_confidence
        ]
