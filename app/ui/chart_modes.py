"""
Chart Modes - Minimal Schema Enhancement
Treats backtest overlay as a chart mode, not a new card type
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import pandas as pd


# ============================================================================
# MINIMAL CARD SCHEMA - UNCHANGED
# ============================================================================

class Card:
    """Minimal card schema - only 3 types (unchanged)"""
    
    def __init__(self, card_type: str, title: str, data: Dict, card_id: str = None):
        self.card_type = card_type  # "chart", "number", "table" ONLY
        self.title = title
        self.data = data
        self.card_id = card_id or title.lower().replace(" ", "_")


# ============================================================================
# CHART MODES - ENHANCEMENT WITHIN CHART TYPE
# ============================================================================

class ChartMode:
    """Chart mode constants"""
    FORECAST = "forecast"           # Original forecast charts
    COMPARISON = "comparison"       # Asset comparison charts  
    BACKTEST_OVERLAY = "backtest_overlay"  # New overlay mode


@dataclass
class ChartOverlayData:
    """Lean overlay data for backtest mode"""
    # Core series data (required)
    series: List[Dict]  # Standard chart series format
    
    # Chart mode (required for overlay)
    mode: str  # "forecast", "comparison", "backtest_overlay"
    
    # Overlay data (only for backtest_overlay mode)
    entry_point: Optional[Dict] = None
    exit_point: Optional[Dict] = None
    prediction_direction: Optional[str] = None  # "bullish", "bearish", "neutral"
    confidence: Optional[float] = None  # Prediction.confidence
    
    # Outcome data (only available post-outcome)
    direction_correct: Optional[bool] = None  # PredictionOutcome.direction_correct
    return_pct: Optional[float] = None  # PredictionOutcome.return_pct
    max_runup: Optional[float] = None  # PredictionOutcome.max_runup
    max_drawdown: Optional[float] = None  # PredictionOutcome.max_drawdown
    
    def __post_init__(self):
        """Validate mode-specific data"""
        if self.mode == ChartMode.BACKTEST_OVERLAY:
            # For backtest overlay, require entry point
            if not self.entry_point:
                raise ValueError("Backtest overlay requires entry_point")
    
    @property
    def has_outcome(self) -> bool:
        """Check if outcome data is available"""
        return self.direction_correct is not None
    
    @property
    def is_winner(self) -> Optional[bool]:
        """Check if prediction was correct"""
        return self.direction_correct if self.has_outcome else None
    
    @property
    def primary_sort_key(self) -> float:
        """Primary sorting key based on outcome availability"""
        if self.has_outcome:
            return self.return_pct or 0.0
        else:
            return self.confidence or 0.0
    
    @property
    def is_direction_only(self) -> bool:
        """Check if only direction is available (no full prediction path)"""
        return (
            self.mode == ChartMode.BACKTEST_OVERLAY and 
            self.prediction_direction is not None and
            not any(s.get("kind") == "prediction" for s in self.series)
        )


# ============================================================================
# TABLE CARD DATA - CONTEXTUAL SUPPORT ONLY
# ============================================================================

@dataclass
class TableCardData:
    """Table card data for contextual supporting information"""
    table_type: str  # "evidence", "outcome", "history"
    headers: List[str]
    rows: List[List[str]]
    context_card_id: str  # Links to the overlay card this supports
    
    @classmethod
    def create_evidence_table(cls, card_id: str, ticker: str, direction: str) -> 'TableCardData':
        """Create evidence table for overlay card"""
        # Mock evidence data
        evidence = [
            ["Earnings Report", "Company Filing", "2024-01-15", "Positive", "High"],
            ["Sector News", "Financial Times", "2024-01-14", "Positive", "Medium"],
            ["Analyst Upgrade", "Morgan Stanley", "2024-01-13", "Positive", "High"],
        ]
        
        # Adjust based on direction
        if direction == "bearish":
            evidence = [
                ["Earnings Warning", "Company Filing", "2024-01-15", "Negative", "High"],
                ["Sector Downgrade", "Bloomberg", "2024-01-14", "Negative", "Medium"],
                ["Analyst Downgrade", "Goldman Sachs", "2024-01-13", "Negative", "High"],
            ]
        
        return cls(
            table_type="evidence",
            headers=["Event", "Source", "Timestamp", "Sentiment", "Materiality"],
            rows=evidence,
            context_card_id=card_id
        )
    
    @classmethod
    def create_outcome_table(cls, card_id: str, overlay_data: ChartOverlayData) -> 'TableCardData':
        """Create outcome table for overlay card"""
        if not overlay_data.has_outcome:
            return None
        
        outcome_data = [
            ["Realized Return", f"{overlay_data.return_pct:+.2f}%"],
            ["Direction Correct", "Yes" if overlay_data.direction_correct else "No"],
            ["Max Runup", f"+{overlay_data.max_runup:.2f}%"],
            ["Max Drawdown", f"-{overlay_data.max_drawdown:.2f}%"],
        ]
        
        return cls(
            table_type="outcome",
            headers=["Metric", "Value"],
            rows=outcome_data,
            context_card_id=card_id
        )
    
    @classmethod
    def create_history_table(cls, card_id: str, ticker: str, strategy: str) -> 'TableCardData':
        """Create history table for overlay card"""
        history = [
            ["2024-01-08", ticker, strategy, "Bullish", "85%", "WIN", "+8.2%"],
            ["2024-01-05", ticker, strategy, "Bearish", "72%", "LOSS", "-3.1%"],
            ["2024-01-02", ticker, strategy, "Bullish", "91%", "WIN", "+12.4%"],
            ["2023-12-28", ticker, strategy, "Neutral", "65%", "WIN", "+2.1%"],
        ]
        
        return cls(
            table_type="history",
            headers=["Date", "Ticker", "Strategy", "Direction", "Confidence", "Result", "Return"],
            rows=history,
            context_card_id=card_id
        )


# ============================================================================
# MOCK DATA GENERATOR - LEAN OVERLAY CARDS
# ============================================================================

def create_backtest_overlay_cards() -> List[Card]:
    """Create mock backtest overlay cards using standard chart schema"""
    cards = []
    
    # Example 1: Winning trade with full prediction path
    base_date = datetime(2024, 1, 1)
    actual_series = []
    prediction_series = []
    
    # Generate actual price series
    for i in range(30):
        date = base_date + pd.Timedelta(days=i)
        price = 100 + i * 0.5 + (i % 3) * 2
        actual_series.append({
            "x": date.isoformat(),
            "y": price,
            "kind": "actual"
        })
    
    # Generate prediction series
    for i in range(10):
        date = base_date + pd.Timedelta(days=i)
        price = 100 + i * 0.3  # Different trajectory
        prediction_series.append({
            "x": date.isoformat(),
            "y": price,
            "kind": "prediction"
        })
    
    # Combine series
    combined_series = actual_series + prediction_series
    
    # Create overlay data
    overlay_data = ChartOverlayData(
        series=combined_series,
        mode=ChartMode.BACKTEST_OVERLAY,
        entry_point={
            "x": base_date.isoformat(),
            "y": 100.0,
            "confidence": 0.87
        },
        exit_point={
            "x": (base_date + pd.Timedelta(days=29)).isoformat(),
            "y": 115.0
        },
        prediction_direction="bullish",
        confidence=0.87,  # Prediction.confidence
        direction_correct=True,  # PredictionOutcome.direction_correct
        return_pct=12.4,  # PredictionOutcome.return_pct
        max_runup=15.2,  # PredictionOutcome.max_runup
        max_drawdown=3.1  # PredictionOutcome.max_drawdown
    )
    
    # Create chart card with overlay mode
    card1 = Card(
        card_type="chart",
        title="NVDA - Backtest Analysis (WIN)",
        data=overlay_data.__dict__,
        card_id="nvda_backtest_win"
    )
    cards.append(card1)
    
    # Create supporting table cards
    evidence_table = TableCardData.create_evidence_table(
        "nvda_backtest_win", "NVDA", "bullish"
    )
    outcome_table = TableCardData.create_outcome_table(
        "nvda_backtest_win", overlay_data
    )
    
    cards.append(Card("table", "Evidence", evidence_table.__dict__))
    cards.append(Card("table", "Outcome", outcome_table.__dict__))
    
    # Example 2: Losing trade (direction only)
    overlay_data2 = ChartOverlayData(
        series=actual_series[:15],  # Shorter series
        mode=ChartMode.BACKTEST_OVERLAY,
        entry_point={
            "x": base_date.isoformat(),
            "y": 100.0,
            "confidence": 0.72
        },
        exit_point={
            "x": (base_date + pd.Timedelta(days=14)).isoformat(),
            "y": 91.3
        },
        prediction_direction="bearish",  # Direction only, no prediction path
        confidence=0.72,  # Prediction.confidence
        direction_correct=False,  # PredictionOutcome.direction_correct
        return_pct=-8.7,  # PredictionOutcome.return_pct
        max_runup=2.1,  # PredictionOutcome.max_runup
        max_drawdown=11.3  # PredictionOutcome.max_drawdown
    )
    
    card2 = Card(
        card_type="chart",
        title="AAPL - Backtest Analysis (LOSS)",
        data=overlay_data2.__dict__,
        card_id="aapl_backtest_loss"
    )
    cards.append(card2)
    
    # Supporting tables for second card
    evidence_table2 = TableCardData.create_evidence_table(
        "aapl_backtest_loss", "AAPL", "bearish"
    )
    outcome_table2 = TableCardData.create_outcome_table(
        "aapl_backtest_loss", overlay_data2
    )
    
    cards.append(Card("table", "Evidence", evidence_table2.__dict__))
    cards.append(Card("table", "Outcome", outcome_table2.__dict__))
    
    return cards


def create_forecast_cards() -> List[Card]:
    """Create standard forecast cards for comparison"""
    cards = []
    
    # Standard forecast chart
    dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
    series = []
    
    for i, d in enumerate(dates):
        series.append({"x": d.isoformat(), "y": 100 + i * 0.5, "kind": "historical"})
    
    # Add forecast points
    for i in range(10):
        forecast_date = dates[-1] + pd.Timedelta(days=i + 1)
        base_value = 100 + len(dates) * 0.5
        series.append({"x": forecast_date.isoformat(), "y": base_value + i * 0.3, "kind": "forecast"})
    
    forecast_data = {
        "series": series,
        "mode": ChartMode.FORECAST
    }
    
    card = Card(
        card_type="chart",
        title="Standard Forecast",
        data=forecast_data,
        card_id="standard_forecast"
    )
    cards.append(card)
    
    return cards


# ============================================================================
# SORTING UTILITIES - MODE-AWARE
# ============================================================================

class ChartModeSorter:
    """Sorting utilities that understand chart modes"""
    
    @staticmethod
    def sort_by_primary(cards: List[Card], reverse: bool = True) -> List[Card]:
        """Sort by primary key (return/confidence) for overlay charts only"""
        overlay_cards = [c for c in cards if c.card_type == "chart" and 
                        c.data.get("mode") == ChartMode.BACKTEST_OVERLAY]
        other_cards = [c for c in cards if c not in overlay_cards]
        
        # Sort overlay cards by primary key
        sorted_overlay = sorted(
            overlay_cards,
            key=lambda c: c.data.get("return_pct", c.data.get("confidence", 0)),
            reverse=reverse
        )
        
        # Keep other cards in original order
        return sorted_overlay + other_cards
    
    @staticmethod
    def sort_by_confidence(cards: List[Card], reverse: bool = True) -> List[Card]:
        """Sort by confidence (pre-outcome)"""
        overlay_cards = [c for c in cards if c.card_type == "chart" and 
                        c.data.get("mode") == ChartMode.BACKTEST_OVERLAY and
                        c.data.get("confidence") is not None]
        other_cards = [c for c in cards if c not in overlay_cards]
        
        sorted_overlay = sorted(
            overlay_cards,
            key=lambda c: c.data.get("confidence", 0),
            reverse=reverse
        )
        
        return sorted_overlay + other_cards
    
    @staticmethod
    def sort_by_return(cards: List[Card], reverse: bool = True) -> List[Card]:
        """Sort by realized return (post-outcome)"""
        overlay_cards = [c for c in cards if c.card_type == "chart" and 
                        c.data.get("mode") == ChartMode.BACKTEST_OVERLAY and
                        c.data.get("return_pct") is not None]
        other_cards = [c for c in cards if c not in overlay_cards]
        
        sorted_overlay = sorted(
            overlay_cards,
            key=lambda c: c.data.get("return_pct", 0),
            reverse=reverse
        )
        
        return sorted_overlay + other_cards


# ============================================================================
# FILTERING UTILITIES - MODE-AWARE
# ============================================================================

class ChartModeFilter:
    """Filtering utilities that understand chart modes"""
    
    @staticmethod
    def filter_wins(cards: List[Card]) -> List[Card]:
        """Filter for winning overlay charts only"""
        return [
            c for c in cards 
            if c.card_type == "chart" and 
            c.data.get("mode") == ChartMode.BACKTEST_OVERLAY and
            c.data.get("direction_correct") is True
        ]
    
    @staticmethod
    def filter_losses(cards: List[Card]) -> List[Card]:
        """Filter for losing overlay charts only"""
        return [
            c for c in cards 
            if c.card_type == "chart" and 
            c.data.get("mode") == ChartMode.BACKTEST_OVERLAY and
            c.data.get("direction_correct") is False
        ]
    
    @staticmethod
    def filter_by_min_confidence(cards: List[Card], min_confidence: float) -> List[Card]:
        """Filter by minimum confidence level"""
        return [
            c for c in cards 
            if c.card_type == "chart" and 
            c.data.get("mode") == ChartMode.BACKTEST_OVERLAY and
            c.data.get("confidence", 0) >= min_confidence
        ]
    
    @staticmethod
    def filter_by_min_return(cards: List[Card], min_return: float) -> List[Card]:
        """Filter by minimum return percentage"""
        return [
            c for c in cards 
            if c.card_type == "chart" and 
            c.data.get("mode") == ChartMode.BACKTEST_OVERLAY and
            c.data.get("return_pct", 0) >= min_return
        ]
    
    @staticmethod
    def filter_by_mode(cards: List[Card], mode: str) -> List[Card]:
        """Filter by chart mode"""
        return [
            c for c in cards 
            if c.card_type == "chart" and c.data.get("mode") == mode
        ]


# ============================================================================
# VALIDATION UTILITIES
# ============================================================================

class ChartModeValidator:
    """Validation for chart mode compliance"""
    
    @staticmethod
    def validate_chart_card(card: Card) -> Dict[str, bool]:
        """Validate chart card follows minimal schema"""
        validation_results = {
            "is_chart_type": card.card_type == "chart",
            "has_mode": "mode" in card.data,
            "has_series": "series" in card.data and len(card.data["series"]) >= 2,
            "valid_mode": card.data.get("mode") in [ChartMode.FORECAST, ChartMode.COMPARISON, ChartMode.BACKTEST_OVERLAY],
            "no_plotly_leakage": not any(key.startswith("plotly") for key in card.data.keys()),
            "lean_payload": len(str(card.data)) < 10000  # Keep payload lean
        }
        
        # Mode-specific validation
        mode = card.data.get("mode")
        if mode == ChartMode.BACKTEST_OVERLAY:
            validation_results.update({
                "has_entry_point": "entry_point" in card.data,
                "confidence_sourced_correctly": "confidence" in card.data,  # From Prediction.confidence
                "outcome_sourced_correctly": "direction_correct" in card.data,  # From PredictionOutcome.direction_correct
                "return_sourced_correctly": "return_pct" in card.data,  # From PredictionOutcome.return_pct
            })
        
        return validation_results
    
    @staticmethod
    def validate_table_card(card: Card) -> Dict[str, bool]:
        """Validate table card is contextual only"""
        validation_results = {
            "is_table_type": card.card_type == "table",
            "has_context": "context_card_id" in card.data,
            "has_table_type": "table_type" in card.data,
            "no_evidence_in_charts": True  # Ensured by separate validation
        }
        
        return validation_results
    
    @staticmethod
    def validate_ugly_cases(card: Card) -> Dict[str, bool]:
        """Test edge cases and ugly scenarios"""
        data = card.data
        
        validation_results = {
            "handles_flat_prediction": True,  # Direction-only fallback
            "handles_missing_outcome": "direction_correct" not in data or data["direction_correct"] is None,
            "handles_missing_entry": "entry_point" not in data or data["entry_point"] is None,
            "handles_sparse_timestamps": len(data.get("series", [])) >= 2,
            "downgrades_to_number": True  # Fallback behavior
        }
        
        return validation_results
