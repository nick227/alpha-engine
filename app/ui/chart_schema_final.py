"""
Final Chart Schema - Locked Architecture
Minimal schema with chart modes, canonical shape, and clean separation of concerns
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime
import pandas as pd


# ============================================================================
# FINAL LOCKED SCHEMA - MINIMAL CARD TYPES
# ============================================================================

class Card:
    """Minimal card schema - only 3 types (locked)"""
    
    def __init__(self, card_type: str, title: str, data: Dict, card_id: str = None):
        self.card_type = card_type  # "chart", "number", "table" ONLY
        self.title = title
        self.data = data
        self.card_id = card_id or title.lower().replace(" ", "_")


# ============================================================================
# CHART MODES - LOCKED ENUM
# ============================================================================

class ChartMode:
    """Chart mode constants (locked)"""
    FORECAST = "forecast"
    COMPARISON = "comparison"
    BACKTEST_OVERLAY = "backtest_overlay"


# ============================================================================
# CANONICAL CHART SHAPE - ONE SHAPE FOR ALL MODES
# ============================================================================

@dataclass
class ChartData:
    """Canonical chart data shape - same for all modes"""
    # Required fields for all charts
    series: List[Dict]  # Standard series format
    mode: str  # Required: ChartMode constant
    
    # Optional overlay summary fields (only for backtest_overlay mode)
    entry_point: Optional[Dict] = None
    exit_point: Optional[Dict] = None
    prediction_direction: Optional[str] = None
    confidence: Optional[float] = None  # Raw Prediction.confidence, untouched
    direction_correct: Optional[bool] = None  # From PredictionOutcome.direction_correct
    return_pct: Optional[float] = None  # From PredictionOutcome.return_pct
    max_runup: Optional[float] = None
    max_drawdown: Optional[float] = None
    
    def __post_init__(self):
        """Validate required fields"""
        if not self.mode or self.mode not in [ChartMode.FORECAST, ChartMode.COMPARISON, ChartMode.BACKTEST_OVERLAY]:
            raise ValueError(f"Invalid mode: {self.mode}. Must use ChartMode constants.")
        
        if self.mode == ChartMode.BACKTEST_OVERLAY and not self.entry_point:
            raise ValueError("Backtest overlay mode requires entry_point.")
    
    @property
    def has_outcome(self) -> bool:
        """Check if outcome data is available"""
        return self.direction_correct is not None
    
    @property
    def primary_sort_key(self) -> float:
        """Primary sorting key based on outcome availability"""
        if self.has_outcome:
            return self.return_pct or 0.0  # post-outcome: return_pct
        else:
            return self.confidence or 0.0  # pre-outcome: confidence
    
    @property
    def is_direction_only(self) -> bool:
        """Check if only direction is available"""
        return (
            self.mode == ChartMode.BACKTEST_OVERLAY and 
            self.prediction_direction is not None and
            not any(s.get("kind") == "prediction" for s in self.series)
        )
    
    def to_dict(self) -> Dict:
        """Convert to dict for API response"""
        return {
            "series": self.series,
            "mode": self.mode,
            "entry_point": self.entry_point,
            "exit_point": self.exit_point,
            "prediction_direction": self.prediction_direction,
            "confidence": self.confidence,  # Raw, untouched
            "direction_correct": self.direction_correct,
            "return_pct": self.return_pct,
            "max_runup": self.max_runup,
            "max_drawdown": self.max_drawdown
        }


# ============================================================================
# TABLE CARD DATA - CONTEXTUAL SUPPORT ONLY
# ============================================================================

@dataclass
class TableData:
    """Table card data - contextual supporting information only"""
    table_type: str  # "evidence", "outcome", "history"
    headers: List[str]
    rows: List[List[str]]
    context_card_id: str  # Stable and deterministic link to chart
    
    def to_dict(self) -> Dict:
        """Convert to dict for API response"""
        return {
            "table_type": self.table_type,
            "headers": self.headers,
            "rows": self.rows,
            "context_card_id": self.context_card_id
        }


# ============================================================================
# NUMBER CARD DATA - SIMPLE METRICS
# ============================================================================

@dataclass
class NumberData:
    """Number card data - simple metrics only"""
    primary_value: str
    confidence: Optional[float] = None  # Raw Prediction.confidence
    subtitle: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dict for API response"""
        return {
            "primary_value": self.primary_value,
            "confidence": self.confidence,
            "subtitle": self.subtitle
        }


# ============================================================================
# DATA LAYER - TIMESTAMP NORMALIZATION
# ============================================================================

class DataLayerNormalizer:
    """Data layer normalization - timestamps normalized once, never in renderer"""
    
    @staticmethod
    def normalize_chart_data(chart_data: ChartData) -> ChartData:
        """Normalize all timestamps in data layer, not renderer"""
        normalized_series = []
        
        for point in chart_data.series:
            normalized_point = point.copy()
            if "x" in normalized_point:
                if isinstance(normalized_point["x"], (datetime, pd.Timestamp)):
                    normalized_point["x"] = pd.to_datetime(normalized_point["x"]).isoformat()
            normalized_series.append(normalized_point)
        
        # Normalize entry/exit points
        normalized_entry = None
        if chart_data.entry_point:
            normalized_entry = chart_data.entry_point.copy()
            if "x" in normalized_entry:
                if isinstance(normalized_entry["x"], (datetime, pd.Timestamp)):
                    normalized_entry["x"] = pd.to_datetime(normalized_entry["x"]).isoformat()
        
        normalized_exit = None
        if chart_data.exit_point:
            normalized_exit = chart_data.exit_point.copy()
            if "x" in normalized_exit:
                if isinstance(normalized_exit["x"], (datetime, pd.Timestamp)):
                    normalized_exit["x"] = pd.to_datetime(normalized_exit["x"]).isoformat()
        
        # Return new normalized object
        return ChartData(
            series=normalized_series,
            mode=chart_data.mode,
            entry_point=normalized_entry,
            exit_point=normalized_exit,
            prediction_direction=chart_data.prediction_direction,
            confidence=chart_data.confidence,  # Raw, untouched
            direction_correct=chart_data.direction_correct,
            return_pct=chart_data.return_pct,
            max_runup=chart_data.max_runup,
            max_drawdown=chart_data.max_drawdown
        )
    
    @staticmethod
    def validate_minimum_series(chart_data: ChartData) -> bool:
        """Enforce 2 time points minimum rule in data layer"""
        return len(chart_data.series) >= 2
    
    @staticmethod
    def extend_series_for_markers(chart_data: ChartData) -> ChartData:
        """Extend series if entry/exit timestamps fall outside plotted range"""
        if not chart_data.series:
            return chart_data
        
        # Get series timestamp range
        series_timestamps = [pd.to_datetime(p["x"]) for p in chart_data.series if "x" in p]
        if not series_timestamps:
            return chart_data
        
        min_time = min(series_timestamps)
        max_time = max(series_timestamps)
        
        extended_series = chart_data.series.copy()
        
        # Extend for entry point if needed
        if chart_data.entry_point and "x" in chart_data.entry_point:
            entry_time = pd.to_datetime(chart_data.entry_point["x"])
            if entry_time < min_time:
                # Add entry point to series start
                extended_series.insert(0, {
                    "x": chart_data.entry_point["x"],
                    "y": chart_data.entry_point["y"],
                    "kind": "actual"  # Treat as actual for plotting
                })
            elif entry_time > max_time:
                # Add entry point to series end
                extended_series.append({
                    "x": chart_data.entry_point["x"],
                    "y": chart_data.entry_point["y"],
                    "kind": "actual"
                })
        
        # Extend for exit point if needed
        if chart_data.exit_point and "x" in chart_data.exit_point:
            exit_time = pd.to_datetime(chart_data.exit_point["x"])
            if exit_time < min_time:
                extended_series.insert(0, {
                    "x": chart_data.exit_point["x"],
                    "y": chart_data.exit_point["y"],
                    "kind": "actual"
                })
            elif exit_time > max_time:
                extended_series.append({
                    "x": chart_data.exit_point["x"],
                    "y": chart_data.exit_point["y"],
                    "kind": "actual"
                })
        
        return ChartData(
            series=extended_series,
            mode=chart_data.mode,
            entry_point=chart_data.entry_point,
            exit_point=chart_data.exit_point,
            prediction_direction=chart_data.prediction_direction,
            confidence=chart_data.confidence,
            direction_correct=chart_data.direction_correct,
            return_pct=chart_data.return_pct,
            max_runup=chart_data.max_runup,
            max_drawdown=chart_data.max_drawdown
        )


# ============================================================================
# FALLBACK PRECEDENCE - EXPLICIT RULES
# ============================================================================

class FallbackHandler:
    """Explicit fallback precedence for chart rendering"""
    
    @staticmethod
    def determine_render_type(chart_data: ChartData) -> str:
        """Determine render type with explicit precedence"""
        # Rule 1: Valid series → chart
        if DataLayerNormalizer.validate_minimum_series(chart_data):
            return "chart"
        
        # Rule 2: Invalid series but useful metrics → number
        if (chart_data.confidence is not None or 
            chart_data.return_pct is not None or
            chart_data.direction_correct is not None):
            return "number"
        
        # Rule 3: No usable data → empty-state card
        return "empty"
    
    @staticmethod
    def create_fallback_card(original_card: Card, render_type: str) -> Card:
        """Create fallback card with explicit precedence"""
        chart_data = original_card.data
        
        if render_type == "number":
            # Create number card from chart metrics
            primary_value = "No data"
            subtitle = "Insufficient series"
            
            if chart_data.return_pct is not None:
                primary_value = f"{chart_data.return_pct:+.2f}%"
                subtitle = "Realized Return"
            elif chart_data.confidence is not None:
                primary_value = f"{chart_data.confidence:.1%}"
                subtitle = "Confidence (Pending)"
            
            number_data = NumberData(
                primary_value=primary_value,
                confidence=chart_data.confidence,
                subtitle=subtitle
            )
            
            return Card("number", original_card.title, number_data.to_dict(), original_card.card_id)
        
        elif render_type == "empty":
            # Create empty-state card
            empty_data = {"message": "No data available"}
            return Card("number", original_card.title, empty_data, original_card.card_id)
        
        # Default: return original card
        return original_card


# ============================================================================
# MOCK DATA FACTORY - MIXED RESPONSES
# ============================================================================

def create_mixed_response() -> List[Card]:
    """Create mixed response with all card types and modes for testing"""
    cards = []
    
    # 1. Forecast chart (default mode)
    forecast_series = []
    base_date = datetime(2024, 1, 1)
    for i in range(30):
        date = base_date + pd.Timedelta(days=i)
        forecast_series.append({
            "x": date.isoformat(),
            "y": 100 + i * 0.5,
            "kind": "historical"
        })
    
    forecast_data = ChartData(
        series=forecast_series,
        mode=ChartMode.FORECAST
    )
    cards.append(Card("chart", "Standard Forecast", forecast_data.to_dict(), "standard_forecast"))
    
    # 2. Comparison chart
    comparison_series = []
    for i in range(30):
        date = base_date + pd.Timedelta(days=i)
        comparison_series.append({
            "x": date.isoformat(),
            "y": 100 + i * 0.4,
            "kind": "comparison",
            "label": "NVDA"
        })
        comparison_series.append({
            "x": date.isoformat(),
            "y": 100 + i * 0.2,
            "kind": "comparison", 
            "label": "AMD"
        })
    
    comparison_data = ChartData(
        series=comparison_series,
        mode=ChartMode.COMPARISON
    )
    cards.append(Card("chart", "Asset Comparison", comparison_data.to_dict(), "asset_comparison"))
    
    # 3. Backtest overlay chart
    overlay_series = []
    for i in range(30):
        date = base_date + pd.Timedelta(days=i)
        price = 100 + i * 0.3
        overlay_series.append({
            "x": date.isoformat(),
            "y": price,
            "kind": "actual"
        })
    
    # Add prediction path
    for i in range(10):
        date = base_date + pd.Timedelta(days=i)
        pred_price = 100 + i * 0.4
        overlay_series.append({
            "x": date.isoformat(),
            "y": pred_price,
            "kind": "prediction"
        })
    
    overlay_data = ChartData(
        series=overlay_series,
        mode=ChartMode.BACKTEST_OVERLAY,
        entry_point={
            "x": base_date.isoformat(),
            "y": 100.0
        },
        exit_point={
            "x": (base_date + pd.Timedelta(days=29)).isoformat(),
            "y": 108.7
        },
        prediction_direction="bullish",
        confidence=0.87,  # Raw Prediction.confidence
        direction_correct=True,  # From PredictionOutcome.direction_correct
        return_pct=8.7,  # From PredictionOutcome.return_pct
        max_runup=12.1,
        max_drawdown=2.3
    )
    cards.append(Card("chart", "Backtest Analysis", overlay_data.to_dict(), "backtest_analysis"))
    
    # 4. Number card
    number_data = NumberData(
        primary_value="+12.4%",
        confidence=0.91,
        subtitle="Expected Move"
    )
    cards.append(Card("number", "Top Pick", number_data.to_dict(), "top_pick"))
    
    # 5. Evidence table card (contextual)
    evidence_data = TableData(
        table_type="evidence",
        headers=["Event", "Source", "Sentiment", "Materiality"],
        rows=[
            ["Earnings Beat", "Q4 2023", "Positive", "High"],
            ["AI Momentum", "Sector News", "Positive", "Medium"],
            ["Technical Signal", "Golden Cross", "Positive", "Low"]
        ],
        context_card_id="backtest_analysis"  # Stable and deterministic
    )
    cards.append(Card("table", "Evidence", evidence_data.to_dict(), "evidence_table"))
    
    # 6. Outcome table card (contextual)
    outcome_data = TableData(
        table_type="outcome",
        headers=["Metric", "Value", "Benchmark"],
        rows=[
            ["Realized Return", "+8.7%", "S&P: +1.2%"],
            ["Max Runup", "+12.1%", "Avg: +8.3%"],
            ["Max Drawdown", "-2.3%", "Avg: -4.1%"]
        ],
        context_card_id="backtest_analysis"  # Stable and deterministic
    )
    cards.append(Card("table", "Outcome", outcome_data.to_dict(), "outcome_table"))
    
    # Apply data layer normalization
    for card in cards:
        if card.card_type == "chart":
            chart_data = ChartData(**card.data)
            normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
            extended_chart = DataLayerNormalizer.extend_series_for_markers(normalized_chart)
            
            # Check fallback precedence
            render_type = FallbackHandler.determine_render_type(extended_chart)
            if render_type != "chart":
                # Replace with fallback card
                fallback_card = FallbackHandler.create_fallback_card(card, render_type)
                cards[cards.index(card)] = fallback_card
            else:
                # Update with normalized data
                card.data = extended_chart.to_dict()
    
    return cards


# ============================================================================
# SORTING LOGIC - SEMANTIC API RESPONSES
# ============================================================================

class SemanticSorter:
    """Semantic sorting - API responses are semantic, not visual"""
    
    @staticmethod
    def sort_cards(cards: List[Card], sort_option: str = "default") -> List[Card]:
        """Sort cards with semantic logic"""
        if sort_option == "default":
            return cards
        
        # Separate chart cards for mode-aware sorting
        chart_cards = [c for c in cards if c.card_type == "chart"]
        other_cards = [c for c in cards if c.card_type != "chart"]
        
        if sort_option == "confidence":
            # Pre-outcome: confidence
            sorted_chart = sorted(
                [c for c in chart_cards if c.data.get("confidence") is not None],
                key=lambda c: c.data.get("confidence", 0),
                reverse=True
            )
        elif sort_option == "return":
            # Post-outcome: return_pct
            sorted_chart = sorted(
                [c for c in chart_cards if c.data.get("return_pct") is not None],
                key=lambda c: c.data.get("return_pct", 0),
                reverse=True
            )
        elif sort_option == "primary":
            # Primary key (confidence pre-outcome, return post-outcome)
            sorted_chart = sorted(
                chart_cards,
                key=lambda c: ChartData(**c.data).primary_sort_key,
                reverse=True
            )
        else:
            sorted_chart = chart_cards
        
        # Combine with other cards in original order
        return sorted_chart + other_cards


# ============================================================================
# CACHE KEY GENERATION - TRUE QUERY INPUTS ONLY
# ============================================================================

class CacheKeyGenerator:
    """Generate cache keys from true query inputs only, not UI state"""
    
    @staticmethod
    def generate_cache_key(inputs: Dict[str, Any]) -> str:
        """Generate cache key from query inputs only"""
        # Extract only true query inputs
        query_inputs = {
            "tenant": inputs.get("tenant"),
            "ticker": inputs.get("ticker"),
            "view": inputs.get("view"),
            "strategy": inputs.get("strategy"),
            "horizon": inputs.get("horizon")
        }
        
        # Create deterministic key
        key_parts = []
        for k, v in sorted(query_inputs.items()):
            if v is not None:
                key_parts.append(f"{k}:{v}")
        
        return "|".join(key_parts)


# ============================================================================
# EXPORT/REPORT LAYER PREPARATION
# ============================================================================

class ExportPreparation:
    """Prepare data for export/report layer"""
    
    @staticmethod
    def prepare_chart_export(chart_data: ChartData) -> Dict:
        """Prepare chart data for export"""
        return {
            "title": "Chart Export",
            "mode": chart_data.mode,
            "series": chart_data.series,
            "summary": {
                "confidence": chart_data.confidence,
                "return_pct": chart_data.return_pct,
                "direction_correct": chart_data.direction_correct,
                "max_runup": chart_data.max_runup,
                "max_drawdown": chart_data.max_drawdown
            }
        }
    
    @staticmethod
    def prepare_table_export(table_data: TableData) -> Dict:
        """Prepare table data for export"""
        return {
            "title": f"{table_data.table_type.title()} Export",
            "headers": table_data.headers,
            "rows": table_data.rows,
            "context_card_id": table_data.context_card_id
        }
