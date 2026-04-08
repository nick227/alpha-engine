# Chart Overlay Enhancement - Phase-by-Phase Plan

## Overview
Enhance chart widget to compare actual price movement with backtest predictions, including confidence scoring and evaluation metrics.

## Phase 1: Define the Comparison View

### Core Comparison Types
1. **Directional Comparison**: Actual price vs predicted direction (up/down/neutral)
2. **Path Comparison**: Actual price vs predicted path (full forecast series)
3. **Entry/Exit Analysis**: Trade execution quality with timing evaluation

### Required Labels
- **Confidence Badge**: Pre-outcome confidence level
- **Result Badge**: Win/loss outcome after resolution
- **Return Badge**: Realized percentage return
- **Quality Indicators**: Runup/drawdown for trade quality context

### View Modes
- **Single Prediction Detail**: Deep dive on one prediction
- **Ranked Backtest Explorer**: Multiple predictions sorted by performance

## Phase 2: Inventory Existing Usable Fields

### Pre-Outcome Fields
```python
Prediction.confidence  # Primary confidence score
Prediction.direction   # Predicted direction
Prediction.timestamp   # Entry point
Prediction.horizon     # Expected duration
```

### Post-Outcome Fields
```python
PredictionOutcome.direction_correct  # Win/loss boolean
PredictionOutcome.return_pct        # Realized return
PredictionOutcome.max_runup         # Best case scenario
PredictionOutcome.max_drawdown      # Worst case scenario
PredictionOutcome.exit_reason       # Exit trigger
```

### Market Reaction Fields
```python
MRAOutcome.mra_score  # Market reaction analysis
# Secondary for event-reaction style scoring
```

### Missing Aggregates (Need to Derive)
- `win_rate` - Overall success rate
- `backtest_score` - Composite performance metric
- `composite_quality_score` - Multi-factor quality rating

## Phase 3: Design Derived Comparison DTO

### ComparisonCard API Shape
```python
class ComparisonCard:
    card_type: str = "comparison_chart"
    title: str
    data: {
        "actual_series": List[SeriesPoint],     # Real price movement
        "prediction_series": List[SeriesPoint],  # Predicted path (if available)
        "prediction_direction": str,            # Direction only (if no path)
        "entry_point": SeriesPoint,             # Trade entry
        "exit_point": SeriesPoint,              # Trade exit
        "confidence": float,                    # Pre-outcome confidence
        "direction_correct": bool,              # Win/loss result
        "return_pct": float,                    # Realized return
        "max_runup": float,                     # Best case
        "max_drawdown": float,                  # Worst case
        "mra_score": float,                     # Market reaction
    }
```

### Key Design Principles
- Keep Plotly styling out of API
- Support both direction-only and full-path predictions
- Include all evaluation metrics for sorting
- Maintain backward compatibility with existing Card schema

## Phase 4: Build Actual Price Overlay

### Base Layer Implementation
```python
# Render actual historical price as base line
fig.add_trace(go.Scatter(
    x=[p.x for p in actual_series],
    y=[p.y for p in actual_series],
    mode='lines',
    name='Actual Price',
    line=dict(color=COLORS['neutral_800'], width=2),
))
```

### Entry/Exit Markers
```python
# Entry point marker
fig.add_trace(go.Scatter(
    x=[entry_point.x],
    y=[entry_point.y],
    mode='markers',
    name='Entry',
    marker=dict(color=COLORS['success_500'], size=10, symbol='triangle-up')
))

# Exit point marker
fig.add_trace(go.Scatter(
    x=[exit_point.x],
    y=[exit_point.y],
    mode='markers',
    name='Exit',
    marker=dict(color=COLORS['error_500'], size=10, symbol='triangle-down')
))
```

### Time Alignment
- Normalize all timestamps to ISO format
- Ensure entry/exit points align with actual series
- Handle timezone differences consistently

## Phase 5: Add Prediction Overlay

### Direction-Only Prediction
```python
# Show directional zone or marker
if prediction_direction == "bullish":
    fig.add_hline(y=entry_point.y * 1.05, line_dash="dash", 
                  annotation_text="Bullish Prediction")
elif prediction_direction == "bearish":
    fig.add_hline(y=entry_point.y * 0.95, line_dash="dash",
                  annotation_text="Bearish Prediction")
```

### Full Path Prediction
```python
# Render prediction line
fig.add_trace(go.Scatter(
    x=[p.x for p in prediction_series],
    y=[p.y for p in prediction_series],
    mode='lines',
    name='Prediction',
    line=dict(color=COLORS['primary_600'], width=2, dash='dash'),
))
```

### Confidence Label
```python
# Add confidence badge to chart
fig.add_annotation(
    x=entry_point.x,
    y=entry_point.y * 1.1,
    text=f"Confidence: {confidence:.1%}",
    showarrow=True,
    arrowhead=2,
    bgcolor=COLORS['primary_100']
)
```

### Graceful Degradation
- Fall back to direction-only when prediction series unavailable
- Show confidence even without prediction path
- Maintain chart readability with minimal overlays

## Phase 6: Add Evaluation Metrics

### Primary Metrics Display
```python
# Win/Loss Badge
result_color = COLORS['success_500'] if direction_correct else COLORS['error_500']
st.markdown(f"""
<div style="background: {result_color}; color: white; padding: 4px 8px; border-radius: 4px;">
    {'WIN' if direction_correct else 'LOSS'}
</div>
""")

# Return Display
return_color = COLORS['success_500'] if return_pct > 0 else COLORS['error_500']
st.metric("Realized Return", f"{return_pct:+.2f}%", delta=None, delta_color="normal")
```

### Quality Indicators
```python
# Runup/Drawdown Context
col1, col2 = st.columns(2)
with col1:
    st.metric("Max Runup", f"+{max_runup:.2f}%")
with col2:
    st.metric("Max Drawdown", f"-{max_drawdown:.2f}%")
```

### Market Reaction Score
```python
# MRA Score (if available)
if mra_score:
    st.metric("Market Reaction", f"{mra_score:.2f}")
```

## Phase 7: Support Sorting and Ranking

### Pre-Outcome Sorting
```python
# Sort by confidence before outcome known
sorted_predictions = sorted(predictions, key=lambda p: p.confidence, reverse=True)
```

### Post-Outcome Sorting
```python
# Sort by realized return after outcome known
sorted_outcomes = sorted(outcomes, key=lambda o: o.return_pct, reverse=True)

# Optional: Sort by composite quality score
def quality_score(outcome):
    return (outcome.return_pct * 0.5 + 
            outcome.max_runup * 0.3 - 
            outcome.max_drawdown * 0.2)
```

### Filtered Views
```python
# Filter options
filter_options = ["All", "Wins Only", "Losses Only", "Best Returns", "Worst Drawdowns"]

if filter == "Wins Only":
    filtered = [o for o in outcomes if o.direction_correct]
elif filter == "Best Returns":
    filtered = [o for o in outcomes if o.return_pct > threshold]
```

## Phase 8: Add Table/Report Support

### Evidence Table
```python
# Source events and scored events
evidence_headers = ["Event", "Source", "Timestamp", "Sentiment", "Materiality"]
evidence_rows = [
    ["Earnings Beat", "Q4 2023", "2024-01-15", "Positive", "High"],
    ["AI Momentum", "Sector News", "2024-01-16", "Positive", "Medium"],
]
```

### Outcome Table
```python
# Realized metrics
outcome_headers = ["Metric", "Value", "Benchmark"]
outcome_rows = [
    ["Return", f"{return_pct:+.2f}%", "S&P: +1.2%"],
    ["Max Runup", f"+{max_runup:.2f}%", "Avg: +3.1%"],
    ["Max Drawdown", f"-{max_drawdown:.2f}%", "Avg: -2.4%"],
]
```

### History Table
```python
# Similar past predictions
history_headers = ["Date", "Ticker", "Direction", "Confidence", "Result"]
history_rows = [
    ["2024-01-10", "NVDA", "Bullish", "87%", "WIN"],
    ["2024-01-08", "AAPL", "Bearish", "72%", "LOSS"],
]
```

## Phase 9: Integrate into Card River

### New Card Type
```python
# Add to existing card types
card_type_options = ["chart", "number", "table", "comparison_chart"]
```

### Control Integration
```python
# Reuse existing controls
view_options = ["best_picks", "dips", "bundles", "compare", "backtest_analysis"]
strategy_options = ["house", "semantic", "quant", "comparison"]
horizon_options = ["1D", "1W", "1M", "3M", "6M", "1Y"]
```

### Multiple Ranked Cards
```python
# Return multiple comparison cards for backtest analysis
if view == "backtest_analysis":
    return [
        ComparisonCard("NVDA - Best Trade", best_trade_data),
        ComparisonCard("AAPL - Worst Trade", worst_trade_data),
        ComparisonCard("MSFT - Most Confident", confident_trade_data),
    ]
```

## Phase 10: Final Cleanup and Validation

### Chart Eligibility Rules
```python
def is_chart_eligible(comparison_data):
    return (
        len(comparison_data.actual_series) >= 2 and
        comparison_data.entry_point is not None and
        comparison_data.exit_point is not None
    )
```

### Timestamp Normalization
```python
def normalize_timestamps(data):
    for point in data.actual_series + data.prediction_series:
        point.x = pd.to_datetime(point.x).isoformat()
    return data
```

### Fallback Behavior
```python
def render_comparison_card(card):
    if not is_chart_eligible(card.data):
        # Fallback to number card with key metrics
        render_fallback_number_card(card)
    else:
        render_full_comparison_chart(card)
```

### Sort Consistency
```python
# Ensure deterministic sorting
def sort_key(comparison):
    return (
        comparison.data.return_pct,
        comparison.data.confidence,
        comparison.data.direction_correct
    )
```

### Empty State Handling
```python
def render_empty_comparison_state():
    st.info("No comparison data available for selected criteria.")
    st.caption("Try different time periods or adjust filters.")
```

## Implementation Priority

1. **Phase 1-3**: Foundation design and data modeling
2. **Phase 4-6**: Core chart rendering and metrics
3. **Phase 7-8**: Sorting, filtering, and table support
4. **Phase 9-10**: Integration and final validation

## Success Metrics

- Users can compare actual vs predicted price movement
- Confidence and outcome metrics clearly displayed
- Sorting and filtering work for analysis
- Graceful fallbacks for incomplete data
- Integration maintains existing card river architecture
