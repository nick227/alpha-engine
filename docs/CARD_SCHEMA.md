# Card River Dashboard - Schema Documentation

## Overview

The Card River Dashboard transforms traditional dashboard design into a flexible, component-based architecture where:

- **Shared Controls**: Single set of controls drives all cards
- **Independent Cards**: Each card is self-contained with its own visualization
- **River Layout**: Cards flow sequentially like a river
- **Multiple Results**: Show multiple top picks, comparisons, or bundles together

## Control Architecture

### Primary Controls

```python
controls = {
    "view": "best_picks" | "dips" | "bundles" | "compare",
    "strategy": "house" | "semantic" | "quant" | "comparison", 
    "horizon": "1D" | "1W" | "1M" | "3M" | "6M" | "1Y"
}
```

### Advanced Filters

```python
filters = {
    "min_confidence": 0.0-1.0,
    "risk_level": "Low" | "Medium" | "High",
    "sectors": ["Technology", "Healthcare", "Finance", ...]
}
```

## Card Schema

### Universal Card Structure

```python
class CardSchema:
    card_type: str        # "chart", "number", "table"
    title: str           # Card title
    subtitle: str         # Optional subtitle
    data: Dict           # Card-specific data payload
    metadata: Dict        # Additional card metadata
```

### Data Point Structure

```python
class SeriesPoint:
    x: Any              # X-axis value (typically timestamp)
    y: Any              # Y-axis value
    kind: str           # Series type for styling
    label: str          # Optional label for series identification
```

## Card Types

### 1. Chart Card

**Purpose**: Display time-series forecasts with confidence bands

**Data Structure**:
```python
{
    "series": [SeriesPoint],
    "chart_type": "line" | "scatter" | "area"
}
```

**Series Types**:
- `historical`: Solid line (actual data)
- `forecast`: Dashed line (predicted future)
- `confidence_upper`: Upper confidence bound
- `confidence_lower`: Lower confidence bound (filled to upper)
- `comparison`: Multiple comparison lines
- `bundle`: Bold bundle line with faint constituents
- `constituent`: Dashed constituent lines
- `signal_marker`: Diamond scatter points for signals

**Plotly Mapping**:
```python
# Historical → Solid line
fig.add_trace(go.Scatter(
    x=[p.x for p in historical],
    y=[p.y for p in historical],
    mode='lines',
    line=dict(color=primary_color, width=2)
))

# Forecast → Dashed line  
fig.add_trace(go.Scatter(
    x=[p.x for p in forecast],
    y=[p.y for p in forecast],
    mode='lines',
    line=dict(color=primary_color, width=2, dash='dash')
))

# Confidence Bands → Filled area
fig.add_trace(go.Scatter(
    x=[p.x for p in confidence_upper],
    y=[p.y for p in confidence_upper],
    line=dict(width=0),
    fill='tonexty',
    fillcolor=f'rgba(33, 150, 243, 0.2)'
))
```

### 2. Number Card

**Purpose**: Display ranked metrics with confidence and trend

**Data Structure**:
```python
{
    "primary_value": Any,        # Main metric value
    "secondary_metrics": Dict,    # Supporting metrics
    "rank": int,               # Optional rank
    "trend": str,             # Trend direction
    "confidence": float         # Confidence level
}
```

**Visual Elements**:
- Primary value with trend-based coloring
- Rank badge (if provided)
- Trend indicator (📈📉➡️)
- Confidence progress bar
- Secondary metrics table

### 3. Table Card

**Purpose**: Display dense supporting data

**Data Structure**:
```python
{
    "headers": [str],          # Column headers
    "rows": [[Any]]           # Table rows
}
```

**Features**:
- Responsive table with alternating row colors
- Sortable columns (future enhancement)
- Compact formatting for dense data
- Scrollable for large datasets

## Data Provider Interface

### Abstract Interface

```python
class CardDataProvider:
    def get_cards(view: str, strategy: str, horizon: str, filters: Dict) -> List[CardSchema]:
        """Return cards based on control selections"""
        raise NotImplementedError
```

### Implementation Example

```python
class BestPicksProvider(CardDataProvider):
    def get_cards(self, view, strategy, horizon, filters):
        if view == "best_picks":
            return [
                # Top pick chart with forecast
                CardData.chart(series=forecast_series, title="NVDA - Top Pick"),
                
                # Metrics card with rank and confidence  
                CardData.number(primary_value="+12.4%", rank=1, confidence=0.87),
                
                # Evidence table
                CardData.table(headers=["Signal", "Source", "Strength"], rows=evidence_data)
            ]
```

## Card → Table Mapping

### Chart Cards
- **Validation Table**: Historical prediction accuracy
- **Reaction Table**: Post-event market response

### Number Cards  
- **Evidence Table**: News/sentiment data
- **Feature Table**: Model input features

### Bundle Cards
- **Composition Table**: Bundle weights and rankings
- **Performance Table**: Constituent contributions

### Comparison Cards
- **Relative Metrics Table**: Performance comparisons
- **Attribution Table**: Factor contributions

## Data Categories

### 1. Source Evidence
**From**: RawEvent, ScoredEvent
**Fields**: source, timestamp, text, category, direction, materiality, concept_tags, explanation_terms
**Used In**: Evidence tables

### 2. Model Inputs  
**From**: ScoredEvent, Prediction.feature_snapshot
**Fields**: company_relevance, direction, confidence, feature values, concept_tags, taxonomy
**Used In**: Feature breakdown tables

### 3. Strategy Context
**From**: StrategyConfig, Prediction
**Fields**: strategy name, strategy type, version, mode, horizon
**Used In**: Strategy report tables

### 4. Historical Validation
**From**: PredictionOutcome
**Fields**: return_pct, direction_correct, max_runup, max_drawdown, exit_reason
**Used In**: Performance tables

### 5. Microstructure/Reaction Data
**From**: MRAOutcome
**Fields**: return_1m, return_5m, return_15m, return_1h, volume_ratio, vwap_distance, range_expansion, continuation_slope, pullback_depth, mra_score
**Used In**: Market reaction tables

## Data Flow Principles

### 1. Cards = Decisions
Cards represent actionable insights, not just data display.

### 2. Tables = Justification  
Tables provide evidence and context for card decisions.

### 3. Contextual Filtering
All data is filtered by:
- Selected ticker
- Selected strategy  
- Selected time horizon
- Applied filters

### 4. No Global Data
Data is always contextual to selected cards, never global dashboard state.

## Example User Flow

1. **User selects**: View="best_picks", Strategy="semantic", Horizon="1M"
2. **System generates**: Multiple cards for top semantic picks over 1 month
3. **Cards display**:
   - Chart card: NVDA forecast with confidence bands
   - Number card: "+12.4% expected return, Rank #1"
   - Table card: Evidence driving the pick
4. **User interacts**: Clicks "Why?" on number card
5. **System shows**: Related evidence, features, and reaction tables
6. **Data remains**: Filtered by semantic strategy, 1M horizon, NVDA ticker

## Benefits

### For Users
- **Consistent Context**: All cards share same controls and filters
- **Multiple Insights**: See multiple opportunities simultaneously  
- **Actionable Focus**: Each card represents a decision point
- **Deep Dive Available**: Tables provide justification for decisions

### For Developers
- **Modular Architecture**: Easy to add new card types
- **Pluggable Providers**: Different data sources per view/strategy
- **Consistent Styling**: Unified design system
- **Testable Components**: Each card type independently testable

### For System
- **Efficient Queries**: Single query serves multiple cards
- **Contextual Caching**: Cache by view/strategy/horizon
- **Scalable Layout**: River layout works for any number of cards
- **Responsive Design**: Cards adapt to screen size

## Future Enhancements

### Card Types
- **Heatmap Card**: Market sentiment/regime heatmaps
- **Gauge Card**: Real-time metrics with gauges
- **Map Card**: Geographic/regional data visualization
- **Tree Map Card**: Hierarchical data visualization

### Interactions
- **Card Expansion**: Click to expand for more detail
- **Card Linking**: Link related cards together
- **Card Export**: Export individual card data
- **Card Sharing**: Share cards with annotations

### Performance
- **Lazy Loading**: Load cards as they come into view
- **Virtual Scrolling**: Handle large numbers of cards efficiently
- **Background Refresh**: Update cards without full reload
- **Smart Caching**: Cache card data by view parameters
