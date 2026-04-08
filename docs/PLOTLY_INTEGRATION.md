# Plotly Integration for Alpha Engine Dashboard

## Overview

This document outlines the integration of Plotly-based time-series charts into the Alpha Engine dashboard, providing rich visualizations for market data, strategy performance, and signal flow analysis.

## Architecture

### Components

1. **Chart Components** (`app/ui/components/chart.py`)
   - `TimeSeriesChart`: Reusable time-series component
   - `create_consensus_timeline()`: Consensus confidence over time
   - `create_strategy_performance_chart()`: Strategy comparison charts
   - `create_signal_flow_chart()`: Real-time signal visualization
   - `create_multi_axis_chart()`: Complex multi-axis visualizations

2. **Dashboard Integration** (`app/ui/dashboard_charts.py`)
   - `DashboardCharts`: Data fetching and chart management
   - Chart rendering functions for each dashboard section
   - Integration helpers for seamless dashboard embedding

3. **Enhanced Dashboard** (`app/ui/dashboard_enhanced.py`)
   - Full dashboard with integrated charts
   - Toggle between original and enhanced views
   - Maintains all existing functionality

## Key Features

### 1. Hero Section - Market Overview
- **Multi-axis visualization**: Confidence, volume, and volatility
- **Regime context**: Background shading for market regimes
- **Interactive controls**: Time range, metric toggles
- **Real-time updates**: Auto-refresh support

### 2. Strategy Performance Timeline
- **Comparative analysis**: Champion vs challenger metrics
- **Multiple metrics**: Win rate, alpha, stability tracking
- **Historical context**: Performance evolution over time
- **Interactive filtering**: Strategy selection and time ranges

### 3. Signal Flow Visualization
- **Direction indicators**: Buy/sell signal scatter plot
- **Confidence trends**: Moving average of signal confidence
- **Strategy filtering**: Filter by strategy type
- **Statistical summary**: Signal counts and averages

### 4. Consensus Timeline
- **Confidence evolution**: Track consensus confidence over time
- **Regime context**: Visual regime change indicators
- **Weight visualization**: Participating strategy weights
- **Interactive exploration**: Zoom and pan capabilities

## Implementation Guide

### Installation

1. Add Plotly to requirements:
```bash
pip install plotly>=5.17.0
```

2. The dependency is already added to `requirements.txt`

### Usage Options

#### Option 1: Replace Main Dashboard
Replace the main function in `dashboard.py`:

```python
# In dashboard.py, replace the main() function with:
from app.ui.dashboard_enhanced import main_enhanced

if __name__ == "__main__":
    main_enhanced()
```

#### Option 2: Use as Separate Page
Create a new dashboard page:

```python
# In pages/enhanced_dashboard.py
from app.ui.dashboard_enhanced import main_enhanced

main_enhanced()
```

#### Option 3: Toggle Integration
Add chart toggle to existing dashboard:

```python
# In dashboard.py main() function, add:
if st.sidebar.checkbox("Enable Charts", value=True):
    from app.ui.dashboard_charts import integrate_charts_into_dashboard
    integrate_charts_into_dashboard(service, state.tenant_id, state.ticker)
```

## Data Requirements

### Service Layer Extensions

The charts require additional data fetching methods. Implement these in `DashboardService`:

```python
def get_consensus_history(self, tenant_id: str, ticker: str, hours: int) -> List[Dict]:
    """Fetch historical consensus data"""
    # Implementation needed
    
def get_strategy_performance_history(self, tenant_id: str, hours: int) -> Dict[str, List[Dict]]:
    """Fetch historical strategy performance data"""
    # Implementation needed
    
def get_signal_history(self, tenant_id: str, ticker: Optional[str], hours: int) -> List[Dict]:
    """Fetch historical signal data"""
    # Implementation needed
```

### Database Schema Considerations

Ensure your database has indexes for time-based queries:

```sql
-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_consensus_timestamp ON consensus_signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_predictions_timestamp ON predictions(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
```

## Customization

### Color Schemes

Modify colors in `chart.py`:

```python
DEFAULT_COLORS = {
    'primary': '#1f77b4',    # Change to brand color
    'secondary': '#ff7f0e', 
    'success': '#2ca02c',
    'danger': '#d62728',
    # ... other colors
}
```

### Chart Themes

The charts automatically support Streamlit's dark/light theme. To customize further:

```python
fig.update_layout(
    template="plotly_white",  # or "plotly_dark"
    font_family="Arial, sans-serif",
    # ... other layout options
)
```

### Time Ranges

Add custom time ranges in chart components:

```python
TIME_RANGES = {
    "1H": 1,
    "6H": 6,
    "24H": 24,
    "3D": 72,
    "7D": 168,
    "30D": 720  # Add 30 days
}
```

## Performance Considerations

### Data Caching

- Charts use `@st.cache_data` for expensive operations
- Consider implementing data sampling for large datasets
- Use time-based cache invalidation for real-time data

### Optimization Tips

1. **Limit data points**: Sample large datasets for better performance
2. **Lazy loading**: Load chart data only when charts are visible
3. **Background refresh**: Use `st_autorefresh` for real-time updates
4. **Database optimization**: Ensure proper indexes for time queries

## Troubleshooting

### Common Issues

1. **Chart not rendering**: Check Plotly installation and import
2. **No data displayed**: Verify data fetching methods return proper format
3. **Performance issues**: Reduce data points or add caching
4. **Theme conflicts**: Check Streamlit theme compatibility

### Debug Mode

Add debug information:

```python
# In chart components
if st.sidebar.checkbox("Debug Mode"):
    st.json(chart_data)  # Display raw data
    st.write(f"Data points: {len(chart_data)}")
```

## Future Enhancements

### Planned Features

1. **Real-time streaming**: WebSocket integration for live data
2. **Advanced analytics**: Technical indicators and overlays
3. **Export capabilities**: Chart export to PNG/SVG
4. **Custom indicators**: User-defined technical indicators
5. **Multi-asset support**: Compare multiple tickers simultaneously

### Extension Points

- Custom chart types (candlestick, heatmap, etc.)
- Additional data sources (news sentiment, social media)
- Alert system integration
- Portfolio visualization components

## Support

For questions or issues:
1. Check the troubleshooting section
2. Verify data source connections
3. Test with sample data first
4. Review Plotly documentation for advanced customization

---

**Note**: This integration maintains full backward compatibility. The original dashboard functionality remains intact, with charts as an optional enhancement.
