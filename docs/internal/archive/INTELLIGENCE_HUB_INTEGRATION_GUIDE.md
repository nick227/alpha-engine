# Intelligence Hub Integration Guide

## Overview

The Intelligence Hub has been successfully integrated with real data sources using a clean service-oriented architecture. This guide explains the new structure and how to use it.

## Architecture

### Component Structure
```
┌─────────────────────────────────────────────────────────┐
│                Intelligence Hub Integration                │
├─────────────────────────────────────────────────────────┤
│  State Management    │  Service Layer    │  Data Access     │
│  IntelligenceHubState │  DashboardService │  EngineReadStore  │
│  StateEvent         │  get_intelligence_state()    │  SQL queries     │
└─────────────────────────────────────────────────────────┘
```

### Key Files
- `app/ui/intelligence_hub_state.py` - State management
- `app/ui/intelligence_hub_dto.py` - Data transfer objects
- `app/ui/intelligence_hub_controller.py` - State orchestration
- `app/ui/intelligence_hub_renderer.py` - UI rendering
- `app/ui/intelligence_hub_session.py` - Session management
- `app/ui/intelligence_hub_integration.py` - Main integration
- `app/ui/intelligence_hub_main.py` - Entry point

## Usage

### Basic Integration
```python
from app.ui.intelligence_hub_integration import create_intelligence_hub_page

# In your Streamlit app
create_intelligence_hub_page()
```

### Advanced Usage with Custom State
```python
from app.ui.intelligence_hub_integration import intelligence_hub_integration
from app.ui.intelligence_hub_state import IntelligenceHubState

# Create custom state
state = IntelligenceHubState(
    ticker='AAPL',
    timeframe='6M',
    horizon=30,
    strategy_ids=['sentiment-v1', 'momentum-v1'],
    filter_mode='Correct only'
)

# Render with custom state
intelligence_hub_integration(service, state)
```

## Features

### Real Data Sources
- **Champion Matrix**: Cross-strategy performance comparison
- **Strategy Rankings**: Efficiency-based strategy evaluation
- **Performance Overlays**: Multi-strategy comparison charts
- **Strategy Timeline**: Detailed prediction history
- **Consensus Data**: Ensemble predictions and market sentiment

### State Management
- **Event-Driven**: All UI changes trigger state events
- **Efficient Reloads**: Only reload changed data components
- **Session Persistence**: State preserved across page refreshes
- **URL Parameters**: Initial state from query parameters

### UI Components
- **Asset Selector**: Dropdown with available tickers
- **Timeframe Selector**: 1M, 3M, 6M, 1Y options
- **Champion Matrix**: Performance grid with alpha rankings
- **Strategy Rankings**: Top performing strategies by efficiency
- **Overlays Chart**: Multi-strategy performance comparison
- **Timeline View**: Detailed strategy prediction history
- **Consensus View**: Market consensus and ensemble data

## Migration from Simulation

### What Changed
- **Data Source**: Simulation data → Real prediction data
- **State Management**: Ad-hoc → Structured state pattern
- **Service Layer**: Direct DB access → Service orchestration
- **UI Updates**: Manual refresh → Event-driven updates

### Benefits
- **Real Performance**: Actual strategy performance, not simulated
- **Scalability**: Efficient data loading and caching
- **Maintainability**: Clean separation of concerns
- **Extensibility**: Easy to add new features
- **Consistency**: Single source of truth for all UI components

## Configuration

### URL Parameters
Access Intelligence Hub with specific state:
```
/intelligence-hub?ticker=AAPL&timeframe=6M&run=run_123
```

### Session Events
The system automatically processes these state change events:
- `asset_change`: New ticker selected
- `timeframe_change`: New timeframe selected
- `run_change`: New prediction run selected
- `strategy_toggle`: Strategy added/removed from comparison
- `filter_change`: Prediction filter changed
- `strategy_select`: Strategy selected for timeline view

## Performance

### Efficient Data Loading
- **Matrix Only**: Asset/timeframe changes reload champion matrix
- **Overlays Only**: Strategy changes reload comparison data
- **Timeline Only**: Strategy selection loads detailed history
- **Full Reload**: Run changes reload all data components

### Caching Strategy
- Service layer implements intelligent caching
- State changes trigger targeted reloads
- No redundant database queries
- Consistent data across all UI components

This integration transforms Intelligence Hub from a simulation tool into a production-ready analytical platform with real strategy performance data.
