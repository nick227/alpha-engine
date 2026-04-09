# Intelligence Hub — Implementation Plan

## Architecture Overview

The Intelligence Hub needs to be wired to real data sources with proper state management and service layer separation.

## State Management

### IntelligenceHubState
```python
@dataclass
class IntelligenceHubState:
    ticker: str = 'NVDA'
    timeframe: str = '3M'  # 1M, 3M, 6M, 1Y
    horizon: int = 7  # 1, 7, 30
    run_id: Optional[str] = None
    strategy_ids: List[str] = field(default_factory=list)
    selected_strategy: Optional[str] = None
    filter_mode: str = 'All predictions'  # All, Correct only, Incorrect only
```

### State Events
```python
class StateEvent:
    ASSET_CHANGE = 'asset_change'
    TIMEFRAME_CHANGE = 'timeframe_change'
    HORIZON_CHANGE = 'horizon_change'
    STRATEGY_TOGGLE = 'strategy_toggle'
    RUN_CHANGE = 'run_change'
    FILTER_CHANGE = 'filter_change'
    STRATEGY_SELECT = 'strategy_select'
```

## Service Layer Integration

### DashboardService Extension
```python
class DashboardService:
    def get_intelligence_state(self, state: IntelligenceHubState) -> IntelligenceHubDTO:
        """Get complete intelligence hub state with real data"""
        
        # Resolve available tickers
        tickers = self.store.get_available_tickers()
        
        # Resolve prediction runs for ticker
        runs = self.store.get_prediction_runs(state.ticker)
        
        # Resolve active run_id
        run_id = state.run_id or runs[0].id if runs else None
        
        # Load champion matrix
        matrix = self.store.get_champion_comparison_matrix(
            ticker=state.ticker,
            timeframe=state.timeframe
        )
        
        # Load efficiency rankings
        rankings = self.store.rank_strategies_by_efficiency(
            ticker=state.ticker,
            timeframe=state.timeframe
        )
        
        # Load strategy overlays
        overlays = self.store.get_series_comparison(
            ticker=state.ticker,
            run_id=run_id,
            strategy_ids=state.strategy_ids
        )
        
        # Load strategy timeline (optional selected)
        timeline = self.store.get_strategy_timeline(
            ticker=state.ticker,
            run_id=run_id,
            strategy_id=state.selected_strategy
        ) if state.selected_strategy else None
        
        # Load consensus data (optional)
        consensus = self.store.get_consensus_data(
            ticker=state.ticker,
            timeframe=state.timeframe
        )
        
        return IntelligenceHubDTO(
            state=state,
            tickers=tickers,
            runs=runs,
            matrix_rows=matrix,
            strategy_rankings=rankings,
            overlay_series=overlays,
            timeline=timeline,
            consensus=consensus
        )
```

### Data Store Extensions
```python
class DataStore:
    # Champion Matrix
    def get_champion_comparison_matrix(self, ticker: str, timeframe: str) -> List[ChampionRow]:
        """Get champion performance matrix across horizons"""
        pass
    
    # Strategy Rankings
    def rank_strategies_by_efficiency(self, ticker: str, timeframe: str) -> List[StrategyRanking]:
        """Rank strategies by efficiency metrics"""
        pass
    
    # Series Comparison
    def get_series_comparison(self, ticker: str, run_id: str, strategy_ids: List[str]) -> ComparisonData:
        """Get strategy comparison data for overlay charts"""
        pass
    
    # Strategy Timeline
    def get_strategy_timeline(self, ticker: str, run_id: str, strategy_id: str) -> TimelineData:
        """Get detailed timeline for single strategy"""
        pass
    
    # Consensus Data
    def get_consensus_data(self, ticker: str, timeframe: str) -> ConsensusData:
        """Get consensus/ensemble predictions"""
        pass
    
    # Prediction Runs
    def list_prediction_runs(self, ticker: str) -> List[PredictionRun]:
        """List available prediction runs for ticker"""
        pass
```

## DTO Structure

### IntelligenceHubDTO
```python
@dataclass
class IntelligenceHubDTO:
    state: IntelligenceHubState
    tickers: List[str]
    runs: List[PredictionRun]
    matrix_rows: List[ChampionRow]
    strategy_rankings: List[StrategyRanking]
    overlay_series: ComparisonData
    timeline: Optional[TimelineData]
    consensus: Optional[ConsensusData]
```

### Supporting DTOs
```python
@dataclass
class ChampionRow:
    horizon: int
    champion_strategy: str
    alpha: float
    mae: float
    samples: int
    recent_alpha: float

@dataclass
class StrategyRanking:
    strategy_id: str
    efficiency_score: float
    alpha: float
    mae: float
    win_rate: float

@dataclass
class ComparisonData:
    strategies: List[StrategySeries]
    actual_series: PriceSeries
    prediction_points: List[PredictionPoint]

@dataclass
class TimelineData:
    strategy_id: str
    predictions: List[PredictionDetail]
    actual_prices: PriceSeries
    news_events: List[NewsEvent]
    performance_metrics: PerformanceMetrics
```

## UI Rendering Architecture

### Controller Pattern
```python
class IntelligenceHubController:
    def __init__(self, service: DashboardService):
        self.service = service
        self.state = IntelligenceHubState()
        self.callbacks = {}
    
    def on_state_change(self, event_type: str, data: Any):
        """Handle state change events"""
        if event_type == StateEvent.ASSET_CHANGE:
            self.state.ticker = data
            self.reload_matrix_and_rankings()
        elif event_type == StateEvent.TIMEFRAME_CHANGE:
            self.state.timeframe = data
            self.reload_matrix()
        elif event_type == StateEvent.STRATEGY_TOGGLE:
            if data in self.state.strategy_ids:
                self.state.strategy_ids.remove(data)
            else:
                self.state.strategy_ids.append(data)
            self.reload_overlays()
        elif event_type == StateEvent.RUN_CHANGE:
            self.state.run_id = data
            self.reload_all()
        elif event_type == StateEvent.STRATEGY_SELECT:
            self.state.selected_strategy = data
            self.load_timeline()
        
        self.render_new_state()
    
    def reload_all(self):
        """Reload all data components"""
        pass
    
    def reload_matrix_and_rankings(self):
        """Reload champion matrix and efficiency rankings"""
        pass
    
    def reload_matrix(self):
        """Reload only champion matrix"""
        pass
    
    def reload_overlays(self):
        """Reload strategy overlay data"""
        pass
    
    def load_timeline(self):
        """Load selected strategy timeline"""
        pass
    
    def render_new_state(self):
        """Render UI with new state"""
        dto = self.service.get_intelligence_state(self.state)
        render_intelligence_hub(dto)
```

### UI Components
```python
def render_intelligence_hub(dto: IntelligenceHubDTO):
    """Main intelligence hub renderer"""
    
    # Asset selector
    render_asset_selector(dto.tickers, dto.state.ticker)
    
    # Timeframe selector
    render_timeframe_selector(['1M', '3M', '6M', '1Y'], dto.state.timeframe)
    
    # Champion matrix
    render_champion_matrix(dto.matrix_rows)
    
    # Strategy cards (current implementation)
    render_strategy_cards(dto.overlay_series, dto.state.filter_mode)
    
    # Strategy overlays
    render_strategy_overlay_chart(dto.overlay_series)
    
    # Strategy timeline (if selected)
    if dto.timeline:
        render_strategy_timeline(dto.timeline)
    
    # Consensus view (optional)
    if dto.consensus:
        render_consensus_view(dto.consensus)
```

## State Transitions

### Event → Action Mapping
| Event | Data | Action | Reload Components |
|--------|-------|---------|------------------|
| ASSET_CHANGE | ticker | Update state.ticker | matrix + rankings |
| TIMEFRAME_CHANGE | timeframe | Update state.timeframe | matrix |
| HORIZON_CHANGE | horizon | Update state.horizon | overlays |
| STRATEGY_TOGGLE | strategy_id | Toggle in state.strategy_ids | overlays |
| RUN_CHANGE | run_id | Update state.run_id | all |
| FILTER_CHANGE | filter_mode | Update state.filter_mode | cards |
| STRATEGY_SELECT | strategy_id | Update state.selected_strategy | timeline |

### Component Dependencies
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Asset Select   │───▶│   Matrix Rows   │───▶│  Strategy Cards  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Timeframe      │───▶│   Rankings      │───▶│   Overlays      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Run Select    │───▶│   Consensus     │───▶│   Timeline      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Implementation Steps

### Phase 1: Data Layer
1. Extend DataStore with intelligence hub methods
2. Implement champion matrix queries
3. Implement strategy ranking queries
4. Implement comparison data queries

### Phase 2: Service Layer
1. Add get_intelligence_state to DashboardService
2. Implement DTO classes
3. Add state management utilities

### Phase 3: UI Integration
1. Replace simulation data with service calls
2. Implement state change handlers
3. Add controller pattern
4. Wire up UI events

### Phase 4: Advanced Features
1. Strategy timeline view
2. Consensus integration
3. Advanced filtering
4. Export capabilities

## Migration Strategy

### Current → Target
| Current Component | Target Component | Migration Approach |
|------------------|-------------------|-------------------|
| get_asset_config() | DataStore.get_ticker_config() | Replace simulation with real data |
| generate_strategy_data() | DataStore.get_strategy_performance() | Use real prediction data |
| get_news_semantics() | DataStore.get_news_events() | Use real news data |
| render_strategy_card() | render_strategy_comparison() | Adapt to real data structure |

### Backward Compatibility
- Maintain current UI structure during migration
- Gradual replacement of data sources
- Feature flags for new vs old implementation
- A/B testing capability

This architecture provides:
- **State consistency** across all components
- **Real data integration** with proper service layer
- **Scalable performance** through efficient caching
- **Extensible design** for future features
- **Clean separation** of concerns
