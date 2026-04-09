"""
Intelligence Hub Service Layer Integration

Extends DashboardService with intelligence-specific methods.
All intelligence logic, scoring, and selection happens here.
"""

import streamlit as st
from app.ui.intelligence.intelligence_hub_state import IntelligenceHubState, StateEvent
from app.ui.intelligence.intelligence_hub_dto import IntelligenceHubDTO, StrategyRanking, ComparisonData, TimelineData, PerformanceMetrics
from app.ui.intelligence.intelligence_hub_renderer import render_intelligence_hub
from app.ui.intelligence.intelligence_hub_session import initialize_session_state, get_session_events
from app.ui.middle.dashboard_service import DashboardService


class IntelligenceHubService:
    """
    Service layer for Intelligence Hub.
    
    All intelligence logic, scoring, and selection happens here.
    UI only consumes DTOs.
    """
    
    def __init__(self, dashboard_service: DashboardService):
        self.service = dashboard_service
        self.state = IntelligenceHubState()
    
    def get_intelligence_state(self, state: IntelligenceHubState) -> IntelligenceHubDTO:
        """
        Get complete intelligence hub state with real data.
        All intelligence logic happens here.
        """
        
        # Resolve available tickers
        tickers = self.service.list_tickers()
        
        # Resolve prediction runs for ticker
        runs = self.service.list_prediction_runs()
        
        # Resolve active run_id
        run_id = state.run_id or (runs[0].id if runs else None)
        
        # Load champion matrix
        matrix = self.service.get_champion_matrix(
            ticker=state.ticker,
            timeframe=state.timeframe
        )
        
        # Load efficiency rankings
        rankings = self.service.get_efficiency_rankings(
            ticker=state.ticker,
            timeframe=state.timeframe
        )
        
        # Load strategy overlays
        overlays = self._get_strategy_overlays(
            run_id=run_id,
            ticker=state.ticker,
            strategy_ids=state.strategy_ids
        ) if state.strategy_ids else None
        
        # Load strategy timeline (optional selected)
        timeline = self._get_strategy_timeline(
            ticker=state.ticker,
            strategy_id=state.selected_strategy,
            limit=90
        ) if state.selected_strategy else None
        
        # Load consensus data (optional)
        consensus = self.service.get_latest_consensus(ticker=state.ticker)
        
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
    
    def _get_strategy_overlays(self, run_id: str, ticker: str, strategy_ids: list) -> ComparisonData:
        """Get multi-strategy overlay data"""
        
        overlay_data = self.service.get_multi_strategy_overlay(
            run_id=run_id,
            ticker=ticker,
            strategy_ids=strategy_ids
        )
        
        # Convert to ComparisonData DTO
        strategies = []
        actual_series = []
        
        if overlay_data and 'strategies' in overlay_data:
            for strategy in overlay_data['strategies']:
                strategies.append({
                    'strategy_id': strategy['strategy_id'],
                    'predicted': [
                        {'timestamp': p['x'], 'value': p['y']} 
                        for p in strategy['predicted']
                    ],
                    'color': self._get_strategy_color(strategy['strategy_id'])
                })
        
        if overlay_data and 'actual' in overlay_data:
            actual_series = [
                {'timestamp': p['x'], 'value': p['y']} 
                for p in overlay_data['actual']
            ]
        
        return ComparisonData(
            strategies=strategies,
            actual_series=actual_series,
            prediction_points=[]  # Could be added if needed
        )
    
    def _get_strategy_timeline(self, ticker: str, strategy_id: str, limit: int = 90) -> TimelineData:
        """Get detailed strategy timeline"""
        
        timeline_views = self.service.get_strategy_timeline(
            ticker=ticker,
            strategy_id=strategy_id,
            limit=limit
        )
        
        if not timeline_views:
            return None
        
        # Convert to TimelineData DTO
        predictions = []
        for view in timeline_views:
            predictions.append({
                'timestamp': view.run_date,
                'strategy_id': view.strategy_id,
                'predicted_return': view.pred_return_pct,
                'actual_return': view.actual_return_pct,
                'direction_correct': view.direction_correct,
                'entry_price': view.entry_price,
                'target_price': view.target_price
            })
        
        # Calculate performance metrics
        if predictions:
            alpha = sum(p['predicted_return'] * p['actual_return'] for p in predictions) / len(predictions)
            win_rate = sum(1 for p in predictions if p['direction_correct']) / len(predictions)
            mae = sum(abs(p['predicted_return'] - p['actual_return']) for p in predictions) / len(predictions)
            total_return = sum(p['actual_return'] for p in predictions)
            
            performance_metrics = PerformanceMetrics(
                alpha=alpha,
                mae=mae * 100,  # Convert to percentage
                win_rate=win_rate,
                total_return=total_return,
                samples=len(predictions)
            )
        else:
            performance_metrics = PerformanceMetrics(0, 0, 0, 0, 0)
        
        return TimelineData(
            strategy_id=strategy_id,
            predictions=predictions,
            actual_prices=[],  # Could be added if needed
            news_events=[],    # Could be added if needed
            performance_metrics=performance_metrics
        )
    
    def _get_strategy_color(self, strategy_id: str) -> str:
        """Get color for strategy based on ID"""
        colors = {
            'sentiment-v1': '#378ADD',
            'momentum-v1': '#1D9E75',
            'mean-rev-v1': '#D85A30',
            'breakout-v1': '#9F77DD',
        }
        
        # Extract base strategy ID from horizon-specific ID
        base_id = strategy_id.split(' @ ')[0] if ' @ ' in strategy_id else strategy_id
        return colors.get(base_id, '#378ADD')
    
    def on_state_change(self, event_type: StateEvent, data):
        """Handle state change events and trigger appropriate reloads"""
        
        if event_type == StateEvent.ASSET_CHANGE:
            self.state.ticker = data
        elif event_type == StateEvent.TIMEFRAME_CHANGE:
            self.state.timeframe = data
        elif event_type == StateEvent.HORIZON_CHANGE:
            self.state.horizon = data
        elif event_type == StateEvent.STRATEGY_TOGGLE:
            if data in self.state.strategy_ids:
                self.state.strategy_ids.remove(data)
            else:
                self.state.strategy_ids.append(data)
        elif event_type == StateEvent.RUN_CHANGE:
            self.state.run_id = data
        elif event_type == StateEvent.FILTER_CHANGE:
            self.state.filter_mode = data
        elif event_type == StateEvent.STRATEGY_SELECT:
            self.state.selected_strategy = data
    
    def get_current_state(self) -> IntelligenceHubState:
        """Get current state"""
        return self.state
    
    def render_intelligence_hub_page(self):
        """Render the complete intelligence hub page"""
        
        # Initialize session state
        initialize_session_state()
        
        # Process any pending session events
        events = get_session_events()
        for event_type, data in events:
            self.on_state_change(event_type, data)
        
        # Get intelligence state
        dto = self.get_intelligence_state(self.state)
        
        # Render the interface
        render_intelligence_hub(dto)


def intelligence_hub_main(dashboard_service: DashboardService):
    """
    Main entry point for Intelligence Hub with clean architecture.
    
    This function demonstrates the proper layering:
    - UI only renders DTOs
    - Service handles all intelligence logic
    - Store provides raw data
    """
    
    # Create intelligence hub service
    ih_service = IntelligenceHubService(dashboard_service)
    
    # Render the page
    ih_service.render_intelligence_hub_page()
