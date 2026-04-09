"""
Intelligence Hub Controller

Manages state transitions and orchestrates data loading through service layer.
"""

from app.ui.intelligence.intelligence_hub_state import IntelligenceHubState, StateEvent
from app.ui.intelligence.intelligence_hub_dto import IntelligenceHubDTO


class IntelligenceHubController:
    """Controller for Intelligence Hub state management and data orchestration"""
    
    def __init__(self, service):
        self.service = service
        self.state = IntelligenceHubState()
        self._current_dto = None
    
    def on_state_change(self, event_type: StateEvent, data):
        """Handle state change events and trigger appropriate reloads"""
        if event_type == StateEvent.ASSET_CHANGE:
            self.state.ticker = data
            self._reload_matrix_and_rankings()
        elif event_type == StateEvent.TIMEFRAME_CHANGE:
            self.state.timeframe = data
            self._reload_matrix()
        elif event_type == StateEvent.HORIZON_CHANGE:
            self.state.horizon = data
            self._reload_overlays()
        elif event_type == StateEvent.STRATEGY_TOGGLE:
            if data in self.state.strategy_ids:
                self.state.strategy_ids.remove(data)
            else:
                self.state.strategy_ids.append(data)
            self._reload_overlays()
        elif event_type == StateEvent.RUN_CHANGE:
            self.state.run_id = data
            self._reload_all()
        elif event_type == StateEvent.FILTER_CHANGE:
            self.state.filter_mode = data
            self._reload_cards()
        elif event_type == StateEvent.STRATEGY_SELECT:
            self.state.selected_strategy = data
            self._load_timeline()
        
        self._render_new_state()
    
    def _reload_all(self):
        """Reload all data components"""
        self._current_dto = self.service.get_intelligence_state(self.state)
    
    def _reload_matrix_and_rankings(self):
        """Reload champion matrix and efficiency rankings"""
        self._current_dto = self.service.get_intelligence_state(self.state)
    
    def _reload_matrix(self):
        """Reload only champion matrix"""
        self._current_dto = self.service.get_intelligence_state(self.state)
    
    def _reload_overlays(self):
        """Reload strategy overlay data"""
        self._current_dto = self.service.get_intelligence_state(self.state)
    
    def _reload_cards(self):
        """Reload strategy cards only"""
        self._current_dto = self.service.get_intelligence_state(self.state)
    
    def _load_timeline(self):
        """Load selected strategy timeline"""
        self._current_dto = self.service.get_intelligence_state(self.state)
    
    def _render_new_state(self):
        """Render UI with new state"""
        if self._current_dto is None:
            self._current_dto = self.service.get_intelligence_state(self.state)
        
        from app.ui.intelligence.intelligence_hub_renderer import render_intelligence_hub
        render_intelligence_hub(self._current_dto)
    
    def get_current_state(self) -> IntelligenceHubState:
        """Get current state for external access"""
        return self.state
    
    def set_initial_state(self, ticker: str = None, timeframe: str = None, run_id: str = None):
        """Set initial state from URL parameters or defaults"""
        if ticker:
            self.state.ticker = ticker
        if timeframe:
            self.state.timeframe = timeframe
        if run_id:
            self.state.run_id = run_id
        
        self._reload_all()
