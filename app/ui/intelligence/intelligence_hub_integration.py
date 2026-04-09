"""
Intelligence Hub Integration

Complete integration layer that replaces simulation with real data service.
"""

import streamlit as st
from app.ui.intelligence.intelligence_hub_session import initialize_session_state, get_session_events, emit_state_change
from app.ui.intelligence.intelligence_hub_controller import IntelligenceHubController
from app.ui.intelligence.intelligence_hub_renderer import render_intelligence_hub
from app.ui.middle.dashboard_service import DashboardService


def intelligence_hub_integration(service: DashboardService):
    """
    Complete Intelligence Hub integration with real data.
    
    Replaces simulation-based interface with:
    - State management via controller pattern
    - Real data through DashboardService  
    - Event-driven UI updates
    - Clean separation of concerns
    """
    
    # Initialize session state
    initialize_session_state()
    
    # Initialize controller
    controller = IntelligenceHubController(service)
    
    # Process any pending session events
    events = get_session_events()
    for event_type, data in events:
        controller.on_state_change(event_type, data)
    
    # Parse URL parameters for initial state
    query_params = st.experimental_get_query_params()
    
    # Set initial state from URL parameters
    if 'ticker' in query_params:
        controller.on_state_change('asset_change', query_params['ticker'][0])
    
    if 'timeframe' in query_params:
        controller.on_state_change('timeframe_change', query_params['timeframe'][0])
    
    if 'run' in query_params:
        controller.on_state_change('run_change', query_params['run'][0])
    
    # Set initial state and trigger initial load
    controller.set_initial_state(
        ticker=query_params.get('ticker', [None])[0] if 'ticker' in query_params else None,
        timeframe=query_params.get('timeframe', [None])[0] if 'timeframe' in query_params else None,
        run_id=query_params.get('run', [None])[0] if 'run' in query_params else None
    )
    
    # Render the interface with real data
    render_intelligence_hub(controller.get_current_state())


def create_intelligence_hub_page():
    """
    Create the Intelligence Hub page with real data integration.
    """
    
    # Initialize service
    service = DashboardService()
    
    # Render integrated intelligence hub
    intelligence_hub_integration(service)
