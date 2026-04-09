"""
Intelligence Hub Session Management

Handles Streamlit session state events and forwards to controller.
"""

import streamlit as st
from app.ui.intelligence.intelligence_hub_state import StateEvent


def initialize_session_state():
    """Initialize session state variables for intelligence hub"""
    
    # Initialize session state variables if they don't exist
    session_vars = [
        'asset_change',
        'timeframe_change', 
        'timeframe_change',
        'horizon_change',
        'strategy_toggle',
        'run_change',
        'filter_change',
        'strategy_select',
        'ih_current_state'
    ]
    
    for var in session_vars:
        if var not in st.session_state:
            st.session_state[var] = None


def get_session_events():
    """Process pending session state events and return event list"""
    
    events = []
    
    # Check for state change events
    if 'asset_change' in st.session_state and st.session_state.asset_change:
        events.append((StateEvent.ASSET_CHANGE, st.session_state.asset_change))
        st.session_state.asset_change = None
    
    if 'timeframe_change' in st.session_state and st.session_state.timeframe_change:
        events.append((StateEvent.TIMEFRAME_CHANGE, st.session_state.timeframe_change))
        st.session_state.timeframe_change = None
    
    if 'horizon_change' in st.session_state and st.session_state.horizon_change:
        events.append((StateEvent.HORIZON_CHANGE, st.session_state.horizon_change))
        st.session_state.horizon_change = None
    
    if 'strategy_toggle' in st.session_state and st.session_state.strategy_toggle:
        events.append((StateEvent.STRATEGY_TOGGLE, st.session_state.strategy_toggle))
        st.session_state.strategy_toggle = None
    
    if 'run_change' in st.session_state and st.session_state.run_change:
        events.append((StateEvent.RUN_CHANGE, st.session_state.run_change))
        st.session_state.run_change = None
    
    if 'filter_change' in st.session_state and st.session_state.filter_change:
        events.append((StateEvent.FILTER_CHANGE, st.session_state.filter_change))
        st.session_state.filter_change = None
    
    if 'strategy_select' in st.session_state and st.session_state.strategy_select:
        events.append((StateEvent.STRATEGY_SELECT, st.session_state.strategy_select))
        st.session_state.strategy_select = None
    
    return events


def emit_state_change(event_type: StateEvent, data):
    """Emit a state change event via session state"""
    
    if event_type == StateEvent.ASSET_CHANGE:
        st.session_state.asset_change = data
    elif event_type == StateEvent.TIMEFRAME_CHANGE:
        st.session_state.timeframe_change = data
    elif event_type == StateEvent.HORIZON_CHANGE:
        st.session_state.horizon_change = data
    elif event_type == StateEvent.STRATEGY_TOGGLE:
        st.session_state.strategy_toggle = data
    elif event_type == StateEvent.RUN_CHANGE:
        st.session_state.run_change = data
    elif event_type == StateEvent.FILTER_CHANGE:
        st.session_state.filter_change = data
    elif event_type == StateEvent.STRATEGY_SELECT:
        st.session_state.strategy_select = data
