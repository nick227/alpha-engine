"""
Enhanced Alpha Engine Dashboard with Plotly Integration

This is the enhanced version of the dashboard with integrated time-series charts.
It maintains all original functionality while adding rich visualizations.

Usage:
    - Replace the main() function call in dashboard.py with main_enhanced()
    - Or use this as a separate dashboard page
"""

import streamlit as st
from typing import Optional, Dict, Any, List

from app.ui.middle.dashboard_service import DashboardService, arrow
from app.ui.dashboard_charts import integrate_charts_into_dashboard

# Optional dependency for auto-refresh functionality
try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore
except Exception:  # pragma: no cover
    st_autorefresh = None

# Import original dashboard components
from app.ui.dashboard import (
    DashboardState,
    render_header,
    render_sidebar_controls,
    render_champions_section,
    render_challengers_section,
    render_signals_section,
    get_dashboard_service,
    DEFAULT_DB_PATH
)


def main_enhanced():
    """Enhanced dashboard with integrated Plotly charts."""
    
    # Initialize service and state
    service = get_dashboard_service(DEFAULT_DB_PATH)
    state = DashboardState(service)
    
    # Render header
    render_header()
    
    # Render sidebar controls and check if refresh is needed
    refresh_needed = render_sidebar_controls(state)
    
    # Refresh data if needed (first load or filters changed)
    if refresh_needed or not hasattr(state, '_initialized'):
        state.refresh_data()
        state._initialized = True
    
    # Only render main content if we have a tenant selected
    if not state.tenant_id:
        st.warning("Please select a tenant from the sidebar to view dashboard data.")
        return
    
    # Chart toggle
    st.sidebar.markdown("---")
    st.sidebar.header("Visualization")
    show_charts = st.sidebar.checkbox(
        "Enable Charts", 
        value=True,
        help="Toggle interactive time-series charts"
    )
    
    if show_charts:
        # Enhanced dashboard with charts
        integrate_charts_into_dashboard(service, state.tenant_id, state.ticker)
        st.divider()
        
        # Compact metrics section below charts
        with st.expander("Detailed Metrics", expanded=False):
            render_champions_section(state)
            st.divider()
            render_challengers_section(state)
    else:
        # Original dashboard layout
        render_champions_section(state)
        st.divider()
        render_challengers_section(state)
        st.divider()
        render_signals_section(state)
    
    # Footer with additional info
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.caption("Last Updated: " + st.session_state.get("last_update", "Never"))
    
    with col2:
        if state.loop_health and state.loop_health.last_write_at:
            st.caption(f"DB Last Write: {state.loop_health.last_write_at}")
    
    with col3:
        st.caption("Alpha Engine v3.0 - Enhanced Dashboard")


if __name__ == "__main__":
    main_enhanced()
