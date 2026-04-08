"""
Alpha Engine Dashboard - Main UI Component

This module provides the main dashboard interface for the Alpha Engine v3.0.
It follows Single Responsibility Principle by separating concerns:
- Configuration and initialization
- Data fetching and state management  
- UI rendering and layout
- Component-specific display functions

Author: Alpha Engine Team
Version: 3.0
"""

import streamlit as st
from typing import Optional, Dict, Any, List, Tuple

from app.ui.middle.dashboard_service import DashboardService, arrow

# Optional dependency for auto-refresh functionality
try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore  # pylint: disable=import-error
except Exception:  # pragma: no cover
    st_autorefresh = None

# ============================================================================
# CONFIGURATION & INITIALIZATION
# ============================================================================

# Page configuration
st.set_page_config(
    layout="wide",
    page_title="Alpha Engine Dashboard",
    page_icon="🚀"
)

# Constants
DEFAULT_DB_PATH = "data/alpha.db"
DEFAULT_MIN_PREDICTIONS = 5
DEFAULT_SIGNALS_LIMIT = 25
DEFAULT_REFRESH_INTERVAL = 2000

# Session state keys (basic/fallback dashboard only)
_BASIC_STATE_KEY = "alpha_engine_basic_dashboard_state"
_WIDGET_PREFIX = "alpha_engine_basic_dashboard_"


@st.cache_resource
def get_dashboard_service(db_path: str) -> DashboardService:
    """
    Initialize and cache the dashboard service instance.
    
    Args:
        db_path: Path to the SQLite database
        
    Returns:
        DashboardService: Cached service instance
    """
    return DashboardService(db_path=db_path)


# ============================================================================
# DATA FETCHING & STATE MANAGEMENT
# ============================================================================

class DashboardState:
    """Manages dashboard state and data fetching operations."""
    
    def __init__(self, service: DashboardService):
        self.service = service
        self.tenant_id: Optional[str] = None
        self.ticker: Optional[str] = None
        self.min_predictions: int = DEFAULT_MIN_PREDICTIONS
        self.signals_limit: int = DEFAULT_SIGNALS_LIMIT
        self.filter_signals_to_ticker: bool = True
        
        # Data cache
        self._champions: Optional[Dict[str, Any]] = None
        self._challengers: Optional[Dict[str, Any]] = None
        self._consensus: Optional[Any] = None
        self._loop_health: Optional[Any] = None
        self._signals: Optional[List[Any]] = None
        self._rankings: Optional[List[Any]] = None
        self._last_refresh_fingerprint: Optional[Tuple[Any, ...]] = None
    
    def refresh_data(self):
        """Refresh all dashboard data from the service."""
        if not self.tenant_id:
            return
            
        self._champions = self.service.get_champions(
            tenant_id=self.tenant_id, 
            min_predictions=self.min_predictions
        )
        self._challengers = self.service.get_challengers(
            tenant_id=self.tenant_id, 
            min_predictions=self.min_predictions
        )
        
        if self.ticker:
            self._consensus = self.service.get_latest_consensus(
                tenant_id=self.tenant_id, 
                ticker=self.ticker
            )
        
        self._loop_health = self.service.get_loop_health(tenant_id=self.tenant_id)
        
        ticker_filter = self.ticker if (self.filter_signals_to_ticker and self.ticker) else None
        self._signals = self.service.get_recent_signals(
            tenant_id=self.tenant_id,
            ticker=ticker_filter,
            limit=self.signals_limit
        )

        
        self._rankings = self.service.get_target_rankings(
            tenant_id=self.tenant_id,
            limit=10
        )

    def refresh_if_needed(self, *, force: bool = False) -> None:
        """
        Refresh data only when dashboard inputs changed (or when forced).
        """
        if not self.tenant_id:
            self._last_refresh_fingerprint = None
            return

        fingerprint = (
            self.tenant_id,
            self.ticker,
            self.min_predictions,
            self.signals_limit,
            self.filter_signals_to_ticker,
        )
        if force or fingerprint != self._last_refresh_fingerprint:
            self.refresh_data()
            self._last_refresh_fingerprint = fingerprint
    
    @property
    def champions(self) -> Dict[str, Any]:
        """Get champions data."""
        return self._champions or {}
    
    @property
    def challengers(self) -> Dict[str, Any]:
        """Get challengers data."""
        return self._challengers or {}
    
    @property
    def consensus(self) -> Optional[Any]:
        """Get consensus data."""
        return self._consensus
    
    @property
    def loop_health(self) -> Optional[Any]:
        """Get loop health data."""
        return self._loop_health
    
    @property
    def signals(self) -> List[Any]:
        """Get signals data."""
        return self._signals or []

    @property
    def rankings(self) -> List[Any]:
        """Get rankings data."""
        return self._rankings or []


# ============================================================================
# UI COMPONENTS
# ============================================================================

def render_header():
    """Render the dashboard header."""
    st.title("🚀 Alpha Engine v3.0")
    st.caption("Recursive • Self-learning • Dual Track")


def _get_basic_state(service: DashboardService) -> DashboardState:
    """
    Get a per-session DashboardState for the basic (fallback) dashboard.
    """
    existing = st.session_state.get(_BASIC_STATE_KEY)
    if isinstance(existing, DashboardState) and existing.service is service:
        return existing

    state = DashboardState(service)
    st.session_state[_BASIC_STATE_KEY] = state
    return state


def render_sidebar_controls(state: DashboardState) -> bool:
    """
    Render sidebar controls and update state based on user input.
    
    Args:
        state: Dashboard state instance to update
        
    Returns:
        bool: True if data should be refreshed
    """
    refresh_needed = False
    
    # Data Selection Section
    st.sidebar.header("📊 Data")
    
    tenants = state.service.list_tenants()
    if not tenants:
        state.tenant_id = None
        state.ticker = None
        st.sidebar.warning("⚠️ No tenants found in DB.")
        return False

    tenant_key = f"{_WIDGET_PREFIX}tenant"
    if tenant_key in st.session_state and st.session_state[tenant_key] not in tenants:
        st.session_state[tenant_key] = tenants[0]

    new_tenant_id = st.sidebar.selectbox(
        "Tenant", 
        options=tenants, 
        index=tenants.index(state.tenant_id) if state.tenant_id in tenants else 0,
        key=tenant_key,
        help="Select the tenant to view data for"
    )
    
    if new_tenant_id != state.tenant_id:
        state.tenant_id = new_tenant_id
        refresh_needed = True
    
    if state.tenant_id:
        all_tickers = state.service.list_tickers(tenant_id=state.tenant_id)
        if all_tickers:
            ticker_key = f"{_WIDGET_PREFIX}ticker"
            if ticker_key in st.session_state and st.session_state[ticker_key] not in all_tickers:
                st.session_state[ticker_key] = all_tickers[0]

            new_ticker = st.sidebar.selectbox(
                "Ticker", 
                options=all_tickers, 
                index=all_tickers.index(state.ticker) if state.ticker in all_tickers else 0,
                key=ticker_key,
                help="Select the ticker to view data for"
            )
            if new_ticker != state.ticker:
                state.ticker = new_ticker
                refresh_needed = True
        else:
            state.ticker = None
            st.sidebar.warning("⚠️ No tickers found in DB (predictions).")
    
    # Auto-refresh Section
    st.sidebar.header("🔄 Refresh")
    
    auto_refresh = st.sidebar.checkbox(
        "Auto-refresh", 
        value=True,
        key=f"{_WIDGET_PREFIX}auto_refresh",
        help="Automatically refresh the dashboard"
    )
    
    refresh_ms = st.sidebar.slider(
        "Interval (ms)", 
        min_value=500, 
        max_value=10000, 
        value=DEFAULT_REFRESH_INTERVAL, 
        step=500,
        key=f"{_WIDGET_PREFIX}refresh_ms",
        help="Auto-refresh interval in milliseconds"
    )
    
    if auto_refresh and st_autorefresh is not None:
        tick = st_autorefresh(interval=refresh_ms, key=f"{_WIDGET_PREFIX}autorefresh")
        if tick is not None and tick > 0:
            refresh_needed = True
    elif auto_refresh:
        st.sidebar.warning("⚠️ Install `streamlit-autorefresh` to enable auto-refresh.")

    if st.sidebar.button("Refresh now", key=f"{_WIDGET_PREFIX}refresh_now"):
        refresh_needed = True
    
    # Filter Controls Section
    st.sidebar.header("⚙️ Filters")
    
    new_min_predictions = st.sidebar.slider(
        "Min predictions", 
        min_value=0, 
        max_value=50, 
        value=state.min_predictions, 
        step=1,
        key=f"{_WIDGET_PREFIX}min_predictions",
        help="Minimum number of predictions required for strategies"
    )
    
    new_signals_limit = st.sidebar.slider(
        "Recent signals", 
        min_value=5, 
        max_value=200, 
        value=state.signals_limit, 
        step=5,
        key=f"{_WIDGET_PREFIX}signals_limit",
        help="Number of recent signals to display"
    )
    
    new_filter_signals = st.sidebar.checkbox(
        "Filter signals to ticker", 
        value=state.filter_signals_to_ticker,
        key=f"{_WIDGET_PREFIX}filter_signals",
        help="Show signals only for the selected ticker"
    )
    
    # Check if any filter changed
    if (new_min_predictions != state.min_predictions or 
        new_signals_limit != state.signals_limit or
        new_filter_signals != state.filter_signals_to_ticker):
        state.min_predictions = new_min_predictions
        state.signals_limit = new_signals_limit
        state.filter_signals_to_ticker = new_filter_signals
        refresh_needed = True
    
    return refresh_needed


def render_strategy_metric(strategy: Any, label: str):
    """
    Render a single strategy metric card.
    
    Args:
        strategy: Strategy object with metrics
        label: Label for display (e.g., Sentiment/Quant)
    """
    if strategy:
        st.metric(
            f"{label} strategy", 
            strategy.strategy_id, 
            f"cw {strategy.confidence_weight:.2f}"
        )
        st.caption(
            f"win_rate {strategy.win_rate:.2f} • "
            f"alpha {strategy.alpha:.4f} • "
            f"stability {strategy.stability:.2f}"
        )
    else:
        st.metric("strategy_id", "—", "no data")


def render_champions_section(state: DashboardState):
    """Render the champions metrics section."""
    st.subheader("🏆 Champions")
    col1, col2, col3, col4 = st.columns(4)
    
    champions = state.champions
    consensus = state.consensus
    
    # Sentiment Champion
    with col1:
        st.write("Sentiment")
        render_strategy_metric(champions.get("sentiment"), "Sentiment")
    
    # Quant Champion
    with col2:
        st.write("Quant")
        render_strategy_metric(champions.get("quant"), "Quant")
    
    # Regime Information
    with col3:
        st.subheader("📈 Regime")
        if consensus:
            st.metric(
                "ACTIVE", 
                consensus.active_regime or "—", 
                f"{consensus.ticker} {arrow(consensus.direction)}"
            )
            hv = consensus.high_vol_strength
            lv = consensus.low_vol_strength
            st.caption(f"HIGH_VOL strength {hv if hv is not None else '—'}")
            st.caption(f"LOW_VOL strength {lv if lv is not None else '—'}")
        else:
            st.metric("ACTIVE", "—", "no consensus")
    
    # Consensus Information
    with col4:
        st.subheader("🤝 Consensus")
        if consensus:
            st.metric(
                "confidence", 
                f"{consensus.confidence:.2f}", 
                f"{arrow(consensus.direction)}"
            )
            st.caption(
                f"total_weight {consensus.total_weight:.2f} • "
                f"participating {consensus.participating_strategies}"
            )
        else:
            st.metric("confidence", "—", "no consensus")


def render_challengers_section(state: DashboardState):
    """Render the challengers section."""
    left, right = st.columns(2)
    
    challengers = state.challengers
    
    with left:
        st.subheader("🥊 Challengers")
        
        for label, key in (("Sentiment", "sentiment"), ("Quant", "quant")):
            st.write(label)
            challenger = challengers.get(key)
            if challenger:
                render_strategy_metric(challenger, label)
            else:
                st.caption(f"No {label.lower()} challenger")
    
    with right:
        render_loop_health_section(state.loop_health)


def render_loop_health_section(loop_health: Any):
    """Render the loop health monitoring section."""
    st.subheader("💓 Loop Health")
    
    if not loop_health:
        st.caption("No loop health data available.")
        return
    
    # Last write timestamp
    st.write("Last write")
    st.code(loop_health.last_write_at or "—")
    
    # Processing rates
    st.write("Rates (per min, ~5m window)")
    st.caption(
        f"signals: {loop_health.signal_rate_per_min if loop_health.signal_rate_per_min is not None else '—'}"
    )
    st.caption(
        f"consensus: {loop_health.consensus_rate_per_min if loop_health.consensus_rate_per_min is not None else '—'}"
    )
    st.caption(
        f"learner: {loop_health.learner_update_rate_per_min if loop_health.learner_update_rate_per_min is not None else '—'}"
    )
    
    # Heartbeats
    st.write("Heartbeats")
    if loop_health.heartbeats:
        for hb in loop_health.heartbeats:
            status_emoji = "✅" if hb.status == "healthy" else "⚠️"
            st.write(f"{status_emoji} {hb.loop_type}: {hb.status} ({hb.last_heartbeat_at})")
            if hb.notes:
                st.caption(hb.notes)
    else:
        st.caption("No heartbeats found.")


def render_signals_section(state: DashboardState):
    """Render the recent signals table."""
    st.subheader("📡 Recent Signals")
    
    signals = state.signals
    if not signals:
        st.caption("No signals found.")
        return
    
    # Transform signals for display
    signal_data = [
        {
            "time": s.time,
            "ticker": s.ticker,
            "direction": arrow(s.direction),
            "strategy": s.strategy,
            "regime": s.regime or "",
            "confidence": round(float(s.confidence), 3),
        }
        for s in signals
    ]
    
    st.dataframe(
        signal_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            "time": st.column_config.DatetimeColumn("Time", format="YYYY-MM-DD HH:mm:ss"),
            "ticker": st.column_config.TextColumn("Ticker"),
            "direction": st.column_config.TextColumn("Direction"),
            "strategy": st.column_config.TextColumn("Strategy"),
            "regime": st.column_config.TextColumn("Regime"),
            "confidence": st.column_config.NumberColumn("Confidence", format="%.3f"),
        }
    )


def render_rankings_section(state: DashboardState):
    """Render the autonomous target rankings section."""
    st.subheader("🎯 Target Rankings")
    
    rankings = state.rankings
    if not rankings:
        st.caption("No rankings available. Run the pipeline to generate rankings.")
        return
    
    # Transform rankings for display
    ranking_data = [
        {
            "ticker": r.ticker,
            "score": r.score,
            "conviction": r.conviction,
            "regime": r.regime,
            "sentiment": r.attribution.get("sentiment", 0.0),
            "macro": r.attribution.get("macro", 0.0),
            "drift": r.attribution.get("drift", 0.0),
            "momentum": r.attribution.get("momentum", 0.0),
        }
        for r in rankings
    ]
    
    st.dataframe(
        ranking_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "score": st.column_config.NumberColumn("Composite", format="%.3f"),
            "conviction": st.column_config.NumberColumn("Conviction", format="%.3f"),
            "regime": st.column_config.TextColumn("Regime"),
            "sentiment": st.column_config.ProgressColumn("Sentiment", min_value=-0.5, max_value=0.5, format="%.3f"),
            "macro": st.column_config.ProgressColumn("Macro", min_value=-0.5, max_value=0.5, format="%.3f"),
            "drift": st.column_config.ProgressColumn("Drift", min_value=-0.5, max_value=0.5, format="%.3f"),
            "momentum": st.column_config.ProgressColumn("Momentum", min_value=-0.5, max_value=0.5, format="%.3f"),
        }
    )


# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Modern dashboard with enhanced UI/UX and integrated Plotly charts."""
    
    # Import modern dashboard implementation
    try:
        from app.ui.dashboard_modern import main_modern
        main_modern()
    except ImportError as e:
        st.error(f"Modern dashboard components not available: {e}")
        st.info("Falling back to basic dashboard...")
        
        # Fallback to basic implementation
        service = get_dashboard_service(DEFAULT_DB_PATH)
        state = _get_basic_state(service)
        
        render_header()
        refresh_needed = render_sidebar_controls(state)
        state.refresh_if_needed(force=refresh_needed)
        
        if not state.tenant_id:
            st.warning("Please select a tenant from the sidebar to view dashboard data.")
            return
        
        render_champions_section(state)
        st.divider()
        render_rankings_section(state)
        st.divider()
        render_challengers_section(state)
        st.divider()
        render_signals_section(state)


if __name__ == "__main__":
    main()
