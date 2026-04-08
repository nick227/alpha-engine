"""
Modern Alpha Engine Dashboard with Enhanced UI/UX
Sophisticated design inspired by iOS design principles
"""

import streamlit as st
from typing import Optional, Dict, Any, List
from datetime import datetime

from app.ui.middle.dashboard_service import DashboardService, arrow
from app.ui.theme_enhanced import apply_theme, get_color_for_direction
from app.ui.components.enhanced import (
    elevated_card, metric_card, strategy_metric_card, 
    signal_indicator, status_badge, divider, info_panel
)

# Optional dependency for auto-refresh functionality
try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore
except Exception:  # pragma: no cover
    st_autorefresh = None

# Import original dashboard components
from app.ui.dashboard import (
    DashboardState,
    get_dashboard_service,
    DEFAULT_DB_PATH
)

# Import chart integration
try:
    from app.ui.dashboard_charts import integrate_charts_into_dashboard
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False


def render_modern_header(state: DashboardState):
    """Sophisticated header with hero section"""
    
    # Status indicators
    health_status = "healthy" if state.loop_health and state.loop_health.last_write_at else "unknown"
    status_badge_html = status_badge(health_status, "sm")
    
    header_html = f"""
    <div style="
        background: linear-gradient(135deg, #FAFAFA 0%, #F5F5F5 100%);
        padding: {40}px;
        border-radius: 16px;
        margin-bottom: 32px;
        border: 1px solid #E0E0E0;
        position: relative;
        overflow: hidden;
    ">
        <div style="
            position: absolute;
            top: 0;
            right: 0;
            width: 200px;
            height: 200px;
            background: linear-gradient(45deg, #2196F320 0%, #2196F310 100%);
            border-radius: 50%;
            transform: translate(50%, -50%);
        "></div>
        
        <div style="display: flex; justify-content: space-between; align-items: center; position: relative; z-index: 1;">
            <div>
                <h1 style="
                    margin: 0; 
                    font-size: 36px; 
                    font-weight: 700; 
                    color: #212121;
                    display: flex;
                    align-items: center;
                    gap: 12px;
                ">
                    🚀 Alpha Engine
                </h1>
                <p style="
                    margin: 8px 0 0 0; 
                    font-size: 16px; 
                    color: #757575;
                    font-weight: 400;
                ">
                    Recursive • Self-learning • Dual Track
                </p>
            </div>
            
            <div style="display: flex; gap: 20px; align-items: center;">
                <div style="
                    text-align: center; 
                    background: white; 
                    border-radius: 12px; 
                    padding: 20px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                    border: 1px solid #E0E0E0;
                    min-width: 120px;
                ">
                    <div style="font-size: 14px; color: #757575; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.5px;">
                        Current Tenant
                    </div>
                    <div style="font-size: 18px; font-weight: 600; color: #1565C0;">
                        {state.tenant_id or 'No Tenant'}
                    </div>
                </div>
                
                <div style="
                    text-align: center; 
                    background: white; 
                    border-radius: 12px; 
                    padding: 20px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                    border: 1px solid #E0E0E0;
                    min-width: 120px;
                ">
                    <div style="font-size: 14px; color: #757575; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.5px;">
                        Selected Ticker
                    </div>
                    <div style="font-size: 18px; font-weight: 600; color: #2E7D32;">
                        {state.ticker or 'No Ticker'}
                    </div>
                </div>
                
                <div style="
                    display: flex;
                    align-items: center;
                    gap: 8px;
                ">
                    {status_badge_html}
                    <div style="font-size: 12px; color: #757575;">
                        System Status
                    </div>
                </div>
            </div>
        </div>
    </div>
    """
    st.markdown(header_html, unsafe_allow_html=True)


def render_modern_sidebar_controls(state: DashboardState) -> bool:
    """Enhanced sidebar with modern styling"""
    
    # Apply custom sidebar styling
    sidebar_css = """
    <style>
    .css-1d391kg {
        background: #FAFAFA;
        border-right: 1px solid #E0E0E0;
    }
    .css-1d391kg .css-17eq0hr {
        background: transparent;
    }
    </style>
    """
    st.markdown(sidebar_css, unsafe_allow_html=True)
    
    with st.sidebar:
        # Brand area
        st.markdown("""
        <div style="
            padding: 20px; 
            text-align: center; 
            border-bottom: 1px solid #E0E0E0; 
            margin-bottom: 20px;
            background: white;
            border-radius: 12px;
            margin-left: -20px;
            margin-right: -20px;
        ">
            <h2 style="margin: 0; color: #1565C0; font-size: 20px; display: flex; align-items: center; justify-content: center; gap: 8px;">
                🚀 Alpha Engine
            </h2>
            <p style="margin: 4px 0 0 0; color: #757575; font-size: 12px;">Control Panel</p>
        </div>
        """, unsafe_allow_html=True)
    
    refresh_needed = False
    
    # Data Selection Section
    with st.sidebar:
        elevated_card(
            title="📊 Data Selection",
            content=f"""
            <div style="margin-bottom: 16px;">
                <label style="font-size: 12px; color: #757575; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; display: block;">
                    Tenant
                </label>
            </div>
            """
        )
        
        tenants = state.service.list_tenants()
        new_tenant_id = st.selectbox(
            "Select Tenant",
            options=tenants,
            index=0,
            label_visibility="collapsed"
        )
        
        if new_tenant_id != state.tenant_id:
            state.tenant_id = new_tenant_id
            refresh_needed = True
        
        if state.tenant_id:
            all_tickers = state.service.list_tickers(tenant_id=state.tenant_id)
            if all_tickers:
                new_ticker = st.selectbox(
                    "Select Ticker",
                    options=all_tickers,
                    index=0,
                    label_visibility="collapsed"
                )
                if new_ticker != state.ticker:
                    state.ticker = new_ticker
                    refresh_needed = True
            else:
                state.ticker = None
                info_panel(
                    "No Tickers Available",
                    "No tickers found in database. Please ensure predictions are being generated.",
                    icon="⚠️",
                    variant="warning"
                )
    
    # Target Stocks Panel (canonical universe)
    with st.sidebar:
        try:
            panel = state.service.get_target_stocks_panel()
            rows = list(panel.get("rows") or [])
            enabled = [r for r in rows if r.get("enabled")]
            active = [r for r in rows if r.get("active")]
            version = str(panel.get("target_universe_version") or "")

            elevated_card(
                title="Target Stocks",
                content=f"""
                <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
                    <span style="color: #757575;">Active</span>
                    <span style="font-weight: 600;">{len(active)}</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
                    <span style="color: #757575;">Enabled</span>
                    <span style="font-weight: 600;">{len(enabled)}</span>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: #757575;">Version</span>
                    <span style="font-family: monospace; font-size: 12px;">{version[:10] if version else '—'}</span>
                </div>
                """,
            )

            with st.expander("Show Target Stocks", expanded=False):
                for r in sorted(rows, key=lambda x: str(x.get("symbol") or "")):
                    sym = str(r.get("symbol") or "")
                    status = "ACTIVE" if r.get("active") else ("DISABLED" if not r.get("enabled") else "INACTIVE")
                    st.caption(f"{sym} • {status}")
        except Exception:
            pass

    # Auto-refresh Section
    with st.sidebar:
        elevated_card(
            title="🔄 Auto-Refresh",
            content=""
        )
        
        auto_refresh = st.checkbox("Enable Auto-refresh", value=True)
        refresh_ms = st.slider("Refresh Interval", 500, 10000, 2000, 500, label_visibility="collapsed")
        
        if auto_refresh:
            if st_autorefresh is not None:
                st_autorefresh(interval=refresh_ms, key="autorefresh")
            else:
                info_panel(
                    "Missing Dependency",
                    "Install `streamlit-autorefresh` to enable auto-refresh functionality.",
                    icon="⚠️",
                    variant="warning"
                )
    
    # Filter Controls Section
    with st.sidebar:
        elevated_card(
            title="⚙️ Filters",
            content=""
        )
        
        new_min_predictions = st.slider(
            "Min Predictions", 0, 50, state.min_predictions, 1, label_visibility="collapsed"
        )
        new_signals_limit = st.slider(
            "Recent Signals", 5, 200, state.signals_limit, 5, label_visibility="collapsed"
        )
        new_filter_signals = st.checkbox("Filter Signals to Ticker", state.filter_signals_to_ticker)
        
        if (new_min_predictions != state.min_predictions or 
            new_signals_limit != state.signals_limit or
            new_filter_signals != state.filter_signals_to_ticker):
            state.min_predictions = new_min_predictions
            state.signals_limit = new_signals_limit
            state.filter_signals_to_ticker = new_filter_signals
            refresh_needed = True
    
    # Visualization Toggle
    with st.sidebar:
        elevated_card(
            title="📈 Visualization",
            content=""
        )
        
        show_charts = st.checkbox("Enable Charts", True, help="Toggle interactive time-series charts")
        
        if not CHARTS_AVAILABLE:
            info_panel(
                "Charts Unavailable",
                "Chart components require Plotly. Install with: pip install plotly>=5.17.0",
                icon="📊",
                variant="info"
            )
            show_charts = False
    
    return refresh_needed, show_charts


def render_modern_champions_section(state: DashboardState):
    """Enhanced champions section with modern cards"""
    
    champions = state.champions
    consensus = state.consensus
    
    st.markdown("### 🏆 Champions Performance")
    
    # Champions Grid
    col1, col2 = st.columns(2)
    
    with col1:
        strategy_metric_card(champions.get("sentiment"), "Sentiment Strategy")
    
    with col2:
        strategy_metric_card(champions.get("quant"), "Quant Strategy")
    
    # Consensus and Regime Section
    col1, col2 = st.columns(2)
    
    with col1:
        if consensus:
            elevated_card(
                title="📈 Market Regime",
                content=f"""
                <div style="margin-bottom: 16px;">
                    <div style="font-size: 14px; color: #757575; margin-bottom: 8px;">Active Regime</div>
                    <div style="font-size: 24px; font-weight: 600; color: #1565C0; margin-bottom: 16px;">
                        {consensus.active_regime or '—'}
                    </div>
                    <div style="font-size: 14px; color: #757575;">
                        {consensus.ticker} {arrow(consensus.direction)}
                    </div>
                </div>
                """,
                footer=f"""
                <div style="display: flex; justify-content: space-between; font-size: 12px;">
                    <span>High Vol: {consensus.high_vol_strength or '—'}</span>
                    <span>Low Vol: {consensus.low_vol_strength or '—'}</span>
                </div>
                """
            )
        else:
            elevated_card(
                title="📈 Market Regime",
                content="No consensus data available."
            )
    
    with col2:
        if consensus:
            elevated_card(
                title="🤝 Consensus Metrics",
                content=f"""
                <div style="margin-bottom: 16px;">
                    <div style="font-size: 14px; color: #757575; margin-bottom: 8px;">Confidence Score</div>
                    <div style="font-size: 32px; font-weight: 700; color: #FF9800; margin-bottom: 16px;">
                        {consensus.confidence:.2f}
                    </div>
                    <div style="font-size: 16px; color: #757575;">
                        {arrow(consensus.direction)}
                    </div>
                </div>
                """,
                footer=f"""
                <div style="display: flex; justify-content: space-between; font-size: 12px;">
                    <span>Weight: {consensus.total_weight:.2f}</span>
                    <span>Strategies: {consensus.participating_strategies}</span>
                </div>
                """
            )
        else:
            elevated_card(
                title="🤝 Consensus Metrics",
                content="No consensus data available."
            )


def render_modern_challengers_section(state: DashboardState):
    """Enhanced challengers section"""
    
    challengers = state.challengers
    
    st.markdown("### 🥊 Challenger Strategies")
    
    col1, col2 = st.columns(2)
    
    with col1:
        elevated_card(
            title="📊 Sentiment Challenger",
            content=""
        )
        
        sent_chall = challengers.get("sentiment")
        if sent_chall:
            strategy_metric_card(sent_chall, "sentiment challenger")
        else:
            st.caption("No sentiment challenger available")
    
    with col2:
        elevated_card(
            title="🔬 Quant Challenger", 
            content=""
        )
        
        quant_chall = challengers.get("quant")
        if quant_chall:
            strategy_metric_card(quant_chall, "quant challenger")
        else:
            st.caption("No quant challenger available")


def render_modern_loop_health_section(state: DashboardState):
    """Enhanced loop health monitoring"""
    
    st.markdown("### 💓 System Health")
    
    loop_health = state.loop_health
    
    if not loop_health:
        info_panel(
            "Health Data Unavailable",
            "No loop health information is currently available.",
            icon="⚠️",
            variant="warning"
        )
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        elevated_card(
            title="📝 Last Activity",
            content=f"""
            <div style="font-family: monospace; font-size: 14px; color: #1565C0; background: #E3F2FD; padding: 12px; border-radius: 8px;">
                {loop_health.last_write_at or 'Never'}
            </div>
            """
        )
        
        elevated_card(
            title="📊 Processing Rates",
            content=f"""
            <div style="display: flex; flex-direction: column; gap: 8px;">
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: #757575;">Signals:</span>
                    <span style="font-weight: 600;">{loop_health.signal_rate_per_min or '—'}/min</span>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: #757575;">Consensus:</span>
                    <span style="font-weight: 600;">{loop_health.consensus_rate_per_min or '—'}/min</span>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: #757575;">Learner:</span>
                    <span style="font-weight: 600;">{loop_health.learner_update_rate_per_min or '—'}/min</span>
                </div>
            </div>
            """
        )
    
    with col2:
        elevated_card(
            title="💗 Component Heartbeats",
            content=""
        )
        
        if loop_health.heartbeats:
            for hb in loop_health.heartbeats:
                status_badge_html = status_badge(hb.status, "sm")
                
                heartbeat_html = f"""
                <div style="
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 8px 0;
                    border-bottom: 1px solid #F0F0F0;
                ">
                    <div>
                        <div style="font-weight: 500; color: #212121;">{hb.loop_type}</div>
                        <div style="font-size: 12px; color: #757575;">{hb.last_heartbeat_at}</div>
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        {status_badge_html}
                    </div>
                </div>
                """
                st.markdown(heartbeat_html, unsafe_allow_html=True)
        else:
            st.caption("No heartbeat data available.")


def render_modern_signals_section(state: DashboardState):
    """Enhanced signals section with visual indicators"""
    
    st.markdown("### 📡 Recent Signals")
    
    signals = state.signals
    if not signals:
        info_panel(
            "No Signals Available",
            "No signals found for the current selection.",
            icon="📡",
            variant="info"
        )
        return
    
    # Signal summary cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        metric_card("Total Signals", str(len(signals)), icon="📊")
    
    with col2:
        buy_signals = len([s for s in signals if s.direction in ('up', 'long', 'buy', '1', '+1')])
        metric_card("Buy Signals", str(buy_signals), color="#4CAF50", icon="📈")
    
    with col3:
        sell_signals = len([s for s in signals if s.direction in ('down', 'short', 'sell', '-1')])
        metric_card("Sell Signals", str(sell_signals), color="#F44336", icon="📉")
    
    with col4:
        avg_confidence = sum(s.confidence for s in signals) / len(signals)
        metric_card("Avg Confidence", f"{avg_confidence:.3f}", icon="🎯")
    
    divider("Signal Details")
    
    # Signal indicators
    for signal in signals[:10]:  # Show top 10 signals
        signal_indicator(
            direction=signal.direction,
            confidence=signal.confidence,
            strategy=signal.strategy
        )
    
    if len(signals) > 10:
        st.caption(f"... and {len(signals) - 10} more signals")


def main_modern():
    """Main modern dashboard application"""
    
    # Apply enhanced theme
    apply_theme()
    
    # Initialize service and state
    service = get_dashboard_service(DEFAULT_DB_PATH)
    state = DashboardState(service)
    
    # Render modern header
    render_modern_header(state)
    
    # Render modern sidebar controls
    refresh_needed, show_charts = render_modern_sidebar_controls(state)
    
    # Refresh data if needed
    if refresh_needed or not hasattr(state, '_initialized'):
        state.refresh_data()
        state._initialized = True
    
    # Only render main content if we have a tenant selected
    if not state.tenant_id:
        info_panel(
            "No Tenant Selected",
            "Please select a tenant from the sidebar to view dashboard data.",
            icon="👤",
            variant="info"
        )
        return
    
    # Render charts if enabled
    if show_charts and CHARTS_AVAILABLE:
        integrate_charts_into_dashboard(service, state.tenant_id, state.ticker)
        divider("Detailed Metrics")
    
    # Render enhanced sections
    render_modern_champions_section(state)
    divider()
    
    render_modern_challengers_section(state)
    divider()
    
    render_modern_loop_health_section(state)
    divider()
    
    render_modern_signals_section(state)
    
    # Footer
    st.markdown("---")
    footer_html = f"""
    <div style="text-align: center; color: #757575; font-size: 12px; margin-top: 32px;">
        <div>Alpha Engine v3.0 - Modern Dashboard</div>
        <div style="margin-top: 4px;">Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </div>
    """
    st.markdown(footer_html, unsafe_allow_html=True)


if __name__ == "__main__":
    main_modern()
