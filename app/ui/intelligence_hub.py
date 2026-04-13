"""
Intelligence Hub - Clean Architecture Implementation

Pure UI renderer that consumes DTO from service layer.
No simulation, no numpy, no scoring logic in UI.
"""

import streamlit as st
import plotly.graph_objects as go
from app.ui.theme import apply_theme
from app.ui.intelligence.intelligence_hub_state import IntelligenceHubState
from app.ui.middle.dashboard_service import DashboardService


def intelligence_hub_main(
    service: DashboardService,
    *,
    show_page_header: bool = True,
    show_local_controls: bool = True,
):
    """
    Main intelligence hub entry point - clean architecture.
    
    Only handles:
    - Controls
    - State
    - Service calls
    - Rendering
    
    All simulation logic is in the service layer.
    """
    if show_page_header or show_local_controls:
        apply_theme()
    
    # Get state from session or initialize
    if "ih_state" not in st.session_state:
        st.session_state.ih_state = IntelligenceHubState()
     
    state = st.session_state.ih_state 

    # Sync global filters (from app/ui/app.py) into IH state when present.
    ui_filters = st.session_state.get("ui_filters", {}) or {}
    desired_tenant = ui_filters.get("tenant_id")
    desired_ticker = ui_filters.get("ticker")
    desired_timeframe = ui_filters.get("timeframe")
    desired_horizon = ui_filters.get("horizon_days")
    desired_run = ui_filters.get("run_id")

    updates = {}
    if desired_tenant and getattr(state, "tenant_id", None) != desired_tenant:
        updates["tenant_id"] = desired_tenant
    if desired_ticker and state.ticker != desired_ticker:
        updates["ticker"] = desired_ticker
    if desired_timeframe and state.timeframe != desired_timeframe:
        updates["timeframe"] = desired_timeframe
    if desired_horizon and state.horizon != int(desired_horizon):
        updates["horizon"] = int(desired_horizon)
    if desired_run is not None and state.run_id != desired_run:
        updates["run_id"] = desired_run
    if updates:
        state = state.copy(**updates)
        st.session_state.ih_state = state
     
    # Handle state changes from session 
    if hasattr(st.session_state, 'asset_change') and st.session_state.asset_change != state.ticker: 
        state = state.copy(ticker=st.session_state.asset_change) 
        st.session_state.ih_state = state 
        del st.session_state.asset_change 
    
    if hasattr(st.session_state, 'timeframe_change') and st.session_state.timeframe_change != state.timeframe:
        state = state.copy(timeframe=st.session_state.timeframe_change)
        st.session_state.ih_state = state
        del st.session_state.timeframe_change
    
    if hasattr(st.session_state, 'filter_change') and st.session_state.filter_change != state.filter_mode:
        state = state.copy(filter_mode=st.session_state.filter_change)
        st.session_state.ih_state = state
        del st.session_state.filter_change
    
    # Get real data from service
    dto = service.get_intelligence_state(state)
    
    if show_page_header:
        _render_hero_section(dto)

    if show_local_controls:
        render_controls(state, dto.tickers, dto.runs)
    
    # Asset header
    st.markdown(f"""
    <div style="display: flex; align-items: baseline; gap: 16px; margin-bottom: 1.5rem;">
        <span style="font-size: 28px; font-weight: 500; color: var(--color-text-primary);">{state.ticker}</span>
        <span style="font-size: 13px; color: var(--color-text-tertiary);">
            {state.timeframe}
        </span>
    </div>
    """, unsafe_allow_html=True)

    # Top 3 strategies section
    _render_top_strategies(dto)
    
    # Explore deeper section - nested expanders for organization
    with st.expander("Explore deeper", expanded=False):
        # Performance Matrix
        with st.expander("Performance Matrix"):
            if dto.matrix_rows:
                render_champion_matrix(dto.matrix_rows, dto.champions)
            else:
                st.info("No matrix data available.")
        
        # Charts & Overlays
        with st.expander("Charts & Overlays"):
            if dto.overlay_series:
                render_strategy_overlays(dto.overlay_series, dto.strategy_rankings)
            else:
                st.info("No overlay data available.")
            
            if dto.timeline:
                render_strategy_timeline(dto.timeline)
        
        # Consensus
        with st.expander("Market Consensus"):
            if dto.consensus:
                render_consensus(dto.consensus)
            else:
                st.info("No consensus data available.")
        
        # Rankings
        with st.expander("Strategy Rankings"):
            if dto.strategy_rankings:
                render_strategy_rankings(dto.strategy_rankings)
            else:
                st.info("No rankings available.")
        
        # Help
        with st.expander("How to use"):
            st.markdown("""
            **Intelligence Hub**
            
            - **Alpha**: Direction reliability (correlation between predicted and actual)
            - **Win Rate**: Percentage of correct direction predictions
            
            **Champions**: Best strategy per (asset + horizon)
            """)
    
    if show_page_header:
        st.markdown("---")
        st.markdown("""
        <div style="text-align: center; color: #757575; font-size: 12px; margin-top: 32px;">
            <div>Intelligence Hub</div>
            <div style="margin-top: 4px;">Strategy performance exploration powered by Alpha Engine</div>
            <div style="margin-top: 8px; font-size: 11px;">
                <strong>Key insights:</strong> Look for declining alpha -> regime change | High MAE -> magnitude errors | 
                Filter incorrect predictions -> failure patterns | News polarity -> causal factors
            </div>
        </div>
        """, unsafe_allow_html=True)


def _render_hero_section(dto):
    """Render the hero section showing the current edge"""
    champions = dto.champions or []
    
    if champions:
        best = champions[0]
        strategy_name = best.get('strategy_id', 'N/A')
        horizon = best.get('horizon', 0)
        alpha = best.get('alpha', 0)
        win_rate = best.get('direction_accuracy', 0) if isinstance(best, dict) else 0
        
        # Relative ranking: compare #1 to #2
        if len(champions) > 1:
            second_alpha = champions[1].get('alpha', 0)
            edge = alpha - second_alpha
            if edge > 0.15:
                status_text = "Strong Edge"
                status_emoji = "🔥"
            elif edge > 0.05:
                status_text = "Working"
                status_emoji = "↑"
            else:
                status_text = "Marginal"
                status_emoji = "→"
        else:
            status_text = "Only Option"
            status_emoji = "?"
        
        # Generate "why this works" based on metrics
        why_text = _generate_why_text(alpha, win_rate)
        
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #1E3A5F 0%, #0D2137 100%);
            color: white;
            padding: 24px 32px;
            border-radius: 12px;
            margin-bottom: 1.5rem;
        ">
            <div style="font-size: 11px; text-transform: uppercase; letter-spacing: 1px; opacity: 0.7; margin-bottom: 8px;">
                Current Edge
            </div>
            <div style="font-size: 28px; font-weight: 600; margin-bottom: 12px;">
                {strategy_name} ({horizon}d) {status_emoji}
            </div>
            <div style="display: flex; gap: 24px; font-size: 14px; margin-bottom: 12px;">
                <div>
                    <span style="opacity: 0.7;">Alpha:</span>
                    <strong> {alpha:.2f}</strong>
                </div>
                <div>
                    <span style="opacity: 0.7;">Win Rate:</span>
                    <strong> {win_rate:.0%}</strong>
                </div>
                <div>
                    <span style="opacity: 0.7;">Status:</span>
                    <strong> {status_text}</strong>
                </div>
            </div>
            <div style="font-size: 12px; opacity: 0.8; font-style: italic;">
                {why_text}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #1E3A5F 0%, #0D2137 100%);
            color: white;
            padding: 24px 32px;
            border-radius: 12px;
            margin-bottom: 1.5rem;
        "">
            <div style="font-size: 11px; text-transform: uppercase; letter-spacing: 1px; opacity: 0.7; margin-bottom: 8px;">
                Current Edge
            </div>
            <div style="font-size: 20px; opacity: 0.7;">
                No strategy data available
            </div>
        </div>
        """, unsafe_allow_html=True)


def _render_top_strategies(dto):
    """Render top 3 strategies section with selection flow"""
    champions = dto.champions or []
    
    if not champions:
        return
    
    st.markdown("### Top Strategies")
    
    # Make #1 larger and highlighted
    cols = st.columns([1.5, 1, 1])
    
    # #1 - larger card
    with cols[0]:
        champ = champions[0]
        strategy_name = champ.get('strategy_id', 'N/A')
        horizon = champ.get('horizon', 0)
        alpha = champ.get('alpha', 0)
        win_rate = champ.get('direction_accuracy', 0) if isinstance(champ, dict) else 0
        
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #E8F5E9 0%, #C8E6C9 100%);
            border: 2px solid #4CAF50;
            border-radius: 10px;
            padding: 20px;
            text-align: center;
        ">
            <div style="font-size: 11px; color: #2E7D32; font-weight: 600; margin-bottom: 4px;">
                #1 BEST
            </div>
            <div style="font-size: 18px; font-weight: 600; margin-bottom: 8px; color: #1B5E20;">
                {strategy_name} ({horizon}d)
            </div>
            <div style="font-size: 13px; color: #388E3C;">
                α: <strong>{alpha:.2f}</strong> | WR: <strong>{win_rate:.0%}</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # #2 and #3 - smaller cards
    for i, champ in enumerate(champions[1:3], start=2):
        with cols[i-1]:
            strategy_name = champ.get('strategy_id', 'N/A')
            horizon = champ.get('horizon', 0)
            alpha = champ.get('alpha', 0)
            win_rate = champ.get('direction_accuracy', 0) if isinstance(champ, dict) else 0
            
            st.markdown(f"""
            <div style="
                background: var(--color-background-primary);
                border: 1px solid var(--color-border);
                border-radius: 8px;
                padding: 14px;
                text-align: center;
            ">
                <div style="font-size: 11px; color: var(--color-text-tertiary); margin-bottom: 4px;">
                    #{i}
                </div>
                <div style="font-size: 14px; font-weight: 500; margin-bottom: 6px;">
                    {strategy_name} ({horizon}d)
                </div>
                <div style="font-size: 12px; color: var(--color-text-secondary);">
                    α: <strong>{alpha:.2f}</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # Selection dropdown for detail view
    st.markdown("<div style='margin: 16px 0;'></div>", unsafe_allow_html=True)
    strategy_options = [f"{c.get('strategy_id', 'N/A')} ({c.get('horizon', 0)}d)" for c in champions[:5]]
    selected = st.selectbox("Select strategy for details", strategy_options, key="ih_strategy_select")
    
    # Find selected strategy data
    selected_idx = strategy_options.index(selected)
    if selected_idx < len(champions):
        selected_champ = champions[selected_idx]
        _render_strategy_detail(selected_champ)


def _generate_why_text(alpha: float, win_rate: float) -> str:
    """Generate 'why this works' explanation based on metrics"""
    reasons = []
    
    if alpha >= 0.5:
        reasons.append("Strong directional signal")
    elif alpha >= 0.3:
        reasons.append("Reliable price direction prediction")
    elif alpha >= 0.1:
        reasons.append("Moderate predictive power")
    else:
        reasons.append("Weak signal - use caution")
    
    if win_rate >= 0.65:
        reasons.append("high win rate")
    elif win_rate >= 0.55:
        reasons.append("positive win rate bias")
    
    if alpha >= 0.3 and win_rate >= 0.55:
        return "Works because: " + ", ".join(reasons) + "."
    elif alpha >= 0.2:
        return "Works because: " + reasons[0] + "."
    else:
        return "Needs monitoring: " + reasons[-1].lower()


def _render_strategy_detail(champ: dict):
    """Render detailed view for selected strategy"""
    strategy_name = champ.get('strategy_id', 'N/A')
    horizon = champ.get('horizon', 0)
    alpha = champ.get('alpha', 0)
    win_rate = champ.get('direction_accuracy', 0) if isinstance(champ, dict) else 0
    samples = champ.get('samples', 0)
    mae = champ.get('mae_pct', 0)
    
    st.markdown(f"#### {strategy_name} ({horizon}d) Detail")
    
    # Simplified metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Alpha", f"{alpha:.2f}")
    with col2:
        st.metric("Win Rate", f"{win_rate:.0%}")
    with col3:
        st.metric("Samples", f"{samples}")
    with col4:
        st.metric("MAE", f"{mae:.1f}%")
    
    # Placeholder for chart - would need to get chart data from dto
    st.info(f"Chart for {strategy_name} would appear here. Configure strategy_charts in DTO to enable.")


def render_strategy_card(strategy: dict):
    """Render individual strategy performance card - pure rendering only"""
    
    metrics = strategy['metrics']
    chart_points = strategy['chart_points']
    
    # Create chart with pre-computed data
    fig = go.Figure()
    
    # Actual line
    fig.add_trace(go.Scatter(
        x=chart_points['actual']['x'],
        y=chart_points['actual']['y'],
        mode='lines',
        name='actual',
        line=dict(color='#888780', width=1.5),
        fill=None
    ))
    
    # Prediction points and projected lines (pre-computed)
    for pred in chart_points['predictions']:
        fig.add_trace(go.Scatter(
            x=[pred['x']],
            y=[pred['y']],
            mode='markers',
            marker=dict(color=pred['color'], size=6),
            name='prediction point',
            showlegend=False
        ))
    
    for line in chart_points['projected_lines']:
        fig.add_trace(go.Scatter(
            x=line['x'],
            y=line['y'],
            mode='lines',
            line=dict(color=line['color'], width=1, dash='dash'),
            name='projected',
            showlegend=False
        ))
    
    for marker in chart_points['horizon_markers']:
        fig.add_trace(go.Scatter(
            x=[marker['x']],
            y=[marker['y']],
            mode='markers',
            marker=dict(color=marker['color'], size=4, symbol='x'),
            name='horizon',
            showlegend=False
        ))
    
    fig.update_layout(
        height=250,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        xaxis=dict(
            showgrid=False,
            showticklabels=True,
            tickfont=dict(size=9, color='#888780'),
            ticks='outside',
            tickcolor='transparent'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(130,130,130,0.08)',
            showticklabels=True,
            tickfont=dict(size=9, color='#888780'),
            ticks='outside',
            tickcolor='transparent'
        ),
        hovermode='x unified',
        plot_bgcolor='white'
    )
    
    # Render strategy card with pre-computed data
    st.markdown(f"""
    <div style="background: var(--color-background-primary); border: 1px solid {strategy['border_color']}; border-radius: var(--border-radius-lg); padding: 1rem; margin-bottom: 1rem;">
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 11px; margin-bottom: 8px;">
            <div><strong>Prediction:</strong> {metrics['current_prediction_pct']:+.1%} ({strategy['horizon']}d)</div>
            <div><strong>Actual:</strong> {metrics['current_actual_pct']:+.1%}</div>
            <div><strong>Error:</strong> {metrics['current_error_pct']:+.1%}</div>
            <div><strong>Direction:</strong> {'correct' if metrics['current_direction_correct'] else 'incorrect'}</div>
        </div>
        
        <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px;">
            <span style="font-size: 13px; font-weight: 500; color: var(--color-text-primary);">{strategy['id']}</span>
            {f'<span style="font-size: 10px; padding: 2px 7px; border-radius: 3px; background: #E6F1FB; color: #0C447C; font-weight: 500;">{strategy["champion_text"]}</span>' if strategy['champion_text'] else ''}
        </div>
        
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 11px; margin-bottom: 8px; color: var(--color-text-tertiary);">
            <div><strong>Made:</strong> {strategy['labels'][-1] if strategy['labels'] else 'N/A'}</div>
            <div><strong>Target:</strong> {strategy['labels'][-1] if strategy['labels'] else 'N/A'}</div>
        </div>
        
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 11px; margin-bottom: 10px;">
            <div><strong>Alpha:</strong> {metrics['alpha']:.2f} (n={metrics['samples']})</div>
            <div><strong>MAE:</strong> {metrics['mae_pct']:.1f}%</div>
            <div><strong>Recent:</strong> {metrics['recent_alpha']:.2f} (30d) {strategy['drift_arrow']}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Legend
    st.markdown(f"""
    <div style="display: flex; gap: 15px; font-size: 10px; color: var(--color-text-tertiary); margin-top: 8px;">
        <span style="display: flex; align-items: center; gap: 4px;">
            <span style="width: 12px; height: 2px; background: #888780; border-radius: 1px;"></span>actual price
        </span>
        <span style="display: flex; align-items: center; gap: 4px;">
            <span style="width: 8px; height: 8px; background: {strategy['color']}; border-radius: 50%;"></span>prediction point
        </span>
        <span style="display: flex; align-items: center; gap: 4px;">
            <span style="width: 12px; height: 2px; background: {strategy['color']}; border-radius: 1px; border-style: dashed; border-width: 1px;"></span>projected line
        </span>
        <span style="display: flex; align-items: center; gap: 4px;">
            <span style="width: 8px; height: 8px; background: {strategy['color']}; border-radius: 1px; transform: rotate(45deg);"></span>horizon marker
        </span>
    </div>
    """, unsafe_allow_html=True)


def render_champion_matrix(matrix_rows, champions):
    """Render champion matrix from real data"""
    import pandas as pd
    
    # Create DataFrame for display
    df = pd.DataFrame([
        {
            "Strategy": row.strategy_id,
            "Ticker": row.ticker,
            "Timeframe": row.timeframe,
            "Horizon": f"{row.forecast_days}d",
            "Alpha": f"{row.alpha_strategy:.3f}",
            "Stars": _format_stars(row.alpha_strategy),  # UI formats stars
            "Win Rate": f"{row.direction_accuracy:.1%}",
            "Samples": row.samples,
            "Entry": f"${row.entry_price:,.2f}" if row.entry_price else "N/A",
            "Target": f"${row.target_price:,.2f}" if row.target_price else "N/A",
        }
        for row in matrix_rows
    ])
    
    st.dataframe(df, use_container_width=True)
    
    # Show champions summary (UI formats stars)
    if champions:
        st.markdown("#### Champions by Horizon")
        for champ in champions:
            # UI formats stars, service returns raw efficiency
            stars = _format_stars(champ.efficiency)
            st.markdown(f"- **{champ.horizon}d**: {champ.strategy_id} (efficiency={champ.efficiency:.3f}, Î±={champ.alpha:.3f}) {stars}")


def _format_stars(value: float) -> str:
    """Format value to stars - UI formatting only"""
    if value >= 0.8:
        return "â­ââââ"
    elif value >= 0.6:
        return "â­â­ââ"
    elif value >= 0.4:
        return "â­âââ"
    elif value >= 0.2:
        return "â­ââ"
    else:
        return "â"


def render_strategy_rankings(rankings):
    """Render strategy efficiency rankings from real data"""
    for i, ranking in enumerate(rankings[:5]):
        with st.container():
            col1, col2, col3, col4 = st.columns([1, 2, 1, 1])
            
            with col1:
                st.write(f"**{i+1}.**")
            
            with col2:
                st.write(f"**{ranking.strategy_id}**")
            
            with col3:
                st.metric("Efficiency", f"{ranking.efficiency_score:.3f}")
            
            with col4:
                st.metric("Win Rate", f"{ranking.win_rate:.1%}")


def render_strategy_overlays(overlay_series, rankings=None):
    """Render strategy overlays from real data with merged metadata"""
    if not overlay_series or not overlay_series.get('strategies'):
        st.info("No overlay data available")
        return
    
    # Create metadata lookup from rankings
    ranking_lookup = {}
    if rankings:
        for r in rankings:
            ranking_lookup[r.strategy_id] = {
                'alpha': r.alpha_strategy,
                'horizon': r.forecast_days,
                'efficiency': r.avg_efficiency_rating
            }
    
    # Create chart
    fig = go.Figure()
    
    # Add actual series
    if overlay_series.get('actual'):
        actual_data = overlay_series['actual']
        fig.add_trace(go.Scatter(
            x=[p['x'] for p in actual_data],
            y=[p['y'] for p in actual_data],
            mode='lines',
            name='Actual',
            line=dict(color='#888780', width=2)
        ))
    
    # Add strategy series with metadata
    for strategy in overlay_series['strategies']:
        strategy_id = strategy['strategy_id']
        metadata = ranking_lookup.get(strategy_id, {})
        
        predicted_data = strategy['predicted']
        fig.add_trace(go.Scatter(
            x=[p['x'] for p in predicted_data],
            y=[p['y'] for p in predicted_data],
            mode='lines',
            name=f"{strategy_id} (Î±={metadata.get('alpha', 0):.2f})",
            line=dict(width=1.5)
        ))
    
    fig.update_layout(
        title=f"{overlay_series.get('ticker', 'Unknown')} Strategy Overlay",
        height=400,
        xaxis_title="Time",
        yaxis_title="Price",
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show strategy metadata
    if rankings:
        st.markdown("#### Strategy Performance")
        for strategy in overlay_series['strategies']:
            strategy_id = strategy['strategy_id']
            metadata = ranking_lookup.get(strategy_id)
            if metadata:
                st.markdown(f"- **{strategy_id}**: Horizon={metadata.get('horizon', 'N/A')}d, Efficiency={metadata.get('efficiency', 0):.3f}")


def render_strategy_timeline(timeline):
    """Render strategy timeline from real data"""
    # This would render the timeline data
    st.info("Strategy timeline visualization - to be implemented")


def render_consensus(consensus):
    """Render consensus data from real data"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Direction", consensus.direction)
    
    with col2:
        st.metric("Confidence", f"{consensus.confidence:.1%}")
    
    with col3:
        st.metric("Total Weight", f"{consensus.total_weight:.1f}")
    
    if consensus.participating_strategies:
        st.markdown("**Participating Strategies:**")
        # Handle both integer and string cases
        try:
            if isinstance(consensus.participating_strategies, (list, tuple)):
                st.write(", ".join(str(s) for s in consensus.participating_strategies))
            else:
                st.write(f"{consensus.participating_strategies} strategies")
        except Exception as e:
            st.write(f"Error displaying strategies: {e}")
            st.write(f"Raw value: {consensus.participating_strategies}")


def render_controls(state: IntelligenceHubState, tickers: list[str], runs: list):
    """Render control panel with state management"""
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        ticker = st.selectbox(
            "Asset", 
            options=tickers, 
            index=tickers.index(state.ticker) if state.ticker in tickers else 0,
            key="ih_ticker",
            label_visibility="collapsed"
        )
        
        if ticker != state.ticker:
            st.session_state.asset_change = ticker
    
    with col2:
        timeframes = ['1M', '3M', '6M', '1Y']
        timeframe = st.selectbox(
            "Time Window", 
            options=timeframes,
            index=timeframes.index(state.timeframe) if state.timeframe in timeframes else 1,
            key="ih_timeframe",
            label_visibility="collapsed"
        )
        
        if timeframe != state.timeframe:
            st.session_state.timeframe_change = timeframe
    
    with col3:
        filter_options = ['All predictions', 'Correct only', 'Incorrect only']
        selected_filter = st.selectbox(
            "Filter", 
            options=filter_options,
            index=filter_options.index(state.filter_mode) if state.filter_mode in filter_options else 0,
            key="ih_filter",
            label_visibility="collapsed"
        )
        
        if selected_filter != state.filter_mode:
            st.session_state.filter_change = selected_filter


if __name__ == "__main__":
    # Mock service for testing
    service = DashboardService()
    intelligence_hub_main(service)
