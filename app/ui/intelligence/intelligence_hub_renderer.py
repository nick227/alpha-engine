"""
Intelligence Hub Renderer

Renders Intelligence Hub UI from DTO data without direct database access.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from app.ui.theme import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.components.enhanced import (
    elevated_card, metric_card, status_badge, divider
)


def render_intelligence_hub(dto: 'IntelligenceHubDTO'):
    """Main intelligence hub renderer from DTO"""
    
    # Apply theme
    apply_theme()
    
    # Header
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #1565C0 0%, #0D47A1 100%);
        color: white;
        padding: 40px;
        border-radius: 16px;
        margin-bottom: 32px;
        text-align: center;
    ">
        <h1 style="margin: 0; font-size: 36px; font-weight: 700; margin-bottom: 8px;">
            Intelligence Hub
        </h1>
        <p style="margin: 0; font-size: 18px; opacity: 0.9;">
            Strategy performance explorer across time periods
        </p>
        <p style="margin: 8px 0 0; font-size: 14px; opacity: 0.7;">
            Analyze prediction accuracy, identify regime changes, and understand market context
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Controls section
    render_controls(dto.state, dto.tickers, dto.runs)
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Champion matrix
        render_champion_matrix(dto.matrix_rows)
        
        # Strategy rankings
        render_strategy_rankings(dto.strategy_rankings)
    
    with col2:
        # Strategy details
        if dto.overlay_series:
            render_strategy_overlays(dto.overlay_series, dto.state.filter_mode)
        
        # Timeline (if selected)
        if dto.timeline:
            render_strategy_timeline(dto.timeline)
    
    # Consensus (if available)
    if dto.consensus:
        render_consensus_view(dto.consensus)


def render_controls(state: 'IntelligenceHubState', tickers: List[str], runs: List['PredictionRunView']):
    """Render control panel with state management"""
    
    # Asset selector
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        st.markdown("**Asset**")
        ticker = st.selectbox(
            "Select asset", 
            options=tickers, 
            index=tickers.index(state.ticker) if state.ticker in tickers else 0,
            key="ih_ticker"
        )
        
        # Emit state change event
        if ticker != state.ticker:
            st.session_state.asset_change = ticker
    
    with col2:
        st.markdown("**Timeframe**")
        timeframes = ['1M', '3M', '6M', '1Y']
        timeframe = st.selectbox(
            "Select window", 
            options=timeframes,
            index=timeframes.index(state.timeframe) if state.timeframe in timeframes else 1,
            key="ih_timeframe"
        )
        
        # Emit state change event
        if timeframe != state.timeframe:
            st.session_state.timeframe_change = timeframe
    
    with col3:
        st.markdown("**Prediction Run**")
        if runs:
            run_labels = [f"{run.id[:8]} ({run.timeframe})" for run in runs]
            run = st.selectbox(
                "Select run",
                options=run_labels,
                index=0,
                key="ih_run"
            )
            
            # Get actual run ID
            selected_run_id = runs[run_labels.index(run)].id if run in run_labels else runs[0].id
            
            # Emit state change event
            if selected_run_id != state.run_id:
                st.session_state.run_change = selected_run_id


def render_champion_matrix(matrix_rows: List['ChampionMatrixView']):
    """Render champion performance matrix"""
    
    if not matrix_rows:
        st.info("No champion data available")
        return
    
    st.markdown("### Champion Performance Matrix")
    
    # Group by ticker for display
    df = pd.DataFrame([
        {
            "Ticker": row.ticker,
            "Timeframe": row.timeframe,
            "Strategy": row.strategy_id,
            "Alpha": f"{row.alpha_strategy:.3f}",
            "Win Rate": f"{row.direction_accuracy:.1%}",
            "Samples": row.samples,
            "Entry": f"${row.entry_price:,.2f}" if row.entry_price else "N/A",
            "Target": f"${row.target_price:,.2f}" if row.target_price else "N/A",
        }
        for row in matrix_rows
    ])
    
    # Display matrix
    st.dataframe(df, use_container_width=True)


def render_strategy_rankings(rankings: List['StrategyEfficiencyView']):
    """Render strategy efficiency rankings"""
    
    if not rankings:
        return
    
    st.markdown("#### Strategy Efficiency Rankings")
    
    for i, ranking in enumerate(rankings[:5]):  # Top 5
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


def render_strategy_overlays(overlay_data: 'ComparisonData', filter_mode: str):
    """Render multi-strategy overlay charts"""
    
    st.markdown("### Strategy Performance Comparison")
    
    # Filter controls
    filter_options = ['All predictions', 'Correct only', 'Incorrect only']
    selected_filter = st.selectbox(
        "Filter predictions",
        options=filter_options,
        index=filter_options.index(filter_mode) if filter_mode in filter_options else 0,
        key="ih_filter"
    )
    
    # Emit filter change event
    if selected_filter != filter_mode:
        st.session_state.filter_change = selected_filter
    
    # Chart
    if overlay_data and overlay_data.strategies:
        fig = go.Figure()
        
        # Actual price line
        if overlay_data.actual_series:
            actual_points = overlay_data.actual_series
            fig.add_trace(go.Scatter(
                x=[p.timestamp for p in actual_points],
                y=[p.value for p in actual_points],
                mode='lines',
                name='Actual',
                line=dict(color='#888780', width=2)
            ))
        
        # Strategy prediction lines
        for strategy in overlay_data.strategies:
            if strategy.predicted:
                fig.add_trace(go.Scatter(
                    x=[p.timestamp for p in strategy.predicted],
                    y=[p.value for p in strategy.predicted],
                    mode='lines+markers',
                    name=strategy.strategy_id,
                    line=dict(color=strategy.color or '#378ADD', width=1, dash='dash')
                ))
        
        fig.update_layout(
            title="Strategy Performance Comparison",
            xaxis_title="Time",
            yaxis_title="Price",
            height=400,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)


def render_strategy_timeline(timeline_data: 'StrategyTimelineView'):
    """Render detailed strategy timeline"""
    
    st.markdown(f"### Strategy Timeline: {timeline_data.strategy_id}")
    
    if not timeline_data.predictions:
        st.info("No timeline data available")
        return
    
    # Timeline chart
    fig = go.Figure()
    
    # Add predictions
    pred_dates = [p.timestamp for p in timeline_data.predictions]
    pred_returns = [p.predicted_return * 100 for p in timeline_data.predictions]
    actual_returns = [p.actual_return * 100 for p in timeline_data.predictions]
    
    fig.add_trace(go.Scatter(
        x=pred_dates,
        y=pred_returns,
        mode='lines+markers',
        name='Predicted Return %',
        line=dict(color='#4CAF50', width=2, dash='dot'),
        marker=dict(size=6)
    ))
    
    fig.add_trace(go.Scatter(
        x=pred_dates,
        y=actual_returns,
        mode='lines',
        name='Actual Return %',
        line=dict(color='#FF9800', width=2)
    ))
    
    # Highlight direction misses
    misses = [p for p in timeline_data.predictions if not p.direction_correct]
    if misses:
        miss_dates = [p.timestamp for p in misses]
        miss_returns = [p.predicted_return * 100 for p in misses]
        
        fig.add_trace(go.Scatter(
            x=miss_dates,
            y=miss_returns,
            mode='markers',
            name='Direction Miss',
            marker=dict(color='red', size=8, symbol='x')
        ))
    
    fig.update_layout(
        title=f"Strategy Performance: {timeline_data.strategy_id}",
        xaxis_title="Time",
        yaxis_title="Return %",
        height=400,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Performance metrics
    if timeline_data.performance_metrics:
        metrics = timeline_data.performance_metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Alpha", f"{metrics.alpha:.3f}")
        
        with col2:
            st.metric("Win Rate", f"{metrics.win_rate:.1%}")
        
        with col3:
            st.metric("MAE", f"{metrics.mae:.2f}%")
        
        with col4:
            st.metric("Total Return", f"{metrics.total_return:.2%}")


def render_consensus_view(consensus_data: 'ConsensusView'):
    """Render consensus/ensemble predictions"""
    
    st.markdown("### Market Consensus")
    
    # Consensus metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Direction", consensus_data.direction)
    
    with col2:
        st.metric("Confidence", f"{consensus_data.confidence:.1%}")
    
    with col3:
        st.metric("Total Weight", f"{consensus_data.total_weight:.1f}")
    
    # Participating strategies
    if consensus_data.participating_strategies:
        st.markdown("**Participating Strategies:**")
        st.write(", ".join(consensus_data.participating_strategies))
    
    # Regime information
    if consensus_data.active_regime:
        st.markdown(f"**Active Regime:** {consensus_data.active_regime}")
