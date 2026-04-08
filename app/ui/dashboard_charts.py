"""
Dashboard Chart Integration
Extends the main dashboard with Plotly-based time-series visualizations
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from app.ui.components.chart import (
    TimeSeriesChart,
    create_consensus_timeline,
    create_strategy_performance_chart,
    create_signal_flow_chart,
    create_multi_axis_chart,
    DEFAULT_COLORS
)
from app.ui.middle.dashboard_service import DashboardService


class DashboardCharts:
    """Manages chart data fetching and rendering for the dashboard."""
    
    def __init__(self, service: DashboardService):
        self.service = service
        self._chart_cache = {}
    
    def get_consensus_timeline_data(self, tenant_id: str, ticker: str, hours: int = 24) -> List[Dict]:
        """Fetch consensus data for timeline visualization."""
        try:
            # Get historical consensus data
            # This would need to be implemented in the service layer
            # For now, return sample data structure
            return [
                {
                    'timestamp': (datetime.now() - timedelta(hours=i)).isoformat(),
                    'confidence': 0.7 + (i % 10) * 0.05,
                    'regime': 'HIGH_VOL' if i % 8 < 4 else 'LOW_VOL',
                    'total_weight': 1.0 + (i % 5) * 0.1
                }
                for i in range(hours, 0, -1)
            ]
        except Exception as e:
            st.error(f"Error fetching consensus timeline: {e}")
            return []
    
    def get_strategy_performance_data(self, tenant_id: str, hours: int = 48) -> Dict[str, List[Dict]]:
        """Fetch strategy performance data for comparison."""
        try:
            # This would fetch historical performance data
            # Sample structure for demonstration
            return {
                'Champion - Sentiment': [
                    {
                        'timestamp': (datetime.now() - timedelta(hours=i)).isoformat(),
                        'win_rate': 0.65 + (i % 8) * 0.03,
                        'alpha': 0.02 + (i % 10) * 0.005,
                        'stability': 0.8 + (i % 6) * 0.02
                    }
                    for i in range(hours, 0, -1)
                ],
                'Champion - Quant': [
                    {
                        'timestamp': (datetime.now() - timedelta(hours=i)).isoformat(),
                        'win_rate': 0.62 + (i % 7) * 0.04,
                        'alpha': 0.018 + (i % 9) * 0.006,
                        'stability': 0.75 + (i % 5) * 0.03
                    }
                    for i in range(hours, 0, -1)
                ],
                'Challenger - Sentiment': [
                    {
                        'timestamp': (datetime.now() - timedelta(hours=i)).isoformat(),
                        'win_rate': 0.58 + (i % 9) * 0.03,
                        'alpha': 0.015 + (i % 8) * 0.004,
                        'stability': 0.7 + (i % 4) * 0.02
                    }
                    for i in range(hours, 0, -1)
                ]
            }
        except Exception as e:
            st.error(f"Error fetching strategy performance: {e}")
            return {}
    
    def get_signal_flow_data(self, tenant_id: str, ticker: Optional[str] = None, hours: int = 12) -> List[Dict]:
        """Fetch signal data for flow visualization."""
        try:
            # Get recent signals and transform for charting
            signals = self.service.get_recent_signals(
                tenant_id=tenant_id,
                ticker=ticker,
                limit=100
            )
            
            return [
                {
                    'timestamp': s.time,
                    'confidence': float(s.confidence),
                    'direction': s.direction,
                    'strategy': s.strategy,
                    'ticker': s.ticker
                }
                for s in signals
            ]
        except Exception as e:
            st.error(f"Error fetching signal flow: {e}")
            return []
    
    def get_market_overview_data(self, tenant_id: str, hours: int = 24) -> Dict[str, Any]:
        """Fetch comprehensive market overview data."""
        try:
            # This would aggregate multiple data sources
            return {
                'consensus': self.get_consensus_timeline_data(tenant_id, "SPY", hours),
                'volume': [
                    {
                        'timestamp': (datetime.now() - timedelta(hours=i)).isoformat(),
                        'volume': 1000000 + (i % 20) * 50000
                    }
                    for i in range(hours, 0, -1)
                ],
                'volatility': [
                    {
                        'timestamp': (datetime.now() - timedelta(hours=i)).isoformat(),
                        'volatility': 0.15 + (i % 12) * 0.02
                    }
                    for i in range(hours, 0, -1)
                ]
            }
        except Exception as e:
            st.error(f"Error fetching market overview: {e}")
            return {}


def render_hero_chart_section(charts: DashboardCharts, tenant_id: str, ticker: Optional[str]):
    """Render the hero section with market overview chart."""
    st.markdown("### Market Overview")
    st.markdown("---")
    
    # Chart controls
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        time_range = st.selectbox(
            "Time Range",
            options=["1H", "6H", "24H", "3D", "7D"],
            index=2,
            key="hero_time_range"
        )
    
    with col2:
        show_volume = st.checkbox("Show Volume", value=True, key="show_volume")
    
    with col3:
        show_volatility = st.checkbox("Show Volatility", value=True, key="show_volatility")
    
    # Get data based on selection
    hours_map = {"1H": 1, "6H": 6, "24H": 24, "3D": 72, "7D": 168}
    hours = hours_map[time_range]
    
    market_data = charts.get_market_overview_data(tenant_id, hours)
    
    if market_data.get('consensus'):
        # Create multi-axis chart
        data_configs = [
            {
                'data': market_data['consensus'],
                'y_col': 'confidence',
                'name': 'Consensus Confidence',
                'secondary_y': False
            }
        ]
        
        if show_volume and market_data.get('volume'):
            data_configs.append({
                'data': market_data['volume'],
                'y_col': 'volume',
                'name': 'Volume',
                'secondary_y': True
            })
        
        if show_volatility and market_data.get('volatility'):
            data_configs.append({
                'data': market_data['volatility'],
                'y_col': 'volatility',
                'name': 'Volatility',
                'secondary_y': True
            })
        
        fig = create_multi_axis_chart(data_configs, height=450)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No market overview data available.")


def render_strategy_performance_section(charts: DashboardCharts, tenant_id: str):
    """Render strategy performance comparison charts."""
    st.markdown("### Strategy Performance Analysis")
    st.markdown("---")
    
    # Performance controls
    col1, col2 = st.columns([1, 2])
    
    with col1:
        performance_range = st.selectbox(
            "Performance Range",
            options=["12H", "24H", "48H", "7D"],
            index=2,
            key="performance_range"
        )
        
        metric_focus = st.selectbox(
            "Focus Metric",
            options=["All Metrics", "Win Rate", "Alpha", "Stability"],
            index=0,
            key="metric_focus"
        )
    
    with col2:
        st.info("Compare champion and challenger performance over time. Track win rates, alpha generation, and stability metrics.")
    
    hours_map = {"12H": 12, "24H": 24, "48H": 48, "7D": 168}
    hours = hours_map[performance_range]
    
    strategy_data = charts.get_strategy_performance_data(tenant_id, hours)
    
    if strategy_data:
        chart = create_strategy_performance_chart(strategy_data, height=350)
        chart.render()
    else:
        st.warning("No strategy performance data available.")


def render_signal_flow_section(charts: DashboardCharts, tenant_id: str, ticker: Optional[str]):
    """Render signal flow visualization."""
    st.markdown("### Signal Flow Analysis")
    st.markdown("---")
    
    # Signal controls
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        signal_range = st.selectbox(
            "Signal Range",
            options=["6H", "12H", "24H"],
            index=1,
            key="signal_range"
        )
    
    with col2:
        strategy_filter = st.multiselect(
            "Filter Strategies",
            options=["Sentiment", "Quant", "Consensus"],
            default=["Sentiment", "Quant"],
            key="strategy_filter"
        )
    
    with col3:
        st.info("Real-time signal flow visualization shows confidence levels and direction indicators for different strategies.")
    
    hours_map = {"6H": 6, "12H": 12, "24H": 24}
    hours = hours_map[signal_range]
    
    signals_data = charts.get_signal_flow_data(tenant_id, ticker, hours)
    
    # Filter by strategy if selected
    if strategy_filter and signals_data:
        signals_data = [
            s for s in signals_data 
            if any(filter_strategy.lower() in s['strategy'].lower() for filter_strategy in strategy_filter)
        ]
    
    if signals_data:
        chart = create_signal_flow_chart(signals_data, height=400)
        chart.render()
        
        # Signal statistics
        with st.expander("Signal Statistics"):
            df = pd.DataFrame(signals_data)
            
            if not df.empty:
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Signals", len(df))
                
                with col2:
                    avg_confidence = df['confidence'].mean()
                    st.metric("Avg Confidence", f"{avg_confidence:.3f}")
                
                with col3:
                    buy_signals = len(df[df['direction'].isin(['up', 'long', 'buy', '1', '+1'])])
                    st.metric("Buy Signals", buy_signals)
                
                with col4:
                    sell_signals = len(df[df['direction'].isin(['down', 'short', 'sell', '-1'])])
                    st.metric("Sell Signals", sell_signals)
    else:
        st.warning("No signal data available for the selected criteria.")


def render_consensus_timeline_section(charts: DashboardCharts, tenant_id: str, ticker: Optional[str]):
    """Render consensus timeline with regime context."""
    st.markdown("### Consensus Timeline & Regime Analysis")
    st.markdown("---")
    
    # Timeline controls
    col1, col2 = st.columns([1, 3])
    
    with col1:
        timeline_range = st.selectbox(
            "Timeline Range",
            options=["12H", "24H", "48H", "7D"],
            index=1,
            key="timeline_range"
        )
        
        show_regimes = st.checkbox("Show Regime Context", value=True, key="show_regimes")
    
    with col2:
        st.info("Consensus confidence evolution with regime context. Observe how different market regimes affect strategy consensus.")
    
    hours_map = {"12H": 12, "24H": 24, "48H": 48, "7D": 168}
    hours = hours_map[timeline_range]
    
    consensus_data = charts.get_consensus_timeline_data(tenant_id, ticker or "SPY", hours)
    
    if consensus_data:
        chart = create_consensus_timeline(consensus_data, height=350)
        chart.render()
    else:
        st.warning("No consensus timeline data available.")


def integrate_charts_into_dashboard(service: DashboardService, tenant_id: str, ticker: Optional[str]):
    """Main integration function to add charts to the dashboard."""
    
    # Initialize charts manager
    charts = DashboardCharts(service)
    
    # Add chart sections to dashboard
    render_hero_chart_section(charts, tenant_id, ticker)
    st.divider()
    
    render_consensus_timeline_section(charts, tenant_id, ticker)
    st.divider()
    
    render_strategy_performance_section(charts, tenant_id)
    st.divider()
    
    render_signal_flow_section(charts, tenant_id, ticker)
