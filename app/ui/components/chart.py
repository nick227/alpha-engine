"""
Alpha Engine Chart Components
Provides reusable Plotly-based chart components for time-series visualization
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timezone
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

# Chart configuration constants
DEFAULT_COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e', 
    'success': '#2ca02c',
    'danger': '#d62728',
    'warning': '#ff9800',
    'info': '#17a2b8',
    'light': '#f8f9fa',
    'dark': '#343a40'
}

REGIME_COLORS = {
    'HIGH_VOL': 'rgba(255, 0, 0, 0.1)',
    'LOW_VOL': 'rgba(0, 255, 0, 0.1)',
    'NEUTRAL': 'rgba(128, 128, 128, 0.05)'
}


class TimeSeriesChart:
    """Reusable time-series chart component with built-in Alpha Engine theming."""
    
    def __init__(self, title: str, height: int = 400):
        self.title = title
        self.height = height
        self.fig = go.Figure()
        
    def add_trace(self, 
                  data: pd.DataFrame, 
                  x_col: str = 'timestamp',
                  y_col: str = 'value',
                  name: str = 'Series',
                  color: str = DEFAULT_COLORS['primary'],
                  line_type: str = 'line',
                  show_legend: bool = True):
        """Add a trace to the chart."""
        
        if line_type == 'line':
            self.fig.add_trace(go.Scatter(
                x=data[x_col],
                y=data[y_col],
                mode='lines',
                name=name,
                line=dict(color=color, width=2),
                showlegend=show_legend
            ))
        elif line_type == 'area':
            self.fig.add_trace(go.Scatter(
                x=data[x_col],
                y=data[y_col],
                mode='lines',
                name=name,
                fill='tonexty' if len(self.fig.data) > 0 else 'tozeroy',
                line=dict(color=color, width=1),
                fillcolor=color.replace('rgb', 'rgba').replace(')', ', 0.3)'),
                showlegend=show_legend
            ))
        elif line_type == 'scatter':
            self.fig.add_trace(go.Scatter(
                x=data[x_col],
                y=data[y_col],
                mode='markers',
                name=name,
                marker=dict(color=color, size=6),
                showlegend=show_legend
            ))
    
    def add_regime_background(self, regimes: pd.DataFrame):
        """Add regime changes as background shading."""
        for _, regime in regimes.iterrows():
            self.fig.add_vrect(
                x0=regime['start_time'],
                x1=regime['end_time'],
                fillcolor=REGIME_COLORS.get(regime['regime'], REGIME_COLORS['NEUTRAL']),
                layer="below",
                line_width=0,
                annotation_text=regime['regime'],
                annotation_position="top left"
            )
    
    def add_confidence_bands(self, 
                           data: pd.DataFrame,
                           x_col: str = 'timestamp',
                           mean_col: str = 'mean',
                           lower_col: str = 'lower',
                           upper_col: str = 'upper',
                           name: str = 'Confidence'):
        """Add confidence bands around a mean line."""
        # Add upper band
        self.fig.add_trace(go.Scatter(
            x=data[x_col],
            y=data[upper_col],
            mode='lines',
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip"
        ))
        
        # Add lower band
        self.fig.add_trace(go.Scatter(
            x=data[x_col],
            y=data[lower_col],
            mode='lines',
            line=dict(width=0),
            fill='tonexty',
            fillcolor=f'rgba(100, 149, 237, 0.2)',
            name=f"{name} Band",
            showlegend=True,
            hoverinfo="skip"
        ))
        
        # Add mean line
        self.fig.add_trace(go.Scatter(
            x=data[x_col],
            y=data[mean_col],
            mode='lines',
            line=dict(color=DEFAULT_COLORS['primary'], width=2),
            name=name,
            showlegend=True
        ))
    
    def finalize(self, 
                xaxis_title: str = "Time",
                yaxis_title: str = "Value",
                show_legend: bool = True,
                legend_orientation: str = "h"):
        """Finalize chart layout and styling."""
        self.fig.update_layout(
            title=dict(text=self.title, x=0.5),
            height=self.height,
            xaxis_title=xaxis_title,
            yaxis_title=yaxis_title,
            showlegend=show_legend,
            legend=dict(orientation=legend_orientation, yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=50, r=50, t=50, b=50),
            hovermode="x unified",
            template="plotly_white"
        )
        
        # Dark mode support
        if st.get_option("theme.base") == "dark":
            self.fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
            )
    
    def render(self):
        """Render the chart in Streamlit."""
        st.plotly_chart(self.fig, width="stretch")


def create_consensus_timeline(consensus_data: List[Dict], height: int = 400) -> TimeSeriesChart:
    """
    Create a consensus confidence timeline with regime context.
    
    Args:
        consensus_data: List of consensus dictionaries with timestamp, confidence, regime
        height: Chart height in pixels
        
    Returns:
        TimeSeriesChart: Configured chart ready to render
    """
    if not consensus_data:
        chart = TimeSeriesChart("No Consensus Data Available", height)
        chart.finalize()
        return chart
    
    # Convert to DataFrame
    df = pd.DataFrame(consensus_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    chart = TimeSeriesChart("Consensus Confidence Timeline", height)
    
    # Add main confidence line
    chart.add_trace(
        df, 
        y_col='confidence', 
        name='Consensus Confidence',
        color=DEFAULT_COLORS['primary']
    )
    
    # Add participating strategies weight
    if 'total_weight' in df.columns:
        chart.add_trace(
            df,
            y_col='total_weight', 
            name='Total Weight',
            color=DEFAULT_COLORS['secondary'],
            line_type='area'
        )
    
    # Add regime shading if available
    if 'regime' in df.columns:
        regimes = df[df['regime'].notna()].copy()
        if not regimes.empty:
            # Create regime segments
            regime_segments = []
            current_regime = None
            start_time = None
            
            for _, row in regimes.iterrows():
                if current_regime != row['regime']:
                    if current_regime is not None and start_time is not None:
                        regime_segments.append({
                            'regime': current_regime,
                            'start_time': start_time,
                            'end_time': row['timestamp']
                        })
                    current_regime = row['regime']
                    start_time = row['timestamp']
            
            # Add final segment
            if current_regime is not None and start_time is not None:
                regime_segments.append({
                    'regime': current_regime,
                    'start_time': start_time,
                    'end_time': df['timestamp'].iloc[-1]
                })
            
            if regime_segments:
                regime_df = pd.DataFrame(regime_segments)
                chart.add_regime_background(regime_df)
    
    chart.finalize(
        xaxis_title="Time",
        yaxis_title="Confidence Score",
        show_legend=True
    )
    
    return chart


def create_strategy_performance_chart(strategy_data: Dict[str, List[Dict]], height: int = 350) -> TimeSeriesChart:
    """
    Create comparative strategy performance chart.
    
    Args:
        strategy_data: Dictionary with strategy names as keys and performance data as values
        height: Chart height in pixels
        
    Returns:
        TimeSeriesChart: Configured chart ready to render
    """
    chart = TimeSeriesChart("Strategy Performance Comparison", height)
    
    colors = [DEFAULT_COLORS['primary'], DEFAULT_COLORS['secondary'], DEFAULT_COLORS['success'], DEFAULT_COLORS['warning']]
    
    for i, (strategy_name, data) in enumerate(strategy_data.items()):
        if not data:
            continue
            
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        color = colors[i % len(colors)]
        
        # Add win rate line
        if 'win_rate' in df.columns:
            chart.add_trace(
                df,
                y_col='win_rate',
                name=f"{strategy_name} Win Rate",
                color=color
            )
        
        # Add alpha as secondary metric
        if 'alpha' in df.columns:
            chart.add_trace(
                df,
                y_col='alpha',
                name=f"{strategy_name} Alpha",
                color=color,
                line_type='scatter'
            )
    
    chart.finalize(
        xaxis_title="Time",
        yaxis_title="Performance Metric",
        show_legend=True,
        legend_orientation="v"
    )
    
    return chart


def create_signal_flow_chart(signals_data: List[Dict], height: int = 400) -> TimeSeriesChart:
    """
    Create real-time signal flow visualization.
    
    Args:
        signals_data: List of signal dictionaries
        height: Chart height in pixels
        
    Returns:
        TimeSeriesChart: Configured chart ready to render
    """
    if not signals_data:
        chart = TimeSeriesChart("No Signal Data Available", height)
        chart.finalize()
        return chart
    
    df = pd.DataFrame(signals_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    chart = TimeSeriesChart("Signal Flow Visualization", height)
    
    # Separate signals by direction
    up_signals = df[df['direction'].isin(['up', 'long', 'buy', '1', '+1'])]
    down_signals = df[df['direction'].isin(['down', 'short', 'sell', '-1'])]
    
    # Add buy signals
    if not up_signals.empty:
        chart.add_trace(
            up_signals,
            y_col='confidence',
            name='Buy Signals',
            color=DEFAULT_COLORS['success'],
            line_type='scatter'
        )
    
    # Add sell signals
    if not down_signals.empty:
        chart.add_trace(
            down_signals,
            y_col='confidence',
            name='Sell Signals',
            color=DEFAULT_COLORS['danger'],
            line_type='scatter'
        )
    
    # Add confidence trend line
    confidence_avg = df.groupby(df['timestamp'].dt.floor('1H'))['confidence'].mean().reset_index()
    if not confidence_avg.empty:
        chart.add_trace(
            confidence_avg,
            x_col='timestamp',
            y_col='confidence',
            name='Confidence Trend',
            color=DEFAULT_COLORS['info'],
            line_type='line'
        )
    
    chart.finalize(
        xaxis_title="Time",
        yaxis_title="Signal Confidence",
        show_legend=True
    )
    
    return chart


def create_multi_axis_chart(data_configs: List[Dict], height: int = 450) -> go.Figure:
    """
    Create a multi-axis chart for complex data visualization.
    
    Args:
        data_configs: List of data configuration dictionaries
        height: Chart height in pixels
        
    Returns:
        go.Figure: Plotly figure with multiple y-axes
    """
    fig = make_subplots(
        specs=[[{"secondary_y": True}]],
        shared_xaxes=True,
        vertical_spacing=0.1
    )
    
    colors = [DEFAULT_COLORS['primary'], DEFAULT_COLORS['secondary'], DEFAULT_COLORS['success']]
    
    for i, config in enumerate(data_configs):
        df = pd.DataFrame(config['data'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        color = colors[i % len(colors)]
        secondary_y = config.get('secondary_y', False)
        
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df[config['y_col']],
                mode='lines',
                name=config['name'],
                line=dict(color=color, width=2)
            ),
            secondary_y=secondary_y
        )
    
    fig.update_layout(
        height=height,
        title_text="Multi-Axis Performance View",
        hovermode="x unified",
        template="plotly_white"
    )
    
    return fig
