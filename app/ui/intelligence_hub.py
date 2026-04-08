"""
AI-Powered Intelligence Hub
Advanced analytics and AI-driven insights for Alpha Engine
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import numpy as np

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.components.enhanced import (
    elevated_card, metric_card, status_badge, divider, info_panel
)


class IntelligenceHub:
    """Advanced AI-powered analytics and insights"""
    
    def __init__(self, service):
        self.service = service
        
    def get_market_sentiment_heatmap(self) -> Dict:
        """Generate market sentiment heatmap data"""
        sectors = ["Technology", "Healthcare", "Finance", "Energy", "Consumer", "Industrial"]
        regions = ["US", "Europe", "Asia", "Emerging"]
        
        data = []
        for sector in sectors:
            for region in regions:
                sentiment = np.random.normal(0.5, 0.2)  # Simulated sentiment
                confidence = np.random.uniform(0.6, 0.95)
                
                data.append({
                    "sector": sector,
                    "region": region,
                    "sentiment": max(0, min(1, sentiment)),
                    "confidence": confidence,
                    "volume": np.random.randint(1000, 100000)
                })
        
        return {"data": data, "sectors": sectors, "regions": regions}
    
    def get_ai_confidence_index(self, hours: int = 24) -> List[Dict]:
        """Calculate AI confidence index over time"""
        data = []
        base_confidence = 0.75
        
        for i in range(hours, 0, -1):
            timestamp = datetime.now() - timedelta(hours=i)
            
            # Simulate confidence with some volatility
            confidence = base_confidence + np.random.normal(0, 0.05)
            confidence = max(0.3, min(0.95, confidence))
            
            # Add prediction accuracy
            accuracy = confidence + np.random.normal(0, 0.02)
            accuracy = max(0.2, min(0.98, accuracy))
            
            data.append({
                "timestamp": timestamp,
                "confidence": confidence,
                "accuracy": accuracy,
                "predictions": np.random.randint(50, 200),
                "market_volatility": np.random.uniform(0.1, 0.4)
            })
        
        return data
    
    def detect_anomalies(self, hours: int = 12) -> List[Dict]:
        """Detect market anomalies using AI"""
        anomalies = []
        
        for i in range(5):  # Generate 5 random anomalies
            timestamp = datetime.now() - timedelta(hours=np.random.randint(1, hours))
            
            anomaly_types = [
                "Unusual Volume Spike",
                "Price Movement Anomaly", 
                "Sentiment Divergence",
                "Cross-Asset Correlation Break",
                "Volatility Regime Change"
            ]
            
            anomalies.append({
                "timestamp": timestamp,
                "type": np.random.choice(anomaly_types),
                "severity": np.random.choice(["Low", "Medium", "High"]),
                "confidence": np.random.uniform(0.7, 0.95),
                "description": f"Detected unusual pattern in {np.random.choice(['SPY', 'QQQ', 'AAPL', 'TSLA'])}",
                "impact_score": np.random.uniform(0.3, 0.9)
            })
        
        return sorted(anomalies, key=lambda x: x["timestamp"], reverse=True)
    
    def get_news_impact_analysis(self) -> Dict:
        """Analyze news impact on market sentiment"""
        news_items = [
            {
                "headline": "Fed Signals Rate Pause Amid Economic Uncertainty",
                "sentiment": -0.3,
                "impact": 0.8,
                "timestamp": datetime.now() - timedelta(hours=2),
                "affected_assets": ["SPY", "QQQ", "DXY"],
                "category": "Monetary Policy"
            },
            {
                "headline": "Tech Giants Report Strong Q3 Earnings",
                "sentiment": 0.6,
                "impact": 0.7,
                "timestamp": datetime.now() - timedelta(hours=4),
                "affected_assets": ["AAPL", "MSFT", "GOOGL"],
                "category": "Earnings"
            },
            {
                "headline": "Oil Prices Surge on Supply Concerns",
                "sentiment": -0.2,
                "impact": 0.6,
                "timestamp": datetime.now() - timedelta(hours=6),
                "affected_assets": ["XLE", "CVX", "XOM"],
                "category": "Commodities"
            },
            {
                "headline": "New AI Breakthrough Boosts Tech Sector",
                "sentiment": 0.8,
                "impact": 0.9,
                "timestamp": datetime.now() - timedelta(hours=8),
                "affected_assets": ["NVDA", "AMD", "SMH"],
                "category": "Technology"
            }
        ]
        
        return {"news_items": news_items}
    
    def get_economic_calendar(self) -> List[Dict]:
        """Get upcoming economic events"""
        events = [
            {
                "date": datetime.now() + timedelta(days=1),
                "event": "FOMC Interest Rate Decision",
                "impact": "High",
                "forecast": "5.25%-5.50%",
                "previous": "5.00%-5.25%"
            },
            {
                "date": datetime.now() + timedelta(days=2),
                "event": "CPI Data Release",
                "impact": "High", 
                "forecast": "3.2%",
                "previous": "3.7%"
            },
            {
                "date": datetime.now() + timedelta(days=3),
                "event": "Non-Farm Payrolls",
                "impact": "High",
                "forecast": "180K",
                "previous": "150K"
            }
        ]
        
        return events


def render_market_sentiment_heatmap(hub: IntelligenceHub):
    """Render market sentiment heatmap"""
    st.markdown("### Global Market Sentiment Heatmap")
    
    heatmap_data = hub.get_market_sentiment_heatmap()
    df = pd.DataFrame(heatmap_data["data"])
    
    # Create pivot table for heatmap
    pivot_df = df.pivot(index='sector', columns='region', values='sentiment')
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale='RdYlGn',
        text=np.round(pivot_df.values, 2),
        texttemplate='%{text}',
        textfont={"size": 12},
        hoverongaps=False,
        colorbar=dict(title="Sentiment Score")
    ))
    
    fig.update_layout(
        title="Market Sentiment by Sector & Region",
        xaxis_title="Region",
        yaxis_title="Sector",
        height=400,
        margin=dict(l=100, r=100, t=50, b=50)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Sentiment summary cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_sentiment = df['sentiment'].mean()
        metric_card("Avg Sentiment", f"{avg_sentiment:.2f}", "Global average", icon=" Globe")
    
    with col2:
        high_sentiment = len(df[df['sentiment'] > 0.6])
        metric_card("Bullish Sectors", str(high_sentiment), "Sentiment > 0.6", icon=" Trending Up")
    
    with col3:
        low_sentiment = len(df[df['sentiment'] < 0.4])
        metric_card("Bearish Sectors", str(low_sentiment), "Sentiment < 0.4", icon=" Trending Down")
    
    with col4:
        avg_confidence = df['confidence'].mean()
        metric_card("Confidence", f"{avg_confidence:.2f}", "AI confidence level", icon=" Brain")


def render_ai_confidence_dashboard(hub: IntelligenceHub):
    """Render AI confidence index dashboard"""
    st.markdown("### AI Confidence Index")
    
    confidence_data = hub.get_ai_confidence_index()
    df = pd.DataFrame(confidence_data)
    
    # Create multi-axis chart
    fig = go.Figure()
    
    # Add confidence line
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['confidence'],
        mode='lines+markers',
        name='AI Confidence',
        line=dict(color=COLORS['primary_800'], width=3),
        fill='tonexty'
    ))
    
    # Add accuracy line
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['accuracy'],
        mode='lines+markers',
        name='Prediction Accuracy',
        line=dict(color=COLORS['success_500'], width=2)
    ))
    
    # Add volume as bars
    fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['predictions'],
        name='Prediction Volume',
        yaxis='y2',
        marker_color=COLORS['neutral_300'],
        opacity=0.6
    ))
    
    fig.update_layout(
        title="AI Performance Metrics Over Time",
        xaxis_title="Time",
        yaxis_title="Confidence / Accuracy",
        yaxis2=dict(
            title="Prediction Volume",
            overlaying='y',
            side='right'
        ),
        height=400,
        hovermode="x unified"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Current metrics
    latest = df.iloc[-1]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        metric_card("Current Confidence", f"{latest['confidence']:.2f}", "AI confidence level", 
                   color=COLORS['primary_800'], icon=" Cpu")
    
    with col2:
        metric_card("Accuracy Rate", f"{latest['accuracy']:.2f}", "Prediction accuracy",
                   color=COLORS['success_500'], icon=" Target")
    
    with col3:
        metric_card("Predictions (24h)", str(latest['predictions']), "Total predictions",
                   icon=" Activity")
    
    with col4:
        metric_card("Market Volatility", f"{latest['market_volatility']:.2f}", "Current volatility",
                   color=COLORS['warning_500'] if latest['market_volatility'] > 0.3 else COLORS['success_500'],
                   icon=" Alert Triangle")


def render_anomaly_detection(hub: IntelligenceHub):
    """Render anomaly detection dashboard"""
    st.markdown("### AI Anomaly Detection")
    
    anomalies = hub.detect_anomalies()
    
    if not anomalies:
        info_panel("No Anomalies Detected", "AI systems are operating normally with no unusual patterns detected.", 
                  icon=" Check Circle", variant="success")
        return
    
    # Anomaly summary
    col1, col2, col3 = st.columns(3)
    
    with col1:
        high_severity = len([a for a in anomalies if a['severity'] == 'High'])
        metric_card("High Severity", str(high_severity), "Requires attention", 
                   color=COLORS['error_500'], icon=" Alert Triangle")
    
    with col2:
        medium_severity = len([a for a in anomalies if a['severity'] == 'Medium'])
        metric_card("Medium Severity", str(medium_severity), "Monitor closely",
                   color=COLORS['warning_500'], icon=" Alert Circle")
    
    with col3:
        avg_confidence = np.mean([a['confidence'] for a in anomalies])
        metric_card("Avg Confidence", f"{avg_confidence:.2f}", "Detection confidence",
                   icon=" Brain")
    
    # Anomaly details
    divider("Recent Anomalies")
    
    for anomaly in anomalies[:5]:  # Show top 5
        severity_color = {
            'High': COLORS['error_500'],
            'Medium': COLORS['warning_500'], 
            'Low': COLORS['info_500']
        }.get(anomaly['severity'], COLORS['neutral_500'])
        
        anomaly_html = f"""
        <div style="
            background: white;
            border: 1px solid {COLORS['border_light']};
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 12px;
            border-left: 4px solid {severity_color};
        ">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px;">
                <div>
                    <div style="font-weight: 600; color: {COLORS['neutral_900']}; margin-bottom: 4px;">
                        {anomaly['type']}
                    </div>
                    <div style="font-size: 14px; color: {COLORS['neutral_700']};">
                        {anomaly['description']}
                    </div>
                </div>
                <div style="display: flex; flex-direction: column; align-items: flex-end; gap: 4px;">
                    {status_badge(anomaly['severity'], 'sm')}
                    <div style="font-size: 12px; color: {COLORS['neutral_600']};">
                        {anomaly['timestamp'].strftime('%H:%M')}
                    </div>
                </div>
            </div>
            <div style="display: flex; justify-content: space-between; font-size: 12px; color: {COLORS['neutral_600']};">
                <span>Confidence: {anomaly['confidence']:.2f}</span>
                <span>Impact: {anomaly['impact_score']:.2f}</span>
            </div>
        </div>
        """
        st.markdown(anomaly_html, unsafe_allow_html=True)


def render_news_impact_analysis(hub: IntelligenceHub):
    """Render news impact analysis"""
    st.markdown("### News Impact Analysis")
    
    news_data = hub.get_news_impact_analysis()
    news_items = news_data['news_items']
    
    # News summary
    col1, col2, col3 = st.columns(3)
    
    with col1:
        positive_news = len([n for n in news_items if n['sentiment'] > 0])
        metric_card("Positive News", str(positive_news), "Bullish sentiment",
                   color=COLORS['success_500'], icon=" Trending Up")
    
    with col2:
        negative_news = len([n for n in news_items if n['sentiment'] < 0])
        metric_card("Negative News", str(negative_news), "Bearish sentiment",
                   color=COLORS['error_500'], icon=" Trending Down")
    
    with col3:
        avg_impact = np.mean([n['impact'] for n in news_items])
        metric_card("Avg Impact", f"{avg_impact:.2f}", "Market impact score",
                   icon=" Zap")
    
    # News items
    divider("Recent News & Impact")
    
    for news in news_items:
        sentiment_color = COLORS['success_500'] if news['sentiment'] > 0 else COLORS['error_500'] if news['sentiment'] < 0 else COLORS['neutral_500']
        sentiment_icon = " Trending Up" if news['sentiment'] > 0 else " Trending Down" if news['sentiment'] < 0 else " Minus"
        
        news_html = f"""
        <div style="
            background: white;
            border: 1px solid {COLORS['border_light']};
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 12px;
        ">
            <div style="margin-bottom: 12px;">
                <div style="font-weight: 600; color: {COLORS['neutral_900']}; margin-bottom: 8px; line-height: 1.4;">
                    {news['headline']}
                </div>
                <div style="display: flex; gap: 12px; align-items: center; font-size: 12px; color: {COLORS['neutral_600']};">
                    <span>{news['timestamp'].strftime('%H:%M')}</span>
                    {status_badge(news['category'], 'sm')}
                    <span style="color: {sentiment_color}; font-weight: 500;">
                        {sentiment_icon} {news['sentiment']:+.2f}
                    </span>
                    <span>Impact: {news['impact']:.2f}</span>
                </div>
            </div>
            <div style="font-size: 13px; color: {COLORS['neutral_700']};">
                <strong>Affected Assets:</strong> {', '.join(news['affected_assets'])}
            </div>
        </div>
        """
        st.markdown(news_html, unsafe_allow_html=True)


def render_economic_calendar(hub: IntelligenceHub):
    """Render economic calendar"""
    st.markdown("### Economic Calendar")
    
    events = hub.get_economic_calendar()
    
    for event in events:
        impact_color = {
            'High': COLORS['error_500'],
            'Medium': COLORS['warning_500'],
            'Low': COLORS['info_500']
        }.get(event['impact'], COLORS['neutral_500'])
        
        event_html = f"""
        <div style="
            background: white;
            border: 1px solid {COLORS['border_light']};
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 12px;
            border-left: 4px solid {impact_color};
        ">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px;">
                <div>
                    <div style="font-weight: 600; color: {COLORS['neutral_900']}; margin-bottom: 4px;">
                        {event['event']}
                    </div>
                    <div style="font-size: 14px; color: {COLORS['neutral_600']};">
                        {event['date'].strftime('%Y-%m-%d %H:%M')}
                    </div>
                </div>
                {status_badge(event['impact'], 'sm')}
            </div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; font-size: 13px;">
                <div>
                    <span style="color: {COLORS['neutral_600']};">Forecast: </span>
                    <span style="color: {COLORS['neutral_900']}; font-weight: 500;">{event['forecast']}</span>
                </div>
                <div>
                    <span style="color: {COLORS['neutral_600']};">Previous: </span>
                    <span style="color: {COLORS['neutral_900']}; font-weight: 500;">{event['previous']}</span>
                </div>
            </div>
        </div>
        """
        st.markdown(event_html, unsafe_allow_html=True)


def render_prediction_analytics(service):
    """Render the prediction analytics module."""
    st.markdown("### Prediction Analytics")
    
    from app.ui.middle.dashboard_service import PredictionAnalyticsQuery
    
    # Surgical Selectors
    col1, col2, col3 = st.columns(3)
    
    with col1:
        runs = service.list_prediction_runs()
        if not runs:
            st.warning("No prediction runs found.")
            return
        run_options = {r.id: r.label for r in runs}
        selected_run_id = st.selectbox("Select Prediction Run", options=list(run_options.keys()), format_func=lambda x: run_options[x])
    
    with col2:
        tickers = service.list_run_tickers(run_id=selected_run_id)
        selected_ticker = st.selectbox("Select Ticker", options=["Select..."] + tickers)
        if selected_ticker == "Select...":
            selected_ticker = None
            
    with col3:
        strategies = service.list_run_strategies(run_id=selected_run_id)
        selected_strategies = st.multiselect("Select Strategies", options=strategies, default=[strategies[0]] if strategies else [])
        primary_strategy = selected_strategies[0] if selected_strategies else None

    # Fetch Data
    query = PredictionAnalyticsQuery(run_id=selected_run_id, ticker=selected_ticker, strategy_id=primary_strategy)
    result = service.get_prediction_analytics(query)
    
    # Render Cards
    if result.metric_cards:
        m_cols = st.columns(len(result.metric_cards))
        for i, card in enumerate(result.metric_cards):
            with m_cols[i]:
                metric_card(card["label"], card["value"], "", icon=f" {card['icon']}")
    
    if result.chart_card and selected_ticker:
        # Check if we should render multi-strategy overlay
        if len(selected_strategies) > 1:
            overlay_data = service.get_multi_strategy_overlay(
                run_id=selected_run_id,
                ticker=selected_ticker,
                strategy_ids=selected_strategies
            )
            st.markdown(f"#### {overlay_data['title']}")
            fig = go.Figure()
            # Actual
            fig.add_trace(go.Scatter(
                x=[p["x"] for p in overlay_data["actual"]],
                y=[p["y"] for p in overlay_data["actual"]],
                name="Actual",
                line=dict(color=COLORS['primary_800'], width=3)
            ))
            # Predictions
            colors = [COLORS['success_500'], COLORS['warning_500'], COLORS['info_500'], COLORS['secondary_500']]
            for idx, strat in enumerate(overlay_data["strategies"]):
                c = colors[idx % len(colors)]
                fig.add_trace(go.Scatter(
                    x=[p["x"] for p in strat["predicted"]],
                    y=[p["y"] for p in strat["predicted"]],
                    name=strat["strategy_id"],
                    line=dict(color=c, width=2, dash='dash')
                ))
            fig.update_layout(height=450, margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Single strategy with Alpha Badge
            alpha = result.chart_card.get("alpha", 0.0)
            st.markdown(f"""
                <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px;">
                    <h4 style="margin: 0;">{result.chart_card['title']}</h4>
                    <span style="background: {COLORS['primary_50']}; color: {COLORS['primary_700']}; 
                          padding: 4px 12px; border-radius: 12px; font-size: 0.85rem; font-weight: 600;
                          border: 1px solid {COLORS['primary_200']};">
                        Alpha: {alpha:.3f}
                    </span>
                </div>
            """, unsafe_allow_html=True)
            
            pred_df = pd.DataFrame(result.chart_card["predicted"])
            act_df = pd.DataFrame(result.chart_card["actual"])
            
            fig = go.Figure()
            if not act_df.empty:
                fig.add_trace(go.Scatter(x=act_df["x"], y=act_df["y"], name="Actual", line=dict(color=COLORS['primary_800'], width=3)))
            if not pred_df.empty:
                fig.add_trace(go.Scatter(x=pred_df["x"], y=pred_df["y"], name="Predicted", line=dict(color=COLORS['success_500'], width=2, dash='dash')))
                
            fig.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=0), hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
        
    if result.leaderboard_card:
        st.markdown(f"#### {result.leaderboard_card['title']}")
        st.table(result.leaderboard_card["data"])


def intelligence_hub_main(service):
    """Main intelligence hub dashboard"""
    
    # Apply theme
    apply_theme()
    
    # Initialize hub
    hub = IntelligenceHub(service)
    
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
            AI-Powered Intelligence Hub
        </h1>
        <p style="margin: 0; font-size: 18px; opacity: 0.9;">
            Advanced analytics and AI-driven market insights
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Navigation tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        " Market Sentiment", "AI Confidence", "Anomaly Detection", "News Impact", "Economic Calendar", "Prediction Analytics"
    ])
    
    with tab1:
        render_market_sentiment_heatmap(hub)
    
    with tab2:
        render_ai_confidence_dashboard(hub)
    
    with tab3:
        render_anomaly_detection(hub)
    
    with tab4:
        render_news_impact_analysis(hub)
    
    with tab5:
        render_economic_calendar(hub)
        
    with tab6:
        render_prediction_analytics(service)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #757575; font-size: 12px; margin-top: 32px;">
        <div>AI-Powered Intelligence Hub</div>
        <div style="margin-top: 4px;">Real-time market insights powered by Alpha Engine AI</div>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    # Mock service for testing
    class MockService:
        pass
    
    intelligence_hub_main(MockService())
