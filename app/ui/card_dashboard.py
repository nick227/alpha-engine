"""
Card-Driven Dashboard Architecture
River of cards powered by shared controls with independent rendering
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.middle.dashboard_service import DashboardService


# ============================================================================
# CARD SCHEMA DEFINITIONS
# ============================================================================

class CardSchema:
    """Universal card schema for dashboard rendering"""
    
    def __init__(self, 
                 card_type: str,
                 title: str,
                 subtitle: str = None,
                 data: Dict = None,
                 metadata: Dict = None):
        self.card_type = card_type  # "chart", "number", "table"
        self.title = title
        self.subtitle = subtitle
        self.data = data or {}
        self.metadata = metadata or {}


class SeriesPoint:
    """Universal data point for time series"""
    def __init__(self, x: Any, y: Any, kind: str = None, label: str = None):
        self.x = x
        self.y = y
        self.kind = kind  # "historical", "forecast", "confidence_upper", "confidence_lower", etc.
        self.label = label


class CardData:
    """Data payload for different card types"""
    
    @staticmethod
    def chart(series: List[SeriesPoint], 
              title: str,
              subtitle: str = None,
              chart_type: str = "line",
              metadata: Dict = None) -> CardSchema:
        """Create chart card data"""
        return CardSchema(
            card_type="chart",
            title=title,
            subtitle=subtitle,
            data={"series": series, "chart_type": chart_type},
            metadata=metadata
        )
    
    @staticmethod
    def number(primary_metric: str,
              primary_value: Any,
              secondary_metrics: Dict = None,
              rank: int = None,
              trend: str = None,
              confidence: float = None,
              metadata: Dict = None) -> CardSchema:
        """Create number card data"""
        return CardSchema(
            card_type="number",
            title=primary_metric,
            subtitle=f"Rank #{rank}" if rank else None,
            data={
                "primary_value": primary_value,
                "secondary_metrics": secondary_metrics or {},
                "rank": rank,
                "trend": trend,
                "confidence": confidence
            },
            metadata=metadata
        )
    
    @staticmethod
    def table(headers: List[str],
              rows: List[List[Any]],
              title: str,
              subtitle: str = None,
              metadata: Dict = None) -> CardSchema:
        """Create table card data"""
        return CardSchema(
            card_type="table",
            title=title,
            subtitle=subtitle,
            data={"headers": headers, "rows": rows},
            metadata=metadata
        )


# ============================================================================
# DATA PROVIDER INTERFACE
# ============================================================================

class CardDataProvider:
    """Abstract interface for card data providers"""
    
    def __init__(self, service: DashboardService):
        self.service = service
    
    def get_cards(self, 
                 view: str, 
                 strategy: str, 
                 horizon: str,
                 filters: Dict = None) -> List[CardSchema]:
        """Get cards based on view, strategy, and horizon"""
        raise NotImplementedError


# ============================================================================
# MOCK DATA PROVIDER (for demonstration)
# ============================================================================

class MockDataProvider(CardDataProvider):
    """Mock data provider for demonstration"""
    
    def get_cards(self, view: str, strategy: str, horizon: str, filters: Dict = None) -> List[CardSchema]:
        """Generate mock cards based on parameters"""
        
        cards = []
        
        if view == "best_picks":
            cards.extend(self._get_best_picks_cards(strategy, horizon))
        elif view == "dips":
            cards.extend(self._get_dips_cards(strategy, horizon))
        elif view == "bundles":
            cards.extend(self._get_bundles_cards(strategy, horizon))
        elif view == "compare":
            cards.extend(self._get_compare_cards(strategy, horizon))
        
        return cards
    
    def _get_best_picks_cards(self, strategy: str, horizon: str) -> List[CardSchema]:
        """Generate best picks cards"""
        cards = []
        
        # Top pick chart card
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        historical_series = [
            SeriesPoint(d, 100 + i*0.5 + (i%3)*2, "historical") 
            for i, d in enumerate(dates)
        ]
        
        # Add forecast
        forecast_dates = pd.date_range(start=dates[-1] + timedelta(days=1), periods=10, freq='D')
        for i, d in enumerate(forecast_dates):
            base_value = 100 + len(dates)*0.5 + (len(dates)%3)*2
            historical_series.append(SeriesPoint(d, base_value + i*0.3, "forecast"))
            historical_series.append(SeriesPoint(d, base_value + i*0.3 + 2, "confidence_upper"))
            historical_series.append(SeriesPoint(d, base_value + i*0.3 - 2, "confidence_lower"))
        
        cards.append(CardData.chart(
            series=historical_series,
            title="NVDA - Top Pick",
            subtitle="AI Confidence: 87%",
            metadata={"asset": "NVDA", "confidence": 0.87}
        ))
        
        # Number card for metrics
        cards.append(CardData.number(
            primary_metric="Expected Move",
            primary_value="+12.4%",
            secondary_metrics={
                "Confidence": "87%",
                "Horizon": horizon,
                "Signal Strength": "Strong"
            },
            rank=1,
            trend="bullish",
            confidence=0.87,
            metadata={"asset": "NVDA"}
        ))
        
        # Evidence table
        evidence_data = [
            ["Earnings Beat", "Q4 2023", "Strong", "Revenue +15% YoY"],
            ["AI Momentum", "Sector Leader", "Strong", "Data center demand"],
            ["Technical Signal", "Golden Cross", "Medium", "50-day > 200-day"],
            ["Analyst Upgrade", "Morgan Stanley", "Strong", "Price target $850"]
        ]
        
        cards.append(CardData.table(
            headers=["Signal", "Source", "Strength", "Details"],
            rows=evidence_data,
            title="Evidence & Signals",
            subtitle="Why NVDA is a top pick"
        ))
        
        return cards
    
    def _get_dips_cards(self, strategy: str, horizon: str) -> List[CardSchema]:
        """Generate dip buying opportunity cards"""
        cards = []
        
        # Dip chart
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        dip_series = [
            SeriesPoint(d, 100 - i*0.3 + (i%5)*3, "historical") 
            for i, d in enumerate(dates)
        ]
        
        # Mark dip point
        dip_point = 15  # Day 15 has a dip
        dip_series[dip_point] = SeriesPoint(dates[dip_point], 85, "signal_marker")
        
        cards.append(CardData.chart(
            series=dip_series,
            title="AAPL - Dip Opportunity",
            subtitle="Oversold - RSI: 28",
            metadata={"asset": "AAPL", "rsi": 28}
        ))
        
        # Dip metrics
        cards.append(CardData.number(
            primary_metric="Discount",
            primary_value="-18.2%",
            secondary_metrics={
                "From High": "-22.5%",
                "RSI": "28 (Oversold)",
                "Volume Spike": "+340%"
            },
            rank=None,
            trend="bearish_short_term",
            confidence=0.79,
            metadata={"asset": "AAPL"}
        ))
        
        return cards
    
    def _get_bundles_cards(self, strategy: str, horizon: str) -> List[CardSchema]:
        """Generate bundle cards"""
        cards = []
        
        # Bundle chart with constituents
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        
        # Main bundle line
        bundle_series = [SeriesPoint(d, 100 + i*0.2, "bundle") for i, d in enumerate(dates)]
        
        # Constituent lines
        nvda_series = [SeriesPoint(d, 100 + i*0.4, "constituent") for i, d in enumerate(dates)]
        amd_series = [SeriesPoint(d, 100 + i*0.1, "constituent") for i, d in enumerate(dates)]
        
        all_series = bundle_series + nvda_series + amd_series
        
        cards.append(CardData.chart(
            series=all_series,
            title="AI Chip Bundle",
            subtitle="NVDA 60% • AMD 40%",
            metadata={"bundle_type": "AI_Semiconductors"}
        ))
        
        # Bundle composition table
        composition_data = [
            ["NVDA", "60%", "Strong", "Data center leader"],
            ["AMD", "40%", "Medium", "AI chip competitor"],
            ["Weight", "100%", "", "Rebalanced weekly"]
        ]
        
        cards.append(CardData.table(
            headers=["Asset", "Weight", "Signal", "Rationale"],
            rows=composition_data,
            title="Bundle Composition",
            subtitle="AI semiconductors exposure"
        ))
        
        return cards
    
    def _get_compare_cards(self, strategy: str, horizon: str) -> List[CardSchema]:
        """Generate comparison cards"""
        cards = []
        
        # Comparison chart
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        
        # Multiple comparison lines
        nvda_series = [SeriesPoint(d, 100 + i*0.5, "comparison", "NVDA") for i, d in enumerate(dates)]
        amd_series = [SeriesPoint(d, 100 + i*0.3, "comparison", "AMD") for i, d in enumerate(dates)]
        msft_series = [SeriesPoint(d, 100 + i*0.2, "comparison", "MSFT") for i, d in enumerate(dates)]
        
        all_series = nvda_series + amd_series + msft_series
        
        cards.append(CardData.chart(
            series=all_series,
            title="Tech Giants Comparison",
            subtitle="30-day performance",
            metadata={"comparison_type": "tech_stocks"}
        ))
        
        # Comparison metrics table
        comparison_data = [
            ["NVDA", "+15.2%", "Strong", "AI leader"],
            ["AMD", "+8.7%", "Medium", "CPU/GPU growth"],
            ["MSFT", "+5.3%", "Stable", "Cloud AI integration"],
            ["QQQ", "+9.8%", "", "Tech ETF benchmark"]
        ]
        
        cards.append(CardData.table(
            headers=["Asset", "Return", "Momentum", "Notes"],
            rows=comparison_data,
            title="Performance Comparison",
            subtitle="Relative strength analysis"
        ))
        
        return cards


# ============================================================================
# CARD RENDERERS
# ============================================================================

class CardRenderer:
    """Renders different card types"""
    
    @staticmethod
    def render_chart_card(card: CardSchema) -> None:
        """Render a chart card with Plotly"""
        series_data = card.data.get("series", [])
        chart_type = card.data.get("chart_type", "line")
        
        if not series_data:
            st.warning(f"No data available for {card.title}")
            return
        
        # Group series by kind
        historical = [p for p in series_data if p.kind == "historical"]
        forecast = [p for p in series_data if p.kind == "forecast"]
        confidence_upper = [p for p in series_data if p.kind == "confidence_upper"]
        confidence_lower = [p for p in series_data if p.kind == "confidence_lower"]
        comparison = [p for p in series_data if p.kind == "comparison"]
        bundle = [p for p in series_data if p.kind == "bundle"]
        constituents = [p for p in series_data if p.kind == "constituent"]
        signal_markers = [p for p in series_data if p.kind == "signal_marker"]
        
        fig = go.Figure()
        
        # Historical data - solid line
        if historical:
            fig.add_trace(go.Scatter(
                x=[p.x for p in historical],
                y=[p.y for p in historical],
                mode='lines',
                name='Historical',
                line=dict(color=COLORS['primary_800'], width=2),
                connectgaps=False
            ))
        
        # Comparison data - multiple lines
        if comparison:
            comparison_groups = {}
            for p in comparison:
                label = p.label or "Unknown"
                if label not in comparison_groups:
                    comparison_groups[label] = []
                comparison_groups[label].append(p)
            
            colors = [COLORS['primary_800'], COLORS['success_500'], COLORS['warning_500'], COLORS['quant_primary']]
            for i, (label, points) in enumerate(comparison_groups.items()):
                fig.add_trace(go.Scatter(
                    x=[p.x for p in points],
                    y=[p.y for p in points],
                    mode='lines',
                    name=label,
                    line=dict(color=colors[i % len(colors)], width=2),
                    connectgaps=False
                ))
        
        # Bundle data - bold line with faint constituents
        if bundle:
            fig.add_trace(go.Scatter(
                x=[p.x for p in bundle],
                y=[p.y for p in bundle],
                mode='lines',
                name='Bundle',
                line=dict(color=COLORS['primary_800'], width=3),
                connectgaps=False
            ))
        
        if constituents:
            for p in constituents:
                fig.add_trace(go.Scatter(
                    x=[p.x for p in [p]],
                    y=[p.y for p in [p]],
                    mode='lines',
                    name=p.label or "Constituent",
                    line=dict(color=COLORS['neutral_400'], width=1, dash='dash'),
                    connectgaps=False
                ))
        
        # Forecast data - dashed line
        if forecast:
            fig.add_trace(go.Scatter(
                x=[p.x for p in forecast],
                y=[p.y for p in forecast],
                mode='lines',
                name='Forecast',
                line=dict(color=COLORS['primary_600'], width=2, dash='dash'),
                connectgaps=False
            ))
        
        # Confidence bands - filled area
        if confidence_upper and confidence_lower:
            # Upper bound
            fig.add_trace(go.Scatter(
                x=[p.x for p in confidence_upper],
                y=[p.y for p in confidence_upper],
                mode='lines',
                name='Upper Bound',
                line=dict(width=0),
                showlegend=False,
                connectgaps=False
            ))
            
            # Lower bound with fill
            fig.add_trace(go.Scatter(
                x=[p.x for p in confidence_lower],
                y=[p.y for p in confidence_lower],
                mode='lines',
                name='Confidence Band',
                line=dict(width=0),
                fill='tonexty',
                fillcolor=f'rgba(33, 150, 243, 0.2)',
                connectgaps=False
            ))
        
        # Signal markers - scatter points
        if signal_markers:
            fig.add_trace(go.Scatter(
                x=[p.x for p in signal_markers],
                y=[p.y for p in signal_markers],
                mode='markers',
                name='Signals',
                marker=dict(
                    color=COLORS['warning_500'],
                    size=10,
                    symbol='diamond'
                ),
                connectgaps=False
            ))
        
        # Update layout
        fig.update_layout(
            title=dict(text=card.title, x=0.5),
            height=300,
            margin=dict(l=50, r=50, t=50, b=50),
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            hovermode="x unified"
        )
        
        # Card container
        card_html = f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['3']};
            padding: {SPACING['4']};
            margin-bottom: {SPACING['4']};
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        ">
            <div style="font-size: {TYPOGRAPHY['text_lg']}; font-weight: {TYPOGRAPHY['weight_semibold']}; color: {COLORS['neutral_900']}; margin-bottom: {SPACING['2']};">
                {card.title}
            </div>
            {f'<div style="font-size: {TYPOGRAPHY["text_sm"]}; color: {COLORS["neutral_600"]}; margin-bottom: {SPACING["4"]};">{card.subtitle}</div>' if card.subtitle else ''}
        </div>
        """
        
        st.markdown(card_html, unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def render_number_card(card: CardSchema) -> None:
        """Render a number card with metrics"""
        data = card.data
        primary_value = data.get("primary_value", "—")
        secondary_metrics = data.get("secondary_metrics", {})
        rank = data.get("rank")
        trend = data.get("trend")
        confidence = data.get("confidence")
        
        # Determine colors based on trend
        trend_colors = {
            "bullish": COLORS['success_500'],
            "bearish": COLORS['error_500'],
            "neutral": COLORS['neutral_500'],
            "bullish_short_term": COLORS['success_500'],
            "bearish_short_term": COLORS['error_500']
        }
        trend_color = trend_colors.get(trend, COLORS['neutral_500'])
        
        # Rank badge
        rank_html = ""
        if rank:
            rank_html = f"""
            <div style="
                background: {COLORS['primary_800']};
                color: white;
                padding: 4px 8px;
                border-radius: {SPACING['2']};
                font-size: {TYPOGRAPHY['text_xs']};
                font-weight: {TYPOGRAPHY['weight_medium']};
                text-transform: uppercase;
                letter-spacing: 0.5px;
            ">
                Rank #{rank}
            </div>
            """
        
        # Trend indicator
        trend_html = ""
        if trend:
            trend_icons = {
                "bullish": "📈",
                "bearish": "📉", 
                "neutral": "➡️",
                "bullish_short_term": "📈",
                "bearish_short_term": "📉"
            }
            trend_html = f"""
            <div style="
                color: {trend_color};
                font-size: {TYPOGRAPHY['text_lg']};
                margin-bottom: {SPACING['2']};
            ">
                {trend_icons.get(trend, '➡️')}
            </div>
            """
        
        # Confidence bar
        confidence_html = ""
        if confidence is not None:
            confidence_width = confidence * 100
            confidence_color = COLORS['success_500'] if confidence > 0.7 else COLORS['warning_500'] if confidence > 0.4 else COLORS['error_500']
            confidence_html = f"""
            <div style="margin-top: {SPACING['2']};">
                <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                    Confidence: {confidence:.1%}
                </div>
                <div style="
                    width: 100%;
                    height: 4px;
                    background: {COLORS['neutral_200']};
                    border-radius: {SPACING['1']};
                    overflow: hidden;
                ">
                    <div style="
                        width: {confidence_width}%;
                        height: 100%;
                        background: {confidence_color};
                        border-radius: {SPACING['1']};
                    "></div>
                </div>
            </div>
            """
        
        # Secondary metrics
        metrics_html = ""
        if secondary_metrics:
            metric_items = []
            for key, value in secondary_metrics.items():
                metric_items.append(f"""
                <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                    <span style="color: {COLORS['neutral_600']};">{key}:</span>
                    <span style="font-weight: {TYPOGRAPHY['weight_medium']}; color: {COLORS['neutral_900']};">{value}</span>
                </div>
                """)
            metrics_html = f"""
            <div style="margin-top: {SPACING['3']}; padding-top: {SPACING['3']}; border-top: 1px solid {COLORS['border_light']};">
                {''.join(metric_items)}
            </div>
            """
        
        # Card HTML
        card_html = f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['3']};
            padding: {SPACING['4']};
            margin-bottom: {SPACING['4']};
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        ">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: {SPACING['3']};">
                <div>
                    <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']}; margin-bottom: {SPACING['1']}; text-transform: uppercase; letter-spacing: 0.5px;">
                        {card.title}
                    </div>
                    <div style="font-size: {TYPOGRAPHY['text_3xl']}; font-weight: {TYPOGRAPHY['weight_bold']}; color: {trend_color}; line-height: 1;">
                        {primary_value}
                    </div>
                    {trend_html}
                </div>
                {rank_html}
            </div>
            {confidence_html}
            {metrics_html}
        </div>
        """
        
        st.markdown(card_html, unsafe_allow_html=True)
    
    @staticmethod
    def render_table_card(card: CardSchema) -> None:
        """Render a table card"""
        headers = card.data.get("headers", [])
        rows = card.data.get("rows", [])
        
        if not rows:
            st.warning(f"No data available for {card.title}")
            return
        
        # Generate table HTML
        header_html = ""
        for header in headers:
            header_html += f"""
            <th style="
                padding: 8px 12px;
                text-align: left;
                font-weight: {TYPOGRAPHY['weight_semibold']};
                color: {COLORS['neutral_700']};
                border-bottom: 2px solid {COLORS['border_medium']};
                font-size: {TYPOGRAPHY['text_sm']};
            ">
                {header}
            </th>
            """
        
        row_html = ""
        for i, row in enumerate(rows):
            bg_color = COLORS['surface'] if i % 2 == 0 else COLORS['neutral_50']
            row_html += "<tr>"
            for cell in row:
                row_html += f"""
                <td style="
                    padding: 8px 12px;
                    border-bottom: 1px solid {COLORS['border_light']};
                    font-size: {TYPOGRAPHY['text_sm']};
                    color: {COLORS['neutral_800']};
                    background: {bg_color};
                ">
                    {cell}
                </td>
                """
            row_html += "</tr>"
        
        table_html = f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['3']};
            padding: {SPACING['4']};
            margin-bottom: {SPACING['4']};
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        ">
            <div style="font-size: {TYPOGRAPHY['text_lg']}; font-weight: {TYPOGRAPHY['weight_semibold']}; color: {COLORS['neutral_900']}; margin-bottom: {SPACING['3']};">
                {card.title}
            </div>
            {f'<div style="font-size: {TYPOGRAPHY["text_sm"]}; color: {COLORS["neutral_600"]}; margin-bottom: {SPACING["3"]};">{card.subtitle}</div>' if card.subtitle else ''}
            <div style="overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr>{header_html}</tr>
                    </thead>
                    <tbody>{row_html}</tbody>
                </table>
            </div>
        </div>
        """
        
        st.markdown(table_html, unsafe_allow_html=True)


# ============================================================================
# MAIN DASHBOARD CONTROLLER
# ============================================================================

class CardRiverDashboard:
    """Main dashboard controller for card river architecture"""
    
    def __init__(self, service: DashboardService):
        self.service = service
        self.data_provider = MockDataProvider(service)
        self.current_cards = []
    
    def render_controls(self) -> Dict:
        """Render shared controls and return selections"""
        with st.sidebar:
            st.markdown("### 🎛️ Controls")
            
            # View selection
            view = st.selectbox(
                "View",
                options=["best_picks", "dips", "bundles", "compare"],
                format_func=lambda x: x.replace("_", " ").title(),
                key="view_select"
            )
            
            # Strategy selection
            strategy = st.selectbox(
                "Strategy",
                options=["house", "semantic", "quant", "comparison"],
                format_func=lambda x: x.title(),
                key="strategy_select"
            )
            
            # Horizon selection
            horizon = st.selectbox(
                "Horizon",
                options=["1D", "1W", "1M", "3M", "6M", "1Y"],
                key="horizon_select"
            )
            
            # Additional filters
            with st.expander("Advanced Filters", expanded=False):
                min_confidence = st.slider(
                    "Min Confidence", 0.0, 1.0, 0.5, 0.05,
                    key="min_confidence"
                )
                
                risk_level = st.selectbox(
                    "Risk Level",
                    options=["Low", "Medium", "High"],
                    key="risk_level"
                )
                
                sectors = st.multiselect(
                    "Sectors",
                    options=["Technology", "Healthcare", "Finance", "Energy", "Consumer"],
                    key="sectors"
                )
            
            # Request button
            if st.button("🔄 Refresh Results", use_container_width=True, key="refresh_button"):
                st.rerun()
            
            return {
                "view": view,
                "strategy": strategy,
                "horizon": horizon,
                "min_confidence": min_confidence,
                "risk_level": risk_level,
                "sectors": sectors
            }
    
    def fetch_cards(self, controls: Dict) -> List[CardSchema]:
        """Fetch cards based on controls"""
        with st.spinner("Fetching cards..."):
            return self.data_provider.get_cards(
                view=controls["view"],
                strategy=controls["strategy"], 
                horizon=controls["horizon"],
                filters=controls
            )
    
    def render_card_river(self, cards: List[CardSchema]) -> None:
        """Render cards as a river"""
        if not cards:
            st.info("No cards found for selected criteria.")
            return
        
        for i, card in enumerate(cards):
            # Add separator between cards
            if i > 0:
                st.markdown(f"""
                <div style="height: 1px; background: {COLORS['border_light']}; margin: {SPACING['4']} 0; border-radius: {SPACING['1']};"></div>
                """, unsafe_allow_html=True)
            
            # Render card based on type
            if card.card_type == "chart":
                CardRenderer.render_chart_card(card)
            elif card.card_type == "number":
                CardRenderer.render_number_card(card)
            elif card.card_type == "table":
                CardRenderer.render_table_card(card)
            else:
                st.error(f"Unknown card type: {card.card_type}")
    
    def render(self):
        """Main render method"""
        # Apply theme
        apply_theme()
        
        # Header
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {COLORS['primary_800']} 0%, {COLORS['primary_900']} 100%);
            color: white;
            padding: {SPACING['6']};
            border-radius: {SPACING['3']};
            margin-bottom: {SPACING['6']};
            text-align: center;
        ">
            <h1 style="margin: 0; font-size: {TYPOGRAPHY['text_4xl']}; font-weight: {TYPOGRAPHY['weight_bold']};">
                🎯 Card River Dashboard
            </h1>
            <p style="margin: {SPACING['2']} 0 0 0; font-size: {TYPOGRAPHY['text_base']}; opacity: 0.9;">
                Independent cards • Shared controls • River layout
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Render controls
        controls = self.render_controls()
        
        # Fetch and render cards
        self.current_cards = self.fetch_cards(controls)
        self.render_card_river(self.current_cards)
        
        # Card summary
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Cards", len(self.current_cards))
        
        with col2:
            chart_cards = len([c for c in self.current_cards if c.card_type == "chart"])
            st.metric("Chart Cards", chart_cards)
        
        with col3:
            table_cards = len([c for c in self.current_cards if c.card_type == "table"])
            st.metric("Table Cards", table_cards)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point for card river dashboard"""
    # Initialize service
    service = DashboardService()
    
    # Create and render dashboard
    dashboard = CardRiverDashboard(service)
    dashboard.render()


if __name__ == "__main__":
    main()
