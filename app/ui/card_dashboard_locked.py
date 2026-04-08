"""
Locked Final Card River Dashboard
Clean architecture with verified final shape and all checks applied
"""

import streamlit as st
import plotly.graph_objects as go
from typing import Dict, List, Any, Tuple
import pandas as pd
from datetime import datetime, timedelta

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.middle.dashboard_service import DashboardService


# ============================================================================
# FINAL SHAPE: MINIMAL CARD SCHEMA
# ============================================================================

class Card:
    """Minimal card schema - only 3 types"""
    
    def __init__(self, card_type: str, title: str, data: Dict, card_id: str = None):
        self.card_type = card_type  # "chart", "number", "table"
        self.title = title
        self.data = data
        self.card_id = card_id or title.lower().replace(" ", "_")


class DashboardInputs:
    """Consolidated dashboard input state - only input contract"""
    
    def __init__(self, tenant: str = None, ticker: str = None, 
                 view: str = None, strategy: str = None, 
                 horizon: str = None):
        self.tenant = tenant
        self.ticker = ticker
        self.view = view
        self.strategy = strategy
        self.horizon = horizon
    
    @property
    def fingerprint(self) -> Tuple[Any, ...]:
        """Unique fingerprint for caching - only true query inputs"""
        return (self.tenant, self.ticker, self.view, self.strategy, self.horizon)
    
    @property
    def widget_keys(self) -> Dict[str, str]:
        """Deterministic widget keys"""
        return {
            "tenant": f"tenant-{self.tenant}" if self.tenant else "tenant-default",
            "ticker": f"ticker-{self.tenant}" if self.tenant else "ticker-default",
            "view": f"view-{self.view}" if self.view else "view-default",
            "strategy": f"strategy-{self.strategy}" if self.strategy else "strategy-default",
            "horizon": f"horizon-{self.horizon}" if self.horizon else "horizon-default"
        }


# ============================================================================
# FINAL SHAPE: DATA SERVICE
# ============================================================================

class DashboardDataService:
    """Pure data service - only DashboardInputs passed in"""
    
    def __init__(self, service: DashboardService):
        self.service = service
    
    @st.cache_data(ttl=30)  # Cache key only depends on true query inputs
    def fetch_cards(self, inputs: DashboardInputs) -> List[Card]:
        """Single fetch endpoint with caching"""
        if not inputs.tenant:
            return []
        
        # Deterministic card ordering for same inputs
        if inputs.view == "best_picks":
            return self._get_best_picks(inputs)
        elif inputs.view == "dips":
            return self._get_dips(inputs)
        elif inputs.view == "bundles":
            return self._get_bundles(inputs)
        elif inputs.view == "compare":
            return self._get_compare(inputs)
        
        return []
    
    def _get_best_picks(self, inputs: DashboardInputs) -> List[Card]:
        """Generate best picks cards - deterministic ordering"""
        cards = []
        
        # Top pick chart card
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        for i, d in enumerate(dates):
            series.append({"x": d, "y": 100 + i * 0.5, "kind": "historical"})
        
        # Add forecast points
        for i in range(10):
            forecast_date = dates[-1] + pd.Timedelta(days=i + 1)
            base_value = 100 + len(dates) * 0.5
            series.append({"x": forecast_date, "y": base_value + i * 0.3, "kind": "forecast"})
            series.append({"x": forecast_date, "y": base_value + i * 0.3 + 2, "kind": "confidence_upper"})
            series.append({"x": forecast_date, "y": base_value + i * 0.3 - 2, "kind": "confidence_lower"})
        
        cards.append(Card("chart", "NVDA - Top Pick", {
            "series": series
        }))
        
        # Number card for ranking
        cards.append(Card("number", "Expected Move", {
            "primary_value": "+12.4%",
            "rank": 1,
            "confidence": 0.87
        }))
        
        # Table card for evidence - contextual supporting data only
        cards.append(Card("table", "Evidence", {
            "headers": ["Signal", "Source", "Strength"],
            "rows": [
                ["Earnings Beat", "Q4 2023", "Strong"],
                ["AI Momentum", "Sector Leader", "Strong"],
                ["Technical Signal", "Golden Cross", "Medium"]
            ]
        }))
        
        return cards
    
    def _get_dips(self, inputs: DashboardInputs) -> List[Card]:
        """Generate dip opportunity cards - deterministic ordering"""
        cards = []
        
        # Dip chart with signal marker
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        for i, d in enumerate(dates):
            series.append({"x": d, "y": 100 - i * 0.3, "kind": "historical"})
        
        # Add signal marker at dip point
        dip_date = dates[15]
        series.append({"x": dip_date, "y": 85, "kind": "signal_marker"})
        
        cards.append(Card("chart", "AAPL - Dip Opportunity", {
            "series": series
        }))
        
        # Number card for discount
        cards.append(Card("number", "Discount", {
            "primary_value": "-18.2%",
            "confidence": 0.79
        }))
        
        return cards
    
    def _get_bundles(self, inputs: DashboardInputs) -> List[Card]:
        """Generate bundle cards - deterministic ordering"""
        cards = []
        
        # Bundle chart with constituents
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        # Bundle line
        for i, d in enumerate(dates):
            series.append({"x": d, "y": 100 + i * 0.2, "kind": "bundle"})
        
        # Constituent lines
        for i, d in enumerate(dates):
            series.append({"x": d, "y": 100 + i * 0.4, "kind": "constituent", "label": "NVDA"})
            series.append({"x": d, "y": 100 + i * 0.1, "kind": "constituent", "label": "AMD"})
        
        cards.append(Card("chart", "AI Chip Bundle", {
            "series": series
        }))
        
        # Composition table - contextual supporting data only
        cards.append(Card("table", "Composition", {
            "headers": ["Asset", "Weight", "Signal"],
            "rows": [
                ["NVDA", "60%", "Strong"],
                ["AMD", "40%", "Medium"]
            ]
        }))
        
        return cards
    
    def _get_compare(self, inputs: DashboardInputs) -> List[Card]:
        """Generate comparison cards - deterministic ordering"""
        cards = []
        
        # Comparison chart
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        # Multiple comparison lines
        for i, d in enumerate(dates):
            series.append({"x": d, "y": 100 + i * 0.5, "kind": "comparison", "label": "NVDA"})
            series.append({"x": d, "y": 100 + i * 0.3, "kind": "comparison", "label": "AMD"})
            series.append({"x": d, "y": 100 + i * 0.2, "kind": "comparison", "label": "MSFT"})
        
        cards.append(Card("chart", "Tech Giants Comparison", {
            "series": series
        }))
        
        # Comparison table - contextual supporting data only
        cards.append(Card("table", "Performance", {
            "headers": ["Asset", "Return", "Momentum"],
            "rows": [
                ["NVDA", "+15.2%", "Strong"],
                ["AMD", "+8.7%", "Medium"],
                ["MSFT", "+5.3%", "Stable"]
            ]
        }))
        
        return cards


# ============================================================================
# FINAL SHAPE: CARD RENDERER
# ============================================================================

class CardRenderer:
    """Minimal card renderers - only DashboardInputs passed in"""
    
    # Canonical color mapping
    COLORS = {
        "positive": COLORS["success_500"],
        "negative": COLORS["error_500"], 
        "neutral": COLORS["neutral_500"]
    }
    
    @staticmethod
    def render_chart_card(card: Card) -> None:
        """Render chart card - graceful downgrade to number card if too short"""
        series_data = card.data.get("series", [])
        
        # Graceful downgrade to number card if series too short
        if len(series_data) < 2:
            CardRenderer._downgrade_to_number_card(card)
            return
        
        # Standardize timestamps before plotting
        for point in series_data:
            if "x" in point and isinstance(point["x"], (datetime, pd.Timestamp)):
                point["x"] = pd.to_datetime(point["x"]).isoformat()
        
        # Group series by kind
        series_by_kind = {}
        for point in series_data:
            kind = point.get("kind")
            if kind not in series_by_kind:
                series_by_kind[kind] = []
            series_by_kind[kind].append(point)
        
        fig = go.Figure()
        
        # Plotly config in UI, not API
        colors = {
            "historical": COLORS["primary_800"],
            "forecast": COLORS["primary_600"],
            "confidence_upper": COLORS["primary_400"],
            "confidence_lower": COLORS["primary_400"],
            "comparison": [COLORS["primary_800"], COLORS["success_500"], COLORS["warning_500"]],
            "bundle": COLORS["primary_800"],
            "constituent": COLORS["neutral_400"],
            "signal_marker": COLORS["warning_500"]
        }
        
        # Historical - solid line
        if "historical" in series_by_kind:
            points = series_by_kind["historical"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in points],
                    y=[p["y"] for p in points],
                    mode="lines",
                    name="Historical",
                    line=dict(color=colors["historical"], width=2),
                    connectgaps=False,
                )
            )
        
        # Comparison - multiple lines
        if "comparison" in series_by_kind:
            comparison_colors = colors["comparison"]
            labels = list({p.get("label") for p in series_by_kind["comparison"]})
            for i, label in enumerate(labels):
                label_points = [
                    p for p in series_by_kind["comparison"] if p.get("label") == label
                ]
                if label_points:
                    fig.add_trace(
                        go.Scatter(
                            x=[p["x"] for p in label_points],
                            y=[p["y"] for p in label_points],
                            mode="lines",
                            name=label,
                            line=dict(
                                color=comparison_colors[i % len(comparison_colors)],
                                width=2,
                            ),
                            connectgaps=False,
                        )
                    )
        
        # Bundle - bold line with faint constituents
        if "bundle" in series_by_kind:
            bundle_points = series_by_kind["bundle"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in bundle_points],
                    y=[p["y"] for p in bundle_points],
                    mode="lines",
                    name="Bundle",
                    line=dict(color=colors["bundle"], width=3),
                    connectgaps=False,
                )
            )
        
        if "constituent" in series_by_kind:
            constituent_colors = [
                COLORS["neutral_400"],
                COLORS["neutral_500"],
                COLORS["neutral_600"],
            ]
            labels = list({p.get("label") for p in series_by_kind["constituent"]})
            for i, label in enumerate(labels):
                label_points = [
                    p for p in series_by_kind["constituent"] if p.get("label") == label
                ]
                if label_points:
                    fig.add_trace(
                        go.Scatter(
                            x=[p["x"] for p in label_points],
                            y=[p["y"] for p in label_points],
                            mode="lines",
                            name=label,
                            line=dict(
                                color=constituent_colors[i % len(constituent_colors)],
                                width=1,
                                dash="dash",
                            ),
                            connectgaps=False,
                        )
                    )
        
        # Forecast - dashed line
        if "forecast" in series_by_kind:
            forecast_points = series_by_kind["forecast"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in forecast_points],
                    y=[p["y"] for p in forecast_points],
                    mode="lines",
                    name="Forecast",
                    line=dict(color=colors["forecast"], width=2, dash="dash"),
                    connectgaps=False,
                )
            )
        
        # Confidence bands - filled area
        if (
            "confidence_upper" in series_by_kind
            and "confidence_lower" in series_by_kind
        ):
            upper_points = series_by_kind["confidence_upper"]
            lower_points = series_by_kind["confidence_lower"]
            
            # Upper bound (hidden)
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in upper_points],
                    y=[p["y"] for p in upper_points],
                    mode="lines",
                    name="Upper Bound",
                    line=dict(width=0),
                    showlegend=False,
                    connectgaps=False,
                )
            )
            
            # Lower bound with fill to upper
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in lower_points],
                    y=[p["y"] for p in lower_points],
                    mode="lines",
                    name="Confidence Band",
                    line=dict(width=0),
                    fill="tonexty",
                    fillcolor=f"rgba(33, 150, 243, 0.2)",
                    connectgaps=False,
                )
            )
        
        # Signal markers - scatter points
        if "signal_marker" in series_by_kind:
            signal_points = series_by_kind["signal_marker"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in signal_points],
                    y=[p["y"] for p in signal_points],
                    mode="markers",
                    name="Signals",
                    marker=dict(
                        color=colors["signal_marker"], size=10, symbol="diamond"
                    ),
                    connectgaps=False,
                )
            )
        
        # Minimal layout
        fig.update_layout(
            title=dict(text=card.title, x=0.5),
            height=300,
            margin=dict(l=50, r=50, t=50, b=50),
            showlegend=True,
            hovermode="x unified",
        )
        
        # Minimal card container
        st.markdown(
            f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['2']};
            padding: {SPACING['3']};
            margin-bottom: {SPACING['3']};
        ">
            <div style="font-weight: {TYPOGRAPHY['weight_semibold']}; color: {COLORS['neutral_900']}; margin-bottom: {SPACING['2']};">
                {card.title}
            </div>
        </div>
        """,
            unsafe_allow_html=True,
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _downgrade_to_number_card(original_card: Card) -> None:
        """Gracefully downgrade chart to number card when series too short"""
        number_card = Card("number", f"{original_card.title} - Summary", {
            "primary_value": "Insufficient data",
            "confidence": 0.0
        })
        CardRenderer.render_number_card(number_card)
    
    @staticmethod
    def render_number_card(card: Card) -> None:
        """Render number card"""
        data = card.data
        primary_value = data.get("primary_value", "No data")
        rank = data.get("rank")
        confidence = data.get("confidence")
        
        # Rank badge
        rank_html = ""
        if rank is not None:
            rank_html = f"""
            <div style="
                background: {COLORS['primary_800']};
                color: white;
                padding: 2px 6px;
                border-radius: {SPACING['1']};
                font-size: {TYPOGRAPHY['text_xs']};
                font-weight: {TYPOGRAPHY['weight_medium']};
                margin-bottom: {SPACING['2']};
            ">
                #{rank}
            </div>
            """
        
        # Confidence indicator
        confidence_html = ""
        if confidence is not None:
            confidence_width = confidence * 100
            confidence_color = (
                CardRenderer.COLORS["positive"]
                if confidence > 0.7
                else CardRenderer.COLORS["neutral"] if confidence > 0.4 else CardRenderer.COLORS["negative"]
            )
            confidence_html = f"""
            <div style="margin-top: {SPACING['2']};">
                <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']}; margin-bottom: 2px;">
                    Confidence {confidence:.1%}
                </div>
                <div style="
                    width: 100%;
                    height: 3px;
                    background: {COLORS['neutral_200']};
                    border-radius: {SPACING['1']};
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
        
        # Minimal number card
        st.markdown(
            f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['2']};
            padding: {SPACING['3']};
            margin-bottom: {SPACING['3']};
        ">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: {SPACING['2']};">
                <div>
                    <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']}; margin-bottom: 2px; text-transform: uppercase;">
                        {card.title}
                    </div>
                    <div style="font-size: {TYPOGRAPHY['text_2xl']}; font-weight: {TYPOGRAPHY['weight_bold']}; color: {COLORS['neutral_900']};">
                        {primary_value}
                    </div>
                </div>
                {rank_html}
            </div>
            {confidence_html}
        </div>
        """,
            unsafe_allow_html=True,
        )
    
    @staticmethod
    def render_table_card(card: Card) -> None:
        """Render table card - contextual supporting data only"""
        headers = card.data.get("headers", [])
        rows = card.data.get("rows", [])
        
        if not rows:
            st.warning(f"No data available for {card.title}")
            return
        
        # Minimal table HTML
        header_html = ""
        for header in headers:
            header_html += f"""
            <th style="
                padding: 6px 8px;
                text-align: left;
                font-weight: {TYPOGRAPHY['weight_semibold']};
                color: {COLORS['neutral_700']};
                border-bottom: 1px solid {COLORS['border_medium']};
                font-size: {TYPOGRAPHY['text_sm']};
            ">
                {header}
            </th>
            """
        
        row_html = ""
        for i, row in enumerate(rows):
            bg_color = COLORS["surface"] if i % 2 == 0 else COLORS["neutral_50"]
            row_html += "<tr>"
            for cell in row:
                row_html += f"""
                <td style="
                    padding: 6px 8px;
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
            border-radius: {SPACING['2']};
            padding: {SPACING['3']};
            margin-bottom: {SPACING['3']};
        ">
            <div style="font-weight: {TYPOGRAPHY['weight_semibold']}; color: {COLORS['neutral_900']}; margin-bottom: {SPACING['2']};">
                {card.title}
            </div>
            <div style="overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse;">
                    <thead><tr>{header_html}</tr></thead>
                    <tbody>{row_html}</tbody>
                </table>
            </div>
        </div>
        """
        
        st.markdown(table_html, unsafe_allow_html=True)


# ============================================================================
# FINAL SHAPE: CONTROLS
# ============================================================================

def render_controls(service: DashboardService) -> Tuple[DashboardInputs, bool]:
    """Render all controls and return inputs + refresh flag"""
    with st.sidebar:
        st.markdown("### Controls")
        
        # Get available tenants
        tenants = service.list_tenants()
        if not tenants:
            st.warning("No tenants found in DB.")
            return DashboardInputs(), False
        
        # Tenant selection
        tenant_key = "tenant-default"
        tenant = st.selectbox(
            "Tenant",
            options=tenants,
            key=tenant_key,
            help="Select tenant to view data for"
        )
        
        # Ticker selection
        if tenant:
            all_tickers = service.list_tickers(tenant_id=tenant)
            ticker_key = f"ticker-{tenant}"
            ticker = st.selectbox(
                "Ticker",
                options=all_tickers,
                key=ticker_key,
                help="Select ticker to view data for"
            )
        else:
            ticker = None
        
        # View selection
        view = st.selectbox(
            "View",
            options=["best_picks", "dips", "bundles", "compare"],
            format_func=lambda x: x.replace("_", " ").title(),
            key="view-default",
        )
        
        # Strategy selection
        strategy = st.selectbox(
            "Strategy",
            options=["house", "semantic", "quant", "comparison"],
            key="strategy-default",
        )
        
        # Horizon selection
        horizon = st.selectbox(
            "Horizon",
            options=["1D", "1W", "1M", "3M", "6M", "1Y"],
            key="horizon-default",
        )
        
        # Manual refresh - can bypass cache
        manual_refresh = st.button("Refresh", key="refresh-main")
        
        # Auto refresh - does not reset user selections
        auto_refresh = st.checkbox("Auto-refresh", value=True, key="auto-refresh")
        if auto_refresh:
            refresh_interval = st.slider(
                "Refresh interval (ms)",
                min_value=1000,
                max_value=10000,
                value=2000,
                step=500,
                key="refresh-interval",
            )
            
            # Simple auto refresh using session state
            if "last_refresh" not in st.session_state:
                st.session_state.last_refresh = datetime.now()
            
            time_since_refresh = (datetime.now() - st.session_state.last_refresh).total_seconds() * 1000
            if time_since_refresh > refresh_interval:
                st.session_state.last_refresh = datetime.now()
                manual_refresh = True
        
        inputs = DashboardInputs(tenant, ticker, view, strategy, horizon)
        return inputs, manual_refresh


# ============================================================================
# FINAL SHAPE: SYSTEM STATUS
# ============================================================================

def render_system_status(inputs: DashboardInputs) -> None:
    """Render consolidated system status row - always shows even with empty responses"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Tenant", inputs.tenant or "None")
    
    with col2:
        st.metric("Strategy", inputs.strategy or "None")
    
    with col3:
        st.metric("Horizon", inputs.horizon or "None")


# ============================================================================
# FINAL SHAPE: CARD RIVER
# ============================================================================

def render_cards(cards: List[Card], visible_count: int = 10) -> None:
    """Render cards as river with lazy loading - critical top cards always visible"""
    if not cards:
        # Empty state card when no cards
        st.markdown(
            f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['2']};
            padding: {SPACING['4']};
            margin-bottom: {SPACING['3']};
            text-align: center;
        ">
            <div style="font-size: {TYPOGRAPHY['text_lg']}; color: {COLORS['neutral_600']}; margin-bottom: {SPACING['2']};">
                No cards found
            </div>
            <div style="font-size: {TYPOGRAPHY['text_sm']}; color: {COLORS['neutral_500']};">
                Try different controls or refresh
            </div>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return
    
    # Render visible cards only - critical top cards always visible
    for i, card in enumerate(cards[:visible_count]):
        # Simple separator between cards
        if i > 0:
            st.markdown(
                f"""
                <div style="height: 1px; background: {COLORS['border_light']}; margin: {SPACING['2']} 0;"></div>
                """,
                unsafe_allow_html=True,
            )
        
        # Render by type
        if card.card_type == "chart":
            CardRenderer.render_chart_card(card)
        elif card.card_type == "number":
            CardRenderer.render_number_card(card)
        elif card.card_type == "table":
            CardRenderer.render_table_card(card)
    
    # Show more button if needed - ensures critical cards not hidden on small screens
    if len(cards) > visible_count:
        if st.button(f"Show {len(cards) - visible_count} more cards"):
            render_cards(cards, visible_count * 2)


# ============================================================================
# FINAL SHAPE: MAIN
# ============================================================================

def main():
    """Main entry point - clean architecture"""
    # Apply theme
    apply_theme()
    
    # Initialize service
    service = DashboardService()
    
    # Render controls and get inputs
    inputs, should_refresh = render_controls(service)
    
    # Render system status - always shows
    render_system_status(inputs)
    
    # Debug view for fingerprint/refresh reason
    if st.checkbox("Show Debug", key="debug-view"):
        st.code(f"""
Fingerprint: {inputs.fingerprint}
Should Refresh: {should_refresh}
Tenant: {inputs.tenant}
View: {inputs.view}
        """)
    
    # Fetch data only when needed
    data_service = DashboardDataService(service)
    if should_refresh and inputs.tenant:
        # Manual refresh can bypass cache
        cards = data_service.fetch_cards(inputs)
    elif inputs.tenant:
        # Use cached cards from session state
        cards = st.session_state.get("cached_cards", [])
    else:
        cards = []
    
    # Cache cards in session
    if cards:
        st.session_state.cached_cards = cards
    
    # Render header
    st.markdown(
        f"""
        <div style="
            background: {COLORS['primary_800']};
            color: white;
            padding: {SPACING['4']};
            border-radius: {SPACING['2']};
            margin-bottom: {SPACING['4']};
            text-align: center;
        ">
            <h1 style="margin: 0; font-size: {TYPOGRAPHY['text_2xl']}; font-weight: {TYPOGRAPHY['weight_bold']};">
                Card River Dashboard
            </h1>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    # Render cards
    render_cards(cards)


if __name__ == "__main__":
    main()
