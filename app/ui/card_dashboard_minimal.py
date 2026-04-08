"""
Minimal Card River Dashboard
Strict adherence to minimal card schema with shared controls
"""

import streamlit as st
import plotly.graph_objects as go
from typing import Dict, List, Any, Optional
import pandas as pd

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.middle.dashboard_service import DashboardService


# ============================================================================
# MINIMAL CARD SCHEMA
# ============================================================================


class Card:
    """Minimal card schema - only 3 types"""

    def __init__(self, card_type: str, title: str, data: Dict, card_id: str = None):
        self.card_type = card_type  # "chart", "number", "table"
        self.title = title
        self.data = data
        self.card_id = card_id or title.lower().replace(" ", "_")


class SeriesPoint:
    """Universal series point"""

    def __init__(self, x: Any, y: Any, kind: str = None, label: str = None):
        self.x = x
        self.y = y
        self.kind = kind  # API-level series type
        self.label = label


# ============================================================================
# MINIMAL DATA PROVIDER
# ============================================================================


class MinimalCardProvider:
    """Minimal data provider - single query endpoint"""

    def __init__(self, service: DashboardService):
        self.service = service

    def get_cards(self, view: str, strategy: str, horizon: str) -> List[Card]:
        """Single query endpoint returns all cards"""
        # Single query context
        query_context = {
            "view": view,
            "strategy": strategy,
            "horizon": horizon,
            "filters": {},
        }

        # Mock data based on view
        if view == "best_picks":
            return self._get_best_picks(query_context)
        elif view == "dips":
            return self._get_dips(query_context)
        elif view == "bundles":
            return self._get_bundles(query_context)
        elif view == "compare":
            return self._get_compare(query_context)

        return []

    def _get_best_picks(self, context: Dict) -> List[Card]:
        """Generate best picks cards"""
        cards = []

        # Top pick chart card (only if ≥2 time points)
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []

        for i, d in enumerate(dates):
            series.append(SeriesPoint(d, 100 + i * 0.5, "historical"))

        # Add forecast points
        for i in range(10):
            forecast_date = dates[-1] + pd.Timedelta(days=i + 1)
            base_value = 100 + len(dates) * 0.5
            series.append(SeriesPoint(forecast_date, base_value + i * 0.3, "forecast"))
            series.append(
                SeriesPoint(forecast_date, base_value + i * 0.3 + 2, "confidence_upper")
            )
            series.append(
                SeriesPoint(forecast_date, base_value + i * 0.3 - 2, "confidence_lower")
            )

        cards.append(
            Card("chart", "NVDA - Top Pick", {"series": [vars(p) for p in series]})
        )

        # Number card for ranking
        cards.append(
            Card(
                "number",
                "Expected Move",
                {"primary_value": "+12.4%", "rank": 1, "confidence": 0.87},
            )
        )

        # Table card for evidence
        cards.append(
            Card(
                "table",
                "Evidence",
                {
                    "headers": ["Signal", "Source", "Strength"],
                    "rows": [
                        ["Earnings Beat", "Q4 2023", "Strong"],
                        ["AI Momentum", "Sector Leader", "Strong"],
                        ["Technical Signal", "Golden Cross", "Medium"],
                    ],
                },
            )
        )

        return cards

    def _get_dips(self, context: Dict) -> List[Card]:
        """Generate dip opportunity cards"""
        cards = []

        # Dip chart with signal marker
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []

        for i, d in enumerate(dates):
            series.append(SeriesPoint(d, 100 - i * 0.3, "historical"))

        # Add signal marker at dip point
        dip_date = dates[15]
        series.append(SeriesPoint(dip_date, 85, "signal_marker"))

        cards.append(
            Card(
                "chart", "AAPL - Dip Opportunity", {"series": [vars(p) for p in series]}
            )
        )

        # Number card for discount
        cards.append(
            Card("number", "Discount", {"primary_value": "-18.2%", "confidence": 0.79})
        )

        return cards

    def _get_bundles(self, context: Dict) -> List[Card]:
        """Generate bundle cards"""
        cards = []

        # Bundle chart with constituents
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []

        # Bundle line
        for i, d in enumerate(dates):
            series.append(SeriesPoint(d, 100 + i * 0.2, "bundle"))

        # Constituent lines
        for i, d in enumerate(dates):
            series.append(SeriesPoint(d, 100 + i * 0.4, "constituent", "NVDA"))
            series.append(SeriesPoint(d, 100 + i * 0.1, "constituent", "AMD"))

        cards.append(
            Card("chart", "AI Chip Bundle", {"series": [vars(p) for p in series]})
        )

        # Composition table
        cards.append(
            Card(
                "table",
                "Composition",
                {
                    "headers": ["Asset", "Weight", "Signal"],
                    "rows": [["NVDA", "60%", "Strong"], ["AMD", "40%", "Medium"]],
                },
            )
        )

        return cards

    def _get_compare(self, context: Dict) -> List[Card]:
        """Generate comparison cards"""
        cards = []

        # Comparison chart
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []

        # Multiple comparison lines
        for i, d in enumerate(dates):
            series.append(SeriesPoint(d, 100 + i * 0.5, "comparison", "NVDA"))
            series.append(SeriesPoint(d, 100 + i * 0.3, "comparison", "AMD"))
            series.append(SeriesPoint(d, 100 + i * 0.2, "comparison", "MSFT"))

        cards.append(
            Card(
                "chart", "Tech Giants Comparison", {"series": [vars(p) for p in series]}
            )
        )

        # Comparison table
        cards.append(
            Card(
                "table",
                "Performance",
                {
                    "headers": ["Asset", "Return", "Momentum"],
                    "rows": [
                        ["NVDA", "+15.2%", "Strong"],
                        ["AMD", "+8.7%", "Medium"],
                        ["MSFT", "+5.3%", "Stable"],
                    ],
                },
            )
        )

        return cards


# ============================================================================
# MINIMAL CARD RENDERERS
# ============================================================================


class MinimalCardRenderer:
    """Minimal card renderers - only 3 types"""

    @staticmethod
    def render_chart_card(card: Card) -> None:
        """Render chart card - only if ≥2 time points"""
        series_data = card.data.get("series", [])

        if len(series_data) < 2:
            st.warning(f"Insufficient data for chart: {card.title}")
            return

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
            "comparison": [
                COLORS["primary_800"],
                COLORS["success_500"],
                COLORS["warning_500"],
            ],
            "bundle": COLORS["primary_800"],
            "constituent": COLORS["neutral_400"],
            "signal_marker": COLORS["warning_500"],
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
                # Group by label for comparison lines
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
    def render_number_card(card: Card) -> None:
        """Render number card"""
        data = card.data
        primary_value = data.get("primary_value", "—")
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
                COLORS["success_500"]
                if confidence > 0.7
                else COLORS["warning_500"] if confidence > 0.4 else COLORS["error_500"]
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
        """Render table card"""
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
# MINIMAL DASHBOARD CONTROLLER
# ============================================================================


class MinimalCardRiverDashboard:
    """Minimal card river dashboard - strict implementation"""

    def __init__(self, service: DashboardService):
        self.service = service
        self.provider = MinimalCardProvider(service)
        self.cache = {}

    def render_controls(self) -> Dict:
        """Render minimal shared controls"""
        with st.sidebar:
            st.markdown("### Controls")

            # Three main controls only
            view = st.selectbox(
                "View",
                options=["best_picks", "dips", "bundles", "compare"],
                format_func=lambda x: x.replace("_", " ").title(),
                key="view",
            )

            strategy = st.selectbox(
                "Strategy",
                options=["house", "semantic", "quant", "comparison"],
                key="strategy",
            )

            horizon = st.selectbox(
                "Horizon", options=["1D", "1W", "1M", "3M", "6M", "1Y"], key="horizon"
            )

            # Single refresh button
            if st.button("Refresh", key="refresh"):
                st.rerun()

            return {"view": view, "strategy": strategy, "horizon": horizon}

    def get_cached_cards(self, view: str, strategy: str, horizon: str) -> List[Card]:
        """Get cards with caching"""
        cache_key = f"{view}_{strategy}_{horizon}"

        if cache_key not in self.cache:
            self.cache[cache_key] = self.provider.get_cards(view, strategy, horizon)

        return self.cache[cache_key]

    def render_card_river(self, cards: List[Card]) -> None:
        """Render simple river layout"""
        if not cards:
            st.info("No cards found.")
            return

        for i, card in enumerate(cards):
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
                MinimalCardRenderer.render_chart_card(card)
            elif card.card_type == "number":
                MinimalCardRenderer.render_number_card(card)
            elif card.card_type == "table":
                MinimalCardRenderer.render_table_card(card)

    def render(self):
        """Main render method"""
        apply_theme()

        # Minimal header
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

        # Controls
        controls = self.render_controls()

        # Get and render cards
        cards = self.get_cached_cards(
            controls["view"], controls["strategy"], controls["horizon"]
        )
        self.render_card_river(cards)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================


def main():
    """Main entry point"""
    service = DashboardService()
    dashboard = MinimalCardRiverDashboard(service)
    dashboard.render()


if __name__ == "__main__":
    main()
