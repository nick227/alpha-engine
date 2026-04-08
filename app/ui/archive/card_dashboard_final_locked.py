"""
Final Locked Card Dashboard - Minimal Schema with Chart Modes
Clean architecture with standardized chart card API supporting all modes
"""

import streamlit as st
import plotly.graph_objects as go
from typing import Dict, List, Any, Tuple
import pandas as pd
from datetime import datetime, timedelta

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.middle.dashboard_service import DashboardService
from app.ui.card_dashboard_locked import DashboardInputs, render_system_status
from app.ui.chart_modes import (
    Card, ChartMode, create_backtest_overlay_cards, create_forecast_cards,
    ChartModeSorter, ChartModeFilter, ChartModeValidator
)
from app.ui.standard_chart_renderer import StandardChartRenderer


# ============================================================================
# FINAL DATA SERVICE - MINIMAL SCHEMA WITH MODES
# ============================================================================

class FinalDashboardDataService:
    """Final data service with minimal schema and chart modes"""
    
    def __init__(self, service: DashboardService):
        self.service = service
    
    @st.cache_data(ttl=30)  # Cache key only depends on true query inputs
    def fetch_cards(self, inputs: DashboardInputs) -> List[Card]:
        """Fetch cards with minimal schema - chart modes handle variations"""
        if not inputs.tenant:
            return []
        
        # Return cards based on view using same minimal schema
        if inputs.view == "best_picks":
            return self._get_best_picks(inputs)
        elif inputs.view == "dips":
            return self._get_dips(inputs)
        elif inputs.view == "bundles":
            return self._get_bundles(inputs)
        elif inputs.view == "compare":
            return self._get_compare(inputs)
        elif inputs.view == "backtest_analysis":
            return self._get_backtest_analysis(inputs)
        
        return []
    
    def _get_best_picks(self, inputs: DashboardInputs) -> List[Card]:
        """Generate best picks using standard forecast mode"""
        cards = []
        
        # Standard forecast chart
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        for i, d in enumerate(dates):
            series.append({"x": d.isoformat(), "y": 100 + i * 0.5, "kind": "historical"})
        
        # Add forecast points
        for i in range(10):
            forecast_date = dates[-1] + pd.Timedelta(days=i + 1)
            base_value = 100 + len(dates) * 0.5
            series.append({"x": forecast_date.isoformat(), "y": base_value + i * 0.3, "kind": "forecast"})
        
        forecast_data = {
            "series": series,
            "mode": ChartMode.FORECAST
        }
        
        cards.append(Card("chart", "NVDA - Top Pick Forecast", forecast_data))
        cards.append(Card("number", "Expected Move", {"primary_value": "+12.4%", "confidence": 0.87}))
        
        return cards
    
    def _get_dips(self, inputs: DashboardInputs) -> List[Card]:
        """Generate dip opportunities using standard forecast mode"""
        cards = []
        
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        for i, d in enumerate(dates):
            series.append({"x": d.isoformat(), "y": 100 - i * 0.3, "kind": "historical"})
        
        forecast_data = {
            "series": series,
            "mode": ChartMode.FORECAST
        }
        
        cards.append(Card("chart", "AAPL - Dip Opportunity", forecast_data))
        cards.append(Card("number", "Discount", {"primary_value": "-18.2%", "confidence": 0.79}))
        
        return cards
    
    def _get_bundles(self, inputs: DashboardInputs) -> List[Card]:
        """Generate bundle analysis using comparison mode"""
        cards = []
        
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        # Bundle line
        for i, d in enumerate(dates):
            series.append({"x": d.isoformat(), "y": 100 + i * 0.2, "kind": "bundle", "label": "AI Bundle"})
        
        # Constituent lines
        for i, d in enumerate(dates):
            series.append({"x": d.isoformat(), "y": 100 + i * 0.4, "kind": "constituent", "label": "NVDA"})
            series.append({"x": d.isoformat(), "y": 100 + i * 0.1, "kind": "constituent", "label": "AMD"})
        
        comparison_data = {
            "series": series,
            "mode": ChartMode.COMPARISON
        }
        
        cards.append(Card("chart", "AI Chip Bundle", comparison_data))
        
        return cards
    
    def _get_compare(self, inputs: DashboardInputs) -> List[Card]:
        """Generate comparison using comparison mode"""
        cards = []
        
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        # Multiple comparison lines
        for i, d in enumerate(dates):
            series.append({"x": d.isoformat(), "y": 100 + i * 0.5, "kind": "comparison", "label": "NVDA"})
            series.append({"x": d.isoformat(), "y": 100 + i * 0.3, "kind": "comparison", "label": "AMD"})
            series.append({"x": d.isoformat(), "y": 100 + i * 0.2, "kind": "comparison", "label": "MSFT"})
        
        comparison_data = {
            "series": series,
            "mode": ChartMode.COMPARISON
        }
        
        cards.append(Card("chart", "Tech Giants Comparison", comparison_data))
        
        return cards
    
    def _get_backtest_analysis(self, inputs: DashboardInputs) -> List[Card]:
        """Generate backtest analysis using overlay mode"""
        return create_backtest_overlay_cards()


# ============================================================================
# FINAL CARD RENDERER - UNIFIED PIPELINE
# ============================================================================

class FinalCardRenderer:
    """Final card renderer - unified pipeline for all card types"""
    
    @staticmethod
    def render_card(card: Card) -> None:
        """Render card using minimal schema - dispatch by type only"""
        if card.card_type == "chart":
            # Chart renderer handles modes internally
            StandardChartRenderer.render_chart_card(card)
        elif card.card_type == "number":
            FinalCardRenderer._render_number_card(card)
        elif card.card_type == "table":
            FinalCardRenderer._render_table_card(card)
        else:
            st.error(f"Unknown card type: {card.card_type}")
    
    @staticmethod
    def _render_number_card(card: Card) -> None:
        """Render number card"""
        data = card.data
        primary_value = data.get("primary_value", "No data")
        confidence = data.get("confidence")
        
        # Confidence indicator
        confidence_html = ""
        if confidence is not None:
            confidence_width = confidence * 100
            confidence_color = (
                COLORS["success_500"] if confidence > 0.7
                else COLORS["neutral_500"] if confidence > 0.4 else COLORS["error_500"]
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
            <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']}; margin-bottom: 2px; text-transform: uppercase;">
                {card.title}
            </div>
            <div style="font-size: {TYPOGRAPHY['text_2xl']}; font-weight: {TYPOGRAPHY['weight_bold']}; color: {COLORS['neutral_900']};">
                {primary_value}
            </div>
            {confidence_html}
        </div>
        """,
            unsafe_allow_html=True,
        )
    
    @staticmethod
    def _render_table_card(card: Card) -> None:
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
# ENHANCED CONTROLS WITH BACKTEST ANALYSIS
# ============================================================================

def render_final_controls(service: DashboardService) -> Tuple[DashboardInputs, bool]:
    """Final controls with backtest analysis option"""
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
        
        # Enhanced view selection with backtest analysis
        view_options = [
            "best_picks", "dips", "bundles", "compare", 
            "backtest_analysis"  # Uses overlay mode within chart type
        ]
        view = st.selectbox(
            "View",
            options=view_options,
            format_func=lambda x: x.replace("_", " ").title(),
            key="view-default",
            help="Select analysis view. Backtest Analysis shows prediction vs actual comparisons."
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
# SORTING AND FILTERING CONTROLS
# ============================================================================

def render_sorting_controls(cards: List[Card]) -> Tuple[str, str, int]:
    """Render sorting controls for backtest analysis"""
    if not any(c.card_type == "chart" and c.data.get("mode") == ChartMode.BACKTEST_OVERLAY for c in cards):
        return "", "", len(cards)
    
    st.markdown("### Sort & Filter")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        sort_option = st.selectbox(
            "Sort by",
            options=[
                "Primary Key (Return/Confidence)",
                "Confidence",
                "Return",
                "Default Order"
            ],
            key="final_sort_option"
        )
    
    with col2:
        filter_option = st.selectbox(
            "Filter",
            options=[
                "All",
                "Wins Only", 
                "Losses Only",
                "High Confidence (>75%)",
                "Best Returns (>5%)"
            ],
            key="final_filter_option"
        )
    
    with col3:
        max_results = st.slider(
            "Max Results",
            min_value=5,
            max_value=50,
            value=10,
            step=5,
            key="final_max_results"
        )
    
    return sort_option, filter_option, max_results


def apply_sorting_and_filtering(cards: List[Card], sort_option: str, filter_option: str, max_results: int) -> List[Card]:
    """Apply sorting and filtering to cards"""
    # Separate overlay cards for special processing
    overlay_cards = [c for c in cards if c.card_type == "chart" and c.data.get("mode") == ChartMode.BACKTEST_OVERLAY]
    other_cards = [c for c in cards if c not in overlay_cards]
    
    # Apply filtering to overlay cards
    if filter_option == "Wins Only":
        overlay_cards = ChartModeFilter.filter_wins(overlay_cards)
    elif filter_option == "Losses Only":
        overlay_cards = ChartModeFilter.filter_losses(overlay_cards)
    elif filter_option == "High Confidence (>75%)":
        overlay_cards = ChartModeFilter.filter_by_min_confidence(overlay_cards, 0.75)
    elif filter_option == "Best Returns (>5%)":
        overlay_cards = ChartModeFilter.filter_by_min_return(overlay_cards, 5.0)
    
    # Apply sorting to overlay cards
    if sort_option == "Primary Key (Return/Confidence)":
        overlay_cards = ChartModeSorter.sort_by_primary(overlay_cards)
    elif sort_option == "Confidence":
        overlay_cards = ChartModeSorter.sort_by_confidence(overlay_cards)
    elif sort_option == "Return":
        overlay_cards = ChartModeSorter.sort_by_return(overlay_cards)
    # "Default Order" keeps original order
    
    # Combine and limit results
    processed_cards = overlay_cards[:max_results] + other_cards
    return processed_cards


# ============================================================================
# FINAL CARD RIVER RENDERING
# ============================================================================

def render_final_cards(cards: List[Card], visible_count: int = 10) -> None:
    """Render cards using unified pipeline"""
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
    
    # Render visible cards only
    for i, card in enumerate(cards[:visible_count]):
        # Simple separator between cards
        if i > 0:
            st.markdown(
                f"""
                <div style="height: 1px; background: {COLORS['border_light']}; margin: {SPACING['2']} 0;"></div>
                """,
                unsafe_allow_html=True,
            )
        
        # Render using unified pipeline
        FinalCardRenderer.render_card(card)
    
    # Show more button if needed
    if len(cards) > visible_count:
        if st.button(f"Show {len(cards) - visible_count} more cards"):
            render_final_cards(cards, visible_count * 2)


# ============================================================================
# VALIDATION MODE
# ============================================================================

def run_final_validation(cards: List[Card]) -> None:
    """Run final validation on minimal schema compliance"""
    st.markdown("# Final Schema Validation")
    
    validator = ChartModeValidator()
    
    st.markdown("## Chart Card Validation")
    chart_cards = [c for c in cards if c.card_type == "chart"]
    
    for i, card in enumerate(chart_cards):
        st.markdown(f"### Chart Card {i+1}: {card.title}")
        validation_results = validator.validate_chart_card(card)
        
        for check, passed in validation_results.items():
            status = "Pass" if passed else "Fail"
            color = COLORS["success_500"] if passed else COLORS["error_500"]
            st.markdown(f"- **{check}**: <span style='color: {color}'>{status}</span>", unsafe_allow_html=True)
    
    st.markdown("## Table Card Validation")
    table_cards = [c for c in cards if c.card_type == "table"]
    
    for i, card in enumerate(table_cards):
        st.markdown(f"### Table Card {i+1}: {card.title}")
        validation_results = validator.validate_table_card(card)
        
        for check, passed in validation_results.items():
            status = "Pass" if passed else "Fail"
            color = COLORS["success_500"] if passed else COLORS["error_500"]
            st.markdown(f"- **{check}**: <span style='color: {color}'>{status}</span>", unsafe_allow_html=True)
    
    st.markdown("## Ugly Case Testing")
    for card in chart_cards[:2]:  # Test a couple cards
        st.markdown(f"### Ugly Cases: {card.title}")
        validation_results = validator.validate_ugly_cases(card)
        
        for check, passed in validation_results.items():
            status = "Pass" if passed else "Fail"
            color = COLORS["success_500"] if passed else COLORS["error_500"]
            st.markdown(f"- **{check}**: <span style='color: {color}'>{status}</span>", unsafe_allow_html=True)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point - final locked implementation"""
    # Apply theme
    apply_theme()
    
    # Initialize service
    service = DashboardService()
    
    # Render final controls and get inputs
    inputs, should_refresh = render_final_controls(service)
    
    # Render system status - always shows
    render_system_status(inputs)
    
    # Debug view for fingerprint/refresh reason
    if st.checkbox("Show Debug", key="debug-view"):
        st.code(f"""
Fingerprint: {inputs.fingerprint}
Should Refresh: {should_refresh}
Tenant: {inputs.tenant}
View: {inputs.view}
Strategy: {inputs.strategy}
        """)
    
    # Fetch data using final service
    final_service = FinalDashboardDataService(service)
    
    if should_refresh and inputs.tenant:
        cards = final_service.fetch_cards(inputs)
    elif inputs.tenant:
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
                Final Card River Dashboard
            </h1>
            <p style="margin: {SPACING['2']} 0 0 0; font-size: {TYPOGRAPHY['text_sm']}; opacity: 0.9;">
                Minimal Schema with Chart Modes
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    # Run validation mode if requested
    if st.sidebar.checkbox("Run Validation Mode", key="validation_mode"):
        run_final_validation(cards)
        return
    
    # Render sorting controls for backtest analysis
    sort_option, filter_option, max_results = render_sorting_controls(cards)
    
    # Apply sorting and filtering
    if sort_option:  # Only apply if controls are shown
        cards = apply_sorting_and_filtering(cards, sort_option, filter_option, max_results)
    
    # Render final cards
    render_final_cards(cards)


if __name__ == "__main__":
    main()
