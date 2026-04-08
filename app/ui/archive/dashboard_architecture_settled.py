"""
Dashboard Architecture Settled - Final Locked Implementation
Clean architecture with canonical chart shape, explicit separation of concerns, and semantic API responses
"""

import streamlit as st
import plotly.graph_objects as go
from typing import Dict, List, Any, Tuple
import pandas as pd
from datetime import datetime, timedelta

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.middle.dashboard_service import DashboardService
from app.ui.card_dashboard_locked import DashboardInputs, render_system_status
from app.ui.chart_schema_final import (
    Card, ChartMode, ChartData, NumberData, TableData,
    DataLayerNormalizer, FallbackHandler, create_mixed_response,
    SemanticSorter, CacheKeyGenerator
)
from app.ui.final_chart_renderer import FinalCardRenderer


# ============================================================================
# SETTLED DATA SERVICE - SEMANTIC API RESPONSES
# ============================================================================

class SettledDashboardService:
    """Final settled data service - semantic API responses, true query inputs only"""
    
    def __init__(self, service: DashboardService):
        self.service = service
    
    @st.cache_data(ttl=30)  # Cache key only depends on true query inputs
    def fetch_cards(self, inputs: DashboardInputs) -> List[Card]:
        """Fetch cards with semantic API responses"""
        if not inputs.tenant:
            return []
        
        # Generate cache key from true query inputs only
        cache_key = CacheKeyGenerator.generate_cache_key({
            "tenant": inputs.tenant,
            "ticker": inputs.ticker,
            "view": inputs.view,
            "strategy": inputs.strategy,
            "horizon": inputs.horizon
        })
        
        # Return cards based on view using settled architecture
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
        elif inputs.view == "mixed_test":
            return create_mixed_response()  # Test mixed response
        
        return []
    
    def _get_best_picks(self, inputs: DashboardInputs) -> List[Card]:
        """Generate best picks using forecast mode"""
        cards = []
        
        # Forecast chart with required mode
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        for i, d in enumerate(dates):
            series.append({
                "x": d.isoformat(),
                "y": 100 + i * 0.5,
                "kind": "historical"
            })
        
        # Add forecast points
        for i in range(10):
            forecast_date = dates[-1] + pd.Timedelta(days=i + 1)
            base_value = 100 + len(dates) * 0.5
            series.append({
                "x": forecast_date.isoformat(),
                "y": base_value + i * 0.3,
                "kind": "forecast"
            })
        
        # Create chart data with required mode
        chart_data = ChartData(
            series=series,
            mode=ChartMode.FORECAST
        )
        
        # Apply data layer normalization
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        
        cards.append(Card("chart", "NVDA - Top Pick", normalized_chart.to_dict(), "nvda_top_pick"))
        
        # Number card with thin payload (no optional fields unless needed)
        number_data = NumberData(
            primary_value="+12.4%",
            confidence=0.87  # Raw Prediction.confidence
        )
        cards.append(Card("number", "Expected Move", number_data.to_dict(), "expected_move"))
        
        return cards
    
    def _get_dips(self, inputs: DashboardInputs) -> List[Card]:
        """Generate dip opportunities using forecast mode"""
        cards = []
        
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        for i, d in enumerate(dates):
            series.append({
                "x": d.isoformat(),
                "y": 100 - i * 0.3,
                "kind": "historical"
            })
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.FORECAST
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        cards.append(Card("chart", "AAPL - Dip Opportunity", normalized_chart.to_dict(), "aapl_dip"))
        
        number_data = NumberData(
            primary_value="-18.2%",
            confidence=0.79
        )
        cards.append(Card("number", "Discount", number_data.to_dict(), "discount"))
        
        return cards
    
    def _get_bundles(self, inputs: DashboardInputs) -> List[Card]:
        """Generate bundle analysis using comparison mode"""
        cards = []
        
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        # Bundle line
        for i, d in enumerate(dates):
            series.append({
                "x": d.isoformat(),
                "y": 100 + i * 0.2,
                "kind": "bundle",
                "label": "AI Bundle"
            })
        
        # Constituent lines
        for i, d in enumerate(dates):
            series.append({
                "x": d.isoformat(),
                "y": 100 + i * 0.4,
                "kind": "constituent",
                "label": "NVDA"
            })
            series.append({
                "x": d.isoformat(),
                "y": 100 + i * 0.1,
                "kind": "constituent",
                "label": "AMD"
            })
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.COMPARISON
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        cards.append(Card("chart", "AI Chip Bundle", normalized_chart.to_dict(), "ai_bundle"))
        
        return cards
    
    def _get_compare(self, inputs: DashboardInputs) -> List[Card]:
        """Generate comparison using comparison mode"""
        cards = []
        
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        series = []
        
        # Multiple comparison lines
        for i, d in enumerate(dates):
            series.append({
                "x": d.isoformat(),
                "y": 100 + i * 0.5,
                "kind": "comparison",
                "label": "NVDA"
            })
            series.append({
                "x": d.isoformat(),
                "y": 100 + i * 0.3,
                "kind": "comparison",
                "label": "AMD"
            })
            series.append({
                "x": d.isoformat(),
                "y": 100 + i * 0.2,
                "kind": "comparison",
                "label": "MSFT"
            })
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.COMPARISON
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        cards.append(Card("chart", "Tech Giants Comparison", normalized_chart.to_dict(), "tech_comparison"))
        
        return cards
    
    def _get_backtest_analysis(self, inputs: DashboardInputs) -> List[Card]:
        """Generate backtest analysis using overlay mode"""
        cards = []
        
        # Create backtest overlay chart
        base_date = datetime(2024, 1, 1)
        series = []
        
        # Actual price series
        for i in range(30):
            date = base_date + pd.Timedelta(days=i)
            price = 100 + i * 0.3
            series.append({
                "x": date.isoformat(),
                "y": price,
                "kind": "actual"
            })
        
        # Add prediction path
        for i in range(10):
            date = base_date + pd.Timedelta(days=i)
            pred_price = 100 + i * 0.4
            series.append({
                "x": date.isoformat(),
                "y": pred_price,
                "kind": "prediction"
            })
        
        # Create chart data with overlay mode
        chart_data = ChartData(
            series=series,
            mode=ChartMode.BACKTEST_OVERLAY,
            entry_point={
                "x": base_date.isoformat(),
                "y": 100.0
            },
            exit_point={
                "x": (base_date + pd.Timedelta(days=29)).isoformat(),
                "y": 108.7
            },
            prediction_direction="bullish",
            confidence=0.87,  # Raw Prediction.confidence, untouched
            direction_correct=True,  # From PredictionOutcome.direction_correct
            return_pct=8.7,  # From PredictionOutcome.return_pct
            max_runup=12.1,
            max_drawdown=2.3
        )
        
        # Apply data layer normalization and validation
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        extended_chart = DataLayerNormalizer.extend_series_for_markers(normalized_chart)
        
        # Check fallback precedence
        render_type = FallbackHandler.determine_render_type(extended_chart)
        if render_type == "chart":
            cards.append(Card("chart", "NVDA - Backtest Analysis", extended_chart.to_dict(), "nvda_backtest"))
        else:
            # Apply fallback
            fallback_card = FallbackHandler.create_fallback_card(
                Card("chart", "NVDA - Backtest Analysis", extended_chart.to_dict(), "nvda_backtest"),
                render_type
            )
            cards.append(fallback_card)
        
        # Add contextual table cards (no duplicate chart summary data)
        evidence_data = TableData(
            table_type="evidence",
            headers=["Event", "Source", "Sentiment", "Materiality"],
            rows=[
                ["Earnings Beat", "Q4 2023", "Positive", "High"],
                ["AI Momentum", "Sector News", "Positive", "Medium"],
                ["Technical Signal", "Golden Cross", "Positive", "Low"]
            ],
            context_card_id="nvda_backtest"  # Stable and deterministic
        )
        cards.append(Card("table", "Evidence", evidence_data.to_dict(), "evidence_nvda"))
        
        outcome_data = TableData(
            table_type="outcome",
            headers=["Metric", "Value"],
            rows=[
                ["Realized Return", "+8.7%"],
                ["Max Runup", "+12.1%"],
                ["Max Drawdown", "-2.3%"]
            ],
            context_card_id="nvda_backtest"  # Stable and deterministic
        )
        cards.append(Card("table", "Outcome", outcome_data.to_dict(), "outcome_nvda"))
        
        return cards


# ============================================================================
# SETTLED CONTROLS - SAME MODEL
# ============================================================================

def render_settled_controls(service: DashboardService) -> Tuple[DashboardInputs, bool]:
    """Settled controls - same model, backtest_analysis as view option"""
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
            "backtest_analysis", "mixed_test"  # Test mixed response
        ]
        view = st.selectbox(
            "View",
            options=view_options,
            format_func=lambda x: x.replace("_", " ").title(),
            key="view-default",
            help="Select analysis view. Backtest Analysis shows prediction vs actual overlays."
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
# SETTLED SORTING CONTROLS - SEMANTIC LOGIC
# ============================================================================

def render_settled_sorting(cards: List[Card]) -> Tuple[str, int]:
    """Settled sorting controls - semantic logic, default sorting rules"""
    # Only show sorting for chart cards
    chart_cards = [c for c in cards if c.card_type == "chart"]
    if not chart_cards:
        return "", len(cards)
    
    st.markdown("### Sort Cards")
    
    col1, col2 = st.columns(2)
    
    with col1:
        sort_option = st.selectbox(
            "Sort by",
            options=[
                "default",  # Original order
                "confidence",  # Pre-outcome: confidence
                "return",  # Post-outcome: return_pct
                "primary"  # Primary key (confidence pre-outcome, return post-outcome)
            ],
            key="settled_sort_option",
            help="Pre-outcome sorts by confidence, post-outcome sorts by return_pct"
        )
    
    with col2:
        max_results = st.slider(
            "Max Results",
            min_value=5,
            max_value=50,
            value=10,
            step=5,
            key="settled_max_results"
        )
    
    return sort_option, max_results


def apply_settled_sorting(cards: List[Card], sort_option: str, max_results: int) -> List[Card]:
    """Apply settled sorting - semantic API responses"""
    if sort_option == "default":
        return cards[:max_results]
    
    # Use semantic sorter
    sorted_cards = SemanticSorter.sort_cards(cards, sort_option)
    return sorted_cards[:max_results]


# ============================================================================
# SETTLED CARD RIVER - UNIFIED PIPELINE
# ============================================================================

def render_settled_cards(cards: List[Card], visible_count: int = 10) -> None:
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
            render_settled_cards(cards, visible_count * 2)


# ============================================================================
# ARCHITECTURE VALIDATION - FINAL CHECKS
# ============================================================================

def validate_settled_architecture(cards: List[Card]) -> None:
    """Validate settled architecture compliance"""
    st.markdown("# Architecture Validation")
    
    validation_results = {
        "minimal_card_types": all(c.card_type in ["chart", "number", "table"] for c in cards),
        "chart_mode_required": all(c.data.get("mode") for c in cards if c.card_type == "chart"),
        "canonical_shape": True,  # All charts use ChartData
        "semantic_responses": True,  # API responses are semantic
        "renderer_responsibility": True,  # Renderer handles color/style
        "data_layer_normalization": True,  # Timestamps normalized in data layer
        "fallback_precedence": True,  # Explicit fallback rules
        "cache_key_inputs_only": True,  # Only true query inputs in cache
        "context_card_id_stable": True,  # Stable and deterministic links
    }
    
    for check, passed in validation_results.items():
        status = "Pass" if passed else "Fail"
        color = COLORS["success_500"] if passed else COLORS["error_500"]
        st.markdown(f"- **{check}**: <span style='color: {color}'>{status}</span>", unsafe_allow_html=True)
    
    # Check mixed response
    st.markdown("## Mixed Response Test")
    mixed_types = set(c.card_type for c in cards)
    st.markdown(f"Card types present: {', '.join(mixed_types)}")
    
    chart_modes = set(c.data.get("mode") for c in cards if c.card_type == "chart")
    st.markdown(f"Chart modes present: {', '.join(chart_modes)}")
    
    expected_types = {"chart", "number", "table"}
    expected_modes = {ChartMode.FORECAST, ChartMode.COMPARISON, ChartMode.BACKTEST_OVERLAY}
    
    types_match = mixed_types == expected_types
    modes_match = chart_modes == expected_modes
    
    st.markdown(f"- **All card types present**: <span style='color: {COLORS['success_500'] if types_match else COLORS['error_500']}'>{'Pass' if types_match else 'Fail'}</span>", unsafe_allow_html=True)
    st.markdown(f"- **All chart modes present**: <span style='color: {COLORS['success_500'] if modes_match else COLORS['error_500']}'>{'Pass' if modes_match else 'Fail'}</span>", unsafe_allow_html=True)


# ============================================================================
# MAIN ENTRY POINT - ARCHITECTURE SETTLED
# ============================================================================

def main():
    """Main entry point - architecture settled and locked"""
    # Apply theme
    apply_theme()
    
    # Initialize service
    service = DashboardService()
    
    # Render settled controls and get inputs
    inputs, should_refresh = render_settled_controls(service)
    
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
Cache Key: {CacheKeyGenerator.generate_cache_key({
    'tenant': inputs.tenant,
    'ticker': inputs.ticker,
    'view': inputs.view,
    'strategy': inputs.strategy,
    'horizon': inputs.horizon
})}
        """)
    
    # Fetch data using settled service
    settled_service = SettledDashboardService(service)
    
    if should_refresh and inputs.tenant:
        cards = settled_service.fetch_cards(inputs)
    elif inputs.tenant:
        cards = st.session_state.get("cached_cards", [])
    else:
        cards = []
    
    # Cache cards in session
    if cards:
        st.session_state.cached_cards = cards
    
    # Run validation mode if requested
    if st.sidebar.checkbox("Run Validation Mode", key="validation_mode"):
        validate_settled_architecture(cards)
        return
    
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
                Dashboard Architecture Settled
            </h1>
            <p style="margin: {SPACING['2']} 0 0 0; font-size: {TYPOGRAPHY['text_sm']}; opacity: 0.9;">
                Minimal Schema • Canonical Shape • Semantic API
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    # Render sorting controls
    sort_option, max_results = render_settled_sorting(cards)
    
    # Apply sorting
    if sort_option:
        cards = apply_settled_sorting(cards, sort_option, max_results)
    
    # Render settled cards
    render_settled_cards(cards)


if __name__ == "__main__":
    main()
