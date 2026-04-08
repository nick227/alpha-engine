"""
Polished Dashboard - UI/UX Refinements & Performance Optimizations
Architecture settled with enhanced user experience and performance
"""

import streamlit as st
import plotly.graph_objects as go
from typing import Dict, List, Any, Tuple
import pandas as pd
from datetime import datetime, timedelta
import time

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
# PERFORMANCE MONITORING
# ============================================================================

class PerformanceMonitor:
    """Performance monitoring for dashboard operations"""
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
    
    def start_timer(self, operation: str):
        """Start timing an operation"""
        self.start_times[operation] = time.time()
    
    def end_timer(self, operation: str):
        """End timing an operation"""
        if operation in self.start_times:
            duration = time.time() - self.start_times[operation]
            self.metrics[operation] = self.metrics.get(operation, []) + [duration]
    
    def get_average_time(self, operation: str) -> float:
        """Get average time for operation"""
        if operation not in self.metrics:
            return 0.0
        return sum(self.metrics[operation]) / len(self.metrics[operation])
    
    def get_performance_summary(self) -> Dict[str, float]:
        """Get performance summary for debug"""
        return {op: self.get_average_time(op) for op in self.metrics.keys()}


# ============================================================================
# ENHANCED DATA SERVICE WITH PERFORMANCE
# ============================================================================

class PolishedDashboardService:
    """Enhanced data service with performance monitoring and optimizations"""
    
    def __init__(self, service: DashboardService):
        self.service = service
        self.performance = PerformanceMonitor()
    
    @st.cache_data(ttl=30)  # Cache key only depends on true query inputs
    def fetch_cards(self, inputs: DashboardInputs) -> List[Card]:
        """Fetch cards with performance monitoring"""
        self.performance.start_timer("fetch_cards")
        
        if not inputs.tenant:
            self.performance.end_timer("fetch_cards")
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
            cards = self._get_best_picks(inputs)
        elif inputs.view == "dips":
            cards = self._get_dips(inputs)
        elif inputs.view == "bundles":
            cards = self._get_bundles(inputs)
        elif inputs.view == "compare":
            cards = self._get_compare(inputs)
        elif inputs.view == "backtest_analysis":
            cards = self._get_backtest_analysis(inputs)
        elif inputs.view == "mixed_test":
            cards = create_mixed_response()
        else:
            cards = []
        
        self.performance.end_timer("fetch_cards")
        return cards
    
    def _get_best_picks(self, inputs: DashboardInputs) -> List[Card]:
        """Generate best picks with optimizations"""
        self.performance.start_timer("generate_best_picks")
        
        cards = []
        
        # Optimized series generation with vectorized operations
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        
        # Use numpy for faster calculations
        import numpy as np
        historical_values = 100 + np.arange(30) * 0.5
        forecast_values = np.arange(10) * 0.3 + historical_values[-1]
        
        # Build series efficiently
        series = [
            {"x": dates[i].isoformat(), "y": float(historical_values[i]), "kind": "historical"}
            for i in range(len(dates))
        ]
        
        # Add forecast points
        for i, value in enumerate(forecast_values):
            forecast_date = dates[-1] + pd.Timedelta(days=i + 1)
            series.append({
                "x": forecast_date.isoformat(),
                "y": float(value),
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
        
        # Enhanced number card with animations
        number_data = NumberData(
            primary_value="+12.4%",
            confidence=0.87,
            subtitle="Expected Move"
        )
        cards.append(Card("number", "Expected Move", number_data.to_dict(), "expected_move"))
        
        self.performance.end_timer("generate_best_picks")
        return cards
    
    def _get_dips(self, inputs: DashboardInputs) -> List[Card]:
        """Generate dip opportunities with optimizations"""
        self.performance.start_timer("generate_dips")
        
        cards = []
        
        # Vectorized calculations
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        import numpy as np
        values = 100 - np.arange(30) * 0.3
        
        series = [
            {"x": dates[i].isoformat(), "y": float(values[i]), "kind": "historical"}
            for i in range(len(dates))
        ]
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.FORECAST
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        cards.append(Card("chart", "AAPL - Dip Opportunity", normalized_chart.to_dict(), "aapl_dip"))
        
        number_data = NumberData(
            primary_value="-18.2%",
            confidence=0.79,
            subtitle="Discount Opportunity"
        )
        cards.append(Card("number", "Discount", number_data.to_dict(), "discount"))
        
        self.performance.end_timer("generate_dips")
        return cards
    
    def _get_bundles(self, inputs: DashboardInputs) -> List[Card]:
        """Generate bundle analysis with optimizations"""
        self.performance.start_timer("generate_bundles")
        
        cards = []
        
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        import numpy as np
        bundle_values = 100 + np.arange(30) * 0.2
        nvda_values = 100 + np.arange(30) * 0.4
        amd_values = 100 + np.arange(30) * 0.1
        
        series = []
        for i in range(len(dates)):
            date = dates[i].isoformat()
            series.append({"x": date, "y": float(bundle_values[i]), "kind": "bundle", "label": "AI Bundle"})
            series.append({"x": date, "y": float(nvda_values[i]), "kind": "constituent", "label": "NVDA"})
            series.append({"x": date, "y": float(amd_values[i]), "kind": "constituent", "label": "AMD"})
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.COMPARISON
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        cards.append(Card("chart", "AI Chip Bundle", normalized_chart.to_dict(), "ai_bundle"))
        
        self.performance.end_timer("generate_bundles")
        return cards
    
    def _get_compare(self, inputs: DashboardInputs) -> List[Card]:
        """Generate comparison with optimizations"""
        self.performance.start_timer("generate_compare")
        
        cards = []
        
        dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
        import numpy as np
        nvda_values = 100 + np.arange(30) * 0.5
        amd_values = 100 + np.arange(30) * 0.3
        msft_values = 100 + np.arange(30) * 0.2
        
        series = []
        for i in range(len(dates)):
            date = dates[i].isoformat()
            series.append({"x": date, "y": float(nvda_values[i]), "kind": "comparison", "label": "NVDA"})
            series.append({"x": date, "y": float(amd_values[i]), "kind": "comparison", "label": "AMD"})
            series.append({"x": date, "y": float(msft_values[i]), "kind": "comparison", "label": "MSFT"})
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.COMPARISON
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        cards.append(Card("chart", "Tech Giants Comparison", normalized_chart.to_dict(), "tech_comparison"))
        
        self.performance.end_timer("generate_compare")
        return cards
    
    def _get_backtest_analysis(self, inputs: DashboardInputs) -> List[Card]:
        """Generate backtest analysis with optimizations"""
        self.performance.start_timer("generate_backtest")
        
        cards = []
        
        # Optimized data generation
        base_date = datetime(2024, 1, 1)
        import numpy as np
        actual_values = 100 + np.arange(30) * 0.3
        pred_values = 100 + np.arange(10) * 0.4
        
        series = []
        for i in range(30):
            date = base_date + pd.Timedelta(days=i)
            series.append({
                "x": date.isoformat(),
                "y": float(actual_values[i]),
                "kind": "actual"
            })
        
        for i in range(10):
            date = base_date + pd.Timedelta(days=i)
            series.append({
                "x": date.isoformat(),
                "y": float(pred_values[i]),
                "kind": "prediction"
            })
        
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
            confidence=0.87,
            direction_correct=True,
            return_pct=8.7,
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
            fallback_card = FallbackHandler.create_fallback_card(
                Card("chart", "NVDA - Backtest Analysis", extended_chart.to_dict(), "nvda_backtest"),
                render_type
            )
            cards.append(fallback_card)
        
        # Add contextual table cards
        from app.ui.chart_schema_final import TableData
        evidence_data = TableData(
            table_type="evidence",
            headers=["Event", "Source", "Sentiment", "Materiality"],
            rows=[
                ["Earnings Beat", "Q4 2023", "Positive", "High"],
                ["AI Momentum", "Sector News", "Positive", "Medium"],
                ["Technical Signal", "Golden Cross", "Positive", "Low"]
            ],
            context_card_id="nvda_backtest"
        )
        cards.append(Card("table", "Evidence", evidence_data.to_dict(), "evidence_nvda"))
        
        outcome_data = TableData(
            table_type="outcome",
            headers=["Metric", "Value", "Benchmark"],
            rows=[
                ["Realized Return", "+8.7%", "S&P: +1.2%"],
                ["Max Runup", "+12.1%", "Avg: +8.3%"],
                ["Max Drawdown", "-2.3%", "Avg: -4.1%"]
            ],
            context_card_id="nvda_backtest"
        )
        cards.append(Card("table", "Outcome", outcome_data.to_dict(), "outcome_nvda"))
        
        self.performance.end_timer("generate_backtest")
        return cards


# ============================================================================
# ENHANCED CONTROLS WITH UI/UX IMPROVEMENTS
# ============================================================================

def render_polished_controls(service: DashboardService) -> Tuple[DashboardInputs, bool]:
    """Polished controls with enhanced UX"""
    with st.sidebar:
        st.markdown("### 🎛️ Controls")
        
        # Get available tenants
        tenants = service.list_tenants()
        if not tenants:
            st.warning("No tenants found in DB.")
            return DashboardInputs(), False
        
        # Enhanced tenant selection with descriptions
        tenant_key = "tenant-default"
        tenant = st.selectbox(
            "🏢 Tenant",
            options=tenants,
            key=tenant_key,
            help="Select tenant to view data for",
            index=0 if tenants else None
        )
        
        # Enhanced ticker selection with search
        if tenant:
            all_tickers = service.list_tickers(tenant_id=tenant)
            ticker_key = f"ticker-{tenant}"
            
            # Add search functionality for large ticker lists
            if len(all_tickers) > 10:
                search_term = st.text_input("🔍 Search Ticker", key=f"search-{tenant}")
                if search_term:
                    filtered_tickers = [t for t in all_tickers if search_term.lower() in t.lower()]
                else:
                    filtered_tickers = all_tickers
            else:
                filtered_tickers = all_tickers
            
            ticker = st.selectbox(
                "📈 Ticker",
                options=filtered_tickers,
                key=ticker_key,
                help="Select ticker to view data for"
            )
        else:
            ticker = None
        
        # Enhanced view selection with icons and descriptions
        view_options = [
            ("best_picks", "🏆 Best Picks", "Top performing predictions"),
            ("dips", "📉 Dip Opportunities", "Undervalued assets"),
            ("bundles", "📦 Thematic Bundles", "Sector-based analysis"),
            ("compare", "📊 Asset Comparison", "Side-by-side analysis"),
            ("backtest_analysis", "🔬 Backtest Analysis", "Prediction vs actual"),
            ("mixed_test", "🧪 Mixed Test", "All card types and modes")
        ]
        
        view_descriptions = {opt[0]: opt[2] for opt in view_options}
        view_labels = [opt[1] for opt in view_options]
        view_values = [opt[0] for opt in view_options]
        
        selected_view_index = st.selectbox(
            "👁️ View",
            options=view_labels,
            index=view_values.index("best_picks") if "best_picks" in view_values else 0,
            key="view-default",
            help="Select analysis view"
        )
        
        # Get the actual view value
        view = view_values[view_labels.index(selected_view_index)]
        
        # Show view description
        if view in view_descriptions:
            st.caption(f"ℹ️ {view_descriptions[view]}")
        
        # Enhanced strategy selection
        strategy_options = [
            ("house", "🏠 House", "Internal strategies"),
            ("semantic", "🧠 Semantic", "NLP-based analysis"),
            ("quant", "📊 Quantitative", "Mathematical models"),
            ("comparison", "⚖️ Comparison", "Relative analysis")
        ]
        
        strategy_labels = [opt[1] for opt in strategy_options]
        strategy_values = [opt[0] for opt in strategy_options]
        
        selected_strategy_index = st.selectbox(
            "🎯 Strategy",
            options=strategy_labels,
            index=0,
            key="strategy-default",
            help="Select analysis strategy"
        )
        
        strategy = strategy_values[strategy_labels.index(selected_strategy_index)]
        
        # Enhanced horizon selection with time periods
        horizon_options = [
            ("1D", "📅 1 Day", "Intraday analysis"),
            ("1W", "📆 1 Week", "Short-term trends"),
            ("1M", "📅 1 Month", "Monthly analysis"),
            ("3M", "📊 3 Months", "Quarterly trends"),
            ("6M", "📈 6 Months", "Semi-annual"),
            ("1Y", "📊 1 Year", "Annual analysis")
        ]
        
        horizon_labels = [opt[1] for opt in horizon_options]
        horizon_values = [opt[0] for opt in horizon_options]
        
        selected_horizon_index = st.selectbox(
            "⏰ Horizon",
            options=horizon_labels,
            index=2,  # Default to 1M
            key="horizon-default",
            help="Select time horizon for analysis"
        )
        
        horizon = horizon_values[horizon_labels.index(selected_horizon_index)]
        
        # Enhanced refresh controls
        st.markdown("---")
        st.markdown("### 🔄 Refresh")
        
        col1, col2 = st.columns(2)
        
        with col1:
            manual_refresh = st.button(
                "🔄 Refresh Now",
                key="refresh-main",
                help="Force refresh of all data",
                use_container_width=True
            )
        
        with col2:
            auto_refresh = st.checkbox(
                "⏱️ Auto-refresh",
                value=True,
                key="auto-refresh",
                help="Automatically refresh data at intervals"
            )
        
        # Enhanced auto-refresh with visual feedback
        if auto_refresh:
            refresh_interval = st.slider(
                "⚡ Refresh Interval",
                min_value=1000,
                max_value=30000,
                value=5000,
                step=1000,
                key="refresh-interval",
                help="Auto-refresh interval in milliseconds",
                format="%d ms"
            )
            
            # Visual auto-refresh indicator
            if "last_refresh" not in st.session_state:
                st.session_state.last_refresh = datetime.now()
                st.session_state.refresh_count = 0
            
            time_since_refresh = (datetime.now() - st.session_state.last_refresh).total_seconds() * 1000
            if time_since_refresh > refresh_interval:
                st.session_state.last_refresh = datetime.now()
                st.session_state.refresh_count += 1
                manual_refresh = True
            else:
                # Show countdown
                remaining_ms = refresh_interval - time_since_refresh
                remaining_seconds = remaining_ms / 1000
                st.caption(f"⏳ Next refresh in {remaining_seconds:.1f}s")
        
        # Performance metrics
        if st.checkbox("📊 Show Performance", key="show-performance"):
            st.markdown("### 📊 Performance Metrics")
            # This will be populated by the service
        
        inputs = DashboardInputs(tenant, ticker, view, strategy, horizon)
        return inputs, manual_refresh


# ============================================================================
# ENHANCED SORTING WITH VISUAL FEEDBACK
# ============================================================================

def render_polished_sorting(cards: List[Card]) -> Tuple[str, int]:
    """Polished sorting controls with visual feedback"""
    # Only show sorting for chart cards
    chart_cards = [c for c in cards if c.card_type == "chart"]
    if not chart_cards:
        return "", len(cards)
    
    st.markdown("### 🎛️ Sort & Filter")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        sort_option = st.selectbox(
            "📊 Sort by",
            options=[
                ("default", "🔄 Default Order", "Original order"),
                ("confidence", "🎯 Confidence", "Pre-outcome: confidence"),
                ("return", "💰 Return", "Post-outcome: return_pct"),
                ("primary", "⭐ Primary Key", "Confidence pre-outcome, Return post-outcome")
            ],
            format_func=lambda x: x[1],
            key="polished_sort_option",
            help="Choose sorting criteria"
        )
        sort_value = sort_option[0]
        
        # Show sort description
        sort_descriptions = {
            "default": "Original card order",
            "confidence": "Sort by prediction confidence (pre-outcome)",
            "return": "Sort by realized return (post-outcome)",
            "primary": "Smart sort: confidence pre-outcome, return post-outcome"
        }
        if sort_value in sort_descriptions:
            st.caption(f"ℹ️ {sort_descriptions[sort_value]}")
    
    with col2:
        filter_option = st.selectbox(
            "🔍 Filter",
            options=[
                ("all", "📋 All", "Show all cards"),
                ("wins", "🏆 Wins Only", "Winning predictions only"),
                ("losses", "📉 Losses Only", "Losing predictions only"),
                ("high_confidence", "🎯 High Confidence", "Confidence > 75%"),
                ("best_returns", "💰 Best Returns", "Return > 5%")
            ],
            format_func=lambda x: x[1],
            key="polished_filter_option",
            help="Filter cards by criteria"
        )
        filter_value = filter_option[0]
    
    with col3:
        max_results = st.slider(
            "📊 Max Results",
            min_value=5,
            max_value=50,
            value=10,
            step=5,
            key="polished_max_results",
            help="Maximum number of cards to display"
        )
        
        # Show result count
        st.metric("Cards to Show", max_results)
    
    return sort_value, filter_value, max_results


def apply_polished_sorting_and_filtering(cards: List[Card], sort_option: str, filter_option: str, max_results: int) -> List[Card]:
    """Apply sorting and filtering with enhanced logic"""
    # Separate chart cards for special processing
    overlay_cards = [c for c in cards if c.card_type == "chart"]
    other_cards = [c for c in cards if c.card_type != "chart"]
    
    # Apply filtering to overlay cards
    if filter_option == "wins":
        overlay_cards = [c for c in overlay_cards if c.data.get("direction_correct") is True]
    elif filter_option == "losses":
        overlay_cards = [c for c in overlay_cards if c.data.get("direction_correct") is False]
    elif filter_option == "high_confidence":
        overlay_cards = [c for c in overlay_cards if c.data.get("confidence", 0) > 0.75]
    elif filter_option == "best_returns":
        overlay_cards = [c for c in overlay_cards if c.data.get("return_pct", 0) > 5.0]
    # "all" shows all overlay cards
    
    # Apply sorting to overlay cards
    if sort_option == "confidence":
        overlay_cards = sorted(
            [c for c in overlay_cards if c.data.get("confidence") is not None],
            key=lambda c: c.data.get("confidence", 0),
            reverse=True
        )
    elif sort_option == "return":
        overlay_cards = sorted(
            [c for c in overlay_cards if c.data.get("return_pct") is not None],
            key=lambda c: c.data.get("return_pct", 0),
            reverse=True
        )
    elif sort_option == "primary":
        overlay_cards = sorted(
            overlay_cards,
            key=lambda c: ChartData(**c.data).primary_sort_key,
            reverse=True
        )
    # "default" keeps original order
    
    # Combine and limit results
    processed_cards = overlay_cards[:max_results] + other_cards
    return processed_cards


# ============================================================================
# ENHANCED CARD RIVER WITH ANIMATIONS
# ============================================================================

def render_polished_cards(cards: List[Card], visible_count: int = 10, performance_monitor: PerformanceMonitor = None) -> None:
    """Render cards with polished UI and animations"""
    if not cards:
        # Enhanced empty state
        st.markdown(
            f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['2']};
            padding: {SPACING['6']};
            margin-bottom: {SPACING['3']};
            text-align: center;
        ">
            <div style="font-size: {TYPOGRAPHY['text_xl']}; color: {COLORS['neutral_600']}; margin-bottom: {SPACING['2']};">
                🎯 No cards found
            </div>
            <div style="font-size: {TYPOGRAPHY['text_sm']}; color: {COLORS['neutral_500']};">
                Try different controls or refresh to explore available data
            </div>
        </div>
        """,
            unsafe_allow_html=True,
        )
        return
    
    # Performance monitoring for rendering
    if performance_monitor:
        performance_monitor.start_timer("render_cards")
    
    # Render visible cards with enhanced separators
    for i, card in enumerate(cards[:visible_count]):
        # Enhanced card separator with subtle animation
        if i > 0:
            st.markdown(
                f"""
                <div style="
                    height: 1px; 
                    background: linear-gradient(90deg, {COLORS['border_light']}, {COLORS['neutral_200']}, {COLORS['border_light']}); 
                    margin: {SPACING['3']} 0; 
                    animation: fadeIn 0.5s ease-in;
                "></div>
                <style>
                @keyframes fadeIn {{
                    from {{ opacity: 0; }}
                    to {{ opacity: 1; }}
                }}
                </style>
                """,
                unsafe_allow_html=True,
            )
        
        # Enhanced card container with hover effects
        with st.container():
            # Add subtle card number indicator
            st.markdown(
                f"""
                <div style="
                    position: relative;
                    margin-bottom: {SPACING['2']};
                ">
                    <div style="
                        position: absolute;
                        top: -8px;
                        left: -8px;
                        background: {COLORS['primary_600']};
                        color: white;
                        border-radius: 50%;
                        width: 24px;
                        height: 24px;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        font-size: {TYPOGRAPHY['text_xs']};
                        font-weight: {TYPOGRAPHY['weight_bold']};
                        z-index: 10;
                    ">
                        {i + 1}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            
            # Render using unified pipeline
            FinalCardRenderer.render_card(card)
    
    # Enhanced "show more" button with progress indicator
    if len(cards) > visible_count:
        remaining_cards = len(cards) - visible_count
        progress_percentage = (visible_count / len(cards)) * 100
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            st.write("")  # Spacer
        
        with col2:
            # Progress bar
            st.markdown(
                f"""
                <div style="
                    margin-bottom: {SPACING['2']};
                ">
                    <div style="
                        width: 100%;
                        height: 8px;
                        background: {COLORS['neutral_200']};
                        border-radius: {SPACING['1']};
                        overflow: hidden;
                    ">
                        <div style="
                            width: {progress_percentage}%;
                            height: 100%;
                            background: linear-gradient(90deg, {COLORS['primary_500']}, {COLORS['primary_600']});
                            border-radius: {SPACING['1']};
                            transition: width 0.3s ease;
                        "></div>
                    </div>
                    <div style="
                        text-align: center;
                        font-size: {TYPOGRAPHY['text_xs']};
                        color: {COLORS['neutral_600']};
                        margin-top: {SPACING['1']};
                    ">
                        Showing {visible_count} of {len(cards)} cards
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            
            # Enhanced button
            if st.button(f"📋 Show {remaining_cards} More Cards", key="show-more-cards"):
                render_polished_cards(cards, visible_count * 2, performance_monitor)
        
        with col3:
            st.write("")  # Spacer
    
    if performance_monitor:
        performance_monitor.end_timer("render_cards")


# ============================================================================
# ENHANCED HEADER WITH STATUS
# ============================================================================

def render_polished_header(inputs: DashboardInputs, performance_monitor: PerformanceMonitor = None) -> None:
    """Render polished header with status indicators"""
    # Dynamic header based on view
    view_emojis = {
        "best_picks": "🏆",
        "dips": "📉", 
        "bundles": "📦",
        "compare": "📊",
        "backtest_analysis": "🔬",
        "mixed_test": "🧪"
    }
    
    view_descriptions = {
        "best_picks": "Top Performing Predictions",
        "dips": "Undervalued Opportunities",
        "bundles": "Thematic Analysis",
        "compare": "Asset Comparison",
        "backtest_analysis": "Prediction vs Actual",
        "mixed_test": "All Features Demo"
    }
    
    emoji = view_emojis.get(inputs.view, "📊")
    description = view_descriptions.get(inputs.view, "Analysis")
    
    # Performance summary
    perf_summary = ""
    if performance_monitor:
        perf_metrics = performance_monitor.get_performance_summary()
        if perf_metrics:
            avg_fetch = perf_metrics.get("fetch_cards", 0) * 1000  # Convert to ms
            avg_render = perf_metrics.get("render_cards", 0) * 1000
            perf_summary = f"⚡ Fetch: {avg_fetch:.0f}ms | Render: {avg_render:.0f}ms"
    
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, {COLORS['primary_800']}, {COLORS['primary_600']});
            color: white;
            padding: {SPACING['5']};
            border-radius: {SPACING['3']};
            margin-bottom: {SPACING['4']};
            text-align: center;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: linear-gradient(45deg, transparent 30%, rgba(255,255,255,0.05) 50%, transparent 70%);
                animation: shimmer 2s infinite;
            "></div>
            <style>
            @keyframes shimmer {{
                0% {{ transform: translateX(-100%); }}
                100% {{ transform: translateX(100%); }}
            }}
            </style>
            <h1 style="margin: 0; font-size: {TYPOGRAPHY['text_2xl']}; font-weight: {TYPOGRAPHY['weight_bold']}; position: relative; z-index: 1;">
                {emoji} Polished Dashboard
            </h1>
            <p style="margin: {SPACING['2']} 0 0 0; font-size: {TYPOGRAPHY['text_sm']}; opacity: 0.9; position: relative; z-index: 1;">
                {description}
            </p>
            {f'<div style="margin-top: {SPACING["2"]}; font-size: {TYPOGRAPHY["text_xs"]}; opacity: 0.8; font-family: monospace; position: relative; z-index: 1;">{perf_summary}</div>' if perf_summary else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================================
# MAIN APPLICATION WITH POLISH
# ============================================================================

def main():
    """Main entry point - polished dashboard with performance monitoring"""
    # Apply theme
    apply_theme()
    
    # Initialize service with performance monitoring
    service = DashboardService()
    polished_service = PolishedDashboardService(service)
    
    # Render polished controls and get inputs
    inputs, should_refresh = render_polished_controls(service)
    
    # Render system status with enhancements
    render_system_status(inputs)
    
    # Enhanced debug view
    if st.checkbox("🔍 Show Debug", key="debug-view"):
        st.markdown("### 🔍 Debug Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Inputs**")
            st.code(f"""
Fingerprint: {inputs.fingerprint}
Should Refresh: {should_refresh}
Tenant: {inputs.tenant}
View: {inputs.view}
Strategy: {inputs.strategy}
Horizon: {inputs.horizon}
            """)
        
        with col2:
            st.markdown("**Performance**")
            perf_metrics = polished_service.performance.get_performance_summary()
            if perf_metrics:
                for operation, avg_time in perf_metrics.items():
                    st.metric(f"{operation.replace('_', ' ').title()}", f"{avg_time*1000:.1f}ms")
        
        # Cache key
        cache_key = CacheKeyGenerator.generate_cache_key({
            "tenant": inputs.tenant,
            "ticker": inputs.ticker,
            "view": inputs.view,
            "strategy": inputs.strategy,
            "horizon": inputs.horizon
        })
        st.code(f"Cache Key: {cache_key}")
    
    # Fetch data using polished service
    if should_refresh and inputs.tenant:
        cards = polished_service.fetch_cards(inputs)
    elif inputs.tenant:
        cards = st.session_state.get("cached_cards", [])
    else:
        cards = []
    
    # Cache cards in session
    if cards:
        st.session_state.cached_cards = cards
    
    # Render polished header
    render_polished_header(inputs, polished_service.performance)
    
    # Render sorting controls
    sort_value, filter_value, max_results = render_polished_sorting(cards)
    
    # Apply sorting and filtering
    if sort_value:
        # Convert to expected format
        sort_option = sort_value
        filter_option = filter_value
        
        # Apply filtering logic
        if filter_option == "wins":
            cards = [c for c in cards if c.card_type == "chart" and c.data.get("direction_correct") is True]
        elif filter_option == "losses":
            cards = [c for c in cards if c.card_type == "chart" and c.data.get("direction_correct") is False]
        elif filter_option == "high_confidence":
            cards = [c for c in cards if c.card_type == "chart" and c.data.get("confidence", 0) > 0.75]
        elif filter_option == "best_returns":
            cards = [c for c in cards if c.card_type == "chart" and c.data.get("return_pct", 0) > 5.0]
        # "all" shows all cards
        
        # Apply sorting
        if sort_option == "confidence":
            cards = sorted(
                [c for c in cards if c.card_type == "chart" and c.data.get("confidence") is not None],
                key=lambda c: c.data.get("confidence", 0),
                reverse=True
            )
        elif sort_option == "return":
            cards = sorted(
                [c for c in cards if c.card_type == "chart" and c.data.get("return_pct") is not None],
                key=lambda c: c.data.get("return_pct", 0),
                reverse=True
            )
        elif sort_option == "primary":
            cards = sorted(
                [c for c in cards if c.card_type == "chart"],
                key=lambda c: ChartData(**c.data).primary_sort_key,
                reverse=True
            )
        # "default" keeps original order
        
        cards = cards[:max_results]
    
    # Render polished cards
    render_polished_cards(cards, max_results, polished_service.performance)


if __name__ == "__main__":
    main()
