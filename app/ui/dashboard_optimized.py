"""
Optimized Dashboard - Performance without Architecture Changes
Maintains settled schema, API contracts, and rendering pipeline with performance optimizations
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import hashlib

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

class PerformanceProfiler:
    """Lightweight performance profiler for bottleneck identification"""
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
    
    def start(self, operation: str):
        """Start timing operation"""
        self.start_times[operation] = time.perf_counter()
    
    def end(self, operation: str):
        """End timing operation"""
        if operation in self.start_times:
            duration = time.perf_counter() - self.start_times[operation]
            if operation not in self.metrics:
                self.metrics[operation] = []
            self.metrics[operation].append(duration)
    
    def get_summary(self) -> Dict[str, float]:
        """Get performance summary"""
        return {
            op: sum(times) / len(times) * 1000  # Convert to ms
            for op, times in self.metrics.items()
        }
    
    def log_bottlenecks(self):
        """Log identified bottlenecks"""
        summary = self.get_summary()
        bottlenecks = [(op, time_ms) for op, time_ms in summary.items() if time_ms > 100]  # >100ms threshold
        
        if bottlenecks:
            print("🔍 Performance Bottlenecks Identified:")
            for op, time_ms in sorted(bottlenecks, key=lambda x: x[1], reverse=True):
                print(f"  {op}: {time_ms:.1f}ms")


# Global profiler instance
profiler = PerformanceProfiler()


# ============================================================================
# OPTIMIZED DATA SERVICE
# ============================================================================

class OptimizedDashboardService:
    """Optimized data service with performance improvements"""
    
    def __init__(self, service: DashboardService):
        self.service = service
        self._cached_series = {}  # Cache for series data
    
    @st.cache_data(ttl=30)  # Cache based only on true query inputs
    def fetch_cards(self, inputs: DashboardInputs) -> List[Card]:
        """Optimized card fetching with minimal recomputation"""
        profiler.start("fetch_cards")
        
        if not inputs.tenant:
            profiler.end("fetch_cards")
            return []
        
        # Generate deterministic cache key
        cache_key = self._generate_cache_key(inputs)
        
        # Return cards based on view using optimized generation
        if inputs.view == "best_picks":
            cards = self._get_best_picks_optimized(inputs, cache_key)
        elif inputs.view == "dips":
            cards = self._get_dips_optimized(inputs, cache_key)
        elif inputs.view == "bundles":
            cards = self._get_bundles_optimized(inputs, cache_key)
        elif inputs.view == "compare":
            cards = self._get_compare_optimized(inputs, cache_key)
        elif inputs.view == "backtest_analysis":
            cards = self._get_backtest_optimized(inputs, cache_key)
        elif inputs.view == "mixed_test":
            cards = self._get_mixed_test_optimized(inputs, cache_key)
        else:
            cards = []
        
        # Precompute sorting keys in data service (avoid per-card sorting in renderer)
        for card in cards:
            if card.card_type == "chart":
                chart_data = ChartData(**card.data)
                card.data["primary_sort_key"] = chart_data.primary_sort_key
        
        profiler.end("fetch_cards")
        return cards
    
    def _generate_cache_key(self, inputs: DashboardInputs) -> str:
        """Generate cache key from true query inputs only"""
        # Extract only true query inputs
        query_components = [
            inputs.tenant or "",
            inputs.ticker or "",
            inputs.view or "",
            inputs.strategy or "",
            inputs.horizon or ""
        ]
        
        # Create deterministic key
        cache_string = "|".join(query_components)
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    def _get_optimized_series(self, base_date: datetime, periods: int, trend: float = 0.5) -> List[Dict]:
        """Generate optimized series with caching"""
        cache_key = f"series_{base_date}_{periods}_{trend}"
        
        if cache_key in self._cached_series:
            return self._cached_series[cache_key]
        
        # Vectorized generation with NumPy
        dates = pd.date_range(start=base_date, periods=periods, freq="D")
        values = 100 + np.arange(periods) * trend
        
        series = [
            {"x": dates[i].isoformat(), "y": float(values[i]), "kind": "historical"}
            for i in range(periods)
        ]
        
        # Cache the result
        self._cached_series[cache_key] = series
        return series
    
    def _get_best_picks_optimized(self, inputs: DashboardInputs, cache_key: str) -> List[Card]:
        """Optimized best picks generation"""
        profiler.start("generate_best_picks")
        
        # Use cached series generation
        base_date = datetime(2024, 1, 1)
        historical_series = self._get_optimized_series(base_date, 30, 0.5)
        
        # Vectorized forecast generation
        last_value = historical_series[-1]["y"]
        forecast_values = np.arange(10) * 0.3 + last_value
        
        forecast_series = [
            {"x": (base_date + pd.Timedelta(days=i+30)).isoformat(), 
             "y": float(forecast_values[i]), "kind": "forecast"}
            for i in range(10)
        ]
        
        # Combine series
        all_series = historical_series + forecast_series
        
        # Create chart data with required mode
        chart_data = ChartData(
            series=all_series,
            mode=ChartMode.FORECAST
        )
        
        # Normalize timestamps once in data layer
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        
        cards = [
            Card("chart", "NVDA - Top Pick", normalized_chart.to_dict(), "nvda_top_pick"),
            Card("number", "Expected Move", {"primary_value": "+12.4%", "confidence": 0.87}, "expected_move")
        ]
        
        profiler.end("generate_best_picks")
        return cards
    
    def _get_dips_optimized(self, inputs: DashboardInputs, cache_key: str) -> List[Card]:
        """Optimized dips generation"""
        profiler.start("generate_dips")
        
        base_date = datetime(2024, 1, 1)
        historical_series = self._get_optimized_series(base_date, 30, -0.3)
        
        chart_data = ChartData(
            series=historical_series,
            mode=ChartMode.FORECAST
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        
        cards = [
            Card("chart", "AAPL - Dip Opportunity", normalized_chart.to_dict(), "aapl_dip"),
            Card("number", "Discount", {"primary_value": "-18.2%", "confidence": 0.79}, "discount")
        ]
        
        profiler.end("generate_dips")
        return cards
    
    def _get_bundles_optimized(self, inputs: DashboardInputs, cache_key: str) -> List[Card]:
        """Optimized bundles generation"""
        profiler.start("generate_bundles")
        
        base_date = datetime(2024, 1, 1)
        dates = pd.date_range(start=base_date, periods=30, freq="D")
        
        # Vectorized calculations for all series
        bundle_values = 100 + np.arange(30) * 0.2
        nvda_values = 100 + np.arange(30) * 0.4
        amd_values = 100 + np.arange(30) * 0.1
        
        # Build series efficiently
        series = []
        for i in range(30):
            date = dates[i].isoformat()
            series.append({"x": date, "y": float(bundle_values[i]), "kind": "bundle", "label": "AI Bundle"})
            series.append({"x": date, "y": float(nvda_values[i]), "kind": "constituent", "label": "NVDA"})
            series.append({"x": date, "y": float(amd_values[i]), "kind": "constituent", "label": "AMD"})
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.COMPARISON
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        
        cards = [Card("chart", "AI Chip Bundle", normalized_chart.to_dict(), "ai_bundle")]
        
        profiler.end("generate_bundles")
        return cards
    
    def _get_compare_optimized(self, inputs: DashboardInputs, cache_key: str) -> List[Card]:
        """Optimized comparison generation"""
        profiler.start("generate_compare")
        
        base_date = datetime(2024, 1, 1)
        dates = pd.date_range(start=base_date, periods=30, freq="D")
        
        # Vectorized comparison calculations
        nvda_values = 100 + np.arange(30) * 0.5
        amd_values = 100 + np.arange(30) * 0.3
        msft_values = 100 + np.arange(30) * 0.2
        
        series = []
        for i in range(30):
            date = dates[i].isoformat()
            series.append({"x": date, "y": float(nvda_values[i]), "kind": "comparison", "label": "NVDA"})
            series.append({"x": date, "y": float(amd_values[i]), "kind": "comparison", "label": "AMD"})
            series.append({"x": date, "y": float(msft_values[i]), "kind": "comparison", "label": "MSFT"})
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.COMPARISON
        )
        
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        
        cards = [Card("chart", "Tech Giants Comparison", normalized_chart.to_dict(), "tech_comparison")]
        
        profiler.end("generate_compare")
        return cards
    
    def _get_backtest_optimized(self, inputs: DashboardInputs, cache_key: str) -> List[Card]:
        """Optimized backtest generation"""
        profiler.start("generate_backtest")
        
        base_date = datetime(2024, 1, 1)
        
        # Vectorized actual and prediction series
        actual_values = 100 + np.arange(30) * 0.3
        pred_values = 100 + np.arange(10) * 0.4
        
        # Build series efficiently
        series = []
        for i in range(30):
            date = base_date + pd.Timedelta(days=i)
            series.append({"x": date.isoformat(), "y": float(actual_values[i]), "kind": "actual"})
        
        for i in range(10):
            date = base_date + pd.Timedelta(days=i)
            series.append({"x": date.isoformat(), "y": float(pred_values[i]), "kind": "prediction"})
        
        chart_data = ChartData(
            series=series,
            mode=ChartMode.BACKTEST_OVERLAY,
            entry_point={"x": base_date.isoformat(), "y": 100.0},
            exit_point={"x": (base_date + pd.Timedelta(days=29)).isoformat(), "y": 108.7},
            prediction_direction="bullish",
            confidence=0.87,
            direction_correct=True,
            return_pct=8.7,
            max_runup=12.1,
            max_drawdown=2.3
        )
        
        # Normalize and validate once
        normalized_chart = DataLayerNormalizer.normalize_chart_data(chart_data)
        extended_chart = DataLayerNormalizer.extend_series_for_markers(normalized_chart)
        
        # Check fallback precedence
        render_type = FallbackHandler.determine_render_type(extended_chart)
        
        cards = []
        if render_type == "chart":
            cards.append(Card("chart", "NVDA - Backtest Analysis", extended_chart.to_dict(), "nvda_backtest"))
        else:
            fallback_card = FallbackHandler.create_fallback_card(
                Card("chart", "NVDA - Backtest Analysis", extended_chart.to_dict(), "nvda_backtest"),
                render_type
            )
            cards.append(fallback_card)
        
        # Add contextual table cards
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
        
        profiler.end("generate_backtest")
        return cards
    
    def _get_mixed_test_optimized(self, inputs: DashboardInputs, cache_key: str) -> List[Card]:
        """Optimized mixed test generation"""
        profiler.start("generate_mixed")
        
        # Reuse optimized generation methods
        best_picks_cards = self._get_best_picks_optimized(inputs, cache_key + "_best")
        compare_cards = self._get_compare_optimized(inputs, cache_key + "_compare")
        backtest_cards = self._get_backtest_optimized(inputs, cache_key + "_backtest")
        
        # Combine all card types
        all_cards = best_picks_cards + compare_cards[:1] + backtest_cards[:2]
        
        profiler.end("generate_mixed")
        return all_cards


# ============================================================================
# OPTIMIZED CHART RENDERER
# ============================================================================

class OptimizedChartRenderer:
    """Optimized chart renderer with performance improvements"""
    
    # Reusable figure templates to avoid rebuilding
    _figure_templates = {}
    
    @classmethod
    def render_chart_card_optimized(cls, card: Card) -> None:
        """Optimized chart rendering with figure reuse"""
        profiler.start("render_chart")
        
        chart_data = ChartData(**card.data)
        
        # Check if we have a cached figure for this data
        data_hash = cls._get_data_hash(chart_data)
        
        if data_hash in cls._figure_templates:
            fig = cls._figure_templates[data_hash]
        else:
            # Create new figure with optimizations
            fig = cls._create_optimized_figure(chart_data)
            cls._figure_templates[data_hash] = fig
        
        cls._render_optimized_chart_container(fig, card.title)
        
        # Render evaluation metrics only for backtest overlay
        if chart_data.mode == ChartMode.BACKTEST_OVERLAY:
            cls._render_evaluation_metrics_optimized(chart_data)
        
        profiler.end("render_chart")
    
    @staticmethod
    def _get_data_hash(chart_data: ChartData) -> str:
        """Generate hash for figure caching"""
        # Create hash from series data and mode
        series_str = str([(p.get("x"), p.get("y"), p.get("kind")) for p in chart_data.series[:10]])  # Sample for performance
        return hashlib.md5(f"{chart_data.mode}_{series_str}".encode()).hexdigest()
    
    @classmethod
    def _create_optimized_figure(cls, chart_data: ChartData) -> go.Figure:
        """Create optimized Plotly figure"""
        profiler.start("create_figure")
        
        # Downsample large series before rendering
        max_points = 100  # Limit for performance
        series = chart_data.series[:max_points] if len(chart_data.series) > max_points else chart_data.series
        
        # Group series by kind efficiently
        series_by_kind = {}
        for point in series:
            kind = point.get("kind")
            if kind not in series_by_kind:
                series_by_kind[kind] = []
            series_by_kind[kind].append(point)
        
        fig = go.Figure()
        
        # Add traces based on mode
        if chart_data.mode == ChartMode.FORECAST:
            cls._add_forecast_traces_optimized(fig, series_by_kind)
        elif chart_data.mode == ChartMode.COMPARISON:
            cls._add_comparison_traces_optimized(fig, series_by_kind)
        elif chart_data.mode == ChartMode.BACKTEST_OVERLAY:
            cls._add_backtest_traces_optimized(fig, series_by_kind, chart_data)
        
        # Optimized layout with minimal overhead
        fig.update_layout(
            title=dict(text=chart_data.title if hasattr(chart_data, 'title') else "", x=0.5, font=dict(size=16)),
            height=350,  # Reduced height
            margin=dict(l=40, r=40, t=40, b=40),  # Reduced margins
            showlegend=True,
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            xaxis_title="Date",
            yaxis_title="Price",
            # Disable animations for performance
            transition_duration=0,
        )
        
        profiler.end("create_figure")
        return fig
    
    @staticmethod
    def _add_forecast_traces_optimized(fig: go.Figure, series_by_kind: Dict) -> None:
        """Add forecast traces with optimizations"""
        if "historical" in series_by_kind:
            points = series_by_kind["historical"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in points],
                    y=[p["y"] for p in points],
                    mode="lines",
                    name="Historical",
                    line=dict(color="#475569", width=2),
                    connectgaps=False,
                    # Simplified hover template for performance
                    hovertemplate="<b>Historical</b><br>Date: %{x}<br>Price: %{y:.2f}"
                )
            )
        
        if "forecast" in series_by_kind:
            forecast_points = series_by_kind["forecast"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in forecast_points],
                    y=[p["y"] for p in forecast_points],
                    mode="lines",
                    name="Forecast",
                    line=dict(color="#2196F3", width=2, dash="dash"),
                    connectgaps=False,
                    hovertemplate="<b>Forecast</b><br>Date: %{x}<br>Price: %{y:.2f}"
                )
            )
    
    @staticmethod
    def _add_comparison_traces_optimized(fig: go.Figure, series_by_kind: Dict) -> None:
        """Add comparison traces with optimizations"""
        colors = ["#475569", "#10B981", "#F59E0B"]
        
        # Group by label efficiently
        series_by_label = {}
        for point in series_by_kind.get("comparison", []):
            label = point.get("label", "default")
            if label not in series_by_label:
                series_by_label[label] = []
            series_by_label[label].append(point)
        
        color_idx = 0
        for label, points in series_by_label.items():
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in points],
                    y=[p["y"] for p in points],
                    mode="lines",
                    name=label,
                    line=dict(color=colors[color_idx % len(colors)], width=2),
                    connectgaps=False,
                    hovertemplate=f"<b>{label}</b><br>Date: %{x}<br>Price: %{y:.2f}"
                )
            )
            color_idx += 1
    
    @staticmethod
    def _add_backtest_traces_optimized(fig: go.Figure, series_by_kind: Dict, chart_data: ChartData) -> None:
        """Add backtest traces with optimizations"""
        # Actual price
        if "actual" in series_by_kind:
            actual_points = series_by_kind["actual"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in actual_points],
                    y=[p["y"] for p in actual_points],
                    mode="lines",
                    name="Actual Price",
                    line=dict(color="#6B7280", width=2),
                    hovertemplate="<b>Actual</b><br>Date: %{x}<br>Price: %{y:.2f}"
                )
            )
        
        # Prediction path
        if "prediction" in series_by_kind:
            pred_points = series_by_kind["prediction"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in pred_points],
                    y=[p["y"] for p in pred_points],
                    mode="lines",
                    name="Prediction",
                    line=dict(color="#2196F3", width=2, dash="dash"),
                    hovertemplate="<b>Prediction</b><br>Date: %{x}<br>Price: %{y:.2f}"
                )
            )
        
        # Entry/exit markers (only if available)
        if chart_data.entry_point:
            fig.add_trace(
                go.Scatter(
                    x=[chart_data.entry_point["x"]],
                    y=[chart_data.entry_point["y"]],
                    mode="markers",
                    name="Entry",
                    marker=dict(color="#10B981", size=10, symbol='triangle-up'),
                    hovertemplate=f"<b>Entry</b><br>Date: {chart_data.entry_point['x']}<br>Price: {chart_data.entry_point['y']:.2f}"
                )
            )
        
        if chart_data.exit_point:
            fig.add_trace(
                go.Scatter(
                    x=[chart_data.exit_point["x"]],
                    y=[chart_data.exit_point["y"]],
                    mode="markers",
                    name="Exit",
                    marker=dict(color="#EF4444", size=10, symbol='triangle-down'),
                    hovertemplate=f"<b>Exit</b><br>Date: {chart_data.exit_point['x']}<br>Price: {chart_data.exit_point['y']:.2f}"
                )
            )
    
    @staticmethod
    def _render_optimized_chart_container(fig: go.Figure, title: str) -> None:
        """Render optimized chart container"""
        # Use config to disable unnecessary features
        config = {
            'displayModeBar': False,
            'displaylogo': False,
            'modeBarButtonsToRemove': ['pan2d', 'lasso2d', 'select2d'],
            'toImageButtonOptions': {
                'format': 'png',
                'filename': f'chart_{title}',
                'height': 500,
                'width': 800,
            }
        }
        
        st.plotly_chart(fig, use_container_width=True, config=config)
    
    @staticmethod
    def _render_evaluation_metrics_optimized(chart_data: ChartData) -> None:
        """Render evaluation metrics with optimizations"""
        # Simple metrics display without complex calculations
        col1, col2, col3 = st.columns(3)
        
        with col1:
            confidence = chart_data.confidence or 0
            st.metric("Confidence", f"{confidence:.1%}")
        
        with col2:
            if chart_data.direction_correct is not None:
                result = "WIN" if chart_data.direction_correct else "LOSS"
                st.metric("Result", result)
            else:
                st.metric("Result", "Pending")
        
        with col3:
            if chart_data.return_pct is not None:
                return_val = chart_data.return_pct
                st.metric("Return", f"{return_val:+.2f}%")
            else:
                st.metric("Return", "Pending")


# ============================================================================
# OPTIMIZED CARD RENDERER
# ============================================================================

class OptimizedCardRenderer:
    """Optimized card renderer with performance improvements"""
    
    @staticmethod
    def render_card_optimized(card: Card) -> None:
        """Optimized card rendering with minimal overhead"""
        profiler.start(f"render_{card.card_type}")
        
        if card.card_type == "chart":
            OptimizedChartRenderer.render_chart_card_optimized(card)
        elif card.card_type == "number":
            OptimizedCardRenderer._render_number_card_optimized(card)
        elif card.card_type == "table":
            OptimizedCardRenderer._render_table_card_optimized(card)
        
        profiler.end(f"render_{card.card_type}")
    
    @staticmethod
    def _render_number_card_optimized(card: Card) -> None:
        """Optimized number card rendering"""
        data = card.data
        primary_value = data.get("primary_value", "No data")
        confidence = data.get("confidence")
        
        # Simple HTML without complex calculations
        confidence_html = ""
        if confidence is not None:
            confidence_width = confidence * 100
            confidence_color = "#10B981" if confidence > 0.7 else "#F59E0B" if confidence > 0.4 else "#EF4444"
            confidence_html = f"""
            <div style="margin-top: 8px;">
                <div style="font-size: 12px; color: #6B7280; margin-bottom: 2px;">
                    Confidence {confidence:.1%}
                </div>
                <div style="width: 100%; height: 3px; background: #E5E7EB; border-radius: 4px;">
                    <div style="width: {confidence_width}%; height: 100%; background: {confidence_color}; border-radius: 4px;"></div>
                </div>
            </div>
            """
        
        st.markdown(f"""
        <div style="background: #F9FAFB; border: 1px solid #E5E7EB; border-radius: 8px; padding: 16px; margin-bottom: 12px;">
            <div style="font-size: 12px; color: #6B7280; margin-bottom: 4px; text-transform: uppercase;">
                {card.title}
            </div>
            <div style="font-size: 24px; font-weight: bold; color: #111827;">
                {primary_value}
            </div>
            {confidence_html}
        </div>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def _render_table_card_optimized(card: Card) -> None:
        """Optimized table card with lazy loading"""
        table_data = TableData(**card.data)
        
        if not table_data.rows:
            st.warning(f"No data available for {card.title}")
            return
        
        # Lazy loading - show limited rows initially
        max_initial_rows = 10
        show_all = st.checkbox(f"Show all rows ({len(table_data.rows)} total)", key=f"show_all_{card.card_id}")
        
        rows_to_show = table_data.rows if show_all else table_data.rows[:max_initial_rows]
        
        # Simple table rendering
        df = pd.DataFrame(rows_to_show, columns=table_data.headers)
        
        # Use st.dataframe with minimal configuration
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            height=min(300, len(rows_to_show) * 35 + 50)  # Dynamic height
        )


# ============================================================================
# FINGERPRINT-BASED REFRESH GATE
# ============================================================================

def get_session_fingerprint(inputs: DashboardInputs) -> str:
    """Generate fingerprint for session state comparison"""
    return hashlib.md5(str(inputs.fingerprint).encode()).hexdigest()


def should_refresh_data(inputs: DashboardInputs) -> bool:
    """Fingerprint-based refresh gate - only refresh when inputs change"""
    current_fingerprint = get_session_fingerprint(inputs)
    stored_fingerprint = st.session_state.get("inputs_fingerprint", "")
    
    # Only refresh if fingerprint changed
    if current_fingerprint != stored_fingerprint:
        st.session_state["inputs_fingerprint"] = current_fingerprint
        st.session_state["last_refresh_time"] = time.time()
        return True
    
    # Optional manual refresh
    return st.session_state.get("force_refresh", False)


# ============================================================================
# OPTIMIZED CONTROLS
# ============================================================================

def render_optimized_controls(service: DashboardService) -> Tuple[DashboardInputs, bool]:
    """Optimized controls with stable widget keys and minimal rerenders"""
    with st.sidebar:
        st.markdown("### Controls")
        
        # Get available tenants once
        tenants = service.list_tenants()
        if not tenants:
            st.warning("No tenants found in DB.")
            return DashboardInputs(), False
        
        # Stable widget keys - avoid conditional widget creation
        tenant = st.selectbox(
            "Tenant",
            options=tenants,
            key="tenant_select"  # Stable key
        )
        
        # Conditional ticker selection with stable key
        ticker = None
        if tenant:
            all_tickers = service.list_tickers(tenant_id=tenant)
            if all_tickers:
                ticker = st.selectbox(
                    "Ticker",
                    options=all_tickers,
                    key=f"ticker_select_{tenant}"  # Stable key with tenant
                )
        
        # View selection with stable key
        view_options = ["best_picks", "dips", "bundles", "compare", "backtest_analysis", "mixed_test"]
        view = st.selectbox(
            "View",
            options=view_options,
            format_func=lambda x: x.replace("_", " ").title(),
            key="view_select"  # Stable key
        )
        
        # Strategy selection with stable key
        strategy = st.selectbox(
            "Strategy",
            options=["house", "semantic", "quant", "comparison"],
            key="strategy_select"  # Stable key
        )
        
        # Horizon selection with stable key
        horizon = st.selectbox(
            "Horizon",
            options=["1D", "1W", "1M", "3M", "6M", "1Y"],
            key="horizon_select"  # Stable key
        )
        
        # Manual refresh with stable key
        force_refresh = st.button("Refresh", key="refresh_button")
        
        inputs = DashboardInputs(tenant, ticker, view, strategy, horizon)
        return inputs, force_refresh


# ============================================================================
# OPTIMIZED CARD RIVER
# ============================================================================

def render_optimized_card_river(cards: List[Card], visible_count: int = 10) -> None:
    """Optimized card river with batch rendering and lazy loading"""
    profiler.start("render_card_river")
    
    if not cards:
        st.info("No cards found for selected criteria.")
        profiler.end("render_card_river")
        return
    
    # Batch render cards in single loop
    for i, card in enumerate(cards[:visible_count]):
        # Simple separator
        if i > 0:
            st.markdown("---")
        
        # Render card with optimized renderer
        OptimizedCardRenderer.render_card_optimized(card)
    
    # Lazy loading for remaining cards
    if len(cards) > visible_count:
        if st.button(f"Show {len(cards) - visible_count} more cards"):
            render_optimized_card_river(cards, visible_count * 2)
    
    profiler.end("render_card_river")


# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main entry point - optimized dashboard"""
    # Apply theme
    apply_theme()
    
    # Initialize optimized service
    service = DashboardService()
    optimized_service = OptimizedDashboardService(service)
    
    # Render optimized controls
    inputs, force_refresh = render_optimized_controls(service)
    
    # Render system status
    render_system_status(inputs)
    
    # Fingerprint-based refresh gate
    should_refresh = force_refresh or should_refresh_data(inputs)
    
    # Fetch data only when needed
    if should_refresh and inputs.tenant:
        cards = optimized_service.fetch_cards(inputs)
    elif inputs.tenant:
        cards = st.session_state.get("cached_cards", [])
    else:
        cards = []
    
    # Cache cards in session state (minimal)
    if cards:
        st.session_state.cached_cards = cards
    
    # Render header
    st.markdown("# Optimized Dashboard")
    st.markdown("*Performance optimized while maintaining architecture*")
    
    # Performance debug info
    if st.sidebar.checkbox("Show Performance"):
        st.markdown("### Performance Metrics")
        summary = profiler.get_summary()
        for operation, time_ms in summary.items():
            st.metric(operation.replace("_", " ").title(), f"{time_ms:.1f}ms")
        
        # Log bottlenecks
        profiler.log_bottlenecks()
        
        # Session state info
        st.markdown("### Session State")
        st.json({
            "inputs_fingerprint": st.session_state.get("inputs_fingerprint"),
            "last_refresh_time": st.session_state.get("last_refresh_time"),
            "cached_cards_count": len(st.session_state.get("cached_cards", []))
        })
    
    # Render optimized card river
    render_optimized_card_river(cards)


if __name__ == "__main__":
    main()
