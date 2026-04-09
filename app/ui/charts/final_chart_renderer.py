"""
Final Chart Renderer - Locked Architecture
Canonical renderer for all chart modes with explicit separation of concerns
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from app.ui.theme import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.charts.chart_schema_final import (
    Card, ChartMode, ChartData, NumberData, TableData,
    DataLayerNormalizer, FallbackHandler
)


# ============================================================================
# FINAL CHART RENDERER - CANONICAL SHAPE, MODE DISPATCH
# ============================================================================

class FinalChartRenderer:
    """Canonical chart renderer - one shape for all modes, mode dispatch only"""
    
    # Canonical color mapping - renderer responsibility only
    COLORS = {
        "actual": COLORS["neutral_800"],
        "prediction": COLORS["primary_600"],
        "forecast": COLORS["primary_600"],
        "comparison": [COLORS["primary_800"], COLORS["success_500"], COLORS["warning_500"]],
        "entry": COLORS["success_500"],
        "exit": COLORS["error_500"],
        "win": COLORS["success_500"],
        "loss": COLORS["error_500"],
        "neutral": COLORS["neutral_500"],
        "confidence": COLORS["primary_500"],
        "historical": COLORS["neutral_700"],
        "bundle": COLORS["primary_800"],
        "constituent": COLORS["neutral_400"]
    }
    
    @classmethod
    def render_chart_card(cls, card: Card) -> None:
        """Main chart renderer - canonical shape, mode dispatch only"""
        # Data layer already normalized timestamps and validated minimum series
        chart_data = ChartData(**card.data)
        
        # Apply fallback precedence (data layer already validated)
        render_type = FallbackHandler.determine_render_type(chart_data)
        
        if render_type == "chart":
            # Dispatch by mode only - no payload branches
            if chart_data.mode == ChartMode.FORECAST:
                cls._render_forecast_mode(chart_data, card.title)
            elif chart_data.mode == ChartMode.COMPARISON:
                cls._render_comparison_mode(chart_data, card.title)
            elif chart_data.mode == ChartMode.BACKTEST_OVERLAY:
                cls._render_backtest_overlay_mode(chart_data, card.title)
            else:
                st.error(f"Unknown chart mode: {chart_data.mode}")
        elif render_type == "number":
            cls._render_number_fallback(card)
        elif render_type == "empty":
            cls._render_empty_state(card)


# ============================================================================
# MODE RENDERERS - CANONICAL SHAPE, NO PAYLOAD BRANCHES
# ============================================================================

    @classmethod
    def _render_forecast_mode(cls, chart_data: ChartData, title: str) -> None:
        """Render forecast mode - canonical shape"""
        # Group series by kind (canonical shape)
        series_by_kind = {}
        for point in chart_data.series:
            kind = point.get("kind")
            if kind not in series_by_kind:
                series_by_kind[kind] = []
            series_by_kind[kind].append(point)
        
        fig = go.Figure()
        
        # Historical - solid line (renderer color/style responsibility)
        if "historical" in series_by_kind:
            points = series_by_kind["historical"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in points],
                    y=[p["y"] for p in points],
                    mode="lines",
                    name="Historical",
                    line=dict(color=cls.COLORS["historical"], width=2),
                    connectgaps=False,
                    hovertemplate="<b>Historical</b><br>Date: %{x}<br>Price: %{y:.2f}<extra></extra>"
                )
            )
        
        # Forecast - dashed line (renderer color/style responsibility)
        if "forecast" in series_by_kind:
            forecast_points = series_by_kind["forecast"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in forecast_points],
                    y=[p["y"] for p in forecast_points],
                    mode="lines",
                    name="Forecast",
                    line=dict(color=cls.COLORS["forecast"], width=2, dash="dash"),
                    connectgaps=False,
                    hovertemplate="<b>Forecast</b><br>Date: %{x}<br>Price: %{y:.2f}<extra></extra>"
                )
            )
        
        # Confidence bands (renderer color/style responsibility)
        if "confidence_upper" in series_by_kind and "confidence_lower" in series_by_kind:
            upper_points = series_by_kind["confidence_upper"]
            lower_points = series_by_kind["confidence_lower"]
            
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
        
        cls._update_chart_layout(fig, title)
        cls._render_chart_container(fig)
    
    @classmethod
    def _render_comparison_mode(cls, chart_data: ChartData, title: str) -> None:
        """Render comparison mode - canonical shape"""
        # Group series by label (canonical shape)
        series_by_label = {}
        for point in chart_data.series:
            label = point.get("label", "default")
            if label not in series_by_label:
                series_by_label[label] = []
            series_by_label[label].append(point)
        
        fig = go.Figure()
        
        # Comparison lines (renderer color/style responsibility)
        comparison_colors = cls.COLORS["comparison"]
        color_index = 0
        
        for label, points in series_by_label.items():
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in points],
                    y=[p["y"] for p in points],
                    mode="lines",
                    name=label,
                    line=dict(
                        color=comparison_colors[color_index % len(comparison_colors)],
                        width=2,
                    ),
                    connectgaps=False,
                    hovertemplate=f"<b>{label}</b><br>Date: %{x}<br>Price: %{y:.2f}<extra></extra>"
                )
            )
            color_index += 1
        
        cls._update_chart_layout(fig, title)
        cls._render_chart_container(fig)
    
    @classmethod
    def _render_backtest_overlay_mode(cls, chart_data: ChartData, title: str) -> None:
        """Render backtest overlay mode - canonical shape"""
        # Group series by kind (canonical shape)
        series_by_kind = {}
        for point in chart_data.series:
            kind = point.get("kind")
            if kind not in series_by_kind:
                series_by_kind[kind] = []
            series_by_kind[kind].append(point)
        
        fig = go.Figure()
        
        # Actual price - base layer (renderer color/style responsibility)
        if "actual" in series_by_kind:
            actual_points = series_by_kind["actual"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in actual_points],
                    y=[p["y"] for p in actual_points],
                    mode="lines",
                    name="Actual Price",
                    line=dict(color=cls.COLORS["actual"], width=2),
                    hovertemplate="<b>Actual</b><br>Date: %{x}<br>Price: %{y:.2f}<extra></extra>"
                )
            )
        
        # Prediction path (renderer color/style responsibility)
        if "prediction" in series_by_kind:
            pred_points = series_by_kind["prediction"]
            fig.add_trace(
                go.Scatter(
                    x=[p["x"] for p in pred_points],
                    y=[p["y"] for p in pred_points],
                    mode="lines",
                    name="Prediction",
                    line=dict(color=cls.COLORS["prediction"], width=2, dash="dash"),
                    hovertemplate="<b>Prediction</b><br>Date: %{x}<br>Price: %{y:.2f}<extra></extra>"
                )
            )
        
        # Entry/exit markers (renderer color/style responsibility)
        if chart_data.entry_point:
            cls._add_entry_marker(fig, chart_data.entry_point)
        
        if chart_data.exit_point:
            cls._add_exit_marker(fig, chart_data.exit_point)
        
        # Direction indicator (if no prediction path)
        if chart_data.is_direction_only:
            cls._add_direction_indicator(fig, chart_data)
        
        # Confidence annotation (renderer color/style responsibility)
        if chart_data.confidence is not None:
            cls._add_confidence_annotation(fig, chart_data)
        
        cls._update_chart_layout(fig, title)
        cls._render_chart_container(fig)
        
        # Render evaluation metrics (summary fields second, series first)
        cls._render_evaluation_metrics(chart_data)


# ============================================================================
# MARKER AND ANNOTATION RENDERING - RENDERER RESPONSIBILITY
# ============================================================================

    @classmethod
    def _add_entry_marker(cls, fig: go.Figure, entry_point: Dict) -> None:
        """Add entry marker - renderer color/style responsibility"""
        fig.add_trace(
            go.Scatter(
                x=[entry_point["x"]],
                y=[entry_point["y"]],
                mode="markers",
                name="Entry",
                marker=dict(
                    color=cls.COLORS["entry"],
                    size=12,
                    symbol='triangle-up',
                    line=dict(color='white', width=2)
                ),
                hovertemplate=f"<b>Entry</b><br>Date: {entry_point['x']}<br>Price: {entry_point['y']:.2f}<extra></extra>"
            )
        )
    
    @classmethod
    def _add_exit_marker(cls, fig: go.Figure, exit_point: Dict) -> None:
        """Add exit marker - renderer color/style responsibility"""
        fig.add_trace(
            go.Scatter(
                x=[exit_point["x"]],
                y=[exit_point["y"]],
                mode="markers",
                name="Exit",
                marker=dict(
                    color=cls.COLORS["exit"],
                    size=12,
                    symbol='triangle-down',
                    line=dict(color='white', width=2)
                ),
                hovertemplate=f"<b>Exit</b><br>Date: {exit_point['x']}<br>Price: {exit_point['y']:.2f}<extra></extra>"
            )
        )
    
    @classmethod
    def _add_direction_indicator(cls, fig: go.Figure, chart_data: ChartData) -> None:
        """Add direction indicator - renderer color/style responsibility"""
        if not chart_data.entry_point or not chart_data.prediction_direction:
            return
        
        entry_price = chart_data.entry_point["y"]
        direction = chart_data.prediction_direction.lower()
        
        # Calculate directional offset
        offset = entry_price * 0.05
        
        if direction == "bullish":
            fig.add_hline(
                y=entry_price + offset,
                line_dash="dash",
                line_color=cls.COLORS["prediction"],
                annotation_text="Bullish Prediction",
                annotation_position="top right"
            )
        elif direction == "bearish":
            fig.add_hline(
                y=entry_price - offset,
                line_dash="dash",
                line_color=cls.COLORS["prediction"],
                annotation_text="Bearish Prediction",
                annotation_position="bottom right"
            )
        else:
            # Neutral prediction
            fig.add_hline(
                y=entry_price,
                line_dash="dash",
                line_color=cls.COLORS["neutral"],
                annotation_text="Neutral Prediction",
                annotation_position="top right"
            )
    
    @classmethod
    def _add_confidence_annotation(cls, fig: go.Figure, chart_data: ChartData) -> None:
        """Add confidence annotation - renderer color/style responsibility"""
        if not chart_data.entry_point or chart_data.confidence is None:
            return
        
        entry_time = chart_data.entry_point["x"]
        entry_price = chart_data.entry_point["y"]
        confidence = chart_data.confidence  # Raw, untouched
        
        # Position annotation
        annotation_y = entry_price * 1.1
        
        fig.add_annotation(
            x=entry_time,
            y=annotation_y,
            text=f"Confidence: {confidence:.1%}",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor=cls.COLORS["confidence"],
            bgcolor=COLORS["primary_100"],
            bordercolor=cls.COLORS["confidence"],
            borderwidth=1,
            font=dict(size=12, color=COLORS["neutral_900"])
        )


# ============================================================================
# EVALUATION METRICS - SUMMARY FIELDS SECONDARY
# ============================================================================

    @classmethod
    def _render_evaluation_metrics(cls, chart_data: ChartData) -> None:
        """Render evaluation metrics - summary fields secondary"""
        st.markdown("---")
        st.markdown("### Trade Evaluation")
        
        # Primary metrics row
        col1, col2, col3 = st.columns(3)
        
        with col1:
            cls._render_confidence_metric(chart_data.confidence)
        
        with col2:
            cls._render_result_metric(chart_data.direction_correct, chart_data.return_pct)
        
        with col3:
            cls._render_return_metric(chart_data.return_pct)
        
        # Quality indicators row (only if outcome available)
        if chart_data.has_outcome:
            st.markdown("#### Trade Quality")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                cls._render_runup_metric(chart_data.max_runup)
            
            with col2:
                cls._render_drawdown_metric(chart_data.max_drawdown)
            
            with col3:
                # Quality score is derived, not used in sorting by default
                quality_score = cls._calculate_derived_quality(chart_data)
                cls._render_quality_metric(quality_score)
    
    @staticmethod
    def _calculate_derived_quality(chart_data: ChartData) -> float:
        """Calculate derived quality score (not used in sorting by default)"""
        if not chart_data.has_outcome:
            return 0.0
        
        return_pct = chart_data.return_pct or 0
        confidence = chart_data.confidence or 0
        max_drawdown = chart_data.max_drawdown or 0
        
        # Simple composite: return weighted by confidence, penalized by drawdown
        quality = (return_pct * confidence) - (max_drawdown * 0.5)
        return quality
    
    @staticmethod
    def _render_confidence_metric(confidence: Optional[float]) -> None:
        """Render confidence metric - raw Prediction.confidence"""
        if confidence is None:
            st.metric("Confidence", "Pending")
            return
        
        confidence_color = (
            COLORS["success_500"] if confidence > 0.7 
            else COLORS["warning_500"] if confidence > 0.4 
            else COLORS["error_500"]
        )
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Confidence (Pre-outcome)
            </div>
            <div style="font-size: 24px; font-weight: bold; color: {confidence_color};">
                {confidence:.1%}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def _render_result_metric(direction_correct: Optional[bool], return_pct: Optional[float]) -> None:
        """Render result metric - direction_correct secondary to return"""
        if direction_correct is None:
            st.metric("Result", "Pending")
            return
        
        # Show return prominently, direction_correct secondary
        return_color = COLORS["success_500"] if (return_pct and return_pct > 0) else COLORS["error_500"]
        return_sign = "+" if (return_pct and return_pct > 0) else ""
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Result (Return + Direction)
            </div>
            <div style="font-size: 24px; font-weight: bold; color: {return_color};">
                {return_sign}{return_pct:.2f}%
            </div>
            <div style="font-size: 12px; color: {COLORS['neutral_500']}; margin-top: 2px;">
                {'Correct' if direction_correct else 'Incorrect'}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def _render_return_metric(return_pct: Optional[float]) -> None:
        """Render return metric - from PredictionOutcome.return_pct"""
        if return_pct is None:
            st.metric("Return", "Pending")
            return
        
        return_color = COLORS["success_500"] if return_pct > 0 else COLORS["error_500"]
        return_sign = "+" if return_pct > 0 else ""
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Realized Return
            </div>
            <div style="font-size: 24px; font-weight: bold; color: {return_color};">
                {return_sign}{return_pct:.2f}%
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def _render_runup_metric(max_runup: Optional[float]) -> None:
        """Render max runup metric"""
        if max_runup is not None:
            st.metric("Max Runup", f"+{max_runup:.2f}%")
    
    @staticmethod
    def _render_drawdown_metric(max_drawdown: Optional[float]) -> None:
        """Render max drawdown metric"""
        if max_drawdown is not None:
            st.metric("Max Drawdown", f"-{max_drawdown:.2f}%")
    
    @staticmethod
    def _render_quality_metric(quality_score: float) -> None:
        """Render derived quality metric (secondary)"""
        quality_color = (
            COLORS["success_500"] if quality_score > 5 
            else COLORS["warning_500"] if quality_score > 0 
            else COLORS["error_500"]
        )
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Quality Score (Derived)
            </div>
            <div style="font-size: 20px; font-weight: bold; color: {quality_color};">
                {quality_score:.2f}
            </div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================================
# FALLBACK RENDERING - EXPLICIT PRECEDENCE
# ============================================================================

    @classmethod
    def _render_number_fallback(cls, card: Card) -> None:
        """Render number fallback - explicit precedence"""
        chart_data = ChartData(**card.data)
        
        # Extract available metrics
        primary_value = "No data"
        subtitle = "Insufficient series"
        
        if chart_data.return_pct is not None:
            primary_value = f"{chart_data.return_pct:+.2f}%"
            subtitle = "Realized Return"
        elif chart_data.confidence is not None:
            primary_value = f"{chart_data.confidence:.1%}"
            subtitle = "Confidence (Pending)"
        
        # Render number card
        st.markdown(f"""
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
            <div style="font-size: {TYPOGRAPHY['text_lg']}; font-weight: {TYPOGRAPHY['weight_bold']}; color: {COLORS['neutral_900']};">
                {primary_value}
            </div>
            <div style="font-size: {TYPOGRAPHY['text_sm']}; color: {COLORS['neutral_600']}; margin-top: 2px;">
                {subtitle}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    @classmethod
    def _render_empty_state(cls, card: Card) -> None:
        """Render empty state - explicit precedence"""
        st.markdown(f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['2']};
            padding: {SPACING['4']};
            margin-bottom: {SPACING['3']};
            text-align: center;
        ">
            <div style="font-size: {TYPOGRAPHY['text_lg']}; color: {COLORS['neutral_600']}; margin-bottom: {SPACING['2']};">
                No data available
            </div>
            <div style="font-size: {TYPOGRAPHY['text_sm']}; color: {COLORS['neutral_500']};">
                {card.title}
            </div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================================
# CHART LAYOUT AND CONTAINERS - RENDERER RESPONSIBILITY
# ============================================================================

    @classmethod
    def _update_chart_layout(cls, fig: go.Figure, title: str) -> None:
        """Update chart layout - renderer color/style responsibility"""
        fig.update_layout(
            title=dict(text=title, x=0.5, font=dict(size=16)),
            height=400,
            margin=dict(l=50, r=50, t=60, b=50),
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
            yaxis_title="Price"
        )
    
    @staticmethod
    def _render_chart_container(fig: go.Figure) -> None:
        """Render chart container - renderer responsibility"""
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# UNIFIED CARD RENDERER - FINAL PIPELINE
# ============================================================================

class FinalCardRenderer:
    """Final card renderer - unified pipeline for all card types"""
    
    @staticmethod
    def render_card(card: Card) -> None:
        """Render card using unified pipeline"""
        if card.card_type == "chart":
            # Chart renderer handles modes internally
            FinalChartRenderer.render_chart_card(card)
        elif card.card_type == "number":
            FinalCardRenderer._render_number_card(card)
        elif card.card_type == "table":
            FinalCardRenderer._render_table_card(card)
        else:
            st.error(f"Unknown card type: {card.card_type}")
    
    @staticmethod
    def _render_number_card(card: Card) -> None:
        """Render number card"""
        number_data = NumberData(**card.data)
        
        confidence_html = ""
        if number_data.confidence is not None:
            confidence_width = number_data.confidence * 100
            confidence_color = (
                COLORS["success_500"] if number_data.confidence > 0.7
                else COLORS["neutral_500"] if number_data.confidence > 0.4 else COLORS["error_500"]
            )
            confidence_html = f"""
            <div style="margin-top: {SPACING['2']};">
                <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']}; margin-bottom: 2px;">
                    Confidence {number_data.confidence:.1%}
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
                {number_data.primary_value}
            </div>
            {confidence_html}
            {f'<div style="font-size: {TYPOGRAPHY["text_sm"]}; color: {COLORS["neutral_600"]}; margin-top: 2px;">{number_data.subtitle}</div>' if number_data.subtitle else ''}
        </div>
        """,
            unsafe_allow_html=True,
        )
    
    @staticmethod
    def _render_table_card(card: Card) -> None:
        """Render table card - contextual supporting data only"""
        table_data = TableData(**card.data)
        
        if not table_data.rows:
            st.warning(f"No data available for {card.title}")
            return
        
        # Minimal table HTML
        header_html = ""
        for header in table_data.headers:
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
        for i, row in enumerate(table_data.rows):
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
