"""
Standard Chart Renderer - Minimal Schema with Chart Modes
Renders forecast, comparison, and backtest_overlay using same chart card API
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.chart_modes import ChartMode, ChartOverlayData, Card


# ============================================================================
# STANDARD CHART RENDERER - MODES WITHIN CHART TYPE
# ============================================================================

class StandardChartRenderer:
    """Renders all chart modes using the same minimal chart card schema"""
    
    # Canonical color mapping
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
        """Main chart renderer - dispatches by mode"""
        if not card.data or "series" not in card.data:
            cls._downgrade_to_number_card(card)
            return
        
        # Check if series has sufficient data
        series = card.data.get("series", [])
        if len(series) < 2:
            cls._downgrade_to_number_card(card)
            return
        
        # Dispatch by mode
        mode = card.data.get("mode", ChartMode.FORECAST)
        
        if mode == ChartMode.FORECAST:
            cls._render_forecast_chart(card)
        elif mode == ChartMode.COMPARISON:
            cls._render_comparison_chart(card)
        elif mode == ChartMode.BACKTEST_OVERLAY:
            cls._render_backtest_overlay_chart(card)
        else:
            st.error(f"Unknown chart mode: {mode}")
    
    @staticmethod
    def _normalize_timestamps(series: List[Dict]) -> List[Dict]:
        """Normalize timestamps for consistent plotting"""
        normalized = []
        for point in series:
            if "x" in point and isinstance(point["x"], (datetime, pd.Timestamp)):
                point["x"] = pd.to_datetime(point["x"]).isoformat()
            normalized.append(point)
        return normalized


# ============================================================================
# FORECAST MODE - ORIGINAL BEHAVIOR
# ============================================================================

    @classmethod
    def _render_forecast_chart(cls, card: Card) -> None:
        """Render standard forecast chart (original behavior)"""
        series = cls._normalize_timestamps(card.data.get("series", []))
        
        # Group series by kind
        series_by_kind = {}
        for point in series:
            kind = point.get("kind")
            if kind not in series_by_kind:
                series_by_kind[kind] = []
            series_by_kind[kind].append(point)
        
        fig = go.Figure()
        
        # Historical - solid line
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
                    line=dict(color=cls.COLORS["forecast"], width=2, dash="dash"),
                    connectgaps=False,
                )
            )
        
        # Confidence bands
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
        
        cls._update_chart_layout(fig, card.title)
        cls._render_chart_container(fig)


# ============================================================================
# COMPARISON MODE - ORIGINAL BEHAVIOR
# ============================================================================

    @classmethod
    def _render_comparison_chart(cls, card: Card) -> None:
        """Render comparison chart (original behavior)"""
        series = cls._normalize_timestamps(card.data.get("series", []))
        
        # Group series by kind and label
        series_by_label = {}
        for point in series:
            label = point.get("label", "default")
            if label not in series_by_label:
                series_by_label[label] = []
            series_by_label[label].append(point)
        
        fig = go.Figure()
        
        # Comparison lines
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
                )
            )
            color_index += 1
        
        cls._update_chart_layout(fig, card.title)
        cls._render_chart_container(fig)


# ============================================================================
# BACKTEST OVERLAY MODE - NEW ENHANCEMENT
# ============================================================================

    @classmethod
    def _render_backtest_overlay_chart(cls, card: Card) -> None:
        """Render backtest overlay chart (new enhancement)"""
        series = cls._normalize_timestamps(card.data.get("series", []))
        
        # Group series by kind
        series_by_kind = {}
        for point in series:
            kind = point.get("kind")
            if kind not in series_by_kind:
                series_by_kind[kind] = []
            series_by_kind[kind].append(point)
        
        fig = go.Figure()
        
        # Actual price - base layer
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
        
        # Prediction path (if available)
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
        
        # Entry point marker
        entry_point = card.data.get("entry_point")
        if entry_point:
            entry_time = pd.to_datetime(entry_point["x"]).isoformat()
            fig.add_trace(
                go.Scatter(
                    x=[entry_time],
                    y=[entry_point["y"]],
                    mode="markers",
                    name="Entry",
                    marker=dict(
                        color=cls.COLORS["entry"],
                        size=12,
                        symbol='triangle-up',
                        line=dict(color='white', width=2)
                    ),
                    hovertemplate=f"<b>Entry</b><br>Date: {entry_time}<br>Price: {entry_point['y']:.2f}<br>Confidence: {entry_point.get('confidence', 0):.1%}<extra></extra>"
                )
            )
        
        # Exit point marker
        exit_point = card.data.get("exit_point")
        if exit_point:
            exit_time = pd.to_datetime(exit_point["x"]).isoformat()
            fig.add_trace(
                go.Scatter(
                    x=[exit_time],
                    y=[exit_point["y"]],
                    mode="markers",
                    name="Exit",
                    marker=dict(
                        color=cls.COLORS["exit"],
                        size=12,
                        symbol='triangle-down',
                        line=dict(color='white', width=2)
                    ),
                    hovertemplate=f"<b>Exit</b><br>Date: {exit_time}<br>Price: {exit_point['y']:.2f}<extra></extra>"
                )
            )
        
        # Direction indicator (if no prediction path)
        prediction_direction = card.data.get("prediction_direction")
        if prediction_direction and "prediction" not in series_by_kind:
            cls._add_direction_indicator(fig, entry_point, prediction_direction)
        
        # Confidence annotation
        confidence = card.data.get("confidence")
        if confidence and entry_point:
            cls._add_confidence_annotation(fig, entry_point, confidence)
        
        cls._update_chart_layout(fig, card.title)
        cls._render_chart_container(fig)
        
        # Render evaluation metrics
        cls._render_evaluation_metrics(card.data)


# ============================================================================
# OVERLAY SPECIFIC RENDERING
# ============================================================================

    @classmethod
    def _add_direction_indicator(cls, fig: go.Figure, entry_point: Dict, direction: str) -> None:
        """Add direction-only indicator (horizontal line or zone)"""
        if not entry_point:
            return
        
        entry_price = entry_point["y"]
        entry_time = pd.to_datetime(entry_point["x"]).isoformat()
        
        # Calculate directional offset (5% of entry price)
        offset = entry_price * 0.05
        
        if direction.lower() == "bullish":
            fig.add_hline(
                y=entry_price + offset,
                line_dash="dash",
                line_color=cls.COLORS["prediction"],
                annotation_text="Bullish Prediction",
                annotation_position="top right"
            )
        elif direction.lower() == "bearish":
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
    def _add_confidence_annotation(cls, fig: go.Figure, entry_point: Dict, confidence: float) -> None:
        """Add confidence annotation to chart"""
        entry_time = pd.to_datetime(entry_point["x"]).isoformat()
        entry_price = entry_point["y"]
        
        # Position annotation above entry point
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
# EVALUATION METRICS - OVERLAY MODE ONLY
# ============================================================================

    @classmethod
    def _render_evaluation_metrics(cls, data: Dict) -> None:
        """Render evaluation metrics for backtest overlay"""
        st.markdown("---")
        st.markdown("### Trade Evaluation")
        
        # Primary metrics row
        col1, col2, col3 = st.columns(3)
        
        with col1:
            cls._render_confidence_metric(data.get("confidence"))
        
        with col2:
            cls._render_result_metric(data.get("direction_correct"))
        
        with col3:
            cls._render_return_metric(data.get("return_pct"))
        
        # Quality indicators row (only if outcome available)
        if data.get("direction_correct") is not None:
            st.markdown("#### Trade Quality")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                cls._render_runup_metric(data.get("max_runup"))
            
            with col2:
                cls._render_drawdown_metric(data.get("max_drawdown"))
            
            with col3:
                # Quality score is derived, not canonical
                if data.get("return_pct") is not None:
                    quality_score = cls._calculate_derived_quality(data)
                    cls._render_quality_metric(quality_score)
    
    @staticmethod
    def _calculate_derived_quality(data: Dict) -> float:
        """Calculate derived quality score (secondary metric)"""
        return_pct = data.get("return_pct", 0)
        confidence = data.get("confidence", 0)
        max_drawdown = data.get("max_drawdown", 0)
        
        # Simple composite: return weighted by confidence, penalized by drawdown
        quality = (return_pct * confidence) - (max_drawdown * 0.5)
        return quality
    
    @staticmethod
    def _render_confidence_metric(confidence: float) -> None:
        """Render confidence metric (from Prediction.confidence only)"""
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
    def _render_result_metric(direction_correct: bool) -> None:
        """Render result metric (from PredictionOutcome.direction_correct only)"""
        if direction_correct is None:
            st.metric("Result", "Pending")
            return
        
        result_text = "WIN" if direction_correct else "LOSS"
        result_color = COLORS["success_500"] if direction_correct else COLORS["error_500"]
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Result (Post-outcome)
            </div>
            <div style="
                background: {result_color}; 
                color: white; 
                padding: 4px 12px; 
                border-radius: 4px; 
                font-weight: bold;
                display: inline-block;
            ">
                {result_text}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def _render_return_metric(return_pct: float) -> None:
        """Render return metric (from PredictionOutcome.return_pct only)"""
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
    def _render_runup_metric(max_runup: float) -> None:
        """Render max runup metric"""
        if max_runup is not None:
            st.metric("Max Runup", f"+{max_runup:.2f}%")
    
    @staticmethod
    def _render_drawdown_metric(max_drawdown: float) -> None:
        """Render max drawdown metric"""
        if max_drawdown is not None:
            st.metric("Max Drawdown", f"-{max_drawdown:.2f}%")
    
    @staticmethod
    def _render_quality_metric(quality_score: float) -> None:
        """Render derived quality score (secondary metric)"""
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
# CHART LAYOUT AND FALLBACKS
# ============================================================================

    @classmethod
    def _update_chart_layout(cls, fig: go.Figure, title: str) -> None:
        """Update chart layout with consistent styling"""
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
        """Render chart with consistent container styling"""
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _downgrade_to_number_card(card: Card) -> None:
        """Graceful downgrade to number card when chart cannot be rendered"""
        # Extract available metrics
        data = card.data
        mode = data.get("mode", ChartMode.FORECAST)
        
        if mode == ChartMode.BACKTEST_OVERLAY:
            # Show backtest metrics as number card
            confidence = data.get("confidence", 0)
            direction_correct = data.get("direction_correct")
            return_pct = data.get("return_pct")
            
            # Primary value based on outcome availability
            if return_pct is not None:
                primary_value = f"{return_pct:+.2f}%"
                subtitle = "Realized Return" if direction_correct else "Loss"
            else:
                primary_value = f"{confidence:.1%}"
                subtitle = "Confidence (Pending)"
            
            # Render as number card
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
        else:
            # Standard fallback for other modes
            st.markdown(f"""
            <div style="
                background: {COLORS['surface']};
                border: 1px solid {COLORS['border_light']};
                border-radius: {SPACING['2']};
                padding: {SPACING['3']};
                margin-bottom: {SPACING['3']};
            ">
                <div style="font-size: {TYPOGRAPHY['text_sm']}; color: {COLORS['neutral_600']};">
                    Insufficient data for {mode} chart
                </div>
            </div>
            """, unsafe_allow_html=True)
