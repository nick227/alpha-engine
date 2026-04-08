"""
Comparison Chart Renderer - Phase 4-6 Implementation
Renders actual price overlays with prediction paths and evaluation metrics
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.comparison_schema import ComparisonCard, ComparisonCardData, SeriesPoint, TradePoint


# ============================================================================
# PHASE 4: ACTUAL PRICE OVERLAY
# ============================================================================

class ComparisonChartRenderer:
    """Renders comparison charts with actual price and prediction overlays"""
    
    # Canonical color mapping
    COLORS = {
        "actual": COLORS["neutral_800"],
        "prediction": COLORS["primary_600"],
        "entry": COLORS["success_500"],
        "exit": COLORS["error_500"],
        "win": COLORS["success_500"],
        "loss": COLORS["error_500"],
        "neutral": COLORS["neutral_500"],
        "confidence": COLORS["primary_500"],
        "runup": COLORS["success_400"],
        "drawdown": COLORS["error_400"]
    }
    
    @classmethod
    def render_comparison_chart(cls, card: ComparisonCard) -> None:
        """Main rendering method for comparison charts"""
        data = card.data
        
        # Check chart eligibility
        if not cls._is_chart_eligible(data):
            cls._render_fallback_number_card(card)
            return
        
        # Create figure
        fig = go.Figure()
        
        # Phase 4: Add actual price overlay (base layer)
        cls._add_actual_price_series(fig, data.actual_series)
        
        # Phase 4: Add entry/exit markers
        if data.entry_point:
            cls._add_entry_marker(fig, data.entry_point)
        if data.exit_point:
            cls._add_exit_marker(fig, data.exit_point)
        
        # Phase 5: Add prediction overlay
        cls._add_prediction_overlay(fig, data)
        
        # Phase 5: Add confidence annotation
        if data.prediction_metrics:
            cls._add_confidence_annotation(fig, data)
        
        # Update layout
        cls._update_chart_layout(fig, card.title)
        
        # Render chart
        cls._render_chart_container(card.title, fig)
        
        # Phase 6: Add evaluation metrics
        cls._render_evaluation_metrics(data)
    
    @staticmethod
    def _is_chart_eligible(data: ComparisonCardData) -> bool:
        """Check if chart has sufficient data for rendering"""
        return (
            len(data.actual_series) >= 2 and
            data.entry_point is not None
        )
    
    @staticmethod
    def _normalize_timestamps(series: List[SeriesPoint]) -> List[SeriesPoint]:
        """Normalize timestamps for consistent plotting"""
        normalized = []
        for point in series:
            if isinstance(point.x, (datetime, pd.Timestamp)):
                point.x = pd.to_datetime(point.x).isoformat()
            normalized.append(point)
        return normalized
    
    @classmethod
    def _add_actual_price_series(cls, fig: go.Figure, actual_series: List[SeriesPoint]) -> None:
        """Phase 4: Render actual historical price as base line"""
        normalized_series = cls._normalize_timestamps(actual_series)
        
        fig.add_trace(go.Scatter(
            x=[p.x for p in normalized_series],
            y=[p.y for p in normalized_series],
            mode='lines',
            name='Actual Price',
            line=dict(
                color=cls.COLORS["actual"],
                width=2
            ),
            hovertemplate="<b>Actual</b><br>Date: %{x}<br>Price: %{y:.2f}<extra></extra>"
        ))
    
    @classmethod
    def _add_entry_marker(cls, fig: go.Figure, entry_point: TradePoint) -> None:
        """Phase 4: Add entry point marker"""
        entry_time = pd.to_datetime(entry_point.timestamp).isoformat()
        
        fig.add_trace(go.Scatter(
            x=[entry_time],
            y=[entry_point.price],
            mode='markers',
            name='Entry',
            marker=dict(
                color=cls.COLORS["entry"],
                size=12,
                symbol='triangle-up',
                line=dict(color='white', width=2)
            ),
            hovertemplate=f"<b>Entry</b><br>Date: {entry_time}<br>Price: {entry_point.price:.2f}<br>Confidence: {entry_point.confidence:.1%}<extra></extra>"
        ))
    
    @classmethod
    def _add_exit_marker(cls, fig: go.Figure, exit_point: TradePoint) -> None:
        """Phase 4: Add exit point marker"""
        exit_time = pd.to_datetime(exit_point.timestamp).isoformat()
        
        fig.add_trace(go.Scatter(
            x=[exit_time],
            y=[exit_point.price],
            mode='markers',
            name='Exit',
            marker=dict(
                color=cls.COLORS["exit"],
                size=12,
                symbol='triangle-down',
                line=dict(color='white', width=2)
            ),
            hovertemplate=f"<b>Exit</b><br>Date: {exit_time}<br>Price: {exit_point.price:.2f}<br>Reason: {exit_point.reason}<extra></extra>"
        ))


# ============================================================================
# PHASE 5: PREDICTION OVERLAY
# ============================================================================

    @classmethod
    def _add_prediction_overlay(cls, fig: go.Figure, data: ComparisonCardData) -> None:
        """Phase 5: Add prediction overlay (path or direction)"""
        if data.prediction_series and len(data.prediction_series) >= 2:
            # Full prediction path available
            cls._add_prediction_path(fig, data.prediction_series)
        elif data.prediction_direction:
            # Direction-only prediction
            cls._add_direction_indicator(fig, data)
        else:
            # No prediction data available
            pass
    
    @classmethod
    def _add_prediction_path(cls, fig: go.Figure, prediction_series: List[SeriesPoint]) -> None:
        """Add full prediction path overlay"""
        normalized_series = cls._normalize_timestamps(prediction_series)
        
        fig.add_trace(go.Scatter(
            x=[p.x for p in normalized_series],
            y=[p.y for p in normalized_series],
            mode='lines',
            name='Prediction',
            line=dict(
                color=cls.COLORS["prediction"],
                width=2,
                dash='dash'
            ),
            hovertemplate="<b>Prediction</b><br>Date: %{x}<br>Price: %{y:.2f}<extra></extra>"
        ))
    
    @classmethod
    def _add_direction_indicator(cls, fig: go.Figure, data: ComparisonCardData) -> None:
        """Add direction-only indicator (horizontal line or zone)"""
        if not data.entry_point:
            return
        
        entry_price = data.entry_point.price
        entry_time = pd.to_datetime(data.entry_point.timestamp).isoformat()
        
        # Calculate directional offset (5% of entry price)
        offset = entry_price * 0.05
        
        if data.prediction_direction.lower() == "bullish":
            # Bullish prediction - show upward zone
            fig.add_hline(
                y=entry_price + offset,
                line_dash="dash",
                line_color=cls.COLORS["prediction"],
                annotation_text="Bullish Prediction",
                annotation_position="top right"
            )
        elif data.prediction_direction.lower() == "bearish":
            # Bearish prediction - show downward zone
            fig.add_hline(
                y=entry_price - offset,
                line_dash="dash",
                line_color=cls.COLORS["prediction"],
                annotation_text="Bearish Prediction",
                annotation_position="bottom right"
            )
        else:
            # Neutral prediction - show horizontal line
            fig.add_hline(
                y=entry_price,
                line_dash="dash",
                line_color=cls.COLORS["neutral"],
                annotation_text="Neutral Prediction",
                annotation_position="top right"
            )
    
    @classmethod
    def _add_confidence_annotation(cls, fig: go.Figure, data: ComparisonCardData) -> None:
        """Phase 5: Add confidence label/badge to chart"""
        if not data.prediction_metrics or not data.entry_point:
            return
        
        entry_time = pd.to_datetime(data.entry_point.timestamp).isoformat()
        entry_price = data.entry_point.price
        confidence = data.prediction_metrics.confidence
        
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
# PHASE 6: EVALUATION METRICS
# ============================================================================

    @classmethod
    def _render_evaluation_metrics(cls, data: ComparisonCardData) -> None:
        """Phase 6: Add evaluation metrics display"""
        if not data.prediction_metrics:
            return
        
        st.markdown("---")
        st.markdown("### Trade Evaluation")
        
        # Primary metrics row
        col1, col2, col3 = st.columns(3)
        
        with col1:
            cls._render_confidence_metric(data.prediction_metrics.confidence)
        
        with col2:
            if data.has_outcome:
                cls._render_result_metric(data.outcome_metrics.direction_correct)
            else:
                st.metric("Result", "Pending")
        
        with col3:
            if data.has_outcome:
                cls._render_return_metric(data.outcome_metrics.return_pct)
            else:
                st.metric("Return", "Pending")
        
        # Quality indicators row
        if data.has_outcome:
            st.markdown("#### Trade Quality")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                cls._render_runup_metric(data.outcome_metrics.max_runup)
            
            with col2:
                cls._render_drawdown_metric(data.outcome_metrics.max_drawdown)
            
            with col3:
                if data.outcome_metrics.mra_score:
                    cls._render_mra_metric(data.outcome_metrics.mra_score)
                elif data.quality_scores:
                    cls._render_quality_metric(data.quality_scores.composite_quality)
    
    @staticmethod
    def _render_confidence_metric(confidence: float) -> None:
        """Render confidence metric with visual indicator"""
        confidence_color = (
            COLORS["success_500"] if confidence > 0.7 
            else COLORS["warning_500"] if confidence > 0.4 
            else COLORS["error_500"]
        )
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Confidence
            </div>
            <div style="font-size: 24px; font-weight: bold; color: {confidence_color};">
                {confidence:.1%}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def _render_result_metric(direction_correct: bool) -> None:
        """Render win/loss result badge"""
        result_text = "WIN" if direction_correct else "LOSS"
        result_color = COLORS["success_500"] if direction_correct else COLORS["error_500"]
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Result
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
        """Render realized return metric"""
        return_color = COLORS["success_500"] if return_pct > 0 else COLORS["error_500"]
        return_sign = "+" if return_pct > 0 else ""
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Return
            </div>
            <div style="font-size: 24px; font-weight: bold; color: {return_color};">
                {return_sign}{return_pct:.2f}%
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def _render_runup_metric(max_runup: float) -> None:
        """Render max runup metric"""
        st.metric("Max Runup", f"+{max_runup:.2f}%")
    
    @staticmethod
    def _render_drawdown_metric(max_drawdown: float) -> None:
        """Render max drawdown metric"""
        st.metric("Max Drawdown", f"-{max_drawdown:.2f}%")
    
    @staticmethod
    def _render_mra_metric(mra_score: float) -> None:
        """Render market reaction analysis score"""
        st.metric("Market Reaction", f"{mra_score:.2f}")
    
    @staticmethod
    def _render_quality_metric(quality_score: float) -> None:
        """Render composite quality score"""
        quality_color = (
            COLORS["success_500"] if quality_score > 0.5 
            else COLORS["warning_500"] if quality_score > 0 
            else COLORS["error_500"]
        )
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="font-size: 14px; color: {COLORS['neutral_600']}; margin-bottom: 4px;">
                Quality Score
            </div>
            <div style="font-size: 20px; font-weight: bold; color: {quality_color};">
                {quality_score:.2f}
            </div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================================
# CHART LAYOUT AND CONTAINERS
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
    def _render_chart_container(title: str, fig: go.Figure) -> None:
        """Render chart with consistent container styling"""
        st.markdown(f"""
        <div style="
            background: {COLORS['surface']};
            border: 1px solid {COLORS['border_light']};
            border-radius: {SPACING['2']};
            padding: {SPACING['3']};
            margin-bottom: {SPACING['3']};
        ">
            <div style="font-weight: {TYPOGRAPHY['weight_semibold']}; color: {COLORS['neutral_900']}; margin-bottom: {SPACING['2']};">
                {title}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_fallback_number_card(card: ComparisonCard) -> None:
        """Graceful fallback when chart cannot be rendered"""
        data = card.data
        
        st.markdown(f"""
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
            <div style="color: {COLORS['neutral_600']};">
                Insufficient data for chart visualization
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Show available metrics
        if data.prediction_metrics:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Confidence", f"{data.prediction_metrics.confidence:.1%}")
            with col2:
                st.metric("Direction", data.prediction_metrics.direction.title())
        
        if data.has_outcome:
            col1, col2 = st.columns(2)
            with col1:
                result = "WIN" if data.outcome_metrics.direction_correct else "LOSS"
                st.metric("Result", result)
            with col2:
                st.metric("Return", f"{data.outcome_metrics.return_pct:+.2f}%")


# ============================================================================
# INTEGRATION WITH EXISTING CARD RENDERER
# ============================================================================

class EnhancedCardRenderer:
    """Enhanced card renderer that supports comparison charts"""
    
    @staticmethod
    def render_card(card) -> None:
        """Render card based on type - enhanced to support comparison charts"""
        if hasattr(card, 'card_type') and card.card_type == "comparison_chart":
            ComparisonChartRenderer.render_comparison_chart(card)
        elif hasattr(card, 'card_type') and card.card_type == "chart":
            # Original chart rendering
            from app.ui.card_dashboard_locked import CardRenderer
            CardRenderer.render_chart_card(card)
        elif hasattr(card, 'card_type') and card.card_type == "number":
            from app.ui.card_dashboard_locked import CardRenderer
            CardRenderer.render_number_card(card)
        elif hasattr(card, 'card_type') and card.card_type == "table":
            from app.ui.card_dashboard_locked import CardRenderer
            CardRenderer.render_table_card(card)
        else:
            st.error(f"Unknown card type: {getattr(card, 'card_type', 'unknown')}")
