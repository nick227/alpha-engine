"""
Comparison Tables and Sorting - Phase 7-8 Implementation
Support for sorting, filtering, and contextual tables for comparison cards
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.ui.theme_enhanced import apply_theme, COLORS, TYPOGRAPHY, SPACING
from app.ui.comparison_schema import ComparisonCard, ComparisonSorter, ComparisonFilter


# ============================================================================
# PHASE 7: SORTING AND RANKING
# ============================================================================

class ComparisonSortingControls:
    """Controls for sorting and filtering comparison cards"""
    
    @staticmethod
    def render_sorting_controls(cards: List[ComparisonCard]) -> tuple:
        """Render sorting controls and return sort parameters"""
        st.markdown("### Sort & Filter")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            sort_option = st.selectbox(
                "Sort by",
                options=[
                    "Primary Key (Return/Confidence)",
                    "Confidence",
                    "Return",
                    "Quality Score",
                    "Date (Newest First)"
                ],
                key="comparison_sort_option"
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
                key="comparison_filter_option"
            )
        
        with col3:
            max_results = st.slider(
                "Max Results",
                min_value=5,
                max_value=50,
                value=10,
                step=5,
                key="comparison_max_results"
            )
        
        return sort_option, filter_option, max_results
    
    @staticmethod
    def apply_sorting_and_filtering(cards: List[ComparisonCard], 
                                   sort_option: str, 
                                   filter_option: str,
                                   max_results: int) -> List[ComparisonCard]:
        """Apply sorting and filtering to comparison cards"""
        
        # Apply filtering first
        filtered_cards = ComparisonSortingControls._apply_filter(cards, filter_option)
        
        # Apply sorting
        sorted_cards = ComparisonSortingControls._apply_sort(filtered_cards, sort_option)
        
        # Limit results
        return sorted_cards[:max_results]
    
    @staticmethod
    def _apply_filter(cards: List[ComparisonCard], filter_option: str) -> List[ComparisonCard]:
        """Apply filter to cards"""
        if filter_option == "All":
            return cards
        elif filter_option == "Wins Only":
            return ComparisonFilter.filter_wins(cards)
        elif filter_option == "Losses Only":
            return ComparisonFilter.filter_losses(cards)
        elif filter_option == "High Confidence (>75%)":
            return ComparisonFilter.filter_by_min_confidence(cards, 0.75)
        elif filter_option == "Best Returns (>5%)":
            return ComparisonFilter.filter_by_min_return(cards, 5.0)
        else:
            return cards
    
    @staticmethod
    def _apply_sort(cards: List[ComparisonCard], sort_option: str) -> List[ComparisonCard]:
        """Apply sorting to cards"""
        if sort_option == "Primary Key (Return/Confidence)":
            return ComparisonSorter.sort_by_primary(cards)
        elif sort_option == "Confidence":
            return ComparisonSorter.sort_by_confidence(cards)
        elif sort_option == "Return":
            return ComparisonSorter.sort_by_return(cards)
        elif sort_option == "Quality Score":
            return ComparisonSorter.sort_by_quality(cards)
        elif sort_option == "Date (Newest First)":
            return sorted(
                cards, 
                key=lambda c: c.data.prediction_metrics.timestamp if c.data.prediction_metrics else datetime.min,
                reverse=True
            )
        else:
            return cards


# ============================================================================
# PHASE 8: TABLE SUPPORT
# ============================================================================

class ComparisonTableRenderer:
    """Renders contextual tables for comparison cards"""
    
    @staticmethod
    def render_evidence_table(card: ComparisonCard) -> None:
        """Phase 8: Evidence table for source events and scored events"""
        st.markdown("#### Evidence & Sources")
        
        # Mock evidence data - in real implementation, this would come from RawEvent/ScoredEvent
        evidence_data = ComparisonTableRenderer._get_mock_evidence_data(card)
        
        if not evidence_data:
            st.info("No evidence data available for this prediction.")
            return
        
        # Convert to DataFrame for better display
        df = pd.DataFrame(evidence_data, columns=[
            "Event", "Source", "Timestamp", "Sentiment", "Materiality", "Direction"
        ])
        
        # Style the DataFrame
        styled_df = df.style.set_properties(**{
            'background-color': COLORS['neutral_50'],
            'color': COLORS['neutral_900'],
            'border-color': COLORS['border_light']
        }).set_table_styles([
            {'selector': 'th', 'props': [
                ('background-color', COLORS['neutral_200']),
                ('color', COLORS['neutral_900']),
                ('font-weight', 'bold'),
                ('border-color', COLORS['border_medium'])
            ]}
        ])
        
        st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
    @staticmethod
    def render_outcome_table(card: ComparisonCard) -> None:
        """Phase 8: Outcome table for realized metrics"""
        if not card.data.has_outcome:
            return
        
        st.markdown("#### Trade Outcome")
        
        outcome_data = [
            ["Realized Return", f"{card.data.outcome_metrics.return_pct:+.2f}%"],
            ["Direction Correct", "Yes" if card.data.outcome_metrics.direction_correct else "No"],
            ["Max Runup", f"+{card.data.outcome_metrics.max_runup:.2f}%"],
            ["Max Drawdown", f"-{card.data.outcome_metrics.max_drawdown:.2f}%"],
            ["Exit Reason", card.data.outcome_metrics.exit_reason.replace("_", " ").title()],
        ]
        
        if card.data.outcome_metrics.mra_score:
            outcome_data.append(["Market Reaction Score", f"{card.data.outcome_metrics.mra_score:.2f}"])
        
        # Render as styled metrics
        cols = st.columns(2)
        for i, (metric, value) in enumerate(outcome_data):
            with cols[i % 2]:
                st.metric(metric, value)
    
    @staticmethod
    def render_history_table(card: ComparisonCard) -> None:
        """Phase 8: History table for similar past predictions"""
        st.markdown("#### Similar Past Predictions")
        
        # Mock historical data - in real implementation, query similar predictions
        history_data = ComparisonTableRenderer._get_mock_history_data(card)
        
        if not history_data:
            st.info("No similar historical predictions found.")
            return
        
        # Create comparison table
        df = pd.DataFrame(history_data, columns=[
            "Date", "Ticker", "Strategy", "Direction", "Confidence", "Result", "Return"
        ])
        
        # Color code results
        def color_result(val):
            if val == "WIN":
                return 'background-color: rgba(34, 197, 94, 0.1); color: rgb(34, 197, 94);'
            elif val == "LOSS":
                return 'background-color: rgba(239, 68, 68, 0.1); color: rgb(239, 68, 68);'
            else:
                return ''
        
        def color_return(val):
            if isinstance(val, str) and val.endswith('%'):
                try:
                    return_val = float(val.replace('%', '').replace('+', ''))
                    if return_val > 0:
                        return 'color: rgb(34, 197, 94);'
                    elif return_val < 0:
                        return 'color: rgb(239, 68, 68);'
                except ValueError:
                    pass
            return ''
        
        styled_df = df.style.applymap(color_result, subset=['Result']).applymap(color_return, subset=['Return'])
        
        st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
    @staticmethod
    def render_summary_statistics(cards: List[ComparisonCard]) -> None:
        """Render summary statistics for multiple comparison cards"""
        if not cards:
            return
        
        st.markdown("### Summary Statistics")
        
        # Calculate statistics
        total_trades = len(cards)
        completed_trades = [c for c in cards if c.data.has_outcome]
        wins = [c for c in completed_trades if c.data.is_winner]
        losses = [c for c in completed_trades if not c.data.is_winner]
        
        win_rate = len(wins) / len(completed_trades) if completed_trades else 0
        avg_return = sum(c.data.outcome_metrics.return_pct for c in completed_trades) / len(completed_trades) if completed_trades else 0
        avg_confidence = sum(c.data.prediction_metrics.confidence for c in cards if c.data.prediction_metrics) / len(cards) if cards else 0
        
        # Render statistics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Trades", total_trades)
        
        with col2:
            st.metric("Win Rate", f"{win_rate:.1%}")
        
        with col3:
            st.metric("Avg Return", f"{avg_return:+.2f}%")
        
        with col4:
            st.metric("Avg Confidence", f"{avg_confidence:.1%}")
        
        # Performance distribution
        if completed_trades:
            st.markdown("#### Performance Distribution")
            
            returns = [c.data.outcome_metrics.return_pct for c in completed_trades]
            fig = go.Figure(data=[go.Histogram(x=returns, nbinsx=10)])
            fig.update_layout(
                title="Return Distribution",
                xaxis_title="Return (%)",
                yaxis_title="Count",
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# MOCK DATA PROVIDERS (for testing)
# ============================================================================

    @staticmethod
    def _get_mock_evidence_data(card: ComparisonCard) -> List[List[str]]:
        """Generate mock evidence data for testing"""
        ticker = card.data.ticker or "UNKNOWN"
        
        evidence = [
            ["Earnings Report", "Company Filing", "2024-01-15", "Positive", "High", "Bullish"],
            ["Sector News", "Financial Times", "2024-01-14", "Positive", "Medium", "Bullish"],
            ["Analyst Upgrade", "Morgan Stanley", "2024-01-13", "Positive", "High", "Bullish"],
        ]
        
        # Adjust based on prediction direction
        if card.data.prediction_metrics and card.data.prediction_metrics.direction == "bearish":
            evidence = [
                ["Earnings Warning", "Company Filing", "2024-01-15", "Negative", "High", "Bearish"],
                ["Sector Downgrade", "Bloomberg", "2024-01-14", "Negative", "Medium", "Bearish"],
                ["Analyst Downgrade", "Goldman Sachs", "2024-01-13", "Negative", "High", "Bearish"],
            ]
        
        return evidence
    
    @staticmethod
    def _get_mock_history_data(card: ComparisonCard) -> List[List[str]]:
        """Generate mock historical data for testing"""
        ticker = card.data.ticker or "UNKNOWN"
        strategy = card.data.strategy or "unknown"
        
        history = [
            ["2024-01-08", ticker, strategy, "Bullish", "85%", "WIN", "+8.2%"],
            ["2024-01-05", ticker, strategy, "Bearish", "72%", "LOSS", "-3.1%"],
            ["2024-01-02", ticker, strategy, "Bullish", "91%", "WIN", "+12.4%"],
            ["2023-12-28", ticker, strategy, "Neutral", "65%", "WIN", "+2.1%"],
            ["2023-12-22", ticker, strategy, "Bullish", "78%", "WIN", "+5.7%"],
        ]
        
        return history


# ============================================================================
# DETAILED COMPARISON VIEW
# ============================================================================

class DetailedComparisonView:
    """Detailed view for individual comparison cards with all tables"""
    
    @staticmethod
    def render_detailed_view(card: ComparisonCard) -> None:
        """Render detailed view with chart and all supporting tables"""
        st.markdown(f"# {card.title}")
        
        # Render the main comparison chart
        from app.ui.comparison_renderer import ComparisonChartRenderer
        ComparisonChartRenderer.render_comparison_chart(card)
        
        # Render supporting tables
        st.markdown("---")
        
        # Evidence table
        ComparisonTableRenderer.render_evidence_table(card)
        
        # Outcome table (if available)
        if card.data.has_outcome:
            st.markdown("---")
            ComparisonTableRenderer.render_outcome_table(card)
        
        # History table
        st.markdown("---")
        ComparisonTableRenderer.render_history_table(card)


# ============================================================================
# BATCH COMPARISON VIEW
# ============================================================================

class BatchComparisonView:
    """Batch view for multiple comparison cards with sorting and statistics"""
    
    @staticmethod
    def render_batch_view(cards: List[ComparisonCard]) -> None:
        """Render batch comparison with sorting controls"""
        if not cards:
            st.info("No comparison data available.")
            return
        
        # Render sorting controls
        sort_option, filter_option, max_results = ComparisonSortingControls.render_sorting_controls(cards)
        
        # Apply sorting and filtering
        processed_cards = ComparisonSortingControls.apply_sorting_and_filtering(
            cards, sort_option, filter_option, max_results
        )
        
        # Render summary statistics
        ComparisonTableRenderer.render_summary_statistics(processed_cards)
        
        # Render individual cards
        st.markdown("---")
        st.markdown("### Individual Comparisons")
        
        for i, card in enumerate(processed_cards):
            if i > 0:
                st.markdown("---")
            
            # Render card
            from app.ui.comparison_renderer import ComparisonChartRenderer
            ComparisonChartRenderer.render_comparison_chart(card)
            
            # Add expandable details
            with st.expander(f"View Details for {card.title}"):
                ComparisonTableRenderer.render_evidence_table(card)
                if card.data.has_outcome:
                    ComparisonTableRenderer.render_outcome_table(card)


# ============================================================================
# INTEGRATION HELPERS
# ============================================================================

def render_comparison_analysis(cards: List[ComparisonCard]) -> None:
    """Main entry point for comparison analysis"""
    if not cards:
        st.info("No comparison data available for selected criteria.")
        return
    
    # View mode selection
    view_mode = st.radio(
        "View Mode",
        options=["Batch Analysis", "Detailed View"],
        key="comparison_view_mode"
    )
    
    if view_mode == "Batch Analysis":
        BatchComparisonView.render_batch_view(cards)
    else:
        # Select card for detailed view
        card_options = [card.title for card in cards]
        selected_card = st.selectbox(
            "Select Prediction",
            options=card_options,
            key="detailed_card_select"
        )
        
        # Find selected card
        selected_card_obj = next((c for c in cards if c.title == selected_card), None)
        if selected_card_obj:
            DetailedComparisonView.render_detailed_view(selected_card_obj)
