"""
Correlation Dashboard - Enhanced visualization for temporal correlation analysis.

Creates comprehensive dashboards showing correlations between strategy performance
and external time-based indicators like headlines, economic events, and market data.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))

from temporal_correlation_analyzer import TemporalCorrelationAnalyzer


# Set style
plt.style.use('default')
sns.set_palette("husl")


class CorrelationDashboard:
    """
    Enhanced dashboard for temporal correlation analysis.
    """
    
    def __init__(self, correlation_analyzer: TemporalCorrelationAnalyzer):
        self.analyzer = correlation_analyzer
        self.report = None
        
    def create_comprehensive_correlation_dashboard(self, save_path: str = None) -> None:
        """Create comprehensive correlation analysis dashboard."""
        
        # Generate report if not already done
        if self.report is None:
            self.report = self.analyzer.generate_comprehensive_correlation_report()
        
        if 'error' in self.report:
            print(f"Error generating report: {self.report['error']}")
            return
        
        # Create figure with subplots
        fig = plt.figure(figsize=(24, 32))
        fig.suptitle('Temporal Correlation Analysis Dashboard', fontsize=18, fontweight='bold')
        
        # 1. Performance vs Sentiment Correlation
        ax1 = plt.subplot(5, 4, 1)
        self._plot_sentiment_correlation(ax1)
        
        # 2. Economic Event Impact
        ax2 = plt.subplot(5, 4, 2)
        self._plot_economic_event_impact(ax2)
        
        # 3. Market Indicator Correlations
        ax3 = plt.subplot(5, 4, 3)
        self._plot_market_indicator_correlations(ax3)
        
        # 4. Sector Correlation Heatmap
        ax4 = plt.subplot(5, 4, 4)
        self._plot_sector_correlation_heatmap(ax4)
        
        # 5. Monthly Performance Patterns
        ax5 = plt.subplot(5, 4, 5)
        self._plot_monthly_patterns(ax5)
        
        # 6. Quarterly Performance
        ax6 = plt.subplot(5, 4, 6)
        self._plot_quarterly_patterns(ax6)
        
        # 7. Day of Week Analysis
        ax7 = plt.subplot(5, 4, 7)
        self._plot_day_of_week_patterns(ax7)
        
        # 8. Volatility Regime Performance
        ax8 = plt.subplot(5, 4, 8)
        self._plot_volatility_regime_performance(ax8)
        
        # 9. Headline Category Impact
        ax9 = plt.subplot(5, 4, 9)
        self._plot_headline_category_impact(ax9)
        
        # 10. Economic Event Category Performance
        ax10 = plt.subplot(5, 4, 10)
        self._plot_economic_category_performance(ax10)
        
        # 11. Performance Timeline with Events
        ax11 = plt.subplot(5, 4, 11)
        self._plot_performance_timeline_with_events(ax11)
        
        # 12. Correlation Matrix
        ax12 = plt.subplot(5, 4, 12)
        self._plot_correlation_matrix(ax12)
        
        # 13. VIX vs Performance Scatter
        ax13 = plt.subplot(5, 4, 13)
        self._plot_vix_performance_scatter(ax13)
        
        # 14. Sector Performance Comparison
        ax14 = plt.subplot(5, 4, 14)
        self._plot_sector_performance_comparison(ax14)
        
        # 15. Seasonal Heatmap
        ax15 = plt.subplot(5, 4, 15)
        self._plot_seasonal_heatmap(ax15)
        
        # 16. Key Insights Summary
        ax16 = plt.subplot(5, 4, 16)
        self._plot_key_insights_summary(ax16)
        
        # 17. Performance Distribution by Regime
        ax17 = plt.subplot(5, 4, 17)
        self._plot_performance_by_regime(ax17)
        
        # 18. Event Impact Timeline
        ax18 = plt.subplot(5, 4, 18)
        self._plot_event_impact_timeline(ax18)
        
        # 19. Correlation Strength Rankings
        ax19 = plt.subplot(5, 4, 19)
        self._plot_correlation_rankings(ax19)
        
        # 20. Performance Summary Statistics
        ax20 = plt.subplot(5, 4, 20)
        self._plot_performance_summary_stats(ax20)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Correlation dashboard saved to: {save_path}")
        
        plt.show()
    
    def _plot_sentiment_correlation(self, ax):
        """Plot sentiment correlation analysis."""
        
        sentiment_data = self.report.get('sentiment_correlation', {})
        
        if 'error' in sentiment_data or not sentiment_data.get('detailed_analysis'):
            ax.text(0.5, 0.5, 'No sentiment data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        detailed = sentiment_data['detailed_analysis']
        if len(detailed) == 0:
            ax.text(0.5, 0.5, 'No sentiment data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        df = pd.DataFrame(detailed)
        
        # Create scatter plot
        ax.scatter(df['avg_sentiment_score'], df['performance'], alpha=0.6, s=50)
        
        # Add trend line
        if len(df) > 1:
            z = np.polyfit(df['avg_sentiment_score'], df['performance'], 1)
            p = np.poly1d(z)
            x_trend = np.linspace(df['avg_sentiment_score'].min(), df['avg_sentiment_score'].max(), 100)
            ax.plot(x_trend, p(x_trend), "r--", alpha=0.8)
            
            # Add correlation coefficient
            corr = df['performance'].corr(df['avg_sentiment_score'])
            ax.text(0.05, 0.95, f'Correlation: {corr:.2f}', transform=ax.transAxes, 
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        ax.set_title('Performance vs Sentiment Score', fontweight='bold')
        ax.set_xlabel('Average Sentiment Score')
        ax.set_ylabel('Period Performance ($)')
        ax.grid(True, alpha=0.3)
    
    def _plot_economic_event_impact(self, ax):
        """Plot economic event impact analysis."""
        
        economic_data = self.report.get('economic_event_impact', {})
        
        if 'error' in economic_data:
            ax.text(0.5, 0.5, 'No economic data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        high_impact = economic_data.get('high_impact_performance', {})
        
        if not high_impact:
            ax.text(0.5, 0.5, 'No economic data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        categories = ['With High Impact', 'Without High Impact']
        performances = [
            high_impact.get('with_high_impact', {}).get('avg_performance', 0),
            high_impact.get('without_high_impact', {}).get('avg_performance', 0)
        ]
        
        colors = ['red' if p < 0 else 'green' for p in performances]
        bars = ax.bar(categories, performances, color=colors, alpha=0.7)
        
        ax.set_title('Economic Event Impact', fontweight='bold')
        ax.set_ylabel('Average Performance ($)')
        ax.grid(True, alpha=0.3)
        
        # Add value labels
        for bar, value in zip(bars, performances):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'${value:,.0f}', ha='center', va='bottom' if height > 0 else 'top')
    
    def _plot_market_indicator_correlations(self, ax):
        """Plot market indicator correlations."""
        
        indicator_data = self.report.get('market_indicator_correlation', {})
        
        if not indicator_data:
            ax.text(0.5, 0.5, 'No indicator data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        indicators = list(indicator_data.keys())
        correlations = [indicator_data[ind]['performance_correlation'] for ind in indicators]
        
        # Create horizontal bar chart
        y_pos = np.arange(len(indicators))
        colors = ['green' if corr > 0 else 'red' for corr in correlations]
        
        bars = ax.barh(y_pos, correlations, color=colors, alpha=0.7)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(indicators)
        ax.set_xlabel('Correlation with Performance')
        ax.set_title('Market Indicator Correlations', fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.axvline(x=0, color='black', linestyle='--', alpha=0.5)
        
        # Add correlation values
        for bar, corr in zip(bars, correlations):
            width = bar.get_width()
            ax.text(width + (0.01 if width > 0 else -0.01), bar.get_y() + bar.get_height()/2.,
                   f'{corr:.2f}', ha='left' if width > 0 else 'right', va='center')
    
    def _plot_sector_correlation_heatmap(self, ax):
        """Plot sector correlation heatmap."""
        
        sector_data = self.report.get('sector_rotation_impact', {})
        
        if 'error' in sector_data or not sector_data.get('sector_correlations'):
            ax.text(0.5, 0.5, 'No sector data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        correlations = sector_data['sector_correlations']
        
        sectors = list(correlations.keys())
        corr_values = [correlations[sector]['performance_correlation'] for sector in sectors]
        
        # Create heatmap data
        heatmap_data = np.array(corr_values).reshape(-1, 1)
        
        # Create heatmap
        sns.heatmap(heatmap_data, annot=True, fmt='.2f', cmap='RdYlGn', center=0,
                   yticklabels=sectors, xticklabels=['Correlation'], ax=ax,
                   cbar_kws={'label': 'Correlation'})
        
        ax.set_title(' Sector Performance Correlations', fontweight='bold')
    
    def _plot_monthly_patterns(self, ax):
        """Plot monthly performance patterns."""
        
        seasonal_data = self.report.get('seasonal_patterns', {})
        monthly_analysis = seasonal_data.get('monthly_analysis', {})
        
        if not monthly_analysis:
            ax.text(0.5, 0.5, 'No monthly data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        months = list(range(1, 13))
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        # Extract mean performance for each month
        performances = []
        for month in months:
            if month in monthly_analysis:
                performances.append(monthly_analysis[month]['total_pnl']['mean'])
            else:
                performances.append(0)
        
        colors = ['green' if p > 0 else 'red' for p in performances]
        bars = ax.bar(month_names, performances, color=colors, alpha=0.7)
        
        ax.set_title('Monthly Performance Patterns', fontweight='bold')
        ax.set_xlabel('Month')
        ax.set_ylabel('Average Performance ($)')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
        
        # Highlight best and worst months
        best_month_idx = np.argmax(performances)
        worst_month_idx = np.argmin(performances)
        
        bars[best_month_idx].set_edgecolor('gold')
        bars[best_month_idx].set_linewidth(3)
        bars[worst_month_idx].set_edgecolor('darkred')
        bars[worst_month_idx].set_linewidth(3)
    
    def _plot_quarterly_patterns(self, ax):
        """Plot quarterly performance patterns."""
        
        seasonal_data = self.report.get('seasonal_patterns', {})
        quarterly_analysis = seasonal_data.get('quarterly_analysis', {})
        
        if not quarterly_analysis:
            ax.text(0.5, 0.5, 'No quarterly data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        quarters = ['Q1', 'Q2', 'Q3', 'Q4']
        performances = []
        
        for i, quarter in enumerate(quarters, 1):
            if i in quarterly_analysis:
                performances.append(quarterly_analysis[i]['total_pnl']['mean'])
            else:
                performances.append(0)
        
        colors = ['green' if p > 0 else 'red' for p in performances]
        bars = ax.bar(quarters, performances, color=colors, alpha=0.7)
        
        ax.set_title('Quarterly Performance Patterns', fontweight='bold')
        ax.set_xlabel('Quarter')
        ax.set_ylabel('Average Performance ($)')
        ax.grid(True, alpha=0.3)
        
        # Add value labels
        for bar, value in zip(bars, performances):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'${value:,.0f}', ha='center', va='bottom' if height > 0 else 'top')
    
    def _plot_day_of_week_patterns(self, ax):
        """Plot day of week performance patterns."""
        
        seasonal_data = self.report.get('seasonal_patterns', {})
        dow_analysis = seasonal_data.get('day_of_week_analysis', {})
        
        if not dow_analysis:
            ax.text(0.5, 0.5, 'No day-of-week data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
        performances = []
        
        for i, day in enumerate(days):
            if i in dow_analysis:
                performances.append(dow_analysis[i]['total_pnl']['mean'])
            else:
                performances.append(0)
        
        colors = ['green' if p > 0 else 'red' for p in performances]
        bars = ax.bar(days, performances, color=colors, alpha=0.7)
        
        ax.set_title('Day-of-Week Performance Patterns', fontweight='bold')
        ax.set_xlabel('Day of Week')
        ax.set_ylabel('Average Performance ($)')
        ax.grid(True, alpha=0.3)
    
    def _plot_volatility_regime_performance(self, ax):
        """Plot volatility regime performance."""
        
        vol_data = self.report.get('volatility_regime_analysis', {})
        regime_performance = vol_data.get('regime_performance', {})
        
        if not regime_performance:
            ax.text(0.5, 0.5, 'No volatility data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        regimes = ['Low Volatility', 'Normal Volatility', 'High Volatility']
        performances = []
        
        for regime_key in ['low_volatility', 'normal_volatility', 'high_volatility']:
            if regime_key in regime_performance:
                performances.append(regime_performance[regime_key]['avg_performance'])
            else:
                performances.append(0)
        
        colors = ['lightblue', 'yellow', 'lightcoral']
        bars = ax.bar(regimes, performances, color=colors, alpha=0.7)
        
        ax.set_title('Performance by Volatility Regime', fontweight='bold')
        ax.set_xlabel('Volatility Regime')
        ax.set_ylabel('Average Performance ($)')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
        
        # Add value labels
        for bar, value in zip(bars, performances):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'${value:,.0f}', ha='center', va='bottom' if height > 0 else 'top')
    
    def _plot_headline_category_impact(self, ax):
        """Plot headline category impact on performance."""
        
        sentiment_data = self.report.get('sentiment_correlation', {})
        sentiment_performance = sentiment_data.get('sentiment_performance', {})
        
        if not sentiment_performance:
            ax.text(0.5, 0.5, 'No headline category data', ha='center', va='center', transform=ax.transAxes)
            return
        
        categories = list(sentiment_performance.keys())
        performances = [sentiment_performance[cat]['avg_performance'] for cat in categories]
        
        colors = ['green' if p > 0 else 'red' for p in performances]
        bars = ax.bar(categories, performances, color=colors, alpha=0.7)
        
        ax.set_title('Performance by Headline Sentiment', fontweight='bold')
        ax.set_xlabel('Sentiment Category')
        ax.set_ylabel('Average Performance ($)')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
    
    def _plot_economic_category_performance(self, ax):
        """Plot economic event category performance."""
        
        economic_data = self.report.get('economic_event_impact', {})
        category_performance = economic_data.get('category_performance', {})
        
        if not category_performance:
            ax.text(0.5, 0.5, 'No economic category data', ha='center', va='center', transform=ax.transAxes)
            return
        
        categories = list(category_performance.keys())
        performances = [category_performance[cat]['avg_performance'] for cat in categories]
        
        colors = ['green' if p > 0 else 'red' for p in performances]
        bars = ax.bar(categories, performances, color=colors, alpha=0.7)
        
        ax.set_title('Performance by Economic Event Category', fontweight='bold')
        ax.set_xlabel('Event Category')
        ax.set_ylabel('Average Performance ($)')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
    
    def _plot_performance_timeline_with_events(self, ax):
        """Plot performance timeline with key events."""
        
        # Get performance periods
        if not hasattr(self.analyzer.analyzer, 'performance_periods'):
            ax.text(0.5, 0.5, 'No performance timeline data', ha='center', va='center', transform=ax.transAxes)
            return
        
        periods_df = self.analyzer.analyzer.performance_periods.get('performance_periods', pd.DataFrame())
        
        if len(periods_df) == 0:
            ax.text(0.5, 0.5, 'No performance timeline data', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Sort by date
        periods_df = periods_df.sort_values('start_date')
        
        # Create timeline
        ax.plot(periods_df['start_date'], periods_df['total_pnl'], marker='o', linewidth=2)
        
        # Color code by performance
        colors = ['green' if pnl > 0 else 'red' for pnl in periods_df['total_pnl']]
        ax.scatter(periods_df['start_date'], periods_df['total_pnl'], c=colors, s=50, alpha=0.7)
        
        ax.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax.set_title('Performance Timeline', fontweight='bold')
        ax.set_xlabel('Date')
        ax.set_ylabel('Period Performance ($)')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
    
    def _plot_correlation_matrix(self, ax):
        """Plot correlation matrix of all factors."""
        
        # Collect all correlation data
        correlations = {}
        
        # Market indicators
        indicator_data = self.report.get('market_indicator_correlation', {})
        for indicator, data in indicator_data.items():
            correlations[indicator] = data.get('performance_correlation', 0)
        
        # Sector data
        sector_data = self.report.get('sector_rotation_impact', {})
        sector_correlations = sector_data.get('sector_correlations', {})
        for sector, data in sector_correlations.items():
            correlations[sector] = data.get('performance_correlation', 0)
        
        # Sentiment
        sentiment_data = self.report.get('sentiment_correlation', {})
        sentiment_corr = sentiment_data.get('correlation_with_sentiment')
        if sentiment_corr is not None:
            correlations['Sentiment'] = sentiment_corr
        
        # VIX correlation
        vol_data = self.report.get('volatility_regime_analysis', {})
        vix_corr = vol_data.get('vix_correlation')
        if vix_corr is not None:
            correlations['VIX'] = vix_corr
        
        if not correlations:
            ax.text(0.5, 0.5, 'No correlation data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create correlation matrix (just showing correlations with performance)
        df = pd.DataFrame(list(correlations.items()), columns=['Factor', 'Correlation'])
        df = df.sort_values('Correlation', ascending=False)
        
        # Create bar chart
        y_pos = np.arange(len(df))
        colors = ['green' if corr > 0 else 'red' for corr in df['Correlation']]
        
        bars = ax.barh(y_pos, df['Correlation'], color=colors, alpha=0.7)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(df['Factor'])
        ax.set_xlabel('Correlation with Performance')
        ax.set_title('Factor Correlation Rankings', fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.axvline(x=0, color='black', linestyle='--', alpha=0.5)
    
    def _plot_vix_performance_scatter(self, ax):
        """Plot VIX vs performance scatter plot."""
        
        vol_data = self.report.get('volatility_regime_analysis', {})
        detailed_analysis = vol_data.get('transition_analysis', {})
        
        # This would need detailed VIX data per period
        # For now, show a conceptual plot
        ax.text(0.5, 0.5, 'VIX scatter plot requires detailed period data', ha='center', va='center', transform=ax.transAxes)
        ax.set_title('VIX vs Performance Scatter', fontweight='bold')
    
    def _plot_sector_performance_comparison(self, ax):
        """Plot sector performance comparison."""
        
        sector_data = self.report.get('sector_rotation_impact', {})
        sector_correlations = sector_data.get('sector_correlations', {})
        
        if not sector_correlations:
            ax.text(0.5, 0.5, 'No sector comparison data', ha='center', va='center', transform=ax.transAxes)
            return
        
        sectors = list(sector_correlations.keys())
        correlations = [sector_correlations[sector]['performance_correlation'] for sector in sectors]
        avg_changes = [sector_correlations[sector]['avg_sector_change'] for sector in sectors]
        
        # Create scatter plot
        ax.scatter(avg_changes, correlations, alpha=0.7, s=50)
        
        # Add labels for best/worst
        best_idx = np.argmax(correlations)
        worst_idx = np.argmin(correlations)
        
        ax.annotate(sectors[best_idx], (avg_changes[best_idx], correlations[best_idx]), 
                   xytext=(5, 5), textcoords='offset points', fontweight='bold')
        ax.annotate(sectors[worst_idx], (avg_changes[worst_idx], correlations[worst_idx]), 
                   xytext=(5, 5), textcoords='offset points', fontweight='bold')
        
        ax.set_title('Sector Performance vs Correlation', fontweight='bold')
        ax.set_xlabel('Average Sector Change (%)')
        ax.set_ylabel('Correlation with Strategy')
        ax.grid(True, alpha=0.3)
        ax.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax.axvline(x=0, color='black', linestyle='--', alpha=0.5)
    
    def _plot_seasonal_heatmap(self, ax):
        """Plot seasonal performance heatmap."""
        
        seasonal_data = self.report.get('seasonal_patterns', {})
        weekly_analysis = seasonal_data.get('weekly_analysis', {})
        
        if not weekly_analysis:
            ax.text(0.5, 0.5, 'No weekly data for heatmap', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create week vs performance data
        weeks = list(weekly_analysis.keys())
        performances = [weekly_analysis[week]['total_pnl']['mean'] for week in weeks]
        
        # Reshape into weeks x years heatmap (simplified)
        weeks_per_year = 52
        years = len(weeks) // weeks_per_year + 1
        
        heatmap_data = np.array(performances).reshape(-1, 1)
        
        sns.heatmap(heatmap_data, annot=False, cmap='RdYlGn', center=0,
                   yticklabels=[f'Week {w}' for w in weeks[:min(20, len(weeks))]], 
                   xticklabels=['Performance'], ax=ax,
                   cbar_kws={'label': 'Performance ($)'})
        
        ax.set_title('Weekly Performance Heatmap', fontweight='bold')
    
    def _plot_key_insights_summary(self, ax):
        """Plot key insights summary."""
        
        insights = self.report.get('key_insights', [])
        
        if not insights:
            ax.text(0.5, 0.5, 'No insights available', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create text display
        ax.axis('off')
        
        insight_text = "KEY INSIGHTS\n" + "="*30 + "\n\n"
        
        for i, insight in enumerate(insights[:8], 1):  # Show top 8 insights
            insight_text += f"{i}. {insight}\n"
        
        ax.text(0.05, 0.95, insight_text, transform=ax.transAxes, fontsize=9,
               verticalalignment='top', fontfamily='monospace',
               bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        ax.set_title('Key Insights Summary', fontweight='bold', pad=20)
    
    def _plot_performance_by_regime(self, ax):
        """Plot performance distribution by volatility regime."""
        
        vol_data = self.report.get('volatility_regime_analysis', {})
        regime_performance = vol_data.get('regime_performance', {})
        
        if not regime_performance:
            ax.text(0.5, 0.5, 'No regime distribution data', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create box plot data (conceptual)
        regimes = list(regime_performance.keys())
        performances = [regime_performance[regime]['avg_performance'] for regime in regimes]
        
        # Simple bar chart showing average performance
        regime_labels = ['Low Vol', 'Normal Vol', 'High Vol']
        colors = ['lightblue', 'yellow', 'lightcoral']
        
        bars = ax.bar(regime_labels, performances, color=colors, alpha=0.7)
        
        ax.set_title('Average Performance by Regime', fontweight='bold')
        ax.set_xlabel('Volatility Regime')
        ax.set_ylabel('Average Performance ($)')
        ax.grid(True, alpha=0.3)
    
    def _plot_event_impact_timeline(self, ax):
        """Plot event impact timeline."""
        
        # This would require detailed event timing data
        ax.text(0.5, 0.5, 'Event timeline requires detailed timing data', ha='center', va='center', transform=ax.transAxes)
        ax.set_title('Event Impact Timeline', fontweight='bold')
    
    def _plot_correlation_rankings(self, ax):
        """Plot correlation strength rankings."""
        
        # Collect all correlations
        all_correlations = {}
        
        # Market indicators
        indicator_data = self.report.get('market_indicator_correlation', {})
        for indicator, data in indicator_data.items():
            all_correlations[f'{indicator}'] = abs(data.get('performance_correlation', 0))
        
        # Sector data
        sector_data = self.report.get('sector_rotation_impact', {})
        sector_correlations = sector_data.get('sector_correlations', {})
        for sector, data in sector_correlations.items():
            all_correlations[f'{sector}'] = abs(data.get('performance_correlation', 0))
        
        # Sentiment
        sentiment_data = self.report.get('sentiment_correlation', {})
        sentiment_corr = sentiment_data.get('correlation_with_sentiment')
        if sentiment_corr is not None:
            all_correlations['Sentiment'] = abs(sentiment_corr)
        
        if not all_correlations:
            ax.text(0.5, 0.5, 'No correlation rankings available', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Sort by absolute correlation
        sorted_correlations = sorted(all_correlations.items(), key=lambda x: x[1], reverse=True)
        
        # Take top 10
        top_factors = sorted_correlations[:10]
        factors = [f[0] for f in top_factors]
        values = [f[1] for f in top_factors]
        
        # Create horizontal bar chart
        y_pos = np.arange(len(factors))
        colors = plt.cm.RdYlGn(np.array(values))  # Color by correlation strength
        
        bars = ax.barh(y_pos, values, color=colors, alpha=0.7)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(factors)
        ax.set_xlabel('Absolute Correlation')
        ax.set_title('Top 10 Correlation Strength Rankings', fontweight='bold')
        ax.grid(True, alpha=0.3)
    
    def _plot_performance_summary_stats(self, ax):
        """Plot performance summary statistics."""
        
        metadata = self.report.get('analysis_metadata', {})
        
        if not metadata:
            ax.text(0.5, 0.5, 'No summary statistics available', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create stats display
        ax.axis('off')
        
        stats_text = "PERFORMANCE SUMMARY\n" + "="*25 + "\n\n"
        stats_text += f"Periods Analyzed: {metadata.get('periods_analyzed', 'N/A')}\n"
        stats_text += f"Date Range: {metadata.get('date_range', {}).get('start', 'N/A')[:10]} to {metadata.get('date_range', {}).get('end', 'N/A')[:10]}\n"
        stats_text += f"Total P&L: ${metadata.get('total_pnl', 0):,.0f}\n"
        stats_text += f"Average Win Rate: {metadata.get('avg_win_rate', 0):.1%}\n"
        
        # Add key metrics from other analyses
        sentiment_corr = self.report.get('sentiment_correlation', {}).get('correlation_with_sentiment')
        if sentiment_corr is not None:
            stats_text += f"Sentiment Correlation: {sentiment_corr:.2f}\n"
        
        vol_data = self.report.get('volatility_regime_analysis', {})
        if 'vix_correlation' in vol_data:
            stats_text += f"VIX Correlation: {vol_data['vix_correlation']:.2f}\n"
        
        ax.text(0.1, 0.9, stats_text, transform=ax.transAxes, fontsize=10,
               verticalalignment='top', fontfamily='monospace',
               bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))
        
        ax.set_title('Performance Summary', fontweight='bold', pad=20)


def main():
    """Main function to create correlation dashboard."""
    
    print("Creating Temporal Correlation Dashboard...")
    
    # Initialize analyzer
    base_analyzer = StrategyPerformanceAnalyzer()
    
    # Load data and run analysis
    if not base_analyzer.load_historical_data():
        return
    
    if not base_analyzer.calculate_regimes():
        return
    
    base_analyzer.simulate_bear_expansion_strategy()
    
    # Initialize correlation analyzer
    correlation_analyzer = TemporalCorrelationAnalyzer(base_analyzer)
    
    # Create dashboard
    dashboard = CorrelationDashboard(correlation_analyzer)
    dashboard.create_comprehensive_correlation_dashboard('correlation_dashboard.png')
    
    print("Correlation dashboard created successfully!")


if __name__ == "__main__":
    main()
