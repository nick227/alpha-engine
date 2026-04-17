"""
Performance Dashboard - Visualization dashboard for strategy performance periods and streaks.

Creates interactive visualizations to help identify when strategies perform best
and track winning/losing streaks over time.
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))

from strategy_performance_periods import StrategyPerformanceAnalyzer
from streak_monitor import StreakMonitor


# Set style
plt.style.use('default')
sns.set_palette("husl")


class PerformanceDashboard:
    """
    Visualization dashboard for strategy performance analysis.
    """
    
    def __init__(self, analyzer: StrategyPerformanceAnalyzer = None):
        self.analyzer = analyzer or StrategyPerformanceAnalyzer()
        self.streak_monitor = StreakMonitor()
        
    def create_performance_overview(self, save_path: str = None) -> None:
        """Create comprehensive performance overview dashboard."""
        
        if self.analyzer.trades is None or len(self.analyzer.trades) == 0:
            print("No trade data available for visualization")
            return
        
        # Create figure with subplots
        fig = plt.figure(figsize=(20, 24))
        fig.suptitle('Strategy Performance Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Cumulative P&L over time
        ax1 = plt.subplot(4, 3, 1)
        self._plot_cumulative_pnl(ax1)
        
        # 2. Trade distribution
        ax2 = plt.subplot(4, 3, 2)
        self._plot_trade_distribution(ax2)
        
        # 3. Win rate over time
        ax3 = plt.subplot(4, 3, 3)
        self._plot_win_rate_timeline(ax3)
        
        # 4. Monthly performance heatmap
        ax4 = plt.subplot(4, 3, 4)
        self._plot_monthly_heatmap(ax4)
        
        # 5. Streak analysis
        ax5 = plt.subplot(4, 3, 5)
        self._plot_streak_analysis(ax5)
        
        # 6. Performance periods
        ax6 = plt.subplot(4, 3, 6)
        self._plot_performance_periods(ax6)
        
        # 7. Trade P&L by regime
        ax7 = plt.subplot(4, 3, 7)
        self._plot_pnl_by_regime(ax7)
        
        # 8. Holding period analysis
        ax8 = plt.subplot(4, 3, 8)
        self._plot_holding_period_analysis(ax8)
        
        # 9. Risk metrics
        ax9 = plt.subplot(4, 3, 9)
        self._plot_risk_metrics(ax9)
        
        # 10. Position in range analysis
        ax10 = plt.subplot(4, 3, 10)
        self._plot_position_range_analysis(ax10)
        
        # 11. Recent performance trends
        ax11 = plt.subplot(4, 3, 11)
        self._plot_recent_trends(ax11)
        
        # 12. Key statistics
        ax12 = plt.subplot(4, 3, 12)
        self._plot_key_statistics(ax12)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Dashboard saved to: {save_path}")
        
        plt.show()
    
    def _plot_cumulative_pnl(self, ax):
        """Plot cumulative P&L over time."""
        
        trades = self.analyzer.trades.sort_values('entry_date')
        trades['cumulative_pnl'] = trades['pnl'].cumsum()
        
        ax.plot(trades['entry_date'], trades['cumulative_pnl'], linewidth=2, color='green')
        ax.fill_between(trades['entry_date'], trades['cumulative_pnl'], alpha=0.3, color='green')
        ax.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        
        ax.set_title('Cumulative P&L Over Time', fontweight='bold')
        ax.set_xlabel('Date')
        ax.set_ylabel('Cumulative P&L ($)')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
    
    def _plot_trade_distribution(self, ax):
        """Plot trade P&L distribution."""
        
        trades = self.analyzer.trades
        
        # Create histogram
        ax.hist(trades['pnl'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        ax.axvline(x=0, color='red', linestyle='--', linewidth=2, label='Break Even')
        ax.axvline(x=trades['pnl'].mean(), color='green', linestyle='--', linewidth=2, label='Mean')
        
        ax.set_title('Trade P&L Distribution', fontweight='bold')
        ax.set_xlabel('Trade P&L ($)')
        ax.set_ylabel('Frequency')
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    def _plot_win_rate_timeline(self, ax):
        """Plot rolling win rate over time."""
        
        trades = self.analyzer.trades.sort_values('entry_date')
        trades['win'] = trades['pnl'] > 0
        
        # Calculate rolling win rate
        window = min(20, len(trades) // 4)  # Use 25% of trades or 20, whichever is smaller
        if window >= 5:
            trades['rolling_win_rate'] = trades['win'].rolling(window=window, min_periods=5).mean()
            
            ax.plot(trades['entry_date'], trades['rolling_win_rate'], linewidth=2, color='orange')
            ax.axhline(y=0.5, color='black', linestyle='--', alpha=0.5, label='50%')
            ax.axhline(y=trades['win'].mean(), color='green', linestyle='--', alpha=0.5, label='Overall')
        else:
            # If not enough data, show cumulative win rate
            trades['cumulative_win_rate'] = trades['win'].expanding().mean()
            ax.plot(trades['entry_date'], trades['cumulative_win_rate'], linewidth=2, color='orange')
            ax.axhline(y=0.5, color='black', linestyle='--', alpha=0.5, label='50%')
            ax.axhline(y=trades['win'].mean(), color='green', linestyle='--', alpha=0.5, label='Overall')
        
        ax.set_title('Win Rate Over Time', fontweight='bold')
        ax.set_xlabel('Date')
        ax.set_ylabel('Win Rate')
        ax.set_ylim(0, 1)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
    
    def _plot_monthly_heatmap(self, ax):
        """Plot monthly performance heatmap."""
        
        trades = self.analyzer.trades.copy()
        trades['entry_date'] = pd.to_datetime(trades['entry_date'])
        trades['month'] = trades['entry_date'].dt.month
        trades['year'] = trades['entry_date'].dt.year
        
        # Create pivot table
        monthly_pnl = trades.groupby(['year', 'month'])['pnl'].sum().reset_index()
        pivot_table = monthly_pnl.pivot(index='year', columns='month', values='pnl')
        
        # Create heatmap
        sns.heatmap(pivot_table, annot=True, fmt='.0f', cmap='RdYlGn', center=0, ax=ax,
                   cbar_kws={'label': 'Monthly P&L ($)'})
        
        ax.set_title('Monthly Performance Heatmap', fontweight='bold')
        ax.set_xlabel('Month')
        ax.set_ylabel('Year')
    
    def _plot_streak_analysis(self, ax):
        """Plot streak analysis."""
        
        # Simulate streaks from trades
        trades = self.analyzer.trades.sort_values('entry_date')
        trades['win'] = trades['pnl'] > 0
        
        # Find streaks
        streaks = []
        current_streak = {'type': 'win' if trades.iloc[0]['win'] else 'lose', 'length': 1, 'pnl': trades.iloc[0]['pnl']}
        
        for i in range(1, len(trades)):
            if trades.iloc[i]['win'] == (current_streak['type'] == 'win'):
                current_streak['length'] += 1
                current_streak['pnl'] += trades.iloc[i]['pnl']
            else:
                streaks.append(current_streak)
                current_streak = {'type': 'win' if trades.iloc[i]['win'] else 'lose', 'length': 1, 'pnl': trades.iloc[i]['pnl']}
        
        streaks.append(current_streak)
        
        # Separate winning and losing streaks
        winning_streaks = [s for s in streaks if s['type'] == 'win']
        losing_streaks = [s for s in streaks if s['type'] == 'lose']
        
        # Create bar chart
        categories = ['Winning Streaks', 'Losing Streaks']
        avg_lengths = [
            np.mean([s['length'] for s in winning_streaks]) if winning_streaks else 0,
            np.mean([s['length'] for s in losing_streaks]) if losing_streaks else 0
        ]
        max_lengths = [
            max([s['length'] for s in winning_streaks]) if winning_streaks else 0,
            max([s['length'] for s in losing_streaks]) if losing_streaks else 0
        ]
        
        x = np.arange(len(categories))
        width = 0.35
        
        ax.bar(x - width/2, avg_lengths, width, label='Average Length', color='green', alpha=0.7)
        ax.bar(x + width/2, max_lengths, width, label='Maximum Length', color='red', alpha=0.7)
        
        ax.set_title('Streak Analysis', fontweight='bold')
        ax.set_ylabel('Streak Length (trades)')
        ax.set_xticks(x)
        ax.set_xticklabels(categories)
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    def _plot_performance_periods(self, ax):
        """Plot best and worst performance periods."""
        
        if 'performance_periods' not in self.analyzer.performance_periods:
            ax.text(0.5, 0.5, 'No performance periods data', ha='center', va='center', transform=ax.transAxes)
            return
        
        periods_df = self.analyzer.performance_periods['performance_periods']
        
        # Sort by P&L
        periods_sorted = periods_df.sort_values('total_pnl')
        
        # Plot top 5 best and worst periods
        worst_periods = periods_sorted.head(5)
        best_periods = periods_sorted.tail(5)
        
        # Create labels
        worst_labels = [f"{row['start_date'].strftime('%m/%d')}" for _, row in worst_periods.iterrows()]
        best_labels = [f"{row['start_date'].strftime('%m/%d')}" for _, row in best_periods.iterrows()]
        
        # Plot
        y_pos = np.arange(len(worst_periods))
        ax.barh(y_pos, worst_periods['total_pnl'], color='red', alpha=0.7, label='Worst Periods')
        ax.set_yticks(y_pos)
        ax.set_yticklabels(worst_labels)
        ax.set_xlabel('Period P&L ($)')
        ax.set_title('Worst Performance Periods', fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        # Add value labels
        for i, v in enumerate(worst_periods['total_pnl']):
            ax.text(v, i, f'${v:,.0f}', va='center', ha='right' if v < 0 else 'left')
    
    def _plot_pnl_by_regime(self, ax):
        """Plot P&L by regime."""
        
        trades = self.analyzer.trades
        
        if 'regime' not in trades.columns:
            ax.text(0.5, 0.5, 'No regime data available', ha='center', va='center', transform=ax.transAxes)
            return
        
        regime_pnl = trades.groupby('regime')['pnl'].agg(['sum', 'count']).reset_index()
        regime_pnl = regime_pnl.sort_values('sum')
        
        # Create bar plot
        bars = ax.bar(range(len(regime_pnl)), regime_pnl['sum'], 
                     color=['red' if x < 0 else 'green' for x in regime_pnl['sum']], alpha=0.7)
        
        ax.set_title('P&L by Regime', fontweight='bold')
        ax.set_xlabel('Regime')
        ax.set_ylabel('Total P&L ($)')
        ax.set_xticks(range(len(regime_pnl)))
        ax.set_xticklabels(regime_pnl['regime'], rotation=45, ha='right')
        ax.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, value in zip(bars, regime_pnl['sum']):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'${value:,.0f}', ha='center', va='bottom' if height > 0 else 'top')
    
    def _plot_holding_period_analysis(self, ax):
        """Plot holding period analysis."""
        
        trades = self.analyzer.trades
        
        if 'hold_days' not in trades.columns:
            ax.text(0.5, 0.5, 'No holding period data', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create scatter plot of holding days vs P&L
        ax.scatter(trades['hold_days'], trades['pnl'], alpha=0.6, s=30)
        
        # Add trend line
        if len(trades) > 1:
            z = np.polyfit(trades['hold_days'], trades['pnl'], 1)
            p = np.poly1d(z)
            ax.plot(trades['hold_days'], p(trades['hold_days']), "r--", alpha=0.8)
        
        ax.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax.set_title('Holding Period vs P&L', fontweight='bold')
        ax.set_xlabel('Holding Period (days)')
        ax.set_ylabel('Trade P&L ($)')
        ax.grid(True, alpha=0.3)
    
    def _plot_risk_metrics(self, ax):
        """Plot risk metrics visualization."""
        
        trades = self.analyzer.trades
        
        # Calculate risk metrics
        total_pnl = trades['pnl'].sum()
        win_rate = (trades['pnl'] > 0).mean()
        avg_win = trades[trades['pnl'] > 0]['pnl'].mean()
        avg_loss = trades[trades['pnl'] < 0]['pnl'].mean()
        max_drawdown = self._calculate_max_drawdown(trades)
        sharpe_ratio = self._calculate_sharpe_ratio(trades)
        
        # Create metrics display
        metrics = [
            ('Total P&L', f'${total_pnl:,.0f}', 'green' if total_pnl > 0 else 'red'),
            ('Win Rate', f'{win_rate:.1%}', 'green' if win_rate > 0.5 else 'red'),
            ('Avg Win', f'${avg_win:,.0f}', 'green'),
            ('Avg Loss', f'${avg_loss:,.0f}', 'red'),
            ('Max DD', f'${max_drawdown:,.0f}', 'red'),
            ('Sharpe', f'{sharpe_ratio:.2f}', 'green' if sharpe_ratio > 1 else 'orange')
        ]
        
        # Create table
        ax.axis('tight')
        ax.axis('off')
        
        table_data = [[metric, value] for metric, value, _ in metrics]
        table = ax.table(cellText=table_data, colLabels=['Metric', 'Value'],
                       cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        # Color code the values
        for i, (_, _, color) in enumerate(metrics):
            table[(i+1, 1)].set_facecolor(color)
            table[(i+1, 1)].set_text_props(weight='bold', color='white')
        
        ax.set_title('Risk Metrics', fontweight='bold', pad=20)
    
    def _plot_position_range_analysis(self, ax):
        """Plot position in range analysis."""
        
        trades = self.analyzer.trades
        
        if 'position_in_range' not in trades.columns:
            ax.text(0.5, 0.5, 'No position range data', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create bins for position in range
        bins = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
        labels = ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']
        
        trades['range_bin'] = pd.cut(trades['position_in_range'], bins=bins, labels=labels, include_lowest=True)
        
        # Calculate average P&L by bin
        bin_pnl = trades.groupby('range_bin')['pnl'].agg(['mean', 'count']).reset_index()
        
        # Create bar plot
        bars = ax.bar(range(len(bin_pnl)), bin_pnl['mean'], 
                     color=['red' if x < 0 else 'green' for x in bin_pnl['mean']], alpha=0.7)
        
        ax.set_title('P&L by Position in Range', fontweight='bold')
        ax.set_xlabel('Position in Range')
        ax.set_ylabel('Average P&L ($)')
        ax.set_xticks(range(len(bin_pnl)))
        ax.set_xticklabels(bin_pnl['range_bin'])
        ax.grid(True, alpha=0.3)
        
        # Add trade count labels
        for i, (bar, count) in enumerate(zip(bars, bin_pnl['count'])):
            ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + (50 if bar.get_height() > 0 else -50),
                   f'n={count}', ha='center', va='bottom' if bar.get_height() > 0 else 'top')
    
    def _plot_recent_trends(self, ax):
        """Plot recent performance trends."""
        
        trades = self.analyzer.trades.sort_values('entry_date')
        
        # Take last 30 trades or all if less
        recent_trades = trades.tail(30)
        
        if len(recent_trades) < 5:
            ax.text(0.5, 0.5, 'Insufficient recent data', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Calculate cumulative P&L for recent trades
        recent_trades = recent_trades.copy()
        recent_trades['cumulative_pnl'] = recent_trades['pnl'].cumsum()
        
        # Plot
        ax.plot(range(len(recent_trades)), recent_trades['cumulative_pnl'], 
               linewidth=2, marker='o', markersize=4)
        ax.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax.fill_between(range(len(recent_trades)), recent_trades['cumulative_pnl'], alpha=0.3)
        
        ax.set_title('Recent Performance (Last 30 Trades)', fontweight='bold')
        ax.set_xlabel('Trade Number')
        ax.set_ylabel('Cumulative P&L ($)')
        ax.grid(True, alpha=0.3)
        
        # Add trend line
        if len(recent_trades) > 1:
            x = np.arange(len(recent_trades))
            z = np.polyfit(x, recent_trades['cumulative_pnl'], 1)
            p = np.poly1d(z)
            ax.plot(x, p(x), "r--", alpha=0.8, label=f'Trend: ${z[0]:.0f}/trade')
            ax.legend()
    
    def _plot_key_statistics(self, ax):
        """Plot key statistics summary."""
        
        trades = self.analyzer.trades
        
        # Calculate statistics
        stats = {
            'Total Trades': len(trades),
            'Win Rate': f"{(trades['pnl'] > 0).mean():.1%}",
            'Total P&L': f"${trades['pnl'].sum():,.0f}",
            'Avg Trade': f"${trades['pnl'].mean():.0f}",
            'Best Trade': f"${trades['pnl'].max():,.0f}",
            'Worst Trade': f"${trades['pnl'].min():,.0f}",
            'Std Dev': f"${trades['pnl'].std():.0f}",
            'Days Active': f"{(trades['entry_date'].max() - trades['entry_date'].min()).days}"
        }
        
        # Create text display
        ax.axis('off')
        text_str = "KEY STATISTICS\n" + "="*25 + "\n\n"
        
        for key, value in stats.items():
            text_str += f"{key:<15}: {value}\n"
        
        ax.text(0.1, 0.9, text_str, transform=ax.transAxes, fontsize=10,
               verticalalignment='top', fontfamily='monospace',
               bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))
        
        ax.set_title('Key Statistics', fontweight='bold', pad=20)
    
    def _calculate_max_drawdown(self, trades):
        """Calculate maximum drawdown."""
        
        trades_sorted = trades.sort_values('entry_date')
        trades_sorted['cumulative_pnl'] = trades_sorted['pnl'].cumsum()
        
        running_max = trades_sorted['cumulative_pnl'].expanding().max()
        drawdown = trades_sorted['cumulative_pnl'] - running_max
        
        return drawdown.min()
    
    def _calculate_sharpe_ratio(self, trades, risk_free_rate=0.02):
        """Calculate Sharpe ratio."""
        
        if len(trades) < 2:
            return 0
        
        daily_returns = trades['pnl'] / 100000  # Assuming 100k capital
        excess_returns = daily_returns - risk_free_rate/252  # Daily risk-free rate
        
        if excess_returns.std() == 0:
            return 0
        
        return np.sqrt(252) * excess_returns.mean() / excess_returns.std()
    
    def create_streak_dashboard(self, save_path: str = None) -> None:
        """Create streak-specific dashboard."""
        
        # Process trades through streak monitor
        for _, trade in self.analyzer.trades.iterrows():
            self.streak_monitor.add_trade(trade.to_dict())
        
        # Get streak data
        streak_stats = self.streak_monitor.get_streak_statistics()
        
        if not streak_stats:
            print("No streak data available for visualization")
            return
        
        # Create figure
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Streak Analysis Dashboard', fontsize=16, fontweight='bold')
        
        # Streak length distribution
        ax1 = axes[0, 0]
        self._plot_streak_length_distribution(ax1, streak_stats)
        
        # Streak P&L distribution
        ax2 = axes[0, 1]
        self._plot_streak_pnl_distribution(ax2, streak_stats)
        
        # Streak timeline
        ax3 = axes[1, 0]
        self._plot_streak_timeline(ax3, streak_stats)
        
        # Current streak info
        ax4 = axes[1, 1]
        self._plot_current_streak_info(ax4, streak_stats)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Streak dashboard saved to: {save_path}")
        
        plt.show()
    
    def _plot_streak_length_distribution(self, ax, streak_stats):
        """Plot streak length distribution."""
        
        all_streaks = self.streak_monitor.streak_history
        if self.streak_monitor.current_streak:
            all_streaks.append(self.streak_monitor.current_streak)
        
        winning_lengths = [s.length for s in all_streaks if s.streak_type.value == 'winning']
        losing_lengths = [s.length for s in all_streaks if s.streak_type.value == 'losing']
        
        # Create histograms
        ax.hist(winning_lengths, bins=range(1, max(winning_lengths + [1]) + 2), 
                alpha=0.7, label='Winning Streaks', color='green')
        ax.hist(losing_lengths, bins=range(1, max(losing_lengths + [1]) + 2), 
                alpha=0.7, label='Losing Streaks', color='red')
        
        ax.set_title('Streak Length Distribution', fontweight='bold')
        ax.set_xlabel('Streak Length (trades)')
        ax.set_ylabel('Frequency')
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    def _plot_streak_pnl_distribution(self, ax, streak_stats):
        """Plot streak P&L distribution."""
        
        all_streaks = self.streak_monitor.streak_history
        if self.streak_monitor.current_streak:
            all_streaks.append(self.streak_monitor.current_streak)
        
        winning_pnl = [s.current_pnl for s in all_streaks if s.streak_type.value == 'winning']
        losing_pnl = [s.current_pnl for s in all_streaks if s.streak_type.value == 'losing']
        
        # Create box plots
        data = [winning_pnl, losing_pnl]
        labels = ['Winning Streaks', 'Losing Streaks']
        
        bp = ax.boxplot(data, labels=labels, patch_artist=True)
        bp['boxes'][0].set_facecolor('lightgreen')
        bp['boxes'][1].set_facecolor('lightcoral')
        
        ax.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax.set_title('Streak P&L Distribution', fontweight='bold')
        ax.set_ylabel('Streak P&L ($)')
        ax.grid(True, alpha=0.3)
    
    def _plot_streak_timeline(self, ax, streak_stats):
        """Plot streak timeline."""
        
        all_streaks = self.streak_monitor.streak_history
        if self.streak_monitor.current_streak:
            all_streaks.append(self.streak_monitor.current_streak)
        
        if not all_streaks:
            ax.text(0.5, 0.5, 'No streak data', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create timeline
        y_pos = 0
        for i, streak in enumerate(all_streaks[-20:]):  # Show last 20 streaks
            color = 'green' if streak.streak_type.value == 'winning' else 'red'
            width = streak.length
            
            ax.barh(y_pos, width, left=i, height=0.8, color=color, alpha=0.7)
            ax.text(i + width/2, y_pos, f'{streak.length}', ha='center', va='center', fontweight='bold')
            
            y_pos += 1
        
        ax.set_title('Recent Streak Timeline', fontweight='bold')
        ax.set_xlabel('Streak Index')
        ax.set_ylabel('Streak')
        ax.set_yticks([])
        ax.grid(True, alpha=0.3)
    
    def _plot_current_streak_info(self, ax, streak_stats):
        """Plot current streak information."""
        
        current = streak_stats.get('current_streak')
        
        if not current:
            ax.text(0.5, 0.5, 'No current streak', ha='center', va='center', transform=ax.transAxes)
            return
        
        # Create info display
        ax.axis('off')
        
        info_text = f"""
CURRENT STREAK
{'='*25}

Type: {current['streak_type'].upper()}
Length: {current['length']} trades
P&L: ${current['current_pnl']:,.0f}
Avg/Trade: ${current['avg_trade_pnl']:,.0f}
Started: {current['start_date'].strftime('%Y-%m-%d')}
Days Active: {current['days_in_streak']}

Recent Trades:
"""
        
        for trade in current['recent_trades'][-5:]:
            info_text += f"  {trade['ticker']}: ${trade['pnl']:,.0f}\n"
        
        ax.text(0.1, 0.9, info_text, transform=ax.transAxes, fontsize=10,
               verticalalignment='top', fontfamily='monospace',
               bbox=dict(boxstyle='round', facecolor='lightblue' if current['streak_type'] == 'winning' else 'lightcoral', alpha=0.8))
        
        ax.set_title('Current Streak Info', fontweight='bold', pad=20)


def main():
    """Main function to create dashboards."""
    
    print("Creating Performance Dashboard...")
    
    # Initialize analyzer
    analyzer = StrategyPerformanceAnalyzer()
    
    # Load data
    if not analyzer.load_historical_data():
        return
    
    # Calculate regimes
    if not analyzer.calculate_regimes():
        return
    
    # Simulate strategy
    analyzer.simulate_bear_expansion_strategy()
    
    # Create dashboard
    dashboard = PerformanceDashboard(analyzer)
    
    # Create performance overview
    dashboard.create_performance_overview('performance_dashboard.png')
    
    # Create streak dashboard
    dashboard.create_streak_dashboard('streak_dashboard.png')
    
    print("Dashboards created successfully!")


if __name__ == "__main__":
    main()
