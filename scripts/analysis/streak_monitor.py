"""
Streak Monitor - Real-time streak detection and alerting system.

Tracks winning/losing streaks and provides alerts when significant streaks are detected.
Can be integrated into live trading systems for real-time monitoring.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import sys
from pathlib import Path
import json
from dataclasses import dataclass
from enum import Enum

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))


class StreakType(Enum):
    WINNING = "winning"
    LOSING = "losing"
    NEUTRAL = "neutral"


@dataclass
class StreakAlert:
    """Alert configuration for streak monitoring."""
    streak_type: StreakType
    min_length: int
    message_template: str
    severity: str  # "info", "warning", "critical"
    enabled: bool = True


@dataclass
class CurrentStreak:
    """Current streak information."""
    streak_type: StreakType
    length: int
    start_date: datetime
    current_pnl: float
    trades: List[Dict[str, Any]]
    avg_trade_pnl: float


class StreakMonitor:
    """
    Real-time streak monitoring and alerting system.
    """
    
    def __init__(self, db_path: str = "data/alpha.db", config_file: str = "streak_config.json"):
        self.db_path = db_path
        self.config_file = config_file
        self.current_streak = None
        self.streak_history = []
        self.alerts = []
        self.load_config()
        
    def load_config(self):
        """Load streak monitoring configuration."""
        
        default_config = {
            "alerts": [
                {
                    "streak_type": "winning",
                    "min_length": 3,
                    "message_template": "Winning streak of {length} trades! Total P&L: ${pnl:,.0f}",
                    "severity": "info",
                    "enabled": True
                },
                {
                    "streak_type": "winning",
                    "min_length": 5,
                    "message_template": "EXCELLENT: Winning streak of {length} trades! Total P&L: ${pnl:,.0f}",
                    "severity": "warning",
                    "enabled": True
                },
                {
                    "streak_type": "winning",
                    "min_length": 8,
                    "message_template": "OUTSTANDING: Winning streak of {length} trades! Total P&L: ${pnl:,.0f}",
                    "severity": "critical",
                    "enabled": True
                },
                {
                    "streak_type": "losing",
                    "min_length": 3,
                    "message_template": "Losing streak of {length} trades. Total P&L: ${pnl:,.0f}",
                    "severity": "warning",
                    "enabled": True
                },
                {
                    "streak_type": "losing",
                    "min_length": 5,
                    "message_template": "CRITICAL: Losing streak of {length} trades! Total P&L: ${pnl:,.0f}",
                    "severity": "critical",
                    "enabled": True
                }
            ],
            "monitoring": {
                "max_streak_history": 100,
                "pnl_threshold": 1000,  # Alert if P&L exceeds this amount
                "consecutive_days_threshold": 3  # Alert if streak spans multiple days
            }
        }
        
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                self.config = config
        except FileNotFoundError:
            self.config = default_config
            self.save_config()
        
        # Convert config to alert objects
        self.alert_configs = []
        for alert_config in self.config.get('alerts', []):
            alert = StreakAlert(
                streak_type=StreakType(alert_config['streak_type']),
                min_length=alert_config['min_length'],
                message_template=alert_config['message_template'],
                severity=alert_config['severity'],
                enabled=alert_config.get('enabled', True)
            )
            self.alert_configs.append(alert)
    
    def save_config(self):
        """Save current configuration to file."""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2, default=str)
    
    def add_trade(self, trade_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Add a new trade and check for streaks and alerts.
        
        Args:
            trade_data: Dictionary containing trade information
                       Required keys: 'pnl', 'entry_date', 'ticker', 'exit_date'
        
        Returns:
            List of alerts triggered by this trade
        """
        
        pnl = trade_data.get('pnl', 0)
        is_win = pnl > 0
        entry_date = pd.to_datetime(trade_data.get('entry_date', datetime.now()))
        
        # Initialize current streak if None
        if self.current_streak is None:
            streak_type = StreakType.WINNING if is_win else StreakType.LOSING if pnl < 0 else StreakType.NEUTRAL
            self.current_streak = CurrentStreak(
                streak_type=streak_type,
                length=1,
                start_date=entry_date,
                current_pnl=pnl,
                trades=[trade_data],
                avg_trade_pnl=pnl
            )
            return []
        
        # Check if streak continues
        if (self.current_streak.streak_type == StreakType.WINNING and is_win) or \
           (self.current_streak.streak_type == StreakType.LOSING and pnl < 0):
            # Continue current streak
            self.current_streak.length += 1
            self.current_streak.current_pnl += pnl
            self.current_streak.trades.append(trade_data)
            self.current_streak.avg_trade_pnl = self.current_streak.current_pnl / self.current_streak.length
        else:
            # End current streak and start new one
            self.streak_history.append(self.current_streak)
            
            # Limit history size
            max_history = self.config.get('monitoring', {}).get('max_streak_history', 100)
            if len(self.streak_history) > max_history:
                self.streak_history = self.streak_history[-max_history:]
            
            # Start new streak
            new_streak_type = StreakType.WINNING if is_win else StreakType.LOSING if pnl < 0 else StreakType.NEUTRAL
            self.current_streak = CurrentStreak(
                streak_type=new_streak_type,
                length=1,
                start_date=entry_date,
                current_pnl=pnl,
                trades=[trade_data],
                avg_trade_pnl=pnl
            )
        
        # Check for alerts
        return self.check_alerts()
    
    def check_alerts(self) -> List[Dict[str, Any]]:
        """Check if current streak triggers any alerts."""
        
        triggered_alerts = []
        
        if self.current_streak is None:
            return triggered_alerts
        
        for alert_config in self.alert_configs:
            if not alert_config.enabled:
                continue
            
            if (alert_config.streak_type == self.current_streak.streak_type and
                self.current_streak.length >= alert_config.min_length):
                
                # Check if we already sent this alert for this streak length
                already_sent = any(
                    alert['streak_length'] == self.current_streak.length and
                    alert['alert_type'] == alert_config.streak_type.value
                    for alert in self.alerts[-10:]  # Check last 10 alerts
                )
                
                if not already_sent:
                    alert_message = alert_config.message_template.format(
                        length=self.current_streak.length,
                        pnl=self.current_streak.current_pnl,
                        avg_trade=self.current_streak.avg_trade_pnl,
                        start_date=self.current_streak.start_date.strftime('%Y-%m-%d')
                    )
                    
                    alert = {
                        'timestamp': datetime.now(),
                        'alert_type': alert_config.streak_type.value,
                        'streak_length': self.current_streak.length,
                        'message': alert_message,
                        'severity': alert_config.severity,
                        'current_pnl': self.current_streak.current_pnl,
                        'avg_trade_pnl': self.current_streak.avg_trade_pnl,
                        'start_date': self.current_streak.start_date,
                        'trades_in_streak': self.current_streak.length
                    }
                    
                    triggered_alerts.append(alert)
                    self.alerts.append(alert)
        
        # Check P&L threshold alert
        pnl_threshold = self.config.get('monitoring', {}).get('pnl_threshold', 1000)
        if abs(self.current_streak.current_pnl) >= pnl_threshold:
            threshold_alert = {
                'timestamp': datetime.now(),
                'alert_type': 'pnl_threshold',
                'message': f"P&L threshold exceeded: ${self.current_streak.current_pnl:,.0f}",
                'severity': 'critical' if self.current_streak.current_pnl < 0 else 'warning',
                'current_pnl': self.current_streak.current_pnl,
                'streak_length': self.current_streak.length
            }
            
            # Avoid duplicate threshold alerts
            if not any(alert.get('alert_type') == 'pnl_threshold' for alert in self.alerts[-5:]):
                triggered_alerts.append(threshold_alert)
                self.alerts.append(threshold_alert)
        
        return triggered_alerts
    
    def get_current_streak_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the current streak."""
        
        if self.current_streak is None:
            return None
        
        return {
            'streak_type': self.current_streak.streak_type.value,
            'length': self.current_streak.length,
            'start_date': self.current_streak.start_date,
            'current_pnl': self.current_streak.current_pnl,
            'avg_trade_pnl': self.current_streak.avg_trade_pnl,
            'days_in_streak': (datetime.now() - self.current_streak.start_date).days,
            'recent_trades': [
                {
                    'ticker': trade.get('ticker', 'Unknown'),
                    'pnl': trade.get('pnl', 0),
                    'entry_date': trade.get('entry_date'),
                    'exit_date': trade.get('exit_date')
                }
                for trade in self.current_streak.trades[-5:]  # Last 5 trades
            ]
        }
    
    def get_streak_statistics(self) -> Dict[str, Any]:
        """Get statistics about all streaks."""
        
        if not self.streak_history and self.current_streak is None:
            return {}
        
        all_streaks = self.streak_history.copy()
        if self.current_streak is not None:
            all_streaks.append(self.current_streak)
        
        winning_streaks = [s for s in all_streaks if s.streak_type == StreakType.WINNING]
        losing_streaks = [s for s in all_streaks if s.streak_type == StreakType.LOSING]
        
        stats = {
            'total_streaks': len(all_streaks),
            'winning_streaks': len(winning_streaks),
            'losing_streaks': len(losing_streaks),
            'current_streak': self.get_current_streak_info(),
            'longest_winning': max(winning_streaks, key=lambda x: x.length) if winning_streaks else None,
            'longest_losing': max(losing_streaks, key=lambda x: x.length) if losing_streaks else None,
            'most_profitable': max(winning_streaks, key=lambda x: x.current_pnl) if winning_streaks else None,
            'worst_losing': min(losing_streaks, key=lambda x: x.current_pnl) if losing_streaks else None,
            'avg_winning_length': np.mean([s.length for s in winning_streaks]) if winning_streaks else 0,
            'avg_losing_length': np.mean([s.length for s in losing_streaks]) if losing_streaks else 0
        }
        
        # Convert streak objects to dictionaries for serialization
        if stats['longest_winning']:
            stats['longest_winning'] = {
                'length': stats['longest_winning'].length,
                'pnl': stats['longest_winning'].current_pnl,
                'start_date': stats['longest_winning'].start_date,
                'avg_trade_pnl': stats['longest_winning'].avg_trade_pnl
            }
        
        if stats['longest_losing']:
            stats['longest_losing'] = {
                'length': stats['longest_losing'].length,
                'pnl': stats['longest_losing'].current_pnl,
                'start_date': stats['longest_losing'].start_date,
                'avg_trade_pnl': stats['longest_losing'].avg_trade_pnl
            }
        
        if stats['most_profitable']:
            stats['most_profitable'] = {
                'length': stats['most_profitable'].length,
                'pnl': stats['most_profitable'].current_pnl,
                'start_date': stats['most_profitable'].start_date,
                'avg_trade_pnl': stats['most_profitable'].avg_trade_pnl
            }
        
        if stats['worst_losing']:
            stats['worst_losing'] = {
                'length': stats['worst_losing'].length,
                'pnl': stats['worst_losing'].current_pnl,
                'start_date': stats['worst_losing'].start_date,
                'avg_trade_pnl': stats['worst_losing'].avg_trade_pnl
            }
        
        return stats
    
    def get_recent_alerts(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent alerts."""
        return self.alerts[-limit:] if self.alerts else []
    
    def reset_monitoring(self):
        """Reset the monitoring state."""
        self.current_streak = None
        self.streak_history = []
        self.alerts = []
    
    def export_data(self, filename: str = None) -> str:
        """Export streak data to JSON file."""
        
        if filename is None:
            filename = f"streak_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        export_data = {
            'export_timestamp': datetime.now().isoformat(),
            'current_streak': self.get_current_streak_info(),
            'streak_statistics': self.get_streak_statistics(),
            'recent_alerts': self.get_recent_alerts(),
            'configuration': self.config
        }
        
        # Convert streak history to serializable format
        serializable_history = []
        for streak in self.streak_history:
            serializable_history.append({
                'streak_type': streak.streak_type.value,
                'length': streak.length,
                'start_date': streak.start_date.isoformat(),
                'current_pnl': streak.current_pnl,
                'avg_trade_pnl': streak.avg_trade_pnl,
                'trade_count': len(streak.trades)
            })
        
        export_data['streak_history'] = serializable_history
        
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        return filename


def demo_streak_monitor():
    """Demonstrate the streak monitor with sample trades."""
    
    print("Streak Monitor Demo")
    print("=" * 50)
    
    monitor = StreakMonitor()
    
    # Sample trades
    sample_trades = [
        {'pnl': 500, 'ticker': 'AAPL', 'entry_date': '2024-01-01', 'exit_date': '2024-01-02'},
        {'pnl': 300, 'ticker': 'MSFT', 'entry_date': '2024-01-02', 'exit_date': '2024-01-03'},
        {'pnl': -200, 'ticker': 'GOOGL', 'entry_date': '2024-01-03', 'exit_date': '2024-01-04'},
        {'pnl': -150, 'ticker': 'AMZN', 'entry_date': '2024-01-04', 'exit_date': '2024-01-05'},
        {'pnl': -100, 'ticker': 'META', 'entry_date': '2024-01-05', 'exit_date': '2024-01-06'},
        {'pnl': 600, 'ticker': 'TSLA', 'entry_date': '2024-01-06', 'exit_date': '2024-01-07'},
        {'pnl': 400, 'ticker': 'NVDA', 'entry_date': '2024-01-07', 'exit_date': '2024-01-08'},
        {'pnl': 350, 'ticker': 'AMD', 'entry_date': '2024-01-08', 'exit_date': '2024-01-09'},
        {'pnl': 200, 'ticker': 'INTC', 'entry_date': '2024-01-09', 'exit_date': '2024-01-10'},
    ]
    
    # Process trades
    for i, trade in enumerate(sample_trades, 1):
        print(f"\nProcessing Trade {i}: {trade['ticker']} - P&L: ${trade['pnl']}")
        
        alerts = monitor.add_trade(trade)
        
        if alerts:
            print("ALERTS TRIGGERED:")
            for alert in alerts:
                print(f"  [{alert['severity'].upper()}] {alert['message']}")
        
        current = monitor.get_current_streak_info()
        if current:
            print(f"Current Streak: {current['streak_type']} - {current['length']} trades, P&L: ${current['current_pnl']:,.0f}")
    
    # Show final statistics
    print("\n" + "=" * 50)
    print("FINAL STREAK STATISTICS")
    print("=" * 50)
    
    stats = monitor.get_streak_statistics()
    
    print(f"Total Streaks: {stats.get('total_streaks', 0)}")
    print(f"Winning Streaks: {stats.get('winning_streaks', 0)}")
    print(f"Losing Streaks: {stats.get('losing_streaks', 0)}")
    
    if stats.get('longest_winning'):
        lw = stats['longest_winning']
        print(f"Longest Winning: {lw['length']} trades (${lw['pnl']:,.0f})")
    
    if stats.get('longest_losing'):
        ll = stats['longest_losing']
        print(f"Longest Losing: {ll['length']} trades (${ll['pnl']:,.0f})")
    
    # Export data
    export_file = monitor.export_data()
    print(f"\nData exported to: {export_file}")


if __name__ == "__main__":
    demo_streak_monitor()
