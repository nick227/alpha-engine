"""
Live Validation Rules

Hard rules and monitoring for paper trading validation phase.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LiveValidationRules:
    """
    Hard validation rules for paper trading phase.
    """
    
    def __init__(self):
        self.db_path = "data/paper_trading.db"
        self.failure_triggers = {
            'rolling_expectancy_negative': {'threshold': 0, 'window': 20, 'triggered': False},
            'win_rate_low': {'threshold': 0.40, 'window': 30, 'triggered': False},
            'concentration_high': {'threshold': 70, 'triggered': False},
            'drawdown_high': {'threshold': 20.0, 'triggered': False},
            'trade_frequency_low': {'threshold': 0.5, 'window_days': 14, 'triggered': False}
        }
        
    def get_trades_data(self) -> pd.DataFrame:
        """Get all paper trades data"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT trade_id, ticker, sector, entry_time, exit_time, 
               entry_price, exit_price, quantity, realized_pnl, 
               exit_reason, hold_days, position_in_range
        FROM paper_trades
        ORDER BY entry_time
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if len(df) > 0:
            df['entry_time'] = pd.to_datetime(df['entry_time'])
            df['exit_time'] = pd.to_datetime(df['exit_time'])
            df['R_multiple'] = df['realized_pnl'] / (df['quantity'] * 0.02 * df['entry_price'])  # Approximate R
        
        return df
    
    def calculate_rolling_expectancy(self, trades_df: pd.DataFrame, window: int = 20) -> float:
        """Calculate rolling expectancy over last N trades"""
        
        if len(trades_df) < window:
            return trades_df['realized_pnl'].mean()
        
        return trades_df.tail(window)['realized_pnl'].mean()
    
    def calculate_rolling_win_rate(self, trades_df: pd.DataFrame, window: int = 30) -> float:
        """Calculate rolling win rate over last N trades"""
        
        if len(trades_df) < window:
            wins = len(trades_df[trades_df['realized_pnl'] > 0])
            return wins / len(trades_df) if len(trades_df) > 0 else 0
        
        recent_trades = trades_df.tail(window)
        wins = len(recent_trades[recent_trades['realized_pnl'] > 0])
        return wins / len(recent_trades)
    
    def calculate_concentration(self, trades_df: pd.DataFrame) -> float:
        """Calculate top 5 contribution to positive P&L (CORRECTED)"""
        
        if len(trades_df) == 0:
            return 0.0
        
        positive_trades = trades_df[trades_df['realized_pnl'] > 0]
        
        if len(positive_trades) == 0:
            return 0.0
        
        total_positive_pnl = positive_trades['realized_pnl'].sum()
        
        if total_positive_pnl == 0:
            return 0.0
        
        # Top 5 trades by absolute P&L (positive P&L basis)
        top_5 = trades_df.nlargest(5, 'realized_pnl')['realized_pnl'].sum()
        
        # CRITICAL: Use positive P&L as denominator
        return (top_5 / total_positive_pnl) * 100
    
    def calculate_max_drawdown(self, trades_df: pd.DataFrame) -> float:
        """Calculate maximum drawdown from equity curve"""
        
        if len(trades_df) == 0:
            return 0.0
        
        # Build equity curve
        equity_curve = []
        running_capital = 100000.0  # Starting capital
        
        for _, trade in trades_df.iterrows():
            running_capital += trade['realized_pnl']
            equity_curve.append(running_capital)
        
        # Calculate drawdown
        max_drawdown = 0.0
        peak = 100000.0
        
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        return max_drawdown * 100
    
    def calculate_trade_frequency(self, trades_df: pd.DataFrame, window_days: int = 14) -> float:
        """Calculate trades per day over recent window"""
        
        if len(trades_df) == 0:
            return 0.0
        
        # Get trades in last window_days
        cutoff_date = datetime.now() - timedelta(days=window_days)
        recent_trades = trades_df[trades_df['exit_time'] >= cutoff_date]
        
        return len(recent_trades) / window_days
    
    def check_failure_triggers(self) -> Dict[str, Any]:
        """Check all failure triggers"""
        
        trades_df = self.get_trades_data()
        
        if len(trades_df) == 0:
            return {'status': 'no_trades', 'triggers': {}, 'total_trades': 0}
        
        results = {}
        
        # Check 1: Rolling expectancy negative
        rolling_exp = self.calculate_rolling_expectancy(trades_df, 20)
        if len(trades_df) < 15:
            results['rolling_expectancy_negative'] = {
                'triggered': False,
                'value': rolling_exp,
                'threshold': 0,
                'message': f"Insufficient sample ({len(trades_df)} trades, need 15+)"
            }
        elif len(trades_df) < 20 and rolling_exp < 0:
            results['rolling_expectancy_negative'] = {
                'triggered': False,  # Early warning, not failure
                'value': rolling_exp,
                'threshold': 0,
                'message': f"Early warning: Rolling expectancy is ${rolling_exp:,.0f} (negative, {len(trades_df)} trades)"
            }
        elif rolling_exp < 0:
            results['rolling_expectancy_negative'] = {
                'triggered': True,
                'value': rolling_exp,
                'threshold': 0,
                'message': f"Rolling 20-trade expectancy is ${rolling_exp:,.0f} (negative)"
            }
        else:
            results['rolling_expectancy_negative'] = {
                'triggered': False,
                'value': rolling_exp,
                'threshold': 0
            }
        
        # Check 2: Win rate low
        rolling_win_rate = self.calculate_rolling_win_rate(trades_df, 30)
        if len(trades_df) >= 30 and rolling_win_rate < 0.40:
            results['win_rate_low'] = {
                'triggered': True,
                'value': rolling_win_rate,
                'threshold': 0.40,
                'message': f"Rolling 30-trade win rate is {rolling_win_rate:.1%} (below 40%)"
            }
        else:
            results['win_rate_low'] = {
                'triggered': False,
                'value': rolling_win_rate,
                'threshold': 0.40
            }
        
        # Check 3: Concentration high
        concentration = self.calculate_concentration(trades_df)
        if concentration > 70:
            results['concentration_high'] = {
                'triggered': True,
                'value': concentration,
                'threshold': 70,
                'message': f"Top 5 concentration is {concentration:.1f}% (above 70%)"
            }
        else:
            results['concentration_high'] = {
                'triggered': False,
                'value': concentration,
                'threshold': 70
            }
        
        # Check 4: Drawdown high
        max_dd = self.calculate_max_drawdown(trades_df)
        if max_dd > 20:
            results['drawdown_high'] = {
                'triggered': True,
                'value': max_dd,
                'threshold': 20,
                'message': f"Max drawdown is {max_dd:.1f}% (above 20%)"
            }
        else:
            results['drawdown_high'] = {
                'triggered': False,
                'value': max_dd,
                'threshold': 20
            }
        
        # Check 5: Trade frequency low (event-driven system)
        trade_freq = self.calculate_trade_frequency(trades_df, 20)  # Use 20-day window for event-driven
        if len(trades_df) < 10:
            results['trade_frequency_low'] = {
                'triggered': False,
                'value': trade_freq,
                'threshold': 0.5,
                'message': f"Insufficient sample ({len(trades_df)} trades, need 10+ for frequency)"
            }
        elif trade_freq < 0.3:  # Lower threshold for event-driven system
            results['trade_frequency_low'] = {
                'triggered': True,
                'value': trade_freq,
                'threshold': 0.3,
                'message': f"Trade frequency is {trade_freq:.1f} trades/day (below 0.3)"
            }
        else:
            results['trade_frequency_low'] = {
                'triggered': False,
                'value': trade_freq,
                'threshold': 0.3
            }
        
        # Overall status
        any_triggered = any(result['triggered'] for result in results.values())
        
        return {
            'status': 'failure_triggered' if any_triggered else 'healthy',
            'triggers': results,
            'total_trades': len(trades_df)
        }
    
    def print_validation_status(self):
        """Print current validation status"""
        
        check_results = self.check_failure_triggers()
        
        print(f"\n=== LIVE VALIDATION STATUS ===")
        print(f"Total Trades: {check_results['total_trades']}")
        print(f"Overall Status: {check_results['status'].upper()}")
        
        if check_results['status'] == 'failure_triggered':
            print(f"\n!!! FAILURE TRIGGERS ACTIVATED !!!")
            print("STOP AND INVESTIGATE:")
            
            for trigger_name, trigger_data in check_results['triggers'].items():
                if trigger_data['triggered']:
                    print(f"  - {trigger_name}: {trigger_data['message']}")
        
        else:
            print(f"\nAll validation checks passed")
        
        print(f"\nDetailed Status:")
        for trigger_name, trigger_data in check_results['triggers'].items():
            status = "FAIL" if trigger_data['triggered'] else "PASS"
            print(f"  {trigger_name}: {status} (Value: {trigger_data['value']}, Threshold: {trigger_data['threshold']})")
    
    def log_validation_check(self):
        """Log validation check to database"""
        
        check_results = self.check_failure_triggers()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create validation log table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS validation_log (
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT,
            total_trades INTEGER,
            triggers TEXT
        )
        """)
        
        cursor.execute("""
        INSERT INTO validation_log (status, total_trades, triggers)
        VALUES (?, ?, ?)
        """, (check_results['status'], check_results['total_trades'], 
              json.dumps(check_results['triggers'])))
        
        conn.commit()
        conn.close()
    
    def should_stop_trading(self) -> Tuple[bool, str]:
        """
        Check if trading should be stopped based on failure triggers.
        
        Returns:
            (should_stop, reason)
        """
        
        check_results = self.check_failure_triggers()
        
        if check_results['status'] == 'failure_triggered':
            reasons = []
            for trigger_name, trigger_data in check_results['triggers'].items():
                if trigger_data['triggered']:
                    reasons.append(trigger_data['message'])
            
            return True, "; ".join(reasons)
        
        return False, "All checks passed"


def main():
    """Main function for live validation rules"""
    
    validator = LiveValidationRules()
    
    print("Live Validation Rules")
    print("Hard rules for paper trading validation\n")
    
    # Print current status
    validator.print_validation_status()
    
    # Log validation check
    validator.log_validation_check()
    
    # Check if should stop
    should_stop, reason = validator.should_stop_trading()
    
    if should_stop:
        print(f"\n!!! SHOULD STOP TRADING !!!")
        print(f"Reason: {reason}")
    else:
        print(f"\nTrading can continue")


if __name__ == "__main__":
    main()
