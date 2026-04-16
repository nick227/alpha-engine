"""
Early Trade Validator

Immediate checks for first 10 trades to catch structural issues early.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EarlyTradeValidator:
    """
    Immediate validation for first 10 trades.
    """
    
    def __init__(self):
        self.db_path = "data/paper_trading.db"
        
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
        
        return df
    
    def validate_first_10_trades(self) -> Dict[str, Any]:
        """
        Validate first 10 trades for structural issues.
        """
        
        trades_df = self.get_trades_data()
        
        if len(trades_df) == 0:
            return {
                'status': 'no_trades',
                'message': 'No trades yet',
                'issues': [],
                'trades_checked': 0,
                'summary': {}
            }
        
        first_10 = trades_df.head(10)
        issues = []
        
        # Check 1: Large losses in a row
        consecutive_losses = 0
        max_consecutive_losses = 0
        
        for _, trade in first_10.iterrows():
            if trade['realized_pnl'] < 0:
                consecutive_losses += 1
                max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)
            else:
                consecutive_losses = 0
        
        if max_consecutive_losses >= 3:
            issues.append({
                'type': 'consecutive_losses',
                'severity': 'critical',
                'message': f'{max_consecutive_losses} consecutive losses in first {len(first_10)} trades'
            })
        
        # Check 2: Entry behavior (0.30-0.40 zone)
        out_of_band_entries = first_10[
            (first_10['position_in_range'] < 0.30) | 
            (first_10['position_in_range'] > 0.40)
        ]
        
        if len(out_of_band_entries) > 0:
            issues.append({
                'type': 'entry_out_of_band',
                'severity': 'critical',
                'message': f'{len(out_of_band_entries)} trades outside 0.30-0.40 entry band'
            })
        
        # Check 3: Winners larger than losers
        winners = first_10[first_10['realized_pnl'] > 0]
        losers = first_10[first_10['realized_pnl'] < 0]
        
        if len(winners) > 0 and len(losers) > 0:
            avg_win = winners['realized_pnl'].mean()
            avg_loss = abs(losers['realized_pnl'].mean())
            
            if avg_win < avg_loss:
                issues.append({
                    'type': 'win_loss_ratio',
                    'severity': 'warning',
                    'message': f'Avg win ${avg_win:,.0f} < Avg loss ${avg_loss:,.0f}'
                })
        
        # Check 4: Position count violations (would need to check active positions)
        # This is a placeholder - would need real-time position tracking
        
        # Check 5: Trade frequency (event-driven system)
        if len(first_10) >= 5:
            time_span = (first_10['entry_time'].max() - first_10['entry_time'].min()).days
            if time_span > 0:
                freq = len(first_10) / time_span
                if freq > 2.0:  # Too frequent for event-driven
                    issues.append({
                        'type': 'excessive_frequency',
                        'severity': 'warning',
                        'message': f'Trade frequency {freq:.1f}/day (too high for event-driven)'
                    })
        
        # Determine overall status
        critical_issues = [i for i in issues if i['severity'] == 'critical']
        warning_issues = [i for i in issues if i['severity'] == 'warning']
        
        if critical_issues:
            status = 'critical_issues'
            message = f'{len(critical_issues)} critical issues found - STOP IMMEDIATELY'
        elif warning_issues:
            status = 'warnings'
            message = f'{len(warning_issues)} warnings - monitor closely'
        else:
            status = 'healthy'
            message = 'No structural issues detected'
        
        return {
            'status': status,
            'message': message,
            'trades_checked': len(first_10),
            'issues': issues,
            'summary': {
                'consecutive_losses': max_consecutive_losses,
                'out_of_band_entries': len(out_of_band_entries),
                'avg_win': winners['realized_pnl'].mean() if len(winners) > 0 else 0,
                'avg_loss': abs(losers['realized_pnl'].mean()) if len(losers) > 0 else 0
            }
        }
    
    def print_early_validation(self):
        """Print early validation results"""
        
        results = self.validate_first_10_trades()
        
        print(f"\n=== EARLY TRADE VALIDATION ===")
        print(f"Trades Checked: {results['trades_checked']}")
        print(f"Status: {results['status'].upper()}")
        print(f"Message: {results['message']}")
        
        if results['issues']:
            print(f"\nIssues Found:")
            for issue in results['issues']:
                severity_symbol = "!!!" if issue['severity'] == 'critical' else "!"
                print(f"  {severity_symbol} {issue['type']}: {issue['message']}")
        else:
            print(f"\nNo issues detected")
        
        if results['summary']:
            summary = results['summary']
            print(f"\nSummary:")
            print(f"  Max consecutive losses: {summary['consecutive_losses']}")
            print(f"  Out-of-band entries: {summary['out_of_band_entries']}")
            print(f"  Avg win: ${summary['avg_win']:,.0f}")
            print(f"  Avg loss: ${summary['avg_loss']:,.0f}")
        
        # Recommendation
        print(f"\nRecommendation:")
        if results['status'] == 'critical_issues':
            print("  STOP TRADING IMMEDIATELY - Fix structural issues")
        elif results['status'] == 'warnings':
            print("  Monitor closely - May need adjustment")
        else:
            print("  Continue trading - Behavior looks correct")


def main():
    """Main function for early trade validation"""
    
    validator = EarlyTradeValidator()
    
    print("Early Trade Validator")
    print("Immediate checks for first 10 trades\n")
    
    validator.print_early_validation()


if __name__ == "__main__":
    main()
