"""
Paper Trading Monitor

Live validation dashboard for Bear Expansion strategy.
Monitors key metrics during paper trading phase.
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


class PaperTradingMonitor:
    """
    Live validation monitor for paper trading.
    """
    
    def __init__(self):
        self.db_path = "data/paper_trading.db"
        self.setup_database()
        
    def setup_database(self):
        """Setup paper trading database"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create trades table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            sector TEXT NOT NULL,
            entry_time DATETIME NOT NULL,
            exit_time DATETIME,
            entry_price REAL NOT NULL,
            exit_price REAL,
            quantity REAL NOT NULL,
            realized_pnl REAL,
            exit_reason TEXT,
            hold_days INTEGER,
            position_in_range REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create daily metrics table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            date DATE PRIMARY KEY,
            total_pnl REAL,
            trade_count INTEGER,
            win_rate REAL,
            rolling_expectancy_20 REAL,
            top5_contribution REAL,
            sector_exposure TEXT,
            current_regime TEXT,
            max_drawdown REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        conn.commit()
        conn.close()
        
        print("Paper trading database setup complete")
    
    def get_ticker_sector(self, ticker: str) -> str:
        """Simple sector classification"""
        
        if ticker in ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'ADBE', 'CRM']:
            return 'TECH'
        elif ticker in ['JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'AIG']:
            return 'FINANCIAL'
        elif ticker in ['JNJ', 'PFE', 'UNH', 'ABT', 'MRK', 'CVS', 'MDT']:
            return 'HEALTHCARE'
        elif ticker in ['WMT', 'HD', 'MCD', 'NKE', 'KO', 'PEP', 'COST']:
            return 'CONSUMER'
        elif ticker in ['BA', 'CAT', 'GE', 'MMM', 'UPS', 'HON']:
            return 'INDUSTRIAL'
        elif ticker in ['XOM', 'CVX', 'COP', 'SLB', 'HAL']:
            return 'ENERGY'
        elif ticker in ['T', 'VZ', 'TMUS']:
            return 'TELECOM'
        else:
            return 'OTHER'
    
    def log_trade(self, ticker: str, entry_time: datetime, exit_time: datetime,
                  entry_price: float, exit_price: float, quantity: float,
                  exit_reason: str, position_in_range: float) -> int:
        """
        Log a completed paper trade.
        """
        
        sector = self.get_ticker_sector(ticker)
        realized_pnl = quantity * (entry_price - exit_price)  # Short position
        hold_days = (exit_time - entry_time).days
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT INTO paper_trades 
        (ticker, sector, entry_time, exit_time, entry_price, exit_price, 
         quantity, realized_pnl, exit_reason, hold_days, position_in_range)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ticker, sector, entry_time, exit_time, entry_price, exit_price,
              quantity, realized_pnl, exit_reason, hold_days, position_in_range))
        
        trade_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        logger.info(f"Logged paper trade {trade_id}: {ticker} P&L: ${realized_pnl:,.2f}")
        
        return trade_id
    
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
    
    def calculate_rolling_expectancy(self, trades_df: pd.DataFrame, window: int = 20) -> float:
        """Calculate rolling expectancy over last N trades"""
        
        if len(trades_df) < window:
            return trades_df['realized_pnl'].mean()
        
        return trades_df.tail(window)['realized_pnl'].mean()
    
    def calculate_concentration(self, trades_df: pd.DataFrame) -> float:
        """Calculate top 5 contribution to positive P&L"""
        
        if len(trades_df) == 0:
            return 0.0
        
        positive_trades = trades_df[trades_df['realized_pnl'] > 0]
        
        if len(positive_trades) == 0:
            return 0.0
        
        total_positive_pnl = positive_trades['realized_pnl'].sum()
        
        if total_positive_pnl == 0:
            return 0.0
        
        # Top 5 trades by absolute P&L
        top_5 = trades_df.nlargest(5, 'realized_pnl')['realized_pnl'].sum()
        
        return (top_5 / total_positive_pnl) * 100
    
    def calculate_sector_exposure(self, trades_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate sector exposure percentages"""
        
        if len(trades_df) == 0:
            return {}
        
        sector_pnl = trades_df.groupby('sector')['realized_pnl'].sum()
        total_pnl = abs(trades_df['realized_pnl'].sum())
        
        if total_pnl == 0:
            return {}
        
        return (sector_pnl.abs() / total_pnl * 100).round(1).to_dict()
    
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
    
    def update_daily_metrics(self, date: datetime = None):
        """Update daily metrics for dashboard"""
        
        if date is None:
            date = datetime.now().date()
        
        trades_df = self.get_trades_data()
        
        if len(trades_df) == 0:
            return
        
        # Calculate metrics
        total_pnl = trades_df['realized_pnl'].sum()
        trade_count = len(trades_df)
        win_rate = len(trades_df[trades_df['realized_pnl'] > 0]) / trade_count if trade_count > 0 else 0
        rolling_expectancy = self.calculate_rolling_expectancy(trades_df)
        top5_contribution = self.calculate_concentration(trades_df)
        sector_exposure = self.calculate_sector_exposure(trades_df)
        max_drawdown = self.calculate_max_drawdown(trades_df)
        
        # Store metrics
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT OR REPLACE INTO daily_metrics 
        (date, total_pnl, trade_count, win_rate, rolling_expectancy_20, 
         top5_contribution, sector_exposure, current_regime, max_drawdown)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (date, total_pnl, trade_count, win_rate, rolling_expectancy,
              top5_contribution, json.dumps(sector_exposure), "BEAR_EXPANSION", max_drawdown))
        
        conn.commit()
        conn.close()
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        
        trades_df = self.get_trades_data()
        
        if len(trades_df) == 0:
            return {
                'total_trades': 0,
                'total_pnl': 0,
                'win_rate': 0,
                'rolling_expectancy_20': 0,
                'top5_contribution': 0,
                'sector_exposure': {},
                'max_drawdown': 0,
                'last_trade_date': None
            }
        
        total_pnl = trades_df['realized_pnl'].sum()
        trade_count = len(trades_df)
        win_rate = len(trades_df[trades_df['realized_pnl'] > 0]) / trade_count
        rolling_expectancy = self.calculate_rolling_expectancy(trades_df)
        top5_contribution = self.calculate_concentration(trades_df)
        sector_exposure = self.calculate_sector_exposure(trades_df)
        max_drawdown = self.calculate_max_drawdown(trades_df)
        last_trade_date = trades_df['exit_time'].max()
        
        return {
            'total_trades': trade_count,
            'total_pnl': total_pnl,
            'win_rate': win_rate,
            'rolling_expectancy_20': rolling_expectancy,
            'top5_contribution': top5_contribution,
            'sector_exposure': sector_exposure,
            'max_drawdown': max_drawdown,
            'last_trade_date': last_trade_date
        }
    
    def print_dashboard(self):
        """Print current dashboard"""
        
        metrics = self.get_current_metrics()
        
        print(f"\n=== PAPER TRADING DASHBOARD ===")
        print(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"\nPERFORMANCE:")
        print(f"  Total Trades: {metrics['total_trades']}")
        print(f"  Total P&L: ${metrics['total_pnl']:,.2f}")
        print(f"  Win Rate: {metrics['win_rate']:.1%}")
        print(f"  Rolling 20-Trade Expectancy: ${metrics['rolling_expectancy_20']:,.0f}")
        print(f"  Max Drawdown: {metrics['max_drawdown']:.1f}%")
        
        print(f"\nCONCENTRATION:")
        print(f"  Top 5 Contribution: {metrics['top5_contribution']:.1f}%")
        
        print(f"\nSECTOR EXPOSURE:")
        for sector, exposure in metrics['sector_exposure'].items():
            print(f"  {sector}: {exposure:.1f}%")
        
        print(f"\nSTATUS:")
        if metrics['last_trade_date']:
            days_since_last = (datetime.now() - metrics['last_trade_date']).days
            print(f"  Last Trade: {days_since_last} days ago")
        else:
            print(f"  No trades yet")
        
        # Validation checks
        print(f"\nVALIDATION CHECKS:")
        
        if metrics['top5_contribution'] <= 50:
            print("  Concentration: CONTROLLED")
        elif metrics['top5_contribution'] <= 80:
            print("  Concentration: MODERATE")
        else:
            print("  Concentration: HIGH")
        
        if metrics['win_rate'] >= 0.5:
            print("  Win Rate: HEALTHY")
        else:
            print("  Win Rate: LOW")
        
        if metrics['rolling_expectancy_20'] > 0:
            print("  Expectancy: POSITIVE")
        else:
            print("  Expectancy: NEGATIVE")
        
        if metrics['max_drawdown'] <= 20:
            print("  Drawdown: ACCEPTABLE")
        else:
            print("  Drawdown: HIGH")
    
    def export_trade_log(self, filepath: str = "paper_trades.csv"):
        """Export trade log for analysis"""
        
        trades_df = self.get_trades_data()
        
        if len(trades_df) > 0:
            trades_df.to_csv(filepath, index=False)
            print(f"Trade log exported to {filepath}")
        else:
            print("No trades to export")


def main():
    """Main function for paper trading monitor"""
    
    monitor = PaperTradingMonitor()
    
    # Example usage
    print("Paper Trading Monitor")
    print("Live validation dashboard for Bear Expansion strategy\n")
    
    # Print current dashboard
    monitor.print_dashboard()
    
    # Update daily metrics
    monitor.update_daily_metrics()


if __name__ == "__main__":
    main()
