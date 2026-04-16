"""
Debug Trade Invariants

Fastest way to isolate the bug by inspecting data invariants.
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


class DebugTradeInvariants:
    """
    Debug trade invariants to isolate the bug.
    """
    
    def __init__(self):
        self.df = None
        self.regime_data = None
        
    def load_data(self):
        """Load historical data with technical indicators"""
        
        print("Loading historical data for debug...")
        
        conn = sqlite3.connect("data/alpha.db")
        
        # Get raw price data
        query = """
        SELECT 
            ticker, 
            date, 
            close,
            volume,
            open,
            high,
            low
        FROM price_data 
        ORDER BY ticker, date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if len(df) == 0:
            print("ERROR: No historical data found")
            return False
        
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate technical indicators for each ticker
        result_dfs = []
        
        for ticker in df['ticker'].unique():
            ticker_data = df[df['ticker'] == ticker].copy()
            ticker_data = ticker_data.sort_values('date')
            
            if len(ticker_data) < 200:
                continue
            
            # Calculate moving averages
            ticker_data['ma50'] = ticker_data['close'].rolling(window=50, min_periods=50).mean()
            ticker_data['ma200'] = ticker_data['close'].rolling(window=200, min_periods=200).mean()
            
            # Calculate ATR
            ticker_data['prev_close'] = ticker_data['close'].shift(1)
            ticker_data['tr'] = np.maximum.reduce([
                ticker_data['high'] - ticker_data['low'],
                np.abs(ticker_data['high'] - ticker_data['prev_close']),
                np.abs(ticker_data['low'] - ticker_data['prev_close'])
            ])
            ticker_data['atr'] = ticker_data['tr'].rolling(window=14, min_periods=14).mean()
            
            # Calculate position in range
            ticker_data['high_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).max()
            ticker_data['low_20d'] = ticker_data['close'].rolling(window=20, min_periods=20).min()
            ticker_data['position_in_range'] = (
                (ticker_data['close'] - ticker_data['low_20d']) / 
                (ticker_data['high_20d'] - ticker_data['low_20d'])
            )
            
            result_dfs.append(ticker_data)
        
        if not result_dfs:
            print("ERROR: No data after processing")
            return False
        
        # Combine all data
        self.df = pd.concat(result_dfs, ignore_index=True)
        
        # Filter out rows with missing indicators
        self.df = self.df.dropna(subset=['ma50', 'ma200', 'atr', 'position_in_range'])
        
        print(f"Prepared {len(self.df)} rows of data with indicators")
        
        return True
    
    def calculate_regimes(self):
        """Calculate regimes for all data"""
        
        print("Calculating regimes...")
        
        regime_data = []
        
        for ticker in self.df['ticker'].unique():
            ticker_data = self.df[self.df['ticker'] == ticker].copy()
            
            for idx, row in ticker_data.iterrows():
                if idx < 200:
                    continue
                
                # Get historical ATR for percentile calculation
                historical_atr = ticker_data.iloc[:idx]['atr'].dropna().tolist()
                
                if len(historical_atr) < 20:
                    continue
                
                # Calculate regime
                price_vs_ma50 = (row['close'] - row['ma50']) / row['ma50']
                ma50_vs_ma200 = (row['ma50'] - row['ma200']) / row['ma200']
                
                # Trend regime
                if price_vs_ma50 > 0.02 and ma50_vs_ma200 > 0.02:
                    trend_regime = "BULL"
                elif price_vs_ma50 < -0.02 and ma50_vs_ma200 < -0.02:
                    trend_regime = "BEAR"
                else:
                    trend_regime = "CHOP"
                
                # Volatility regime
                atr_percentile = sum(1 for x in historical_atr if x <= row['atr']) / len(historical_atr)
                
                if atr_percentile >= 0.8:
                    volatility_regime = "EXPANSION"
                elif atr_percentile <= 0.2:
                    volatility_regime = "COMPRESSION"
                else:
                    volatility_regime = "NORMAL"
                
                # Store regime data
                regime_data.append({
                    'ticker': ticker,
                    'date': row['date'],
                    'close': row['close'],
                    'high': row['high'],
                    'low': row['low'],
                    'atr': row['atr'],
                    'position_in_range': row['position_in_range'],
                    'trend_regime': trend_regime,
                    'volatility_regime': volatility_regime,
                    'is_bear_expansion': (
                        trend_regime == "BEAR" and 
                        volatility_regime == "EXPANSION"
                    )
                })
        
        self.regime_data = pd.DataFrame(regime_data)
        print(f"Calculated regimes for {len(self.regime_data)} data points")
        
        return True
    
    def get_ticker_sector(self, ticker: str) -> str:
        """Simple sector classification based on ticker patterns"""
        
        # Tech sector
        if ticker in ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'ADBE', 'CRM']:
            return 'TECH'
        
        # Financial sector
        elif ticker in ['JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'AIG']:
            return 'FINANCIAL'
        
        # Healthcare sector
        elif ticker in ['JNJ', 'PFE', 'UNH', 'ABT', 'MRK', 'CVS', 'MDT']:
            return 'HEALTHCARE'
        
        # Consumer sector
        elif ticker in ['WMT', 'HD', 'MCD', 'NKE', 'KO', 'PEP', 'COST']:
            return 'CONSUMER'
        
        # Industrial sector
        elif ticker in ['BA', 'CAT', 'GE', 'MMM', 'UPS', 'HON']:
            return 'INDUSTRIAL'
        
        # Energy sector
        elif ticker in ['XOM', 'CVX', 'COP', 'SLB', 'HAL']:
            return 'ENERGY'
        
        # Telecom sector
        elif ticker in ['T', 'VZ', 'TMUS']:
            return 'TELECOM'
        
        # Default
        else:
            return 'OTHER'
    
    def check_volatility_timing(self, ticker: str, current_atr: float, current_date: datetime, 
                              price_history: Dict[str, List[Dict]], lookback_days: int = 5, 
                              min_increase_pct: float = 0.05) -> bool:
        """
        Check volatility timing confirmation.
        """
        
        if ticker not in price_history:
            return False
        
        # Get recent ATR values
        recent_data = [
            entry for entry in price_history[ticker]
            if (current_date - entry['date']).days <= lookback_days
        ]
        
        if len(recent_data) < 3:
            return False
        
        # Calculate average ATR over lookback period
        avg_atr = np.mean([entry['atr'] for entry in recent_data])
        
        # Check if current ATR is significantly higher
        return current_atr > avg_atr * (1 + min_increase_pct)
    
    def generate_trades_for_debug(self):
        """
        Generate trades for debugging invariants.
        """
        
        print("Generating trades for debug...")
        
        # Strategy parameters
        min_range = 0.30
        max_range = 0.40
        stop_multiplier = 1.25
        target_multiplier = 2.5
        max_positions_total = 2
        max_positions_per_sector = 1
        portfolio_heat = 0.08
        volatility_lookback = 5
        volatility_increase = 0.05
        cooldown_days = 2
        
        # Filter for bear expansion entries
        entries = self.regime_data[self.regime_data['is_bear_expansion']].copy()
        
        # Apply position in range filter
        entries = entries[
            (entries['position_in_range'] >= min_range) &
            (entries['position_in_range'] <= max_range)
        ]
        
        # Sort by date
        entries = entries.sort_values('date')
        
        # Build price history for volatility timing
        price_history = {}
        for _, row in self.regime_data.iterrows():
            ticker = row['ticker']
            if ticker not in price_history:
                price_history[ticker] = []
            
            price_history[ticker].append({
                'date': row['date'],
                'price': row['close'],
                'atr': row['atr']
            })
            
            # Keep only recent data (last 30 days)
            cutoff_date = row['date'] - pd.Timedelta(days=30)
            price_history[ticker] = [
                entry for entry in price_history[ticker]
                if entry['date'] > cutoff_date
            ]
        
        # Simulation variables
        capital = 100000.0
        positions = {}  # Active positions
        trades = []  # Completed trades
        trade_id_counter = 0
        last_entry_dates = {}  # For cooldown tracking
        
        # Risk parameters
        risk_per_trade = portfolio_heat / max_positions_total
        
        # Simulate day by day
        trading_days = sorted(self.regime_data['date'].unique())
        
        for current_date in trading_days:
            # Get current day's data
            day_data = self.regime_data[self.regime_data['date'] == current_date]
            
            # Update existing positions
            positions_to_close = []
            
            for pos_id in list(positions.keys()):
                position = positions[pos_id]
                ticker = position['ticker']
                
                # Get current price
                current_price_data = day_data[day_data['ticker'] == ticker]
                if len(current_price_data) == 0:
                    continue
                
                current_price = current_price_data.iloc[0]['close']
                high_price = current_price_data.iloc[0]['high']
                low_price = current_price_data.iloc[0]['low']
                
                # Check exit conditions
                entry_price = position['entry_price']
                atr = position['atr']
                stop_loss = position['stop_loss']
                target_price = position['target_price']
                
                # For short positions
                should_exit = False
                exit_reason = ""
                exit_price = current_price
                
                # Stop loss hit
                if high_price >= stop_loss:
                    should_exit = True
                    exit_reason = "stop_loss"
                    exit_price = stop_loss
                
                # Target hit
                elif low_price <= target_price:
                    should_exit = True
                    exit_reason = "target_reached"
                    exit_price = target_price
                
                # Max hold period
                elif (current_date - position['entry_date']).days >= 7:
                    should_exit = True
                    exit_reason = "max_hold"
                    exit_price = current_price
                
                if should_exit:
                    positions_to_close.append((pos_id, exit_price, exit_reason))
            
            # Close positions
            for pos_id, exit_price, exit_reason in positions_to_close:
                position = positions[pos_id]
                
                # Calculate P&L for short position
                pnl = position['quantity'] * (position['entry_price'] - exit_price)
                capital += pnl
                
                # Add to trades
                trade_id_counter += 1
                trades.append({
                    'trade_id': trade_id_counter,
                    'ticker': position['ticker'],
                    'sector': position['sector'],
                    'entry_time': position['entry_date'],
                    'exit_time': current_date,
                    'entry_price': position['entry_price'],
                    'exit_price': exit_price,
                    'quantity': position['quantity'],
                    'realized_pnl': pnl,
                    'exit_reason': exit_reason,
                    'hold_days': (current_date - position['entry_date']).days,
                    'position_in_range': position['position_in_range']
                })
                
                del positions[pos_id]
            
            # Look for new entries
            if len(positions) < max_positions_total:
                # Get potential entries for current date
                potential_entries = entries[entries['date'] == current_date]
                
                for _, entry in potential_entries.iterrows():
                    if len(positions) >= max_positions_total:
                        break
                    
                    ticker = entry['ticker']
                    
                    # Skip if already in position
                    if any(pos['ticker'] == ticker for pos in positions.values()):
                        continue
                    
                    # Check cooldown
                    if ticker in last_entry_dates:
                        days_since_entry = (current_date - last_entry_dates[ticker]).days
                        if days_since_entry < cooldown_days:
                            continue
                    
                    # Check sector caps
                    sector = self.get_ticker_sector(ticker)
                    sector_positions = [pos for pos in positions.values() if pos.get('sector') == sector]
                    if len(sector_positions) >= max_positions_per_sector:
                        continue
                    
                    # Check volatility timing
                    if not self.check_volatility_timing(
                        ticker, entry['atr'], current_date, 
                        price_history, volatility_lookback, volatility_increase
                    ):
                        continue
                    
                    # Calculate position size
                    atr = entry['atr']
                    entry_price = entry['close']
                    
                    # For short positions
                    stop_loss = entry_price + (atr * stop_multiplier)
                    target_price = entry_price - (atr * target_multiplier)
                    
                    # Calculate quantity based on risk
                    risk_amount = capital * risk_per_trade
                    risk_per_share = stop_loss - entry_price
                    quantity = risk_amount / risk_per_share if risk_per_share > 0 else 0
                    
                    if quantity <= 0:
                        continue
                    
                    # Create position
                    position = {
                        'ticker': ticker,
                        'sector': sector,
                        'entry_date': current_date,
                        'entry_price': entry_price,
                        'quantity': quantity,
                        'stop_loss': stop_loss,
                        'target_price': target_price,
                        'atr': atr,
                        'position_in_range': entry['position_in_range']
                    }
                    
                    positions[str(len(positions))] = position
                    last_entry_dates[ticker] = current_date
        
        print(f"Generated {len(trades)} trades for debug")
        
        return trades
    
    def run_debug_invariants(self, trades: List[Dict[str, Any]]):
        """
        Run debug invariants to isolate the bug.
        """
        
        print("Running debug invariants...")
        
        if not trades:
            print("ERROR: No trades to debug")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(trades)
        
        print(f"\n=== DEBUG INVARIANTS ===")
        print(f"Total trades: {len(df)}")
        print(f"Unique trade IDs: {df['trade_id'].nunique()}")
        
        # Step 1 — Detect duplicate trades (critical)
        print(f"\n=== STEP 1: DUPLICATE TRADES ===")
        duplicates = df.groupby(["ticker", "entry_time", "exit_time"]).size().sort_values(ascending=False).head(20)
        print("Duplicate counts:")
        print(duplicates)
        
        any_duplicates = (duplicates > 1).any()
        print(f"Any duplicates found: {any_duplicates}")
        
        # Step 2 — Check uniqueness of trade_id
        print(f"\n=== STEP 2: TRADE ID UNIQUENESS ===")
        unique_ids = df["trade_id"].nunique()
        total_trades = len(df)
        print(f"Unique trade IDs: {unique_ids}")
        print(f"Total trades: {total_trades}")
        print(f"IDs match exactly: {unique_ids == total_trades}")
        
        # Step 3 — Check P&L sanity per trade
        print(f"\n=== STEP 3: P&L SANITY ===")
        df["recomputed_pnl"] = df["quantity"] * (df["entry_price"] - df["exit_price"])
        df["pnl_error"] = df["realized_pnl"] - df["recomputed_pnl"]
        
        error_stats = df["pnl_error"].describe()
        print("P&L error statistics:")
        print(error_stats)
        
        max_error = abs(df["pnl_error"]).max()
        print(f"Maximum P&L error: ${max_error:,.2f}")
        print(f"P&L calculation correct: {max_error < 0.01}")
        
        # Step 4 — Identify dominating trades
        print(f"\n=== STEP 4: DOMINATING TRADES ===")
        top_trades = df.sort_values("realized_pnl", ascending=False).head(10)
        print("Top 10 trades by P&L:")
        print(top_trades[['trade_id', 'ticker', 'entry_time', 'exit_time', 'realized_pnl', 'pnl_error']])
        
        # Check for identical patterns
        identical_timestamps = df['entry_time'].duplicated().sum()
        identical_tickers = df['ticker'].duplicated().sum()
        identical_pnl = df['realized_pnl'].duplicated().sum()
        
        print(f"Identical entry timestamps: {identical_timestamps}")
        print(f"Identical tickers: {identical_tickers}")
        print(f"Identical P&L values: {identical_pnl}")
        
        # Step 5 — Validate total P&L consistency
        print(f"\n=== STEP 5: TOTAL P&L CONSISTENCY ===")
        total_pnl = df["realized_pnl"].sum()
        top5 = df.sort_values("realized_pnl", ascending=False).head(5)["realized_pnl"].sum()
        top5_ratio = top5 / total_pnl if total_pnl != 0 else 0
        
        print(f"Total P&L: ${total_pnl:,.2f}")
        print(f"Top 5 P&L: ${top5:,.2f}")
        print(f"Top 5 / Total ratio: {top5_ratio:.3f}")
        
        # Check positive vs negative breakdown
        positive_pnl = df[df["realized_pnl"] > 0]["realized_pnl"].sum()
        negative_pnl = df[df["realized_pnl"] < 0]["realized_pnl"].sum()
        
        print(f"\nPositive P&L total: ${positive_pnl:,.2f}")
        print(f"Negative P&L total: ${negative_pnl:,.2f}")
        print(f"Net P&L: ${total_pnl:,.2f}")
        
        # Check if losses are large enough to explain ratio
        if total_pnl > 0 and abs(negative_pnl) > 0:
            positive_to_negative_ratio = abs(positive_pnl / negative_pnl)
            print(f"Positive to negative ratio: {positive_to_negative_ratio:.2f}")
            
            if positive_to_negative_ratio < 3:
                print("⚠️  Losses are significant - may explain high concentration ratio")
            else:
                print("✅ Losses are small relative to wins")
        
        # Correct concentration metric
        if positive_pnl > 0:
            top5_corrected = (top5 / positive_pnl) * 100
            print(f"\n=== CORRECTED CONCENTRATION METRIC ===")
            print(f"Top 5 contribution to positive P&L: {top5_corrected:.1f}%")
            
            if top5_corrected <= 50:
                print("✅ CONTROLLED EDGE (corrected metric)")
            elif top5_corrected <= 80:
                print("⚠️  MODERATE CONCENTRATION (corrected metric)")
            else:
                print("❌ HIGH CONCENTRATION (corrected metric)")
        
        # Final assessment
        print(f"\n=== FINAL DEBUG ASSESSMENT ===")
        
        issues_found = []
        
        if any_duplicates:
            issues_found.append("DUPLICATE TRADES")
        
        if max_error >= 0.01:
            issues_found.append("P&L CALCULATION ERROR")
        
        if unique_ids != total_trades:
            issues_found.append("TRADE ID DUPLICATION")
        
        if top5_ratio > 1.0:
            issues_found.append("CONCENTRATION RATIO > 100%")
        
        if not issues_found:
            print("✅ NO ISSUES FOUND - Trade generation appears correct")
            print("   High concentration ratio may be due to losses reducing denominator")
        else:
            print(f"❌ ISSUES FOUND: {', '.join(issues_found)}")
            print("   Fix these issues before proceeding")
    
    def run_debug(self):
        """
        Run complete debug pipeline.
        """
        
        print("Debug Trade Invariants")
        print("Fastest way to isolate the bug\n")
        
        # Load data
        if not self.load_data():
            return
        
        # Calculate regimes
        if not self.calculate_regimes():
            return
        
        # Generate trades
        trades = self.generate_trades_for_debug()
        
        # Run debug invariants
        self.run_debug_invariants(trades)


def main():
    """Main debug function"""
    
    debugger = DebugTradeInvariants()
    debugger.run_debug()


if __name__ == "__main__":
    main()
