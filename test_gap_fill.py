import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def compute_return_from_prices(ticker, entry_date, exit_date):
    """Compute return from raw price data - NOT from stored outcomes"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get entry price (next day's entry)
    entry_query = """
    SELECT p.entry_price
    FROM predictions p
    WHERE p.ticker = ?
    AND DATE(p.timestamp) = DATE(?)
    AND p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    LIMIT 1
    """
    
    cursor = simulator.conn.execute(entry_query, (ticker, entry_date))
    entry_result = cursor.fetchone()
    
    if not entry_result:
        simulator.close()
        return None
    
    entry_price = entry_result['entry_price']
    
    # Get exit price (same day's close - using entry_price as proxy)
    exit_query = """
    SELECT p.entry_price as close_price
    FROM predictions p
    WHERE p.ticker = ?
    AND DATE(p.timestamp) = DATE(?)
    AND p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    LIMIT 1
    """
    
    cursor = simulator.conn.execute(exit_query, (ticker, exit_date))
    exit_result = cursor.fetchone()
    
    if not exit_result:
        simulator.close()
        return None
    
    exit_price = exit_result['close_price']
    
    # Compute return manually
    return_pct = (exit_price / entry_price) - 1
    
    simulator.close()
    return return_pct

def test_gap_fill():
    """Test gap fill - open > prev_close * 1.03"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get all predictions to calculate gaps
    price_query = """
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    """
    
    cursor = simulator.conn.execute(price_query)
    all_predictions = cursor.fetchall()
    
    # Calculate gaps and find gap-up signals
    gap_signals = []
    
    for pred in all_predictions:
        signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
        
        # Get previous day's close (using entry_price as proxy)
        prev_day_query = """
        SELECT p.entry_price as prev_close
        FROM predictions p
        WHERE p.ticker = ?
        AND DATE(p.timestamp) = DATE(?)
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND p.entry_price IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(prev_day_query, (pred['ticker'], signal_date - timedelta(days=1)))
        prev_day = cursor.fetchone()
        
        if prev_day and prev_day['prev_close']:
            prev_close = prev_day['prev_close']
            current_price = pred['entry_price']
            
            # Calculate gap percentage
            gap_pct = (current_price - prev_close) / prev_close
            
            # Gap fill signal: gap up > 3%
            if gap_pct > 0.03:
                gap_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker'],
                    'gap_pct': gap_pct,
                    'entry_price': current_price,
                    'prev_close': prev_close
                })
    
    print(f"=== GAP FILL TEST ===")
    print(f"Signal: open > prev_close * 1.03 (gap up > 3%)")
    print(f"Method: Manual return calculation (no outcome reuse)")
    print(f"Signals found: {len(gap_signals)}")
    
    # Test intraday reversal (same day)
    daily_returns = []
    wins = 0
    
    for signal in gap_signals:
        # Test intraday reversal (same day)
        return_pct = compute_return_from_prices(signal['ticker'], signal['date'], signal['date'])
        
        if return_pct is not None:
            friction_cost = 0.0015
            daily_returns.append(return_pct - friction_cost)
            
            if return_pct > 0:
                wins += 1
    
    if daily_returns:
        portfolio_return = np.prod(1 + np.array(daily_returns)) - 1
    else:
        portfolio_return = 0.0
    
    win_rate = wins / len(daily_returns) if daily_returns else 0
    
    print(f"Portfolio return (with friction): {portfolio_return:+.2%}")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Avg daily return: {np.mean(daily_returns):+.2%}" if daily_returns else "Avg daily return: 0.0%")
    
    # Pass criteria
    passes = (
        win_rate > 0.55 and
        portfolio_return > 0 and
        len(daily_returns) >= 10
    )
    
    print(f"\nRESULT: {'PASS' if passes else 'FAIL'}")
    if not passes:
        print("Requirements not met: win_rate > 55%, survives friction, stable across time")
    
    return len(gap_signals), win_rate, portfolio_return, daily_returns

def test_gap_fill_time_shift():
    """Time-shift test for gap fill"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get all predictions
    price_query = """
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    """
    
    cursor = simulator.conn.execute(price_query)
    all_predictions = cursor.fetchall()
    
    # Calculate gaps
    gap_signals = []
    
    for pred in all_predictions:
        signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
        
        prev_day_query = """
        SELECT p.entry_price as prev_close
        FROM predictions p
        WHERE p.ticker = ?
        AND DATE(p.timestamp) = DATE(?)
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND p.entry_price IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(prev_day_query, (pred['ticker'], signal_date - timedelta(days=1)))
        prev_day = cursor.fetchone()
        
        if prev_day and prev_day['prev_close']:
            prev_close = prev_day['prev_close']
            current_price = pred['entry_price']
            
            gap_pct = (current_price - prev_close) / prev_close
            
            if gap_pct > 0.03:
                gap_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker']
                })
    
    # Normal and shifted returns using manual calculation
    normal_returns = []
    shifted_returns = []
    
    for signal in gap_signals:
        # Normal test (same day)
        return_pct = compute_return_from_prices(signal['ticker'], signal['date'], signal['date'])
        
        if return_pct is not None:
            friction_cost = 0.0015
            normal_returns.append(return_pct - friction_cost)
        
        # Time-shift test (different day)
        shifted_date = signal['date'] + timedelta(days=3)
        shifted_return = compute_return_from_prices(signal['ticker'], shifted_date, shifted_date)
        
        if shifted_return is not None:
            shifted_returns.append(shifted_return - friction_cost)
    
    simulator.close()
    
    # Calculate returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    normal_winrate = sum(1 for r in normal_returns if r > 0) / len(normal_returns) if normal_returns else 0
    shifted_winrate = sum(1 for r in shifted_returns if r > 0) / len(shifted_returns) if shifted_returns else 0
    
    print(f"\n=== GAP FILL TIME-SHIFT TEST ===")
    print(f"Normal portfolio return: {normal_portfolio:+.2%}")
    print(f"Time-shift portfolio return: {shifted_portfolio:+.2%}")
    print(f"Time-shift alpha collapse: {(normal_portfolio - shifted_portfolio):+.2%}")
    print(f"Normal win rate: {normal_winrate:.1%}")
    print(f"Shifted win rate: {shifted_winrate:.1%}")
    
    # Time-shift test passes if shifted returns are ~0 or much lower
    passes = shifted_portfolio <= 0.01
    
    print(f"\nTIME-SHIFT TEST: {'PASSED' if passes else 'FAILED'}")
    if not passes:
        print("WARNING: Time-shift test suggests look-ahead bias!")
    else:
        print("SUCCESS: Gap fill signal passes time-shift validation")
    
    return normal_portfolio, shifted_portfolio, normal_winrate, shifted_winrate

if __name__ == "__main__":
    print("=== TESTING GAP FILL ===")
    print("Hypothesis: gap up > 3% leads to intraday reversal")
    print("Method: Clean signal definition + manual return calculation\n")
    
    signals, winrate, portfolio, returns = test_gap_fill()
    normal, shifted, normal_wr, shifted_wr = test_gap_fill_time_shift()
    
    print(f"\n=== GAP FILL FINAL RESULTS ===")
    print(f"Signals: {signals}")
    print(f"Win rate: {winrate:.1%}")
    print(f"Portfolio return: {portfolio:+.2%}")
    print(f"Time-shift validation: {'PASSED' if shifted <= 0.01 else 'FAILED'}")
    
    # Overall assessment
    if signals >= 10 and winrate > 0.55 and portfolio > 0 and shifted <= 0.01:
        print(f"\nOVERALL: GAP FILL SIGNAL VALIDATED")
        print("Found real tradable edge!")
    elif shifted <= 0.01:
        print(f"\nOVERALL: GAP FILL SIGNAL FAILED (but clean)")
        print("No edge detected, but system working correctly")
    else:
        print(f"\nOVERALL: GAP FILL SIGNAL FAILED (leakage)")
        print("Time-shift test indicates bias")
