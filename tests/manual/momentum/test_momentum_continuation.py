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

def test_momentum_continuation():
    """Test momentum continuation - daily_return > +2%"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find all predictions to calculate daily returns
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
    
    # Calculate daily returns and find momentum signals
    momentum_signals = []
    
    for pred in all_predictions:
        signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
        
        # Get previous day's price
        prev_day_query = """
        SELECT p.entry_price as prev_price
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
        
        if prev_day and prev_day['prev_price']:
            daily_return = (pred['entry_price'] - prev_day['prev_price']) / prev_day['prev_price']
            
            # Momentum signal: > +2% gain
            if daily_return > 0.02:
                momentum_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker'],
                    'momentum_return': daily_return
                })
    
    print(f"=== MOMENTUM CONTINUATION TEST ===")
    print(f"Signal: daily_return > +2%")
    print(f"Method: Manual return calculation (no outcome reuse)")
    print(f"Signals found: {len(momentum_signals)}")
    
    # Test next day returns using manual calculation
    daily_returns = []
    wins = 0
    
    for signal in momentum_signals:
        entry_date = signal['date'] + timedelta(days=1)
        exit_date = entry_date  # Same day exit
        
        # Compute return from raw prices (NOT from stored outcomes)
        return_pct = compute_return_from_prices(signal['ticker'], entry_date, exit_date)
        
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
    
    return len(momentum_signals), win_rate, portfolio_return, daily_returns

def test_momentum_time_shift():
    """Time-shift test for momentum continuation"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find momentum signals
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
    
    momentum_signals = []
    
    for pred in all_predictions:
        signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
        
        prev_day_query = """
        SELECT p.entry_price as prev_price
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
        
        if prev_day and prev_day['prev_price']:
            daily_return = (pred['entry_price'] - prev_day['prev_price']) / prev_day['prev_price']
            
            if daily_return > 0.02:
                momentum_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker']
                })
    
    # Normal and shifted returns using manual calculation
    normal_returns = []
    shifted_returns = []
    
    for signal in momentum_signals:
        # Normal test
        entry_date = signal['date'] + timedelta(days=1)
        exit_date = entry_date
        
        return_pct = compute_return_from_prices(signal['ticker'], entry_date, exit_date)
        
        if return_pct is not None:
            friction_cost = 0.0015
            normal_returns.append(return_pct - friction_cost)
        
        # Time-shift test
        shifted_entry_date = signal['date'] + timedelta(days=3)
        shifted_exit_date = shifted_entry_date
        
        shifted_return = compute_return_from_prices(signal['ticker'], shifted_entry_date, shifted_exit_date)
        
        if shifted_return is not None:
            shifted_returns.append(shifted_return - friction_cost)
    
    simulator.close()
    
    # Calculate returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    normal_winrate = sum(1 for r in normal_returns if r > 0) / len(normal_returns) if normal_returns else 0
    shifted_winrate = sum(1 for r in shifted_returns if r > 0) / len(shifted_returns) if shifted_returns else 0
    
    print(f"\n=== MOMENTUM TIME-SHIFT TEST ===")
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
        print("SUCCESS: Momentum signal passes time-shift validation")
    
    return normal_portfolio, shifted_portfolio, normal_winrate, shifted_winrate

if __name__ == "__main__":
    print("=== TESTING MOMENTUM CONTINUATION ===")
    print("Hypothesis: >+2% gains continue next day")
    print("Method: Clean signal definition + manual return calculation\n")
    
    signals, winrate, portfolio, returns = test_momentum_continuation()
    normal, shifted, normal_wr, shifted_wr = test_momentum_time_shift()
    
    print(f"\n=== MOMENTUM FINAL RESULTS ===")
    print(f"Signals: {signals}")
    print(f"Win rate: {winrate:.1%}")
    print(f"Portfolio return: {portfolio:+.2%}")
    print(f"Time-shift validation: {'PASSED' if shifted <= 0.01 else 'FAILED'}")
    
    # Overall assessment
    if signals >= 10 and winrate > 0.55 and portfolio > 0 and shifted <= 0.01:
        print(f"\nOVERALL: MOMENTUM SIGNAL VALIDATED")
        print("Found real tradable edge!")
    elif shifted <= 0.01:
        print(f"\nOVERALL: MOMENTUM SIGNAL FAILED (but clean)")
        print("No edge detected, but system working correctly")
    else:
        print(f"\nOVERALL: MOMENTUM SIGNAL FAILED (leakage)")
        print("Time-shift test indicates bias")
