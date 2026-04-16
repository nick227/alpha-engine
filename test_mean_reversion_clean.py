import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_mean_reversion_clean():
    """Test mean reversion using ONLY price data (no outcomes)"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find extreme drops using ONLY price data (not outcomes)
    # This is the correct way: signal definition from available data
    price_drop_query = """
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    """
    
    cursor = simulator.conn.execute(price_drop_query)
    all_predictions = cursor.fetchall()
    
    print(f"Total predictions to analyze: {len(all_predictions)}")
    
    # Calculate daily returns from price data to find extreme drops
    extreme_drop_signals = []
    
    for pred in all_predictions:
        signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
        
        # Get previous day's price to calculate drop
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
            # Calculate daily return from price data (not outcomes)
            daily_return = (pred['entry_price'] - prev_day['prev_price']) / prev_day['prev_price']
            
            # Signal: extreme drop (>4% down)
            if daily_return < -0.04:
                extreme_drop_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker'],
                    'drop_return': daily_return,
                    'entry_price': pred['entry_price']
                })
    
    print(f"Found {len(extreme_drop_signals)} extreme drop signals")
    
    # Now test if these signals rebound next day (using outcomes for evaluation only)
    daily_returns = []
    wins = 0
    total_signals = 0
    
    for signal in extreme_drop_signals:
        next_day = signal['date'] + timedelta(days=1)
        
        # Get outcome for next day (evaluation only)
        outcome_query = """
        SELECT 
            po.return_pct,
            po.direction_correct
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?)
        AND p.ticker = ?
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND po.return_pct IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(outcome_query, (next_day, signal['ticker']))
        outcome = cursor.fetchone()
        
        if outcome:
            # Apply friction for 1-day trade
            friction_cost = 0.0015
            daily_returns.append(outcome['return_pct'] - friction_cost)
            total_signals += 1
            
            if outcome['return_pct'] > 0:
                wins += 1
    
    simulator.close()
    
    # Calculate portfolio returns
    if daily_returns:
        portfolio_return = np.prod(1 + np.array(daily_returns)) - 1
    else:
        portfolio_return = 0.0
    
    win_rate = wins / total_signals if total_signals > 0 else 0
    
    print("=== MEAN REVERSION TEST (CLEAN) ===")
    print(f"Hypothesis: >4% price drops rebound next day")
    print(f"Signal definition: Price-based (no outcome leakage)")
    print(f"Total signals: {total_signals}")
    print(f"Portfolio return (with friction): {portfolio_return:+.2%}")
    print(f"Portfolio return (raw): {np.prod(1 + np.array([r + 0.0015 for r in daily_returns])) - 1:+.2%}" if daily_returns else "Portfolio return (raw): 0.0%")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Avg daily return: {np.mean(daily_returns):+.2%}" if daily_returns else "Avg daily return: 0.0%")
    
    # Pass criteria
    passes = (
        win_rate > 0.55 and
        portfolio_return > 0 and
        total_signals >= 10
    )
    
    print(f"\nRESULT: {'PASS' if passes else 'FAIL'}")
    if not passes:
        print("Requirements not met: win_rate > 55%, survives friction, stable across time")
    
    return portfolio_return, win_rate, total_signals

def test_mean_reversion_time_shift_clean():
    """Time-shift test for clean mean reversion"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find extreme drops using only price data
    price_drop_query = """
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    """
    
    cursor = simulator.conn.execute(price_drop_query)
    all_predictions = cursor.fetchall()
    
    extreme_drop_signals = []
    
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
            
            if daily_return < -0.04:
                extreme_drop_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker']
                })
    
    normal_returns = []
    shifted_returns = []
    
    for signal in extreme_drop_signals:
        next_day = signal['date'] + timedelta(days=1)
        shifted_day = signal['date'] + timedelta(days=3)
        
        # Normal test
        outcome_query = """
        SELECT po.return_pct
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?)
        AND p.ticker = ?
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND po.return_pct IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(outcome_query, (next_day, signal['ticker']))
        outcome = cursor.fetchone()
        
        if outcome:
            friction_cost = 0.0015
            normal_returns.append(outcome['return_pct'] - friction_cost)
        
        # Time-shift test
        cursor = simulator.conn.execute(outcome_query, (shifted_day, signal['ticker']))
        shifted = cursor.fetchone()
        
        if shifted:
            shifted_returns.append(shifted['return_pct'] - friction_cost)
    
    simulator.close()
    
    # Calculate returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    print(f"\n=== TIME-SHIFT TEST (CLEAN MEAN REVERSION) ===")
    print(f"Normal portfolio return: {normal_portfolio:+.2%}")
    print(f"Time-shift portfolio return: {shifted_portfolio:+.2%}")
    print(f"Time-shift alpha collapse: {(normal_portfolio - shifted_portfolio):+.2%}")
    
    # Time-shift test passes if shifted returns are ~0 or much lower
    passes = shifted_portfolio <= 0.01
    
    print(f"\nTIME-SHIFT TEST: {'PASSED' if passes else 'FAILED'}")
    if not passes:
        print("WARNING: Time-shift test suggests look-ahead bias!")
    
    return normal_portfolio, shifted_portfolio

if __name__ == "__main__":
    portfolio_return, win_rate, signals = test_mean_reversion_clean()
    normal, shifted = test_mean_reversion_time_shift_clean()
    
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Signals found: {signals}")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Portfolio return: {portfolio_return:+.2%}")
    print(f"Time-shift validation: {'PASSED' if shifted <= 0.01 else 'FAILED'}")
    
    # Overall assessment
    if signals >= 10 and win_rate > 0.55 and portfolio_return > 0 and shifted <= 0.01:
        print(f"\nOVERALL: MEAN REVERSION SIGNAL VALIDATED")
    else:
        print(f"\nOVERALL: MEAN REVERSION SIGNAL FAILED")
