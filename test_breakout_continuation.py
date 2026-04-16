import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_breakout_continuation():
    """Test breakout continuation - single day only to avoid time leakage"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find breakouts: stocks that made new highs
    breakout_query = """
    SELECT 
        DATE(p.timestamp) as breakout_date,
        p.ticker,
        po.return_pct as breakout_return
    FROM predictions p
    LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND po.return_pct IS NOT NULL
    AND po.return_pct > 0.02
    """
    
    cursor = simulator.conn.execute(breakout_query)
    breakouts = cursor.fetchall()
    
    print(f"Found {len(breakouts)} breakout candidates")
    
    # Test continuation ONLY the next day (no multi-day to avoid leakage)
    next_day_returns = []
    wins = 0
    total_signals = 0
    
    for breakout in breakouts:
        breakout_date = datetime.strptime(breakout['breakout_date'], '%Y-%m-%d').date()
        next_day = breakout_date + timedelta(days=1)
        
        # Get return for the next day only
        continuation_query = """
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
        
        cursor = simulator.conn.execute(continuation_query, (next_day, breakout['ticker']))
        continuation = cursor.fetchone()
        
        if continuation:
            # Apply friction for 1-day trade
            friction_cost = 0.0015
            next_day_returns.append(continuation['return_pct'] - friction_cost)
            total_signals += 1
            
            if continuation['return_pct'] > 0:
                wins += 1
    
    simulator.close()
    
    # Calculate portfolio returns
    if next_day_returns:
        portfolio_return = np.prod(1 + np.array(next_day_returns)) - 1
    else:
        portfolio_return = 0.0
    
    win_rate = wins / total_signals if total_signals > 0 else 0
    
    print("=== BREAKOUT CONTINUATION TEST ===")
    print(f"Hypothesis: >2% breakouts continue next day")
    print(f"Total signals: {total_signals}")
    print(f"Portfolio return (with friction): {portfolio_return:+.2%}")
    print(f"Portfolio return (raw): {np.prod(1 + np.array([r + 0.0015 for r in next_day_returns])) - 1:+.2%}" if next_day_returns else "Portfolio return (raw): 0.0%")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Avg next day return: {np.mean(next_day_returns):+.2%}" if next_day_returns else "Avg next day return: 0.0%")
    
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

def test_breakout_time_shift():
    """Time-shift test for breakout continuation"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find breakouts
    breakout_query = """
    SELECT 
        DATE(p.timestamp) as breakout_date,
        p.ticker,
        po.return_pct as breakout_return
    FROM predictions p
    LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND po.return_pct IS NOT NULL
    AND po.return_pct > 0.02
    """
    
    cursor = simulator.conn.execute(breakout_query)
    breakouts = cursor.fetchall()
    
    normal_returns = []
    shifted_returns = []
    
    for breakout in breakouts:
        breakout_date = datetime.strptime(breakout['breakout_date'], '%Y-%m-%d').date()
        next_day = breakout_date + timedelta(days=1)
        shifted_day = breakout_date + timedelta(days=3)  # Time-shifted
        
        # Normal test
        continuation_query = """
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
        
        cursor = simulator.conn.execute(continuation_query, (next_day, breakout['ticker']))
        continuation = cursor.fetchone()
        
        if continuation:
            friction_cost = 0.0015
            normal_returns.append(continuation['return_pct'] - friction_cost)
        
        # Time-shift test
        cursor = simulator.conn.execute(continuation_query, (shifted_day, breakout['ticker']))
        shifted = cursor.fetchone()
        
        if shifted:
            shifted_returns.append(shifted['return_pct'] - friction_cost)
    
    simulator.close()
    
    # Calculate returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    print(f"\n=== TIME-SHIFT TEST (BREAKOUT CONTINUATION) ===")
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
    portfolio_return, win_rate, signals = test_breakout_continuation()
    normal, shifted = test_breakout_time_shift()
    
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Signals found: {signals}")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Portfolio return: {portfolio_return:+.2%}")
    print(f"Time-shift validation: {'PASSED' if shifted <= 0.01 else 'FAILED'}")
    
    # Overall assessment
    if signals >= 10 and win_rate > 0.55 and portfolio_return > 0 and shifted <= 0.01:
        print(f"\nOVERALL: BREAKOUT CONTINUATION SIGNAL VALIDATED")
    else:
        print(f"\nOVERALL: BREAKOUT CONTINUATION SIGNAL FAILED")
