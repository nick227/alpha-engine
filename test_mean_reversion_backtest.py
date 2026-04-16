import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_mean_reversion_backtest():
    """Test mean reversion with backtest data"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find all days with extreme drops
    extreme_drop_query = """
    SELECT 
        DATE(p.timestamp) as drop_date,
        p.ticker,
        po.return_pct as drop_return
    FROM predictions p
    LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND po.return_pct IS NOT NULL
    AND po.return_pct < -0.04
    """
    
    cursor = simulator.conn.execute(extreme_drop_query)
    extreme_drops = cursor.fetchall()
    
    print(f"Found {len(extreme_drops)} extreme drops")
    
    # Test rebounds the next day
    daily_returns = []
    wins = 0
    total_signals = 0
    
    for drop in extreme_drops:
        drop_date = datetime.strptime(drop['drop_date'], '%Y-%m-%d').date()
        next_day = drop_date + timedelta(days=1)
        
        # Get return for the next day
        rebound_query = """
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
        
        cursor = simulator.conn.execute(rebound_query, (next_day, drop['ticker']))
        rebound = cursor.fetchone()
        
        if rebound:
            # Apply friction for 1-day trade
            friction_cost = 0.0015
            daily_returns.append(rebound['return_pct'] - friction_cost)
            total_signals += 1
            
            if rebound['return_pct'] > 0:
                wins += 1
    
    simulator.close()
    
    # Calculate portfolio returns
    if daily_returns:
        portfolio_return = np.prod(1 + np.array(daily_returns)) - 1
    else:
        portfolio_return = 0.0
    
    win_rate = wins / total_signals if total_signals > 0 else 0
    
    print("=== MEAN REVERSION TEST (BACKTEST DATA) ===")
    print(f"Hypothesis: >4% drops rebound next day")
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

def test_mean_reversion_time_shift_backtest():
    """Time-shift test for mean reversion with backtest data"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find extreme drops
    extreme_drop_query = """
    SELECT 
        DATE(p.timestamp) as drop_date,
        p.ticker,
        po.return_pct as drop_return
    FROM predictions p
    LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND po.return_pct IS NOT NULL
    AND po.return_pct < -0.04
    """
    
    cursor = simulator.conn.execute(extreme_drop_query)
    extreme_drops = cursor.fetchall()
    
    normal_returns = []
    shifted_returns = []
    
    for drop in extreme_drops:
        drop_date = datetime.strptime(drop['drop_date'], '%Y-%m-%d').date()
        next_day = drop_date + timedelta(days=1)
        shifted_day = drop_date + timedelta(days=3)  # Time-shifted
        
        # Normal test
        rebound_query = """
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
        
        cursor = simulator.conn.execute(rebound_query, (next_day, drop['ticker']))
        rebound = cursor.fetchone()
        
        if rebound:
            friction_cost = 0.0015
            normal_returns.append(rebound['return_pct'] - friction_cost)
        
        # Time-shift test
        cursor = simulator.conn.execute(rebound_query, (shifted_day, drop['ticker']))
        shifted = cursor.fetchone()
        
        if shifted:
            shifted_returns.append(shifted['return_pct'] - friction_cost)
    
    simulator.close()
    
    # Calculate returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    print(f"\n=== TIME-SHIFT TEST (MEAN REVERSION BACKTEST) ===")
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
    portfolio_return, win_rate, signals = test_mean_reversion_backtest()
    normal, shifted = test_mean_reversion_time_shift_backtest()
    
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
