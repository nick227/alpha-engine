import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_earnings_drift():
    """Test post-earnings drift - positive reactions continue"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find all days with positive large moves (potential earnings)
    positive_move_query = """
    SELECT 
        DATE(p.timestamp) as move_date,
        p.ticker,
        po.return_pct as move_return
    FROM predictions p
    LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND po.return_pct IS NOT NULL
    AND po.return_pct > 0.05
    """
    
    cursor = simulator.conn.execute(positive_move_query)
    positive_moves = cursor.fetchall()
    
    print(f"Found {len(positive_moves)} positive moves")
    
    # Test continuation over next 3 days
    multi_day_returns = []
    wins = 0
    total_signals = 0
    
    for move in positive_moves:
        move_date = datetime.strptime(move['move_date'], '%Y-%m-%d').date()
        
        # Collect returns for next 3 days
        signal_returns = []
        
        for days_ahead in range(1, 4):  # Days +1, +2, +3
            future_date = move_date + timedelta(days=days_ahead)
            
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
            
            cursor = simulator.conn.execute(continuation_query, (future_date, move['ticker']))
            continuation = cursor.fetchone()
            
            if continuation:
                signal_returns.append(continuation['return_pct'])
        
        if signal_returns:
            # Apply friction for 3-day hold
            friction_cost = 0.003  # 30 bps for 3 days
            total_return = sum(signal_returns) - friction_cost
            multi_day_returns.append(total_return)
            total_signals += 1
            
            if total_return > 0:
                wins += 1
    
    simulator.close()
    
    # Calculate portfolio returns
    if multi_day_returns:
        portfolio_return = np.prod(1 + np.array(multi_day_returns)) - 1
    else:
        portfolio_return = 0.0
    
    win_rate = wins / total_signals if total_signals > 0 else 0
    
    print("=== EARNINGS DRIFT TEST ===")
    print(f"Hypothesis: >5% positive moves continue over 3 days")
    print(f"Total signals: {total_signals}")
    print(f"Portfolio return (with friction): {portfolio_return:+.2%}")
    print(f"Portfolio return (raw): {np.prod(1 + np.array([r + 0.003 for r in multi_day_returns])) - 1:+.2%}" if multi_day_returns else "Portfolio return (raw): 0.0%")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Avg signal return: {np.mean(multi_day_returns):+.2%}" if multi_day_returns else "Avg signal return: 0.0%")
    
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

def test_earnings_drift_time_shift():
    """Time-shift test for earnings drift"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find positive moves
    positive_move_query = """
    SELECT 
        DATE(p.timestamp) as move_date,
        p.ticker,
        po.return_pct as move_return
    FROM predictions p
    LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND po.return_pct IS NOT NULL
    AND po.return_pct > 0.05
    """
    
    cursor = simulator.conn.execute(positive_move_query)
    positive_moves = cursor.fetchall()
    
    normal_returns = []
    shifted_returns = []
    
    for move in positive_moves:
        move_date = datetime.strptime(move['move_date'], '%Y-%m-%d').date()
        
        # Normal test - collect next 3 days
        signal_returns = []
        for days_ahead in range(1, 4):
            future_date = move_date + timedelta(days=days_ahead)
            
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
            
            cursor = simulator.conn.execute(continuation_query, (future_date, move['ticker']))
            continuation = cursor.fetchone()
            
            if continuation:
                signal_returns.append(continuation['return_pct'])
        
        if signal_returns:
            friction_cost = 0.003
            total_return = sum(signal_returns) - friction_cost
            normal_returns.append(total_return)
        
        # Time-shift test - use different dates
        shifted_returns_list = []
        for days_ahead in range(4, 7):  # Days +4, +5, +6 (shifted)
            future_date = move_date + timedelta(days=days_ahead)
            
            cursor = simulator.conn.execute(continuation_query, (future_date, move['ticker']))
            continuation = cursor.fetchone()
            
            if continuation:
                shifted_returns_list.append(continuation['return_pct'])
        
        if shifted_returns_list:
            friction_cost = 0.003
            total_return = sum(shifted_returns_list) - friction_cost
            shifted_returns.append(total_return)
    
    simulator.close()
    
    # Calculate returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    print(f"\n=== TIME-SHIFT TEST (EARNINGS DRIFT) ===")
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
    portfolio_return, win_rate, signals = test_earnings_drift()
    normal, shifted = test_earnings_drift_time_shift()
    
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Signals found: {signals}")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Portfolio return: {portfolio_return:+.2%}")
    print(f"Time-shift validation: {'PASSED' if shifted <= 0.01 else 'FAILED'}")
    
    # Overall assessment
    if signals >= 10 and win_rate > 0.55 and portfolio_return > 0 and shifted <= 0.01:
        print(f"\nOVERALL: EARNINGS DRIFT SIGNAL VALIDATED")
    else:
        print(f"\nOVERALL: EARNINGS DRIFT SIGNAL FAILED")
