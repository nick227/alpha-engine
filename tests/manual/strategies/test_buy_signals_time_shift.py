import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_buy_signals_time_shift():
    """Test BUY signals with time-shift to detect bias"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 4, 15)
    
    # Normal test
    normal_returns = []
    # Time-shift test (shift outcomes by 5 days)
    shifted_returns = []
    
    current_date = start_date
    while current_date <= end_date:
        # Normal query
        query = """
        SELECT 
            p.id, p.strategy_id, p.ticker, p.timestamp,
            p.prediction, p.confidence, p.horizon,
            p.entry_price, p.mode, p.regime,
            po.return_pct, po.direction_correct, po.max_runup, po.max_drawdown,
            po.evaluated_at
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?)
        AND p.mode = 'discovery'
        AND p.horizon = '5d'
        AND po.return_pct IS NOT NULL
        AND p.prediction = 'BUY'
        AND p.confidence > 0.8
        ORDER BY p.confidence DESC
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(query, (current_date.date(),))
        row = cursor.fetchone()
        
        if row:
            friction_cost = 0.0025
            normal_returns.append(row['return_pct'] - friction_cost)
        
        # Time-shift query (misaligned dates)
        shifted_query = """
        SELECT 
            p.id, p.strategy_id, p.ticker, p.timestamp,
            p.prediction, p.confidence, p.horizon,
            p.entry_price, p.mode, p.regime,
            po.return_pct, po.direction_correct, po.max_runup, po.max_drawdown,
            po.evaluated_at
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?)
        AND p.mode = 'discovery'
        AND p.horizon = '5d'
        AND po.return_pct IS NOT NULL
        AND p.prediction = 'BUY'
        AND p.confidence > 0.8
        ORDER BY p.confidence DESC
        LIMIT 1
        """
        
        # Get prediction but use outcome from 5 days later
        cursor = simulator.conn.execute(shifted_query, (current_date.date(),))
        row = cursor.fetchone()
        
        if row:
            # Get outcome from 5 days later (time-shift)
            outcome_query = """
            SELECT po.return_pct
            FROM predictions p2
            LEFT JOIN prediction_outcomes po ON p2.id = po.prediction_id
            WHERE p2.ticker = ?
            AND DATE(p2.timestamp) = DATE(?)
            AND p2.horizon = '5d'
            AND po.return_pct IS NOT NULL
            LIMIT 1
            """
            
            cursor = simulator.conn.execute(outcome_query, (row['ticker'], (current_date + timedelta(days=5)).date()))
            outcome = cursor.fetchone()
            
            if outcome:
                friction_cost = 0.0025
                shifted_returns.append(outcome['return_pct'] - friction_cost)
        
        current_date += timedelta(days=1)
    
    simulator.close()
    
    # Calculate returns
    if normal_returns:
        normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1
    else:
        normal_portfolio = 0.0
        
    if shifted_returns:
        shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1
    else:
        shifted_portfolio = 0.0
    
    print("=== TIME-SHIFT TEST FOR BUY SIGNALS ===")
    print(f"Normal portfolio return: {normal_portfolio:+.2%}")
    print(f"Time-shift portfolio return: {shifted_portfolio:+.2%}")
    print(f"Time-shift alpha collapse: {(normal_portfolio - shifted_portfolio):+.2%}")
    
    # Time-shift test passes if shifted returns are ~0 or much lower
    passes = shifted_portfolio <= 0.01  # Should be near zero or negative
    
    print(f"\nTIME-SHIFT TEST: {'PASSED' if passes else 'FAILED'}")
    if not passes:
        print("WARNING: Time-shift test suggests look-ahead bias!")
    
    return normal_portfolio, shifted_portfolio

if __name__ == "__main__":
    test_buy_signals_time_shift()
