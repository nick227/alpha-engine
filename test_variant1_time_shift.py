import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_variant1_time_shift():
    """Time-shift test for Variant 1: daily_return < -2%"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find -2% drops
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
    
    drop_signals = []
    
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
            
            if daily_return < -0.02:
                drop_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker']
                })
    
    print(f"Found {len(drop_signals)} -2% drop signals")
    
    # Normal and shifted returns
    normal_returns = []
    shifted_returns = []
    
    for signal in drop_signals:
        next_day = signal['date'] + timedelta(days=1)
        shifted_day = signal['date'] + timedelta(days=3)
        
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
        
        # Normal test
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
    
    normal_winrate = sum(1 for r in normal_returns if r > 0) / len(normal_returns) if normal_returns else 0
    shifted_winrate = sum(1 for r in shifted_returns if r > 0) / len(shifted_returns) if shifted_returns else 0
    
    print(f"\n=== VARIANT 1 TIME-SHIFT TEST ===")
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
        print("SUCCESS: Variant 1 passes time-shift validation")
    
    return normal_portfolio, shifted_portfolio, normal_winrate, shifted_winrate

if __name__ == "__main__":
    test_variant1_time_shift()
