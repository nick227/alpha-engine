import sqlite3
import numpy as np
from datetime import datetime, timedelta
import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))
from app.simulation.portfolio_simulator import PortfolioSimulator

def validate_fixed_system():
    """Validate the fixed system with time-shift test"""
    
    print("=== VALIDATING FIXED SYSTEM ===")
    
    # Check if we have new predictions
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    cursor = simulator.conn.execute("SELECT COUNT(*) FROM predictions")
    pred_count = cursor.fetchone()[0]
    
    cursor = simulator.conn.execute("SELECT COUNT(*) FROM prediction_outcomes")
    outcome_count = cursor.fetchone()[0]
    
    print(f"New predictions: {pred_count}")
    print(f"New outcomes: {outcome_count}")
    
    if pred_count == 0 or outcome_count == 0:
        print("ERROR: No new data generated")
        return
    
    # Test 1: Time-shift test
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 4, 15)
    
    normal_returns = []
    shifted_returns = []
    
    current_date = start_date
    while current_date <= end_date:
        # Get top signal
        query = """
        SELECT 
            p.id, p.strategy_id, p.ticker, p.timestamp,
            p.prediction, p.confidence, p.horizon,
            po.return_pct, po.direction_correct
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?)
        AND p.mode = 'discovery'
        AND p.horizon = '5d'
        AND po.return_pct IS NOT NULL
        ORDER BY p.confidence DESC
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(query, (current_date.date(),))
        row = cursor.fetchone()
        
        if row:
            friction_cost = 0.0025
            normal_returns.append(row['return_pct'] - friction_cost)
            
            # Time-shift: use outcome from different day
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
            
            cursor = simulator.conn.execute(outcome_query, (row['ticker'], (current_date + timedelta(days=3)).date()))
            outcome = cursor.fetchone()
            
            if outcome:
                shifted_returns.append(outcome['return_pct'] - friction_cost)
        
        current_date += timedelta(days=1)
    
    simulator.close()
    
    # Calculate returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    # Test 2: Check for absurd results
    wins = sum(1 for r in normal_returns if r > 0)
    win_rate = wins / len(normal_returns) if normal_returns else 0
    
    print(f"\n=== VALIDATION RESULTS ===")
    print(f"Normal portfolio return: {normal_portfolio:+.2%}")
    print(f"Time-shift portfolio return: {shifted_portfolio:+.2%}")
    print(f"Time-shift alpha collapse: {(normal_portfolio - shifted_portfolio):+.2%}")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Avg daily return: {np.mean(normal_returns):+.2%}" if normal_returns else "Avg daily return: 0.0%")
    
    # Pass/Fail rules
    time_shift_passed = shifted_portfolio <= 0.01  # Should be ~0%
    no_absurd_results = win_rate <= 0.70 and abs(normal_portfolio) <= 0.5  # Believable ranges
    modest_returns = abs(normal_portfolio) <= 0.3  # Not huge
    
    print(f"\n=== VALIDATION CHECKS ===")
    print(f"Time-shift test: {'PASSED' if time_shift_passed else 'FAILED'}")
    print(f"No absurd results: {'PASSED' if no_absurd_results else 'FAILED'}")
    print(f"Modest returns: {'PASSED' if modest_returns else 'FAILED'}")
    
    all_passed = time_shift_passed and no_absurd_results and modest_returns
    print(f"\nOVERALL: {'SYSTEM FIXED' if all_passed else 'STILL LEAKING'}")
    
    return all_passed, normal_portfolio, shifted_portfolio, win_rate

if __name__ == "__main__":
    validate_fixed_system()
