import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_5d_signals():
    """Test 5-day horizon signals with proper methodology"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 4, 15)
    
    daily_returns = []
    wins = 0
    total_days = 0
    
    current_date = start_date
    while current_date <= end_date:
        # Get 5-day predictions
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
        AND p.prediction = 'UP'
        ORDER BY p.confidence DESC
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(query, (current_date.date(),))
        row = cursor.fetchone()
        
        if row:
            # Apply friction for 5-day hold (higher cost)
            friction_cost = 0.0025  # 25 bps for 5-day
            daily_returns.append(row['return_pct'] - friction_cost)
            total_days += 1
            if row['direction_correct']:
                wins += 1
        
        current_date += timedelta(days=1)
    
    simulator.close()
    
    # Portfolio calculation (geometric)
    if daily_returns:
        portfolio_return = np.prod(1 + np.array(daily_returns)) - 1
    else:
        portfolio_return = 0.0
    
    win_rate = wins/total_days if total_days > 0 else 0
    
    print("=== 5-DAY HORIZON TEST ===")
    print(f"Hypothesis: 5-day UP predictions work")
    print(f"Total signals: {total_days}")
    print(f"Portfolio return (with friction): {portfolio_return:+.2%}")
    print(f"Portfolio return (raw): {np.prod(1 + np.array([r + 0.0025 for r in daily_returns])) - 1:+.2%}" if daily_returns else "Portfolio return (raw): 0.0%")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Avg daily return: {np.mean(daily_returns):+.2%}" if daily_returns else "Avg daily return: 0.0%")
    
    # Test if it passes minimum requirements
    passes = (
        win_rate > 0.55 and
        portfolio_return > 0 and
        total_days >= 20  # minimum sample
    )
    
    print(f"\nRESULT: {'PASS' if passes else 'FAIL'}")
    if not passes:
        print("Requirements not met: win_rate > 55%, survives friction, stable across time")
    
    return portfolio_return, win_rate

if __name__ == "__main__":
    test_5d_signals()
