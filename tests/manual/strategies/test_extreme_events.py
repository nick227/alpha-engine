import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_extreme_events():
    """Test narrow hypothesis: extreme moves predict next-day direction"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 4, 15)
    
    daily_returns = []
    wins = 0
    total_days = 0
    
    current_date = start_date
    while current_date <= end_date:
        # Find extreme events from previous day
        extreme_query = """
        SELECT 
            p.ticker,
            po.return_pct as prev_return,
            po.max_runup,
            po.max_drawdown
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?, '-1 day')
        AND p.mode = 'discovery'
        AND p.horizon = '20d'
        AND po.return_pct IS NOT NULL
        AND ABS(po.return_pct) > 0.05
        """
        
        cursor = simulator.conn.execute(extreme_query, (current_date.date(),))
        extreme_stocks = cursor.fetchall()
        
        if extreme_stocks:
            # Test if extreme moves reverse next day
            for stock in extreme_stocks:
                next_day_query = """
                SELECT 
                    po.return_pct,
                    po.direction_correct
                FROM predictions p
                LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
                WHERE DATE(p.timestamp) = DATE(?)
                AND p.ticker = ?
                AND p.mode = 'discovery'
                AND p.horizon = '20d'
                AND po.return_pct IS NOT NULL
                LIMIT 1
                """
                
                cursor = simulator.conn.execute(next_day_query, (current_date.date(), stock['ticker']))
                next_day = cursor.fetchone()
                
                if next_day:
                    # Hypothesis: extreme moves reverse direction
                    predicted_reverse = -1 if stock['prev_return'] > 0 else 1
                    actual_direction = 1 if next_day['return_pct'] > 0 else -1
                    
                    daily_returns.append(next_day['return_pct'])
                    total_days += 1
                    
                    if predicted_reverse == actual_direction:
                        wins += 1
        
        current_date += timedelta(days=1)
    
    simulator.close()
    
    # Apply friction
    friction_adjusted_returns = [r - 0.0015 for r in daily_returns]
    
    if friction_adjusted_returns:
        portfolio_return = np.prod(1 + np.array(friction_adjusted_returns)) - 1
    else:
        portfolio_return = 0.0
    
    win_rate = wins/total_days if total_days > 0 else 0
    
    print("=== EXTREME EVENTS REVERSAL TEST ===")
    print(f"Hypothesis: >5% moves reverse next day")
    print(f"Total extreme events: {total_days}")
    print(f"Portfolio return (with friction): {portfolio_return:+.2%}")
    print(f"Portfolio return (raw): {np.prod(1 + np.array(daily_returns)) - 1:+.2%}" if daily_returns else "Portfolio return (raw): 0.0%")
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
    test_extreme_events()
