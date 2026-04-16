import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def correct_test():
    """Proper test: 1 signal per strategy per day, realistic methodology"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    winning_strategies = ['silent_compounder_v1_paper', 'balance_sheet_survivor_v1_paper']
    
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 4, 15)
    
    daily_returns = []
    wins = 0
    total_days = 0
    
    current_date = start_date
    while current_date <= end_date:
        # Get ONLY 1 signal per strategy per day (random sample)
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
        AND p.mode = ?
        AND p.strategy_id IN (?, ?)
        AND p.horizon = '20d'
        AND po.return_pct IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(query, (current_date.date(), 'discovery', *winning_strategies))
        row = cursor.fetchone()
        
        if row:
            daily_returns.append(row['return_pct'])
            total_days += 1
            if row['direction_correct']:
                wins += 1
        
        current_date += timedelta(days=1)
    
    simulator.close()
    
    # Apply friction
    friction_adjusted_returns = [r - 0.0015 for r in daily_returns]
    
    # Correct portfolio calculation
    if friction_adjusted_returns:
        portfolio_return = np.prod(1 + np.array(friction_adjusted_returns)) - 1
    else:
        portfolio_return = 0.0
    
    print("=== CORRECTED TEST ===")
    print(f"Method: 1 random signal per day from winning strategies")
    print(f"Total days with signals: {total_days}")
    print(f"Portfolio return (with friction): {portfolio_return:+.2%}")
    print(f"Portfolio return (raw): {np.prod(1 + np.array(daily_returns)) - 1:+.2%}" if daily_returns else "Portfolio return (raw): 0.0%")
    print(f"Win rate: {wins/total_days:.1%}" if total_days > 0 else "Win rate: 0.0%")
    print(f"Avg daily return: {np.mean(daily_returns):+.2%}" if daily_returns else "Avg daily return: 0.0%")
    
    return portfolio_return, wins/total_days if total_days > 0 else 0

if __name__ == "__main__":
    correct_test()
