import sqlite3
import random
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def fixed_random_test():
    """Fix horizon mixing and data quality issues"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 4, 15)
    
    daily_returns = []
    wins = 0
    total_days = 0
    
    current_date = start_date
    while current_date <= end_date:
        # Get all signals for this day with FIXED horizon
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
        AND p.horizon = ?
        AND po.return_pct IS NOT NULL
        ORDER BY p.timestamp
        """
        
        # Lock to single horizon (20d)
        cursor = simulator.conn.execute(query, (current_date.date(), 'discovery', '20d'))
        signals = []
        
        for row in cursor:
            signal_time = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
            eval_time = datetime.fromisoformat(row['evaluated_at'].replace('Z', '+00:00'))
            
            signals.append({
                'strategy_id': row['strategy_id'],
                'return_pct': row['return_pct'],
                'direction_correct': bool(row['direction_correct'])
            })
        
        # Require minimum signals per day to avoid random dominance
        if len(signals) >= 5:
            selected = random.choice(signals)
            daily_returns.append(selected['return_pct'])
            total_days += 1
            if selected['direction_correct']:
                wins += 1
        
        current_date += timedelta(days=1)
    
    simulator.close()
    
    # Correct portfolio calculation (geometric)
    if daily_returns:
        portfolio_return = np.prod(1 + np.array(daily_returns)) - 1
    else:
        portfolio_return = 0.0
    
    print("=== FIXED RANDOM TOP-1 TEST ===")
    print(f"Total days with >=5 signals: {total_days}")
    print(f"Daily returns: {len(daily_returns)}")
    print(f"Portfolio return (geometric): {portfolio_return:+.2%}")
    print(f"Win rate: {wins/total_days:.1%}" if total_days > 0 else "Win rate: 0.0%")
    print(f"Avg daily return: {np.mean(daily_returns):+.2%}" if daily_returns else "Avg daily return: 0.0%")
    
    return portfolio_return, wins/total_days if total_days > 0 else 0

if __name__ == "__main__":
    fixed_random_test()
