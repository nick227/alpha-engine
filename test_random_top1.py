import sqlite3
import random
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def run_random_top1_test():
    """Test if confidence is meaningless by using random selection"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 4, 15)
    
    total_return = 0.0
    total_signals = 0
    wins = 0
    
    current_date = start_date
    while current_date <= end_date:
        # Get all signals for this day
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
        AND po.return_pct IS NOT NULL
        ORDER BY p.timestamp
        """
        
        cursor = simulator.conn.execute(query, (current_date.date(), 'discovery'))
        signals = []
        
        for row in cursor:
            signal_time = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
            eval_time = datetime.fromisoformat(row['evaluated_at'].replace('Z', '+00:00'))
            
            signals.append({
                'strategy_id': row['strategy_id'],
                'return_pct': row['return_pct'],
                'direction_correct': bool(row['direction_correct'])
            })
        
        if signals:
            # Pick RANDOM signal (not highest confidence)
            selected = random.choice(signals)
            total_return += selected['return_pct']
            total_signals += 1
            if selected['direction_correct']:
                wins += 1
        
        current_date += timedelta(days=1)
    
    simulator.close()
    
    print("=== RANDOM TOP-1 TEST ===")
    print(f"Total signals: {total_signals}")
    print(f"Portfolio return: {total_return:+.2%}")
    print(f"Win rate: {wins/total_signals:.1%}" if total_signals > 0 else "Win rate: 0.0%")
    
    return total_return, wins/total_signals if total_signals > 0 else 0

if __name__ == "__main__":
    run_random_top1_test()
