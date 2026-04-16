import sqlite3
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_strategy_performance():
    """Test each strategy individually"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get top strategies by volume
    strategies_query = """
    SELECT strategy_id, COUNT(*) as count
    FROM predictions 
    WHERE mode = 'discovery' 
    AND DATE(timestamp) >= '2026-01-01' 
    AND DATE(timestamp) <= '2026-04-15'
    GROUP BY strategy_id 
    ORDER BY count DESC 
    LIMIT 5
    """
    
    cursor = simulator.conn.execute(strategies_query)
    strategies = [row[0] for row in cursor.fetchall()]
    
    print("=== PER-STRATEGY TOP-1 TEST ===")
    
    for strategy in strategies:
        total_return = 0.0
        total_signals = 0
        wins = 0
        
        current_date = datetime(2026, 1, 1)
        end_date = datetime(2026, 4, 15)
        
        while current_date <= end_date:
            # Get top 1 signal for this strategy on this day
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
            AND p.strategy_id = ?
            AND po.return_pct IS NOT NULL
            ORDER BY p.confidence DESC
            LIMIT 1
            """
            
            cursor = simulator.conn.execute(query, (current_date.date(), 'discovery', strategy))
            row = cursor.fetchone()
            
            if row:
                total_return += row['return_pct']
                total_signals += 1
                if row['direction_correct']:
                    wins += 1
            
            current_date += timedelta(days=1)
        
        win_rate = wins/total_signals if total_signals > 0 else 0
        print(f"\n{strategy}:")
        print(f"  Signals: {total_signals}")
        print(f"  Return: {total_return:+.2%}")
        print(f"  Win Rate: {win_rate:.1%}")
    
    simulator.close()

if __name__ == "__main__":
    test_strategy_performance()
