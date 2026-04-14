import sqlite3
from datetime import datetime, timezone

def manual_trust_calculation():
    """Manually calculate trust from outcomes."""
    conn = sqlite3.connect("data/alpha.db")
    conn.row_factory = sqlite3.Row
    
    print("DISCOVERY STRATEGY PERFORMANCE")
    print("=" * 50)
    
    # Get discovery outcomes
    discovery_outcomes = conn.execute("""
        SELECT p.strategy_id, p.ticker, po.return_pct, po.direction_correct
        FROM prediction_outcomes po
        JOIN predictions p ON p.id = po.prediction_id
        WHERE p.mode = 'discovery'
        ORDER BY p.strategy_id
    """).fetchall()
    
    # Group by strategy
    strategy_results = {}
    for outcome in discovery_outcomes:
        strategy = outcome['strategy_id']
        if strategy not in strategy_results:
            strategy_results[strategy] = []
        strategy_results[strategy].append(outcome)
    
    # Calculate metrics for each strategy
    for strategy_id, outcomes in strategy_results.items():
        n = len(outcomes)
        if n == 0:
            continue
            
        correct = sum(1 for o in outcomes if o['direction_correct'])
        accuracy = correct / n
        
        # Simple trust = accuracy * calibration (use accuracy as proxy for calibration)
        # Higher sample size gives more confidence
        sample_weight = min(1.0, n / 10.0)  # Scale up to n=10
        trust = accuracy * sample_weight
        
        print(f"{strategy_id}:")
        print(f"  Predictions: {n}")
        print(f"  Correct: {correct}")
        print(f"  Accuracy: {accuracy:.3f}")
        print(f"  Sample Weight: {sample_weight:.3f}")
        print(f"  Trust: {trust:.3f}")
        print()
    
    print("BASELINE COMPARISON")
    print("=" * 30)
    
    # Get baseline outcomes for comparison
    baseline_outcomes = conn.execute("""
        SELECT p.strategy_id, COUNT(*) as n, SUM(po.direction_correct) as correct
        FROM prediction_outcomes po
        JOIN predictions p ON p.id = po.prediction_id
        WHERE p.mode != 'discovery' AND p.horizon = '1d'
        GROUP BY p.strategy_id
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()
    
    for outcome in baseline_outcomes:
        strategy_id = outcome['strategy_id']
        n = outcome['n']
        correct = outcome['correct']
        accuracy = correct / n if n > 0 else 0
        
        sample_weight = min(1.0, n / 10.0)
        trust = accuracy * sample_weight
        
        print(f"{strategy_id}:")
        print(f"  Predictions: {n}")
        print(f"  Accuracy: {accuracy:.3f}")
        print(f"  Trust: {trust:.3f}")
        print()
    
    conn.close()

if __name__ == "__main__":
    manual_trust_calculation()
