import sqlite3
from datetime import datetime, timezone

# Create test outcomes for our discovery predictions to validate trust calculation
conn = sqlite3.connect("data/alpha.db")

# Get our discovery predictions
predictions = conn.execute("""
    SELECT id, strategy_id, ticker, direction, confidence
    FROM predictions
    WHERE strategy_id LIKE '%_v1_default' AND mode = 'discovery'
""").fetchall()

print(f"Creating outcomes for {len(predictions)} discovery predictions...")

for pred in predictions:
    # Create a simple outcome (for testing)
    outcome_id = f"outcome_{pred[0]}"
    evaluated_at = "2026-04-15T00:00:00Z"
    
    # Random but reasonable outcomes
    import random
    direction_correct = random.choice([0, 1])  # 50% accuracy
    actual_return = random.uniform(-0.1, 0.15)  # -10% to +15%
    max_drawdown = random.uniform(-0.05, 0.02)  # -5% to +2%
    max_runup = random.uniform(0.02, 0.08)  # 2% to 8% runup
    exit_reason = "completed"  # exit reason
    
    conn.execute("""
        INSERT INTO prediction_outcomes (
            prediction_id, tenant_id, evaluated_at, direction_correct, 
            return_pct, max_drawdown, max_runup, exit_price, horizon, exit_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pred[0],           # prediction_id
        "default",         # tenant_id
        evaluated_at,       # evaluated_at
        direction_correct,   # direction_correct
        actual_return,      # return_pct
        max_drawdown,       # max_drawdown
        max_runup,          # max_runup
        100.0,            # exit_price
        "5d",               # horizon
        exit_reason          # exit_reason
    ))
    
    print(f"Created outcome for {pred[1]} {pred[2]}: {'✓' if direction_correct else '✗'} (return: {actual_return:.2%})")

conn.commit()

print(f"\nCreated {len(predictions)} test outcomes")

# Verify outcomes were created
outcome_count = conn.execute("""
    SELECT COUNT(*) FROM prediction_outcomes o
    JOIN predictions p ON p.id = o.prediction_id
    WHERE p.strategy_id LIKE '%_v1_default' AND p.mode = 'discovery'
""").fetchone()

print(f"Discovery predictions with outcomes: {outcome_count[0]}")
