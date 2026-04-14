import sqlite3
import json
from datetime import datetime, timezone

# Create predictions from discovery candidates - fixed version
conn = sqlite3.connect("data/alpha.db")

# Get discovery candidates
candidates = conn.execute("""
    SELECT strategy_type, symbol, score, reason
    FROM discovery_candidates
    WHERE as_of_date = '2026-04-10'
    ORDER BY strategy_type, score DESC
""").fetchall()

print(f"Processing {len(candidates)} discovery candidates...")

# Group by strategy and take top 3 per strategy
strategy_predictions = {}
for candidate in candidates:
    strategy_id = f"{candidate[0]}_v1_default"
    if strategy_id not in strategy_predictions:
        strategy_predictions[strategy_id] = []
    
    if len(strategy_predictions[strategy_id]) < 3:
        direction = "UP" if candidate[2] > 0.7 else "DOWN"
        confidence = float(candidate[2])
        
        prediction_id = f"disc_{candidate[1]}_{strategy_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Create prediction with all required fields
        conn.execute("""
            INSERT INTO predictions (
                id, tenant_id, strategy_id, ticker, timestamp, prediction, confidence, 
                horizon, entry_price, mode, feature_snapshot_json, regime, trend_strength, 
                idempotency_key, scored_event_id, scored_at, run_id, predicted_return, 
                direction, prediction_id, discovery_overlap, playbook_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
            prediction_id,                              # 1. id
            "default",                                   # 2. tenant_id
            strategy_id,                               # 3. strategy_id
            candidate[1],                         # 4. ticker
            "2026-04-10T12:00:00Z",            # 5. timestamp
            direction,                                 # 6. prediction
            confidence,                               # 7. confidence
            "5d",                                   # 8. horizon
            None,                                   # 9. entry_price
            "discovery",                              # 10. mode
            "{}",                                   # 11. feature_snapshot_json
            None,                                   # 12. regime
            None,                                   # 13. trend_strength
            f"{prediction_id}_2026-04-10",        # 14. idempotency_key
            f"disc_{prediction_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",  # 15. scored_event_id
            None,                                   # 16. scored_at
            None,                                   # 17. run_id
            0.0,                                   # 18. predicted_return
            direction,                               # 19. direction
            prediction_id,                             # 20. prediction_id
            0,                                    # 21. discovery_overlap
            None                                   # 22. playbook_id
        ))
        
        strategy_predictions[strategy_id].append(candidate['symbol'])
        
        print(f"Created: {candidate['symbol']} {strategy_id} -> {direction} (conf: {confidence:.2f})")

conn.commit()

print(f"\nCreated predictions for {len(strategy_predictions)} strategies")

# Summary
for strategy_id, symbols in strategy_predictions.items():
    print(f"{strategy_id}: {len(symbols)} predictions")

print(f"\nTotal predictions created: {sum(len(symbols) for symbols in strategy_predictions.values())}")

# Verify count
total = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
print(f"Total predictions in database: {total[0]}")
