import sqlite3
import json
from datetime import datetime, timezone

# Simple approach - create predictions from discovery candidates
conn = sqlite3.connect("data/alpha.db")
cursor = conn.cursor()

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
prediction_count = 0

for candidate in candidates:
    strategy_id = f"{candidate[0]}_v1_default"
    if strategy_id not in strategy_predictions:
        strategy_predictions[strategy_id] = []
    
    if len(strategy_predictions[strategy_id]) < 3:
        direction = "UP" if candidate[2] > 0.7 else "DOWN"
        confidence = float(candidate[2])
        
        # Create prediction
        timestamp = "2026-04-10T12:00:00Z"
        values = (
            f"disc_{candidate[1]}_{datetime.now().strftime('%Y%m%d%H%M%S')}",  # id
            "default",                                   # tenant_id
            strategy_id,                                    # 3. strategy_id
            candidate[1],                                     # 4. ticker
            timestamp,                                     # 5. timestamp
            direction,                                     # 6. prediction
            confidence,                                    # 7. confidence
            "5d",                                   # 8. horizon
            100.0,                                   # 9. entry_price
            "discovery",                                   # 10. mode
            "{}",                                   # 11. feature_snapshot_json
            None,                                         # 12. regime
            None,                                         # 13. trend_strength
            f"disc_{candidate[1]}_2026-04-10",               # 14. idempotency_key
            f"disc_{candidate[1]}_{datetime.now().strftime('%Y%m%d%H%M%S')}",  # 15. scored_event_id
            None,                                         # 16. scored_at
            None,                                         # 17. run_id
            0.0,                                         # 18. predicted_return
            direction,                                     # 19. direction
            prediction_id,                                   # 20. prediction_id
            0,                                            # 21. discovery_overlap
            None                                          # 22. playbook_id
        )
        
        strategy_predictions[strategy_id].append(candidate[1])
        prediction_count += 1
        
        print(f"Created: {candidate[1]} {strategy_id} -> {direction} (conf: {confidence:.2f})")

conn.commit()

print(f"\nCreated {prediction_count} predictions for {len(strategy_predictions)} strategies")

# Verify count
total = conn.execute("SELECT COUNT(*) FROM predictions").fetchone()
print(f"Total predictions in database: {total[0]}")
