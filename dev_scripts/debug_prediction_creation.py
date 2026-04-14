import sqlite3
from datetime import datetime

conn = sqlite3.connect("data/alpha.db")

# Check discovery candidates
candidates = conn.execute("""
    SELECT strategy_type, symbol, score, reason
    FROM discovery_candidates
    WHERE as_of_date = '2026-04-10'
    ORDER BY strategy_type, score DESC
    LIMIT 10
""").fetchall()

print(f"Discovery candidates: {len(candidates)}")
for i, c in enumerate(candidates):
    print(f"  {i+1}. {c[0]} {c[1]}: {c[2]:.3f}")

# Try to create one prediction manually
if candidates:
    candidate = candidates[0]
    strategy_id = f"{candidate[0]}_v1_default"
    direction = "UP" if candidate[2] > 0.7 else "DOWN"
    confidence = float(candidate[2])
    
    pred_id = f"disc_{candidate[1]}_{strategy_id}_test"
    timestamp = "2026-04-10T12:00:00Z"
    
    print(f"\nCreating prediction:")
    print(f"  Strategy: {strategy_id}")
    print(f"  Symbol: {candidate[1]}")
    print(f"  Direction: {direction}")
    print(f"  Confidence: {confidence}")
    print(f"  ID: {pred_id}")
    
    try:
        conn.execute("""
            INSERT INTO predictions (
                id, tenant_id, strategy_id, ticker, timestamp, prediction, confidence, 
                horizon, entry_price, mode, feature_snapshot_json, regime, trend_strength, 
                idempotency_key, scored_event_id, scored_at, run_id, predicted_return, 
                direction, prediction_id, discovery_overlap, playbook_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pred_id,                                     # 1. id
            "default",                                   # 2. tenant_id
            strategy_id,                                 # 3. strategy_id
            candidate[1],                                 # 4. ticker
            timestamp,                                   # 5. timestamp
            direction,                                   # 6. prediction
            confidence,                                  # 7. confidence
            "5d",                                       # 8. horizon
            100.0,                                      # 9. entry_price
            "discovery",                                 # 10. mode
            "{}",                                       # 11. feature_snapshot_json
            None,                                       # 12. regime
            None,                                       # 13. trend_strength
            f"{pred_id}_2026-04-10",                   # 14. idempotency_key
            f"disc_{pred_id}",                          # 15. scored_event_id
            None,                                       # 16. scored_at
            None,                                       # 17. run_id
            0.0,                                       # 18. predicted_return
            direction,                                   # 19. direction
            pred_id,                                     # 20. prediction_id
            0,                                          # 21. discovery_overlap
            None                                        # 22. playbook_id
        ))
        
        conn.commit()
        print("  SUCCESS: Prediction created!")
        
        # Verify it was created
        result = conn.execute("""
            SELECT strategy_id, mode, ticker FROM predictions WHERE id = ?
        """, (pred_id,)).fetchone()
        
        if result:
            print(f"  Verified: {result['strategy_id']} {result['ticker']} ({result['mode']})")
        else:
            print("  ERROR: Prediction not found after insert")
            
    except Exception as e:
        print(f"  ERROR: {e}")
        conn.rollback()

conn.close()
