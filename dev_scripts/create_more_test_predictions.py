import sqlite3
from datetime import datetime

conn = sqlite3.connect("data/alpha.db")

# Create predictions for the active strategies
active_strategies = ["realness_repricer", "narrative_lag"]

# Get candidates for these strategies
for strategy in active_strategies:
    candidates = conn.execute("""
        SELECT symbol, score FROM discovery_candidates
        WHERE as_of_date = '2026-04-10' AND strategy_type = ?
        ORDER BY score DESC
        LIMIT 3
    """, (strategy,)).fetchall()
    
    print(f"Creating predictions for {strategy}: {len(candidates)} candidates")
    
    for symbol, score in candidates:
        strategy_id = f"{strategy}_v1_default"
        direction = "UP" if score > 0.7 else "DOWN"
        confidence = float(score)
        
        pred_id = f"disc_{symbol}_{strategy_id}_20260410"
        timestamp = "2026-04-10T12:00:00Z"
        
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
                symbol,                                      # 4. ticker
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
            
            print(f"  Created: {symbol} -> {direction} (conf: {confidence:.2f})")
            
        except Exception as e:
            print(f"  Error creating {symbol}: {e}")

conn.commit()

# Check total discovery predictions
total = conn.execute("""
    SELECT COUNT(*) FROM predictions 
    WHERE mode = 'discovery' AND strategy_id LIKE '%_v1_default'
""").fetchone()[0]

print(f"\nTotal discovery predictions created: {total}")

# Show strategy breakdown
breakdown = conn.execute("""
    SELECT strategy_id, COUNT(*) FROM predictions 
    WHERE mode = 'discovery' AND strategy_id LIKE '%_v1_default'
    GROUP BY strategy_id
""").fetchall()

print("By strategy:")
for strategy_id, count in breakdown:
    print(f"  {strategy_id}: {count}")

conn.close()
