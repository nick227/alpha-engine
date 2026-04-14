"""
Create predictions from discovery candidates for historical backtesting.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Any


def create_discovery_predictions(
    conn: sqlite3.Connection,
    as_of_date: str,
    max_per_strategy: int = 5
) -> int:
    """
    Create predictions from discovery candidates for a specific date.
    
    Args:
        conn: Database connection
        as_of_date: Date string (YYYY-MM-DD)
        max_per_strategy: Maximum predictions per strategy
        
    Returns:
        Number of predictions created
    """
    # Get discovery candidates for this date
    candidates = conn.execute("""
        SELECT strategy_type, symbol, score, reason
        FROM discovery_candidates
        WHERE as_of_date = ?
        ORDER BY strategy_type, score DESC
    """, (as_of_date,)).fetchall()
    
    if not candidates:
        return 0
    
    # Group by strategy and take top N per strategy
    strategy_predictions = {}
    for candidate in candidates:
        strategy_id = f"{candidate[0]}_v1_default"
        if strategy_id not in strategy_predictions:
            strategy_predictions[strategy_id] = []
        
        if len(strategy_predictions[strategy_id]) < max_per_strategy:
            direction = "UP" if candidate[2] > 0.7 else "DOWN"
            confidence = float(candidate[2])
            
            # Create unique ID
            pred_id = f"disc_{candidate[1]}_{strategy_id}_{as_of_date}_{datetime.now().strftime('%H%M%S')}"
            timestamp = f"{as_of_date}T12:00:00Z"
            
            # Insert prediction
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
                f"{pred_id}_{as_of_date}",                   # 14. idempotency_key
                f"disc_{pred_id}",                          # 15. scored_event_id
                None,                                       # 16. scored_at
                None,                                       # 17. run_id
                0.0,                                       # 18. predicted_return
                direction,                                   # 19. direction
                pred_id,                                     # 20. prediction_id
                0,                                          # 21. discovery_overlap
                None                                        # 22. playbook_id
            ))
            
            strategy_predictions[strategy_id].append(candidate[1])
    
    total_created = sum(len(symbols) for symbols in strategy_predictions.values())
    
    # Commit the transaction to save predictions
    conn.commit()
    
    return total_created


if __name__ == "__main__":
    # Test the function
    conn = sqlite3.connect("data/alpha.db")
    created = create_discovery_predictions(conn, "2026-04-10", max_per_strategy=3)
    print(f"Created {created} predictions")
    conn.close()
