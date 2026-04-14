import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check what predictions the replay loop considers "expired"
# This mimics the logic in ReplayWorker.list_unscored_predictions

replay_date = "2026-04-13T00:00:00+00:00"

# Check unscored predictions (this is what replay looks for)
unscored = conn.execute("""
    SELECT p.id, p.strategy_id, p.ticker, p.timestamp, p.direction, p.confidence
    FROM predictions p
    LEFT JOIN prediction_outcomes o ON o.prediction_id = p.id
    WHERE p.timestamp <= ? AND o.prediction_id IS NULL
    ORDER BY p.timestamp ASC
    LIMIT 10
""", (replay_date,)).fetchall()

print("=== Unscored Predictions (what replay sees) ===")
for row in unscored:
    print(f"{row['strategy_id']} {row['ticker']}: {row['direction']} (conf: {row['confidence']:.2f}) at {row['timestamp']}")

# Check specifically for our discovery predictions
discovery_unscored = conn.execute("""
    SELECT p.id, p.strategy_id, p.ticker, p.timestamp, p.direction, p.confidence
    FROM predictions p
    LEFT JOIN prediction_outcomes o ON o.prediction_id = p.id
    WHERE p.timestamp <= ? AND o.prediction_id IS NULL 
      AND p.strategy_id LIKE '%_v1_default' AND p.mode = 'discovery'
    ORDER BY p.timestamp ASC
""", (replay_date,)).fetchall()

print(f"\n=== Discovery Predictions that should be scored ===")
for row in discovery_unscored:
    print(f"{row['strategy_id']} {row['ticker']}: {row['direction']} (conf: {row['confidence']:.2f}) at {row['timestamp']}")

print(f"\nTotal unscored predictions: {len(unscored)}")
print(f"Discovery predictions that should be scored: {len(discovery_unscored)}")
