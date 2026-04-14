import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check our discovery predictions
discovery_predictions = conn.execute("""
    SELECT strategy_id, ticker, timestamp, direction, confidence
    FROM predictions
    WHERE strategy_id LIKE '%_v1_default' AND mode = 'discovery'
    ORDER BY timestamp DESC
    LIMIT 10
""").fetchall()

print("=== Discovery Predictions Created ===")
for row in discovery_predictions:
    print(f"{row['strategy_id']} {row['ticker']}: {row['direction']} (conf: {row['confidence']:.2f}) at {row['timestamp']}")

# Check if they have outcomes
with_outcomes = conn.execute("""
    SELECT p.strategy_id, COUNT(*) as count
    FROM predictions p
    JOIN prediction_outcomes o ON o.prediction_id = p.id
    WHERE p.strategy_id LIKE '%_v1_default' AND p.mode = 'discovery'
    GROUP BY p.strategy_id
""").fetchall()

print("\n=== Discovery Predictions with Outcomes ===")
for row in with_outcomes:
    print(f"{row['strategy_id']}: {row['count']} with outcomes")

# Check replay cutoff
replay_date = "2026-04-13T00:00:00+00:00"
print(f"\nReplay was run at: {replay_date}")

# Count discovery predictions before replay date
before_replay = conn.execute("""
    SELECT COUNT(*) as count
    FROM predictions
    WHERE strategy_id LIKE '%_v1_default' AND mode = 'discovery' AND timestamp <= ?
""", (replay_date,)).fetchone()

print(f"Discovery predictions before replay: {before_replay['count']}")

# Check unscored predictions
unscored = conn.execute("""
    SELECT COUNT(*) as count
    FROM predictions p
    LEFT JOIN prediction_outcomes o ON o.prediction_id = p.id
    WHERE p.strategy_id LIKE '%_v1_default' AND p.mode = 'discovery' AND o.prediction_id IS NULL
""").fetchone()

print(f"Discovery predictions without outcomes: {unscored['count']}")
