import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check our specific discovery strategy predictions
discovery_strategies = [
    "realness_repricer_v1_default",
    "silent_compounder_v1_default", 
    "narrative_lag_v1_default",
    "balance_sheet_survivor_v1_default",
    "ownership_vacuum_v1_default"
]

print("=== New Discovery Strategy Predictions ===")
total_predictions = 0
for strategy_id in discovery_strategies:
    predictions = conn.execute("""
        SELECT ticker, timestamp, direction, confidence
        FROM predictions
        WHERE strategy_id = ?
        ORDER BY timestamp DESC
        LIMIT 5
    """, (strategy_id,)).fetchall()
    
    print(f"\n{strategy_id}:")
    for row in predictions:
        print(f"  {row['ticker']}: {row['direction']} (conf: {row['confidence']:.2f}) at {row['timestamp']}")
        total_predictions += 1

print(f"\nTotal discovery predictions: {total_predictions}")

# Check if these should be scored by replay
replay_cutoff = "2026-04-12T00:00:00+00:00"
scoreable = conn.execute("""
    SELECT COUNT(*) as count
    FROM predictions
    WHERE strategy_id IN (?, ?, ?, ?, ?) AND timestamp <= ?
""", (*discovery_strategies, replay_cutoff)).fetchone()

print(f"Predictions scoreable by replay: {scoreable['count']}")
