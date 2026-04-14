import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check what the prediction queue actually contains
queue_items = conn.execute("""
    SELECT symbol, source, status, metadata_json
    FROM prediction_queue
    WHERE source = 'discovery' AND as_of_date = '2026-04-10'
    ORDER BY created_at DESC
    LIMIT 5
""").fetchall()

print("=== Prediction Queue Items ===")
for i, row in enumerate(queue_items):
    print(f"{i+1}. {row['symbol']}: {row['status']}")
    if row['metadata_json']:
        import json
        meta = json.loads(row['metadata_json'])
        print(f"   Strategy: {meta.get('strategy_id')}")
        print(f"   Direction: {meta.get('direction')}")
        print(f"   Confidence: {meta.get('confidence')}")

# Check if these strategy_ids exist in strategies table
strategy_ids = set()
for row in queue_items:
    if row['metadata_json']:
        meta = json.loads(row['metadata_json'])
        strategy_ids.add(meta.get('strategy_id', ''))

print(f"\n=== Strategy IDs in Queue ===")
for sid in strategy_ids:
    exists = conn.execute("SELECT id FROM strategies WHERE id = ?", (sid,)).fetchone()
    print(f"{sid}: {'EXISTS' if exists else 'MISSING'}")
