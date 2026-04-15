from app.db.repository import AlphaRepository

db = AlphaRepository()
# Check predictions for today using timestamp
rows = db.conn.execute('SELECT COUNT(*) as count, strategy_id FROM predictions WHERE date(timestamp) = "2026-04-15" GROUP BY strategy_id').fetchall()
print("Predictions for today:")
for row in rows:
    print(f"  Strategy {row['strategy_id']}: {row['count']}")

# Check strategy names
strategies = db.conn.execute('SELECT id, name FROM strategies').fetchall()
strategy_map = {s['id']: s['name'] for s in strategies}
print("\nStrategy details:")
for row in rows:
    name = strategy_map.get(row['strategy_id'], 'Unknown')
    print(f"  {name}: {row['count']}")

# Check if they have outcomes
rows2 = db.conn.execute('SELECT COUNT(*) as count FROM prediction_outcomes po JOIN predictions p ON po.prediction_id = p.id WHERE date(p.timestamp) = "2026-04-15"').fetchone()
print(f"\nTotal with outcomes: {rows2['count']}")

# Check recent predictions
recent = db.conn.execute('SELECT COUNT(*) as count FROM predictions WHERE date(timestamp) >= "2026-04-13"').fetchone()
print(f"\nTotal predictions since 2026-04-13: {recent['count']}")
