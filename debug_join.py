from app.db.repository import AlphaRepository
import datetime

db = AlphaRepository()

# Check strategy IDs in predictions
rows = db.conn.execute('''
    SELECT DISTINCT strategy_id, COUNT(*) as count
    FROM predictions 
    WHERE date(timestamp) = "2026-04-15"
    GROUP BY strategy_id
''').fetchall()

print('Strategy IDs in today predictions:')
for r in rows:
    print(f'  Strategy {r["strategy_id"]}: {r["count"]} predictions')

# Check what strategies exist
rows2 = db.conn.execute('SELECT id, name FROM strategies').fetchall()
print('\nAll strategies:')
for r in rows2:
    print(f'  ID {r["id"]}: {r["name"]}')

# Test the join
rows3 = db.conn.execute('''
    SELECT COUNT(*) as count
    FROM predictions p 
    JOIN strategies s ON p.strategy_id = s.id 
    WHERE date(p.timestamp) = "2026-04-15"
''').fetchone()

print(f'\nPredictions after JOIN: {rows3["count"]}')

# Check for mismatched IDs
prediction_ids = {r['strategy_id'] for r in rows}
strategy_ids = {r['id'] for r in rows2}
missing = prediction_ids - strategy_ids
print(f'\nStrategy IDs in predictions but not in strategies: {missing}')
