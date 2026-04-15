from app.db.repository import AlphaRepository
import datetime

db = AlphaRepository()

# Check raw timestamps
rows = db.conn.execute('SELECT timestamp, ticker, confidence FROM predictions WHERE date(timestamp) = "2026-04-15" LIMIT 5').fetchall()

print('Raw timestamps for today:')
for r in rows:
    print(f'  {r["timestamp"]} - {r["ticker"]} (conf: {r["confidence"]})')

# Check the query bounds
today = datetime.date(2026, 4, 15)
start = datetime.datetime.combine(today, datetime.datetime.min.time())
end = datetime.datetime.combine(today, datetime.datetime.max.time())

print(f'\nQuery bounds:')
print(f'  Start: {start.isoformat()}')
print(f'  End: {end.isoformat()}')

# Test the query
rows2 = db.conn.execute('''
    SELECT COUNT(*) as count
    FROM predictions 
    WHERE timestamp >= ? AND timestamp <= ?
''', (start.isoformat(), end.isoformat())).fetchone()

print(f'\nPredictions in query range: {rows2["count"]}')

# Test without time bounds
rows3 = db.conn.execute('''
    SELECT COUNT(*) as count
    FROM predictions 
    WHERE date(timestamp) = "2026-04-15"
''').fetchone()

print(f'Predictions by date only: {rows3["count"]}')
