from app.db.repository import AlphaRepository
import datetime

db = AlphaRepository()
today = datetime.date(2026, 4, 15)
start = datetime.datetime.combine(today, datetime.datetime.min.time())
end = datetime.datetime.combine(today, datetime.datetime.max.time())

rows = db.conn.execute('''
    SELECT p.ticker, p.confidence, p.prediction, s.name as strategy
    FROM predictions p 
    JOIN strategies s ON p.strategy_id = s.id 
    WHERE p.timestamp >= ? AND p.timestamp <= ? 
    ORDER BY p.confidence DESC 
    LIMIT 10
''', (start.isoformat(), end.isoformat())).fetchall()

print('Top 10 predictions today:')
for i, r in enumerate(rows, 1):
    print(f'{i}. {r["ticker"]}: {r["confidence"]:.3f} - {r["strategy"]} ({r["prediction"]})')

print(f'\nTotal found: {len(rows)}')
