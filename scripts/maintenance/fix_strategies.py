import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))

from app.db.repository import AlphaRepository

db = AlphaRepository()

# Add missing strategy IDs with all required columns
db.conn.execute('INSERT OR IGNORE INTO strategies (id, tenant_id, name, version, strategy_type, mode, active, config_json, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', 
               ('silent_compounder_v1_paper', 'default', 'Silent Compounder v1 Paper', 'v1', 'discovery', 'paper', 1, '{}', 'ACTIVE'))
db.conn.execute('INSERT OR IGNORE INTO strategies (id, tenant_id, name, version, strategy_type, mode, active, config_json, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', 
               ('balance_sheet_survivor_v1_paper', 'default', 'Balance Sheet Survivor v1 Paper', 'v1', 'discovery', 'paper', 1, '{}', 'ACTIVE'))

db.conn.commit()
print('Added missing strategy IDs')

# Verify the fix
rows = db.conn.execute('''
    SELECT p.ticker, p.confidence, s.name as strategy
    FROM predictions p 
    JOIN strategies s ON p.strategy_id = s.id 
    WHERE date(p.timestamp) = "2026-04-15"
    ORDER BY p.confidence DESC 
    LIMIT 5
''').fetchall()

print('\nTop 5 predictions now:')
for i, r in enumerate(rows, 1):
    print(f'{i}. {r["ticker"]}: {r["confidence"]:.3f} - {r["strategy"]}')
