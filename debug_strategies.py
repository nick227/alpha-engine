from app.db.repository import AlphaRepository

db = AlphaRepository()

# Check if these IDs already exist
rows = db.conn.execute('SELECT id, name FROM strategies WHERE id IN (?, ?)', 
                       ('silent_compounder_v1_paper', 'balance_sheet_survivor_v1_paper')).fetchall()

print('Existing paper strategies:')
for r in rows:
    print(f'  {r["id"]}: {r["name"]}')

# Try to insert without IGNORE to see the error
try:
    db.conn.execute('INSERT INTO strategies (id, tenant_id, name, version, strategy_type, mode, active, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', 
                   ('silent_compounder_v1_paper', 'default', 'Silent Compounder v1 Paper', 'v1', 'discovery', 'paper', 1, 'ACTIVE'))
    db.conn.commit()
    print('Inserted silent_compounder_v1_paper')
except Exception as e:
    print(f'Error inserting silent_compounder_v1_paper: {e}')

# Check what's actually in the strategies table that might be conflicting
rows2 = db.conn.execute('SELECT id, name FROM strategies WHERE name LIKE "%Silent%" OR name LIKE "%Balance%"').fetchall()
print('\nSimilar strategies:')
for r in rows2:
    print(f'  {r["id"]}: {r["name"]}')
