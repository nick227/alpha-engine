from app.db.repository import AlphaRepository

db = AlphaRepository()
rows = db.conn.execute('SELECT id, name FROM strategies WHERE id LIKE "%_paper"').fetchall()
print('Paper strategies:')
for r in rows:
    print(f'  {r["id"]}: {r["name"]}')
