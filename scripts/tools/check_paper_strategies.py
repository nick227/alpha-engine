import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))

from app.db.repository import AlphaRepository

db = AlphaRepository()
rows = db.conn.execute('SELECT id, name FROM strategies WHERE id LIKE "%_paper"').fetchall()
print('Paper strategies:')
for r in rows:
    print(f'  {r["id"]}: {r["name"]}')
