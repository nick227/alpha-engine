import sqlite3
import sys
sys.path.insert(0, '/home/administrator/web/alpha-trading-platform/ae-staging')
from app.research_publication.service import load_bars

conn = sqlite3.connect('/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db')
bars = load_bars(conn, ['AAPL'], 2, until='2026-11-25')
print('AAPL BARS:')
for b in bars['AAPL']:
    print(b)
