import sqlite3

conn = sqlite3.connect('/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

c.execute("SELECT * FROM price_bars WHERE ticker = 'AAPL' AND timestamp LIKE '2026-11-25%'")
rows = c.fetchall()
print("AAPL 2026-11-25 ROWS COUNT:", len(rows))
for r in rows:
    print(dict(r))

c.execute("SELECT ticker, timestamp, open, high, low, close, volume FROM price_bars WHERE timestamp LIKE '2026-11-25%' AND timeframe = '1d'")
universe_rows = c.fetchall()
print("\nALL 2026-11-25 1d BARS COUNT:", len(universe_rows))
for r in universe_rows:
    print(dict(r))

conn.close()
