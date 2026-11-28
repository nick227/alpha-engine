import sqlite3

conn = sqlite3.connect('data/alpha.db')
c = conn.cursor()
c.execute("SELECT max(timestamp) FROM price_bars WHERE ticker IN ('AAPL', 'SPY')")
print('MAX TIMESTAMP IN DB:', c.fetchall())

c.execute("SELECT ticker, timeframe, timestamp, close FROM price_bars WHERE ticker IN ('AAPL', 'SPY') ORDER BY timestamp DESC LIMIT 10")
for row in c.fetchall():
    print(row)

c.execute("SELECT DISTINCT date(timestamp) FROM price_bars WHERE timestamp >= '2026-11-20' ORDER BY timestamp DESC")
print('RECENT SESSIONS:', c.fetchall())
conn.close()
