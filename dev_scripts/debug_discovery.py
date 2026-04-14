import sqlite3
c = sqlite3.connect('data/alpha.db')

# Check symbols with data in default tenant
r = c.execute('SELECT ticker, COUNT(*) FROM price_bars WHERE tenant_id="default" AND timeframe="1d" AND DATE(timestamp) >= "2025-01-01" GROUP BY ticker').fetchall()
print(f'Symbols with data: {len(r)}')
print('Sample tickers:', [x[0] for x in r[:10]])

# Check target stocks
from app.core.target_stocks import get_target_stocks
from datetime import date
targets = get_target_stocks(asof=date(2026, 4, 10))
print(f'\nTarget stocks: {len(targets)}')
print('Sample:', targets[:10])

# Check overlap
overlap = set(x[0] for x in r) & set(targets)
print(f'\nOverlap: {len(overlap)}')
print('Overlapping:', list(overlap)[:10])

c.close()
