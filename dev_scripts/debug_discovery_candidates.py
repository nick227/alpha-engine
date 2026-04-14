#!/usr/bin/env python3
"""
Debug discovery candidates to see if ETF filtering is working
"""

from datetime import date
from app.db.repository import AlphaRepository
from app.discovery.runner import run_discovery

repo = AlphaRepository('data/alpha.db')
result = run_discovery(
    db_path='data/alpha.db',
    as_of=date(2026, 4, 13),
    min_avg_dollar_volume_20d=2_000_000,
    use_feature_snapshot=True
)

candidates = result.get('strategies', {}).get('silent_compounder', {}).get('top', [])
print('Top 15 candidates:')
for i, c in enumerate(candidates[:15]):
    print(f'{i+1}. {c["symbol"]} - score: {c["score"]}')
