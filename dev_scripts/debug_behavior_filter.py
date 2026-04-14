#!/usr/bin/env python3
"""
Debug behavior filtering to see candidate counts
"""

from datetime import date
from app.db.repository import AlphaRepository
from app.discovery.runner import run_discovery
from app.discovery.strategies import score_candidates

repo = AlphaRepository('data/alpha.db')
result = run_discovery(
    db_path='data/alpha.db',
    as_of=date(2026, 4, 13),
    min_avg_dollar_volume_20d=2_000_000,
    use_feature_snapshot=True
)

# Get features for silent_compounder
from app.discovery.feature_snapshot import build_feature_snapshot
features = build_feature_snapshot(
    db_path='data/alpha.db',
    as_of=date(2026, 4, 13)
)

print(f'Total features: {len(features)}')

# Test behavior filtering
all_vol = [fr.volatility_20d for fr in features.values() if fr.volatility_20d is not None]
all_ret = [abs(fr.return_63d) for fr in features.values() if fr.return_63d is not None]

vol_p30 = sorted(all_vol)[int(0.3 * len(all_vol))] if all_vol else 0.01
ret_p40 = sorted(all_ret)[int(0.4 * len(all_ret))] if all_ret else 0.02

print(f'Volatility 30th percentile: {vol_p30:.4f}')
print(f'Return 40th percentile: {ret_p40:.4f}')

# Count what passes filters
pass_quality = 0
pass_vol = 0
pass_ret = 0
pass_all = 0

for sym, fr in features.items():
    # Quality gates
    if fr.close is None or fr.close < 10.0:
        continue
    if fr.dollar_volume is None or fr.dollar_volume < 5_000_000:
        continue
    if sym.startswith('^'):
        continue
    pass_quality += 1
    
    # Behavior filter
    if fr.volatility_20d is not None and fr.volatility_20d < vol_p30:
        continue
    pass_vol += 1
    
    if fr.return_63d is not None and abs(fr.return_63d) < ret_p40:
        continue
    pass_ret += 1
    
    pass_all += 1

print(f'Pass quality: {pass_quality}')
print(f'Pass volatility: {pass_vol}')
print(f'Pass return: {pass_ret}')
print(f'Pass all: {pass_all}')

# Test score_candidates
from app.discovery.strategies import silent_compounder

print(f'\nTop 10 silent_compounder scores:')
scores = []
for sym, fr in features.items():
    # Apply same filters
    if fr.close is None or fr.close < 10.0:
        continue
    if fr.dollar_volume is None or fr.dollar_volume < 5_000_000:
        continue
    if sym.startswith('^'):
        continue
    if fr.volatility_20d is not None and fr.volatility_20d < vol_p30:
        continue
    if fr.return_63d is not None and abs(fr.return_63d) < ret_p40:
        continue
    
    raw, reason, meta = silent_compounder(fr)
    if raw is not None:
        scores.append((sym, raw, reason))

scores.sort(key=lambda x: x[1], reverse=True)
for sym, raw, reason in scores[:10]:
    print(f'{sym}: {raw:.3f} - {reason}')

print(f'\nThreshold check (0.70):')
above_threshold = [s for s in scores if s[1] >= 0.70]
print(f'Above 0.70: {len(above_threshold)}')
if above_threshold:
    for sym, raw, reason in above_threshold[:5]:
        print(f'  {sym}: {raw:.3f}')

# Test with lower threshold
candidates = score_candidates(features, strategy_type='silent_compounder')
print(f'\nCurrent candidates (threshold 0.70): {len(candidates)}')
