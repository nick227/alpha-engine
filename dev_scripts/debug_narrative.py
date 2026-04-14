import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.discovery.runner import run_discovery
from app.discovery.strategies import narrative_lag
from app.discovery.types import FeatureRow

# Load features
from app.discovery.runner import _load_features_from_snapshot

features = _load_features_from_snapshot(
    db_path="data/alpha.db",
    as_of_date="2026-04-10",
    symbols=None,
)

# Test narrative_lag manually
passed = []
failed = []

for sym, fr in features.items():
    raw, reason, meta = narrative_lag(fr)
    if raw is not None:
        passed.append((sym, raw, meta))
    else:
        failed.append((sym, reason))

print(f"Passed: {len(passed)}, Failed: {len(failed)}")
print("\nFirst 5 passed:")
for sym, raw, meta in passed[:5]:
    print(f"  {sym}: {raw:.3f} - {meta.get('drivers', [])}")

print("\nFirst 5 failed:")
for sym, reason in failed[:5]:
    print(f"  {sym}: {reason}")
