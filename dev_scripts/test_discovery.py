import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.discovery.runner import run_discovery

# Test discovery with feature_snapshot
result = run_discovery(
    db_path="data/alpha.db",
    as_of="2026-04-10",
    use_target_universe=False,  # Use ALL symbols
    use_feature_snapshot=True,
    min_avg_dollar_volume_20d=1000000,  # $1M min
)

print(f"feature_rows: {result['feature_rows']}")
print(f"strategies: {list(result['strategies'].keys())}")

for strat, data in result['strategies'].items():
    print(f"\n{strat}:")
    print(f"  top: {len(data['top'])} candidates")
    if data['top']:
        print(f"  first: {data['top'][0]['symbol']} score={data['top'][0]['score']:.3f}")
    print(f"  top_lt5: {len(data['top_lt5'])} candidates")
