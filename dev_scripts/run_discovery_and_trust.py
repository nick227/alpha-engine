import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.discovery.runner import run_discovery
from app.engine.trust_engine import TrustEngine
from app.db.repository import AlphaRepository
from datetime import datetime, timezone

# 1. Run discovery with new calibrated settings
print("=== Running Discovery ===")
discovery_result = run_discovery(
    db_path="data/alpha.db",
    as_of="2026-04-10",
    use_target_universe=False,
    use_feature_snapshot=True,
    min_avg_dollar_volume_20d=1000000,
)

print(f"feature_rows: {discovery_result['feature_rows']}")
for strat, data in discovery_result['strategies'].items():
    print(f"{strat}: {len(data['top'])} total, {len(data['top_lt5'])} strong")

# 2. Save discovery candidates to database
print("\n=== Saving Discovery Candidates ===")
repo = AlphaRepository("data/alpha.db")

for strat, data in discovery_result['strategies'].items():
    if data['top']:
        repo.upsert_discovery_candidates(
            as_of_date="2026-04-10",
            candidates=data['top']
        )
        print(f"Saved {len(data['top'])} candidates for {strat}")

# 3. Compute trust scores
print("\n=== Computing Trust Scores ===")
trust_engine = TrustEngine()
conn = repo.conn

# Get all strategies with discovery candidates
strategies_with_candidates = conn.execute("""
    SELECT DISTINCT strategy_type 
    FROM discovery_candidates 
    WHERE as_of_date = '2026-04-10'
""").fetchall()

trust_results = []
for row in strategies_with_candidates:
    strategy_type = row['strategy_type']
    
    # Compute trust for both horizons
    for horizon in ['5d', '20d']:
        try:
            trust_result = trust_engine.compute_strategy_trust(
                conn=conn,
                tenant_id="default",
                strategy_id=strategy_type,
                horizon=horizon,
                as_of=datetime(2026, 4, 10, tzinfo=timezone.utc)
            )
            trust_results.append((strategy_type, horizon, trust_result))
            print(f"{strategy_type} {horizon}: trust={trust_result.trust_score:.3f}, sample={trust_result.sample_size}")
        except Exception as e:
            print(f"Error computing trust for {strategy_type} {horizon}: {e}")

# 4. Summary
print("\n=== Trust Summary ===")
for strategy_type, horizon, trust in trust_results:
    print(f"{strategy_type} {horizon}: trust={trust.trust_score:.3f} (sample={trust.sample_size})")

avg_trust = sum(t.trust_score for _, _, t in trust_results) / len(trust_results)
print(f"\nAverage trust across all strategies: {avg_trust:.3f}")

print("\n=== Trust vs Baseline ===")
print("Previous baseline trust: ~0.42")
print(f"New average trust: {avg_trust:.3f}")
if avg_trust > 0.42:
    print("✅ Trust improved - discovery calibration successful!")
else:
    print("❌ Trust did not improve - may need further tightening")
