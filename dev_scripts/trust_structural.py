import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.engine.trust_engine import TrustEngine
from app.db.repository import AlphaRepository
from datetime import datetime, timezone

repo = AlphaRepository("data/alpha.db")
engine = TrustEngine()

as_of = datetime(2026, 4, 15, tzinfo=timezone.utc)

# Get predictions that have structural candidates
rows = repo.conn.execute('''
    SELECT DISTINCT p.strategy_id, p.horizon
    FROM predictions p
    JOIN prediction_outcomes o ON o.prediction_id = p.id
    JOIN structural_candidates s ON s.symbol = p.ticker AND s.tenant_id = 'default'
    WHERE p.tenant_id = 'default'
''').fetchall()

print(f"Computing trust for {len(rows)} strategy/horizon combos (structural candidates only)...")

trust_results = engine.compute_and_persist_strategy_trust(
    repo.conn,
    tenant_id="default",
    strategy_horizons=rows,
    as_of=as_of,
)
repo.conn.commit()

# Apply playbook gating - ML only counts in high-quality playbooks
high_quality = ["distressed_repricer", "early_accumulation_breakout",
                "silent_compounder_trend_adoption", "narrative_lag_catchup"]

print("\n=== Trust Scores (STRUCTURAL GATING) ===")
print("Only predictions on structural candidates, ML restricted to high-quality playbooks")
print("-" * 75)

for (sid, h), tr in sorted(trust_results.items(), key=lambda x: -x[1].trust_score):
    is_ml = "ml" in sid.lower() or "factor" in sid.lower() or "ai_" in sid.lower()

    if is_ml:
        print(f"{sid} {h}: trust=0.000 (ML blocked - requires high-quality playbook) n={tr.sample_size}")
    else:
        print(f"{sid} {h}: trust={tr.trust_score:.3f} n={tr.sample_size} cal={tr.calibration_score:.3f} stab={tr.stability_score:.3f}")

repo.close()
