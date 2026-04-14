import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.engine.replay_loop_service import ReplayLoopService
from datetime import datetime, timezone

print("Running replay loop to score all expired predictions...")
service = ReplayLoopService(db_path="data/alpha.db", tenant_id="default")

# Run with date after our predictions to score them
result = service.run_once(now=datetime(2026, 4, 15, tzinfo=timezone.utc))
print(f"Replay result: {result}")

# Now recompute trust
from app.db.repository import AlphaRepository
from app.engine.trust_engine import TrustEngine

repo = AlphaRepository("data/alpha.db")
trust_engine = TrustEngine()

rows = repo.conn.execute('''
    SELECT DISTINCT p.strategy_id, p.horizon
    FROM predictions p
    JOIN prediction_outcomes o ON o.prediction_id = p.id
''').fetchall()

print(f"\nComputing trust for {len(rows)} strategy/horizon combos...")
trust_results = trust_engine.compute_and_persist_strategy_trust(
    repo.conn,
    tenant_id="default",
    strategy_horizons=rows,
)
repo.conn.commit()

print("\n=== Trust Scores (with historical outcomes) ===")
for (sid, h), tr in sorted(trust_results.items(), key=lambda x: -x[1].trust_score)[:15]:
    print(f"{sid} {h}: trust={tr.trust_score:.3f} n={tr.sample_size} cal={tr.calibration_score:.3f} stab={tr.stability_score:.3f}")

repo.close()
print(f"\nPopulated {len(trust_results)} rows in strategy_trust table")
