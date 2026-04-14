import sqlite3
from datetime import datetime, timezone
from app.db.repository import AlphaRepository
from app.engine.trust_engine import TrustEngine

# Compute trust specifically for our discovery strategies with 5d horizon
repo = AlphaRepository("data/alpha.db")
trust_engine = TrustEngine()

discovery_strategies = [
    "balance_sheet_survivor_v1_default",
    "narrative_lag_v1_default", 
    "ownership_vacuum_v1_default",
    "realness_repricer_v1_default",
    "silent_compounder_v1_default"
]

print("=== Computing Trust for Discovery Strategies (5d horizon) ===")

for strategy_id in discovery_strategies:
    try:
        result = trust_engine.compute_strategy_trust(
            conn=repo.conn,
            tenant_id="default",
            strategy_id=strategy_id,
            horizon="5d",
            as_of=datetime(2026, 4, 15, tzinfo=timezone.utc)
        )
        
        print(f"{strategy_id}:")
        print(f"  Trust: {result.trust_score:.3f}")
        print(f"  Calibration: {result.calibration_score:.3f}")
        print(f"  Stability: {result.stability_score:.3f}")
        print(f"  Sample Size: {result.sample_size}")
        print()
        
    except Exception as e:
        print(f"Error computing trust for {strategy_id}: {e}")

print("\n=== Baseline Comparison ===")
baseline_strategies = ["sentiment_v1_default", "technical_v2_default"]

for strategy_id in baseline_strategies:
    try:
        result = trust_engine.compute_strategy_trust(
            conn=repo.conn,
            tenant_id="default",
            strategy_id=strategy_id,
            horizon="1d",
            as_of=datetime(2026, 4, 15, tzinfo=timezone.utc)
        )
        
        print(f"{strategy_id}:")
        print(f"  Trust: {result.trust_score:.3f}")
        print(f"  Calibration: {result.calibration_score:.3f}")
        print(f"  Stability: {result.stability_score:.3f}")
        print(f"  Sample Size: {result.sample_size}")
        print()
        
    except Exception as e:
        print(f"Error computing trust for {strategy_id}: {e}")
