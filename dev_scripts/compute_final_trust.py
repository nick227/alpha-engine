import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.engine.trust_engine import TrustEngine

def compute_final_trust():
    """Compute trust scores for discovery strategies."""
    conn = sqlite3.connect("data/alpha.db")
    
    expiry_date = datetime(2026, 3, 30, 13, tzinfo=timezone.utc)
    
    print(f"Computing trust scores as of {expiry_date.isoformat()}")
    print("=" * 60)
    
    trust_engine = TrustEngine()
    
    strategies = [
        "realness_repricer_v1_default",
        "narrative_lag_v1_default", 
        "silent_compounder_v1_default",
        "ownership_vacuum_v1_default",
        "balance_sheet_survivor_v1_default"
    ]
    
    for strategy_id in strategies:
        try:
            result = trust_engine.compute_strategy_trust(
                conn=conn,
                tenant_id="default",
                strategy_id=strategy_id,
                horizon="5d",
                as_of=expiry_date
            )
            
            print(f"{strategy_id}:")
            print(f"  Trust: {result.trust_score:.3f}")
            print(f"  Calibration: {result.calibration_score:.3f}")
            print(f"  Stability: {result.stability_score:.3f}")
            print(f"  Sample Size: {result.sample_size}")
            print()
            
        except Exception as e:
            print(f"{strategy_id}: Error - {e}")
            print()
    
    # Compare with baseline
    print("BASELINE COMPARISON:")
    print("=" * 30)
    
    baseline_strategies = [
        "sentiment_v1_default",
        "technical_v2_default", 
        "quant_v3_default"
    ]
    
    for strategy_id in baseline_strategies:
        try:
            result = trust_engine.compute_strategy_trust(
                conn=conn,
                tenant_id="default",
                strategy_id=strategy_id,
                horizon="1d",
                as_of=expiry_date
            )
            
            print(f"{strategy_id}:")
            print(f"  Trust: {result.trust_score:.3f}")
            print(f"  Sample Size: {result.sample_size}")
            print()
            
        except Exception as e:
            print(f"{strategy_id}: Error - {e}")
            print()
    
    conn.close()

if __name__ == "__main__":
    compute_final_trust()
