import sys
import os
import sqlite3

# Add project root to path
sys.path.append(os.getcwd())

from app.core.canonical_scoring import score_prediction, score_strategy
from app.db.repository import AlphaRepository

def verify_math():
    repo = AlphaRepository("data/alpha.db")
    
    # 1. Pick a few Canonical rows
    rows = repo.conn.execute("""
        SELECT strategy_id, ticker, timeframe, direction_hit_rate, total_return_actual, 
               sync_rate, alpha_prediction, alpha_prev
        FROM prediction_scores 
        WHERE alpha_version = 'canonical_v1'
        LIMIT 3
    """).fetchall()
    
    print("--- Audit: Prediction Alpha (canonical_v1) ---")
    for r in rows:
        # Re-run formula manually
        dir_hit = bool(r["direction_hit_rate"] >= 0.5)
        roi = float(r["total_return_actual"])
        conf = float(r["sync_rate"])
        
        calculated = score_prediction(
            direction_correct=dir_hit,
            return_pct=roi,
            confidence=conf
        )
        
        stored = float(r["alpha_prediction"])
        diff = abs(calculated - stored)
        
        status = "PASSED" if diff < 0.0001 else "FAILED"
        print(f"Strat: {r['strategy_id']} | Ticker: {r['ticker']}")
        print(f"  Inputs: DirHit={dir_hit}, ROI={roi:.4f}, Conf={conf:.4f}")
        print(f"  Calculated: {calculated:.4f} | Stored: {stored:.4f} | Status: {status}")
        print(f"  Regression Check (Old Efficiency): {r['alpha_prev']:.4f}")
        print("-" * 40)

    # 2. Audit rolling strategy logic
    print("\n--- Audit: Strategy Alpha (Rolling Window) ---")
    # Pick a strategy that has multiple samples
    strat_id = rows[0]["strategy_id"]
    preds = repo.get_rolling_predictions(strategy_id=strat_id, limit=50)
    
    if not preds:
        print("No rolling predictions found for strategy.")
        return

    pred_alphas = [p["alpha_prediction"] for p in preds]
    max_dd = max([p.get("max_drawdown", 0) for p in preds]) # Proxy
    
    strat_metrics = score_strategy(
        prediction_scores=pred_alphas,
        max_drawdown=max_dd
    )
    
    print(f"Strategy: {strat_id}")
    print(f"  Sample Size: {len(pred_alphas)}")
    print(f"  Avg Prediction Alpha: {strat_metrics['avg_prediction_alpha']:.4f}")
    print(f"  Variance Penalty: {strat_metrics['variance_penalty']:.4f}")
    print(f"  Drawdown Penalty: {strat_metrics['drawdown_penalty']:.4f}")
    print(f"  Final Alpha Strategy: {strat_metrics['alpha_strategy']:.4f}")
    
    repo.close()

if __name__ == "__main__":
    verify_math()
