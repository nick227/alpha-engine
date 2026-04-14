import sys
import os
import logging
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

from app.db.repository import AlphaRepository
from app.engine.prediction_scoring_runner import PredictionScoringRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

def seed_alpha():
    repo = AlphaRepository("data/alpha.db") # Using the correct DB path found
    runner = PredictionScoringRunner(repository=repo)
    
    # 1. Fetch all prediction runs
    runs = repo.conn.execute("SELECT id, tenant_id FROM prediction_runs").fetchall()
    log.info(f"Found {len(runs)} prediction runs to re-score.")
    
    total_series = 0
    for run in runs:
        run_id = run["id"]
        tenant_id = run["tenant_id"]
        
        log.info(f"Re-scoring run {run_id}...")
        
        # 2. Fetch existing scores to preserve efficiency_rating as alpha_prev
        existing_scores = repo.conn.execute(
            "SELECT strategy_id, ticker, timeframe, efficiency_rating FROM prediction_scores WHERE run_id = ?",
            (run_id,)
        ).fetchall()
        prev_map = {(r["strategy_id"], r["ticker"], r["timeframe"]): r["efficiency_rating"] for r in existing_scores}
        
        # 3. Re-run scoring (this will trigger save_prediction_score with new logic)
        # We need to wrap the save_prediction_score to include the versioning fields
        
        # Monkey-patch or just use a custom loop if we want more control
        # Let's use the runner.score_run but intercept the results
        
        results = runner.score_run(run_id=run_id, tenant_id=tenant_id, materialize_actual=True)
        
        for res in results:
            # Enrich with versioning and metrics
            key = (res["strategy_id"], res["ticker"], res["timeframe"])
            
            # Additional metrics
            # alpha_sample_count: how many series points were compared
            # alpha_window_days: roughly the duration of the comparison
            
            res["alpha_version"] = "canonical_v1"
            res["alpha_prev"] = prev_map.get(key)
            
            # Fetch rolling context for sample count and window
            # (In a real scenario, score_sync would provide this, but we can proxy)
            res["alpha_sample_count"] = res.get("forecast_days", 0) # Simplification
            res["alpha_window_days"] = res.get("forecast_days", 0)
            
            repo.save_prediction_score(res, tenant_id=tenant_id)
            total_series += 1
            
    log.info(f"Successfully re-scored {total_series} series with 'canonical_v1' versioning.")
    repo.close()

if __name__ == "__main__":
    seed_alpha()
