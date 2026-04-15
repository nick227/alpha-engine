import sys
from datetime import datetime, timezone
from typing import List

# Add current directory to path so we can import app
import os
sys.path.append(os.getcwd())

from app.core.types import RawEvent
from app.runtime.pipeline import AlphaPipeline
from app.db.repository import AlphaRepository
from app.core.target_stocks import get_target_stocks

def verify():
    print("--- Starting Ranking Feature Verification ---")
    
    # Use a separate test DB
    if os.path.exists("data/test_alpha.db"):
        os.remove("data/test_alpha.db")
        
    repo = AlphaRepository("data/test_alpha.db")
    pipeline = AlphaPipeline(repository=repo)
    
    # 1. Create mock events for some target stocks
    tickers = get_target_stocks()
    print(f"Target Universe: {tickers}")
    
    raw_events = []
    price_contexts = {}
    
    for i, ticker in enumerate(tickers[:3]):
        event_id = f"evt_{ticker}_{i}"
        raw_events.append(RawEvent(
            id=event_id,
            timestamp=datetime.now(timezone.utc),
            source="test",
            text=f"Record demand for {ticker} chips in datacenter sector.",
            tickers=[ticker]
        ))
        
        # Mock price context
        price_contexts[event_id] = {
            "entry_price": 100.0 + i,
            "realized_volatility": 0.02,
            "historical_volatility": [0.01, 0.02, 0.015],
            "short_trend": 0.05,
            "zscore_20": 1.5,
            "continuation_slope": 0.1
        }

    # 2. Run pipeline
    print("Running pipeline...")
    results = pipeline.run_pipeline(raw_events, price_contexts)
    print(f"Pipeline finished. Predictions generated: {len(results['predictions'])}")
    
    # 3. Finalize run (trigger ranking engine)
    print("Finalizing run (triggering ranking engine)...")
    rankings = pipeline.finalize_run()
    
    print(f"Rankings computed: {len(rankings)}")
    for r in rankings[:5]:
        print(f"  {r.ticker}: Score={r.score}, Conviction={r.conviction}, Regime={r.regime}")
        print(f"    Attribution: {r.attribution}")
        
    # 4. Verify DB storage
    print("Verifying DB storage...")
    latest_rankings = repo.get_latest_rankings()
    print(f"Rankings retrieved from DB: {len(latest_rankings)}")
    
    if len(latest_rankings) > 0:
        print("SUCCESS: Rankings successfully stored and retrieved.")
    else:
        print("FAILURE: No rankings found in DB.")
        sys.exit(1)

if __name__ == "__main__":
    verify()
