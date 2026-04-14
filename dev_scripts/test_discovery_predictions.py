import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from app.engine.predicted_series_builder import PredictedSeriesBuilder, BuildConfig
from app.db.repository import AlphaRepository
from datetime import datetime, timezone
from uuid import uuid4

# Test building predictions for discovery strategies directly
repo = AlphaRepository("data/alpha.db")
builder = PredictedSeriesBuilder(repository=repo)

# Test with our discovery strategy
config = BuildConfig(
    model="directional_drift",
    signal_source="consensus",  # Keep consensus for now
    vol_lookback=20,
    tenant_id="default"
)

# Get a sample discovery candidate
symbol = "AAPL"
run_id = str(uuid4())

print(f"Building prediction series for {symbol} using directional_drift...")

try:
    result = builder.build(
        run_id=run_id,
        ticker=symbol,
        config=config
    )
    print(f"Result: {result}")
    print(f"Points written: {len(result.points) if result.points else 0}")
    print(f"Model used: {result.model_used}")
    print(f"Skip reason: {result.skip_reason}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
