from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from app.core.types import RawEvent
from app.runtime.pipeline import run_pipeline

OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

base_time = datetime.now(timezone.utc) - timedelta(hours=1)
raw_events = [
    RawEvent(
        id=f"evt-{i}",
        timestamp=base_time + timedelta(minutes=i * 5),
        source="demo_news",
        text=text,
        tickers=[ticker],
    )
    for i, (ticker, text) in enumerate([
        ("NVDA", "Nvidia datacenter capex surge points to stronger AI infrastructure demand"),
        ("AMD", "AMD supplier disruption sparks concern over delayed server shipments"),
        ("AAPL", "Apple raises guidance after record services growth of 12%"),
        ("TSLA", "Tesla secondary offering revives dilution concern despite EV demand"),
        ("SMCI", "Super Micro wins regulatory approval for new datacenter rack deployment"),
        ("NVDA", "Nvidia pulls back after sharp breakout as RSI reaches extreme territory"),
        ("TSLA", "Tesla reclaims VWAP with heavy volume into midday session"),
    ], start=1)
]

price_contexts = {
    "evt-1": {"entry_price": 100.0, "return_1m": 0.002, "return_5m": 0.007, "return_15m": 0.015, "return_1h": 0.021, "volume_ratio": 2.8, "vwap_distance": 0.006, "range_expansion": 1.7, "continuation_slope": 0.7, "pullback_depth": 0.001, "short_trend": 0.006, "future_return_5m": 0.007, "future_return_15m": 0.015, "future_return_1h": 0.021, "rsi_14": 66.0, "zscore_20": 1.6, "vwap_reclaim": True},
    "evt-2": {"entry_price": 100.0, "return_1m": -0.001, "return_5m": -0.006, "return_15m": -0.013, "return_1h": -0.018, "volume_ratio": 2.1, "vwap_distance": -0.004, "range_expansion": 1.5, "continuation_slope": 0.6, "pullback_depth": 0.002, "short_trend": -0.005, "future_return_5m": -0.006, "future_return_15m": -0.013, "future_return_1h": -0.018, "rsi_14": 41.0, "zscore_20": -1.4, "vwap_reject": True},
    "evt-3": {"entry_price": 100.0, "return_1m": 0.001, "return_5m": 0.004, "return_15m": 0.009, "return_1h": 0.012, "volume_ratio": 1.9, "vwap_distance": 0.003, "range_expansion": 1.3, "continuation_slope": 0.55, "pullback_depth": 0.001, "short_trend": 0.003, "future_return_5m": 0.004, "future_return_15m": 0.009, "future_return_1h": 0.012, "rsi_14": 58.0, "zscore_20": 0.8, "vwap_reclaim": True},
    "evt-4": {"entry_price": 100.0, "return_1m": -0.002, "return_5m": -0.005, "return_15m": -0.008, "return_1h": -0.011, "volume_ratio": 1.7, "vwap_distance": -0.002, "range_expansion": 1.2, "continuation_slope": 0.45, "pullback_depth": 0.002, "short_trend": -0.003, "future_return_5m": -0.005, "future_return_15m": -0.008, "future_return_1h": -0.011, "rsi_14": 63.0, "zscore_20": 1.2, "vwap_reject": True},
    "evt-5": {"entry_price": 100.0, "return_1m": 0.003, "return_5m": 0.008, "return_15m": 0.012, "return_1h": 0.017, "volume_ratio": 2.4, "vwap_distance": 0.005, "range_expansion": 1.6, "continuation_slope": 0.65, "pullback_depth": 0.001, "short_trend": 0.005, "future_return_5m": 0.008, "future_return_15m": 0.012, "future_return_1h": 0.017, "rsi_14": 61.0, "zscore_20": 1.4, "vwap_reclaim": True},
    "evt-6": {"entry_price": 100.0, "return_1m": -0.001, "return_5m": -0.004, "return_15m": -0.009, "return_1h": -0.006, "volume_ratio": 1.6, "vwap_distance": -0.003, "range_expansion": 1.4, "continuation_slope": 0.35, "pullback_depth": 0.003, "short_trend": -0.002, "future_return_5m": -0.004, "future_return_15m": -0.009, "future_return_1h": -0.006, "rsi_14": 79.0, "zscore_20": 2.3, "vwap_reject": True},
    "evt-7": {"entry_price": 100.0, "return_1m": 0.002, "return_5m": 0.006, "return_15m": 0.011, "return_1h": 0.014, "volume_ratio": 2.7, "vwap_distance": 0.007, "range_expansion": 1.8, "continuation_slope": 0.72, "pullback_depth": 0.001, "short_trend": 0.004, "future_return_5m": 0.006, "future_return_15m": 0.011, "future_return_1h": 0.014, "rsi_14": 54.0, "zscore_20": 0.9, "vwap_reclaim": True},
}

result = run_pipeline(raw_events, price_contexts, persist=True)

pd.DataFrame(result["scored_events"]).to_csv(OUTPUT_DIR / "scored_events.csv", index=False)
pd.DataFrame(result["mra_outcomes"]).to_csv(OUTPUT_DIR / "mra_outcomes.csv", index=False)
pd.DataFrame(result["predictions"]).to_csv(OUTPUT_DIR / "predictions.csv", index=False)
pd.DataFrame([result["summary"]]).to_csv(OUTPUT_DIR / "strategy_performance.csv", index=False)

print("Generated:")
print("-", OUTPUT_DIR / "scored_events.csv")
print("-", OUTPUT_DIR / "mra_outcomes.csv")
print("-", OUTPUT_DIR / "predictions.csv")
print("-", OUTPUT_DIR / "strategy_performance.csv")
print("\nSummary:")
print(pd.DataFrame(result["summary"]).to_string(index=False))
