from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from app.core.price_context import build_price_contexts_from_bars
from app.core.types import MRAOutcome, RawEvent, ScoredEvent, StrategyConfig
from app.strategies.cross_asset.relative_strength import RelativeStrengthVsBenchmarkStrategy
from app.strategies.technical.range_breakout_continuation import RangeBreakoutContinuationStrategy


def _bars(ticker: str, start: datetime, n: int, step_minutes: int, base: float, drift: float) -> pd.DataFrame:
    rows = []
    for i in range(n):
        ts = start + timedelta(minutes=i * step_minutes)
        px = base + drift * i
        rows.append(
            {
                "ticker": ticker,
                "timestamp": ts,
                "open": px,
                "high": px * 1.001,
                "low": px * 0.999,
                "close": px,
                "volume": 1000 + i,
            }
        )
    return pd.DataFrame(rows)


def _scored() -> ScoredEvent:
    return ScoredEvent(
        id="scored_1",
        raw_event_id="raw_1",
        primary_ticker="NVDA",
        category="guidance_raise",
        materiality=0.8,
        direction="positive",
        confidence=0.9,
        company_relevance=0.9,
        concept_tags=[],
        explanation_terms=[],
        scorer_version="v2",
        taxonomy_version="v1",
    )


def _mra() -> MRAOutcome:
    return MRAOutcome(
        id="mra_1",
        scored_event_id="scored_1",
        return_1m=0.0,
        return_5m=0.0,
        return_15m=0.0,
        return_1h=0.0,
        volume_ratio=2.0,
        vwap_distance=0.01,
        range_expansion=1.6,
        continuation_slope=0.004,
        pullback_depth=0.02,
        mra_score=0.5,
        market_context={},
    )


def test_price_context_adds_benchmark_relative_returns() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    # 1d horizon in builder uses 1440 minutes; create 2+ days of 1m bars.
    nvda = _bars("NVDA", start, n=4000, step_minutes=1, base=100.0, drift=0.01)
    spy = _bars("SPY", start, n=4000, step_minutes=1, base=400.0, drift=0.002)
    bars = pd.concat([nvda, spy], ignore_index=True)

    evt_ts = start + timedelta(minutes=3000)
    evt = RawEvent(
        id="raw_1",
        timestamp=evt_ts,
        source="test",
        text="x",
        tickers=["NVDA"],
    )

    ctxs = build_price_contexts_from_bars(raw_events=[evt], bars=bars, horizons_minutes=(1440, 10080, 43200), benchmark_tickers=("SPY",))
    ctx = ctxs["raw_1"]
    assert "benchmarks" in ctx
    assert "rel_return_1d_vs_SPY" in ctx


def test_relative_strength_emits_30d() -> None:
    cfg = StrategyConfig(
        id="rs-v1",
        name="rs",
        version="v1",
        strategy_type="cross_asset_relative_strength",
        mode="backtest",
        config={"benchmark": "SPY", "signal_horizon": "7d", "horizon": "30d", "min_rel_return": 0.01},
        active=True,
    )
    s = RelativeStrengthVsBenchmarkStrategy(cfg)
    ts = datetime(2026, 4, 12, tzinfo=timezone.utc)
    price_ctx = {
        "entry_price": 100.0,
        "return_7d": 0.08,
        "benchmarks": {"SPY": {"return_7d": 0.02}},
        "trend_strength": "STRONG",
    }
    pred = s.maybe_predict(_scored(), _mra(), price_ctx, ts)
    assert pred is not None
    assert pred.horizon == "30d"
    assert pred.feature_snapshot.get("benchmark") == "SPY"


def test_range_breakout_emits_30d() -> None:
    cfg = StrategyConfig(
        id="bo-v1",
        name="bo",
        version="v1",
        strategy_type="technical_range_breakout_continuation",
        mode="backtest",
        config={"horizon": "30d", "breakout_buffer": 0.0, "min_range_expansion": 1.0, "min_volume_ratio": 1.0},
        active=True,
    )
    s = RangeBreakoutContinuationStrategy(cfg)
    ts = datetime(2026, 4, 12, tzinfo=timezone.utc)
    price_ctx = {
        "entry_price": 110.0,
        "rolling_high_20": 109.0,
        "rolling_low_20": 100.0,
        "range_expansion": 1.2,
        "volume_ratio": 1.5,
        "regime": "NORMAL",
    }
    pred = s.maybe_predict(_scored(), _mra(), price_ctx, ts)
    assert pred is not None
    assert pred.horizon == "30d"
    assert pred.prediction == "up"

