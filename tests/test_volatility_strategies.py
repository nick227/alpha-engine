from __future__ import annotations

from datetime import datetime, timezone

from app.core.types import MRAOutcome, ScoredEvent, StrategyConfig
from app.strategies.technical.vol_crush_reversion import VolCrushMeanReversionStrategy
from app.strategies.technical.vol_expansion_continuation import VolExpansionContinuationStrategy


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


def test_vol_expansion_continuation_emits() -> None:
    cfg = StrategyConfig(
        id="vol-exp-v1",
        name="vol exp",
        version="v1",
        strategy_type="technical_vol_expansion_continuation",
        mode="backtest",
        config={"min_vol_z": 0.8, "min_range_expansion": 1.2, "min_abs_slope": 0.001},
        active=True,
    )
    s = VolExpansionContinuationStrategy(cfg)

    ts = datetime(2026, 4, 12, tzinfo=timezone.utc)
    price_ctx = {
        "entry_price": 100.0,
        "realized_volatility": 0.25,
        "historical_volatility_window": [0.10, 0.11, 0.12, 0.10, 0.11],
        "range_expansion": 1.5,
        "short_trend": 0.003,
        "volume_ratio": 2.2,
    }
    pred = s.maybe_predict(_scored(), _mra(), price_ctx, ts)
    assert pred is not None
    assert pred.prediction in {"up", "down"}
    assert pred.feature_snapshot.get("setup") == "vol_expansion_continuation"


def test_vol_crush_mean_reversion_emits() -> None:
    cfg = StrategyConfig(
        id="vol-crush-v1",
        name="vol crush",
        version="v1",
        strategy_type="technical_vol_crush_reversion",
        mode="backtest",
        config={"min_prev_max_vol": 0.02, "crush_ratio": 0.75, "max_vol_z": 0.0, "min_abs_price_z": 1.5},
        active=True,
    )
    s = VolCrushMeanReversionStrategy(cfg)

    ts = datetime(2026, 4, 12, tzinfo=timezone.utc)
    # prior high vol then current lower => "crush"
    price_ctx = {
        "entry_price": 100.0,
        "realized_volatility": 0.03,
        "historical_volatility_window": [0.08, 0.07, 0.06, 0.05, 0.03],
        "zscore_20": 2.0,  # stretched up -> mean revert down
        "range_expansion": 1.3,
    }
    pred = s.maybe_predict(_scored(), _mra(), price_ctx, ts)
    assert pred is not None
    assert pred.prediction == "down"
    assert pred.feature_snapshot.get("setup") == "vol_crush_mean_reversion"


def test_vol_strategies_skip_when_conditions_missing() -> None:
    ts = datetime(2026, 4, 12, tzinfo=timezone.utc)

    cfg1 = StrategyConfig(
        id="vol-exp-v1",
        name="vol exp",
        version="v1",
        strategy_type="technical_vol_expansion_continuation",
        mode="backtest",
        config={"min_vol_z": 2.0},
        active=True,
    )
    s1 = VolExpansionContinuationStrategy(cfg1)
    assert s1.maybe_predict(_scored(), _mra(), {"realized_volatility": 0.1, "historical_volatility_window": [0.1] * 10}, ts) is None

    cfg2 = StrategyConfig(
        id="vol-crush-v1",
        name="vol crush",
        version="v1",
        strategy_type="technical_vol_crush_reversion",
        mode="backtest",
        config={},
        active=True,
    )
    s2 = VolCrushMeanReversionStrategy(cfg2)
    assert s2.maybe_predict(_scored(), _mra(), {"realized_volatility": 0.1, "historical_volatility_window": [0.1] * 10}, ts) is None

