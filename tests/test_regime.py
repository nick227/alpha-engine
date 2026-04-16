from __future__ import annotations

from app.core.regime import (
    apply_trend_modifier,
    build_regime_snapshot,
    classify_trend_regime,
    classify_volatility_regime,
    safe_mean,
    safe_std,
)


def test_safe_mean_and_std_empty_and_singleton() -> None:
    assert safe_mean([]) == 0.0
    assert safe_std([]) == 0.0
    assert safe_std([1.0]) == 0.0


def test_classify_volatility_regime_low_normal_high() -> None:
    # Mean=1, std=1 -> z=-1 => LOW; z=0 => NORMAL; z=+1 => HIGH
    vols = [0.0, 1.0, 2.0]
    assert classify_volatility_regime(0.0, vols) == "LOW"
    assert classify_volatility_regime(1.0, vols) == "NORMAL"
    assert classify_volatility_regime(2.0, vols) == "HIGH"


def test_classify_trend_regime_thresholds() -> None:
    assert classify_trend_regime(None) == "UNKNOWN"
    assert classify_trend_regime(10.0) == "CHOP"
    assert classify_trend_regime(25.0) == "TRENDING"


def test_apply_trend_modifier_normalizes_and_bounds() -> None:
    s, q = apply_trend_modifier(0.8, 0.2, "TRENDING")
    assert 0.0 <= s <= 1.0
    assert 0.0 <= q <= 1.0
    assert abs((s + q) - 1.0) < 1e-9


def test_build_regime_snapshot_weights_sum_to_one() -> None:
    snap = build_regime_snapshot(current_volatility=2.0, recent_volatilities=[0.0, 1.0, 2.0], adx_value=30.0)
    assert snap.volatility_regime == "HIGH"
    assert snap.trend_regime == "TRENDING"
    assert abs((snap.sentiment_weight + snap.quant_weight) - 1.0) < 1e-9
