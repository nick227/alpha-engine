from __future__ import annotations

from datetime import datetime, timezone

from app.engine.regime_service import build_regime_context, decide_strategy_gate


def test_gate_blocks_momentum_in_chop() -> None:
    # adx <= weak_adx => WEAK (chop)
    ctx = build_regime_context(
        price_context={
            "realized_volatility": 0.2,
            "historical_volatility_window": [0.18, 0.19, 0.2, 0.21, 0.2],
            "adx_value": 10.0,
        }
    )
    gate = decide_strategy_gate(
        strategy_type="baseline_momentum",
        strategy_config={},
        regime=ctx,
    )
    assert gate.allowed is False
    assert gate.reason == "default:block_momentum_in_chop"


def test_gate_blocks_mean_reversion_in_strong_trend() -> None:
    # adx >= strong_adx => STRONG
    ctx = build_regime_context(
        price_context={
            "realized_volatility": 0.2,
            "historical_volatility_window": [0.18, 0.19, 0.2, 0.21, 0.2],
            "adx_value": 30.0,
        }
    )
    gate = decide_strategy_gate(
        strategy_type="technical_rsi_reversion",
        strategy_config={},
        regime=ctx,
    )
    assert gate.allowed is False
    assert gate.reason == "default:block_mean_reversion_in_strong_trend"


def test_gate_allows_override_config() -> None:
    ctx = build_regime_context(
        price_context={
            "realized_volatility": 0.2,
            "historical_volatility_window": [0.18, 0.19, 0.2, 0.21, 0.2],
            "adx_value": 10.0,  # WEAK
        }
    )
    gate = decide_strategy_gate(
        strategy_type="baseline_momentum",
        strategy_config={"regime_gating": {"allow_trend_strength": ["WEAK", "NORMAL", "STRONG", "UNKNOWN"]}},
        regime=ctx,
    )
    assert gate.allowed is True


def test_regime_payload_is_json_safe() -> None:
    ctx = build_regime_context(
        price_context={
            "realized_volatility": 0.2,
            "historical_volatility": [0.18, 0.19, 0.2],
            "adx": 25.0,
        }
    )
    # spot-check that the enum field is normalized to a plain string
    assert isinstance(ctx.payload.get("volatility_regime"), str)

