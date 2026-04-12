from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

from app.core.regime_manager import RegimeManager, RegimeSnapshot


@dataclass(frozen=True, slots=True)
class RegimeContext:
    snapshot: RegimeSnapshot
    payload: dict[str, Any]  # JSON-safe regime snapshot
    volatility_regime: str  # LOW|NORMAL|HIGH
    trend_strength: str  # WEAK|NORMAL|STRONG|UNKNOWN


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float(default)


def _first_float(d: dict, keys: Iterable[str]) -> float | None:
    for k in keys:
        if k not in d:
            continue
        try:
            return float(d[k])
        except (TypeError, ValueError):
            return None
    return None


def build_regime_context(
    *,
    price_context: dict[str, Any],
    regime_manager: RegimeManager | None = None,
) -> RegimeContext:
    """
    Build a normalized, JSON-safe regime context from a strategy price_context.

    Expected keys (best-effort):
    - realized volatility: realized_volatility OR realized_vol_20
    - historical volatility window: historical_volatility_window OR historical_volatility
    - adx: adx OR adx_14 OR adx_value
    """
    rm = regime_manager or RegimeManager()

    realized_vol = _first_float(price_context, ("realized_volatility", "realized_vol_20")) or 0.0

    hist = price_context.get("historical_volatility_window")
    if not isinstance(hist, list) or not hist:
        hist = price_context.get("historical_volatility")
    if isinstance(hist, list) and hist:
        hist_window = [_as_float(v, realized_vol) for v in hist if v is not None]
    else:
        hist_window = [float(realized_vol) for _ in range(20)]
    if not hist_window:
        hist_window = [float(realized_vol) for _ in range(20)]

    adx_value = _first_float(price_context, ("adx", "adx_14", "adx_value"))

    snapshot = rm.classify(
        realized_volatility=float(realized_vol),
        historical_volatility_window=[float(v) for v in hist_window],
        adx_value=adx_value,
    )

    payload = asdict(snapshot)
    # Make payload JSON-serializable and stable.
    if getattr(snapshot.volatility_regime, "value", None) is not None:
        payload["volatility_regime"] = snapshot.volatility_regime.value

    vol = str(payload.get("volatility_regime") or "NORMAL")
    trend = str(payload.get("trend_strength") or "UNKNOWN")

    return RegimeContext(
        snapshot=snapshot,
        payload=payload,
        volatility_regime=vol,
        trend_strength=trend,
    )


@dataclass(frozen=True, slots=True)
class GateDecision:
    allowed: bool
    confidence_multiplier: float = 1.0
    reason: str | None = None


def _strategy_family(strategy_type: str) -> str:
    st = str(strategy_type or "").strip().lower()
    if st.startswith("text_") or st.startswith("sentiment"):
        return "sentiment"
    if "rsi_reversion" in st or "bollinger_reversion" in st:
        return "mean_reversion"
    if "baseline_momentum" in st or "vwap_reclaim" in st or "ma_cross" in st:
        return "momentum"
    if st.startswith("ml_"):
        return "ml"
    return "other"


def decide_strategy_gate(
    *,
    strategy_type: str,
    strategy_config: dict[str, Any] | None,
    regime: RegimeContext,
) -> GateDecision:
    """
    Decide whether a strategy should be allowed to emit a prediction under the current regime.

    Override config is optional and lives under the strategy config dict:
      config["regime_gating"] = {
        "allow_volatility_regimes": ["LOW","NORMAL","HIGH"],
        "deny_volatility_regimes": ["HIGH"],
        "allow_trend_strength": ["WEAK","NORMAL","STRONG","UNKNOWN"],
        "deny_trend_strength": ["STRONG"],
        "confidence_multiplier": 0.8
      }
    """
    cfg = dict(strategy_config or {})
    gate = cfg.get("regime_gating") or cfg.get("regime_gate")
    gate = gate if isinstance(gate, dict) else {}

    vol = str(regime.volatility_regime)
    trend = str(regime.trend_strength)

    allow_vol = gate.get("allow_volatility_regimes")
    deny_vol = gate.get("deny_volatility_regimes")
    allow_trend = gate.get("allow_trend_strength")
    deny_trend = gate.get("deny_trend_strength")

    if isinstance(deny_vol, list) and vol in {str(x) for x in deny_vol}:
        return GateDecision(allowed=False, reason=f"deny_volatility_regime:{vol}")
    if isinstance(deny_trend, list) and trend in {str(x) for x in deny_trend}:
        return GateDecision(allowed=False, reason=f"deny_trend_strength:{trend}")
    if isinstance(allow_vol, list) and allow_vol and vol not in {str(x) for x in allow_vol}:
        return GateDecision(allowed=False, reason=f"not_allowed_volatility_regime:{vol}")
    if isinstance(allow_trend, list) and allow_trend and trend not in {str(x) for x in allow_trend}:
        return GateDecision(allowed=False, reason=f"not_allowed_trend_strength:{trend}")

    # If the strategy explicitly declared regime allow/deny lists, treat that as an override
    # and do not apply default family rules unless requested.
    has_explicit_lists = any(
        k in gate for k in ("allow_volatility_regimes", "deny_volatility_regimes", "allow_trend_strength", "deny_trend_strength")
    )
    apply_defaults = bool(gate.get("apply_defaults", False))
    if gate and has_explicit_lists and not apply_defaults:
        mult = gate.get("confidence_multiplier")
        cm = _as_float(mult, 1.0) if mult is not None else 1.0
        cm = max(0.0, min(1.0, cm))
        return GateDecision(
            allowed=True,
            confidence_multiplier=cm,
            reason=str(gate.get("reason")) if gate.get("reason") else None,
        )

    family = _strategy_family(strategy_type)

    # Defaults: keep behavior minimal and auditable.
    if family == "momentum" and trend == "WEAK":
        return GateDecision(allowed=False, reason="default:block_momentum_in_chop")
    if family == "mean_reversion" and trend == "STRONG":
        return GateDecision(allowed=False, reason="default:block_mean_reversion_in_strong_trend")

    mult = gate.get("confidence_multiplier")
    cm = _as_float(mult, 1.0) if mult is not None else 1.0
    cm = max(0.0, min(1.0, cm))
    return GateDecision(allowed=True, confidence_multiplier=cm, reason=str(gate.get("reason")) if gate.get("reason") else None)
