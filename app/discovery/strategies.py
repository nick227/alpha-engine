from __future__ import annotations

import json
from typing import Any, Callable

from app.discovery.scoring import clamp, clamp01
from app.discovery.types import DiscoveryCandidate, FeatureRow


def _as_json(meta: dict[str, Any]) -> str:
    return json.dumps(meta, separators=(",", ":"), sort_keys=True)


def _drivers(items: list[tuple[str, float, str]], top_k: int = 3) -> list[str]:
    items = sorted(items, key=lambda t: float(t[1]), reverse=True)
    return [f"{label}: {display}" for label, _, display in items[:top_k]]


# =========================
# STRATEGIES (ALIGNED)
# =========================

def realness_repricer(fr: FeatureRow):
    if fr.price_percentile_252d is None or fr.return_63d is None:
        return None, "missing data", {}

    depressed = 1.0 - clamp01(fr.price_percentile_252d)
    drawdown_proxy = clamp(-fr.return_63d / 0.5, 0.0, 1.0)

    raw = clamp(0.6 * depressed + 0.4 * drawdown_proxy, 0.0, 1.0)
    raw = raw ** 2.3

    if raw < 0.85:
        return None, "weak repricer", {}

    meta: dict[str, Any] = {}
    meta["drivers"] = _drivers([
        ("price_percentile_252d", depressed, f"{fr.price_percentile_252d:.2f}"),
        ("return_63d", drawdown_proxy, f"{fr.return_63d:.2%}"),
    ])

    return raw, "depressed price + negative trend (repricer setup)", meta


def silent_compounder(fr: FeatureRow) -> tuple[float | None, str, dict[str, Any]]:
    """
    Silent Compounder: Optimal volatility band + steady price appreciation.
    IC: ~0.02-0.03 (weak but positive)
    """
    if fr.volatility_20d is None or fr.return_63d is None:
        return None, "missing data", {}

    if fr.price_percentile_252d is None:
        return None, "missing price percentile", {}

    # Core signal: optimal volatility band (not just low)
    ideal_vol = 0.02  # 2% monthly volatility is sweet spot for equities
    vol_score = max(0.0, 1.0 - abs(fr.volatility_20d - ideal_vol) / ideal_vol)
    
    steady = 1.0 if fr.return_63d > 0 else 0.0

    raw = clamp(0.6 * vol_score + 0.4 * steady, 0.0, 1.0)
    if raw < 0.5:
        return None, "low score", {}

    reason = f"optimal vol ({fr.volatility_20d:.3f}) + {'positive' if steady else 'negative'} drift"
    return raw, reason, {"volatility": fr.volatility_20d, "return_63d": fr.return_63d}


def narrative_lag(fr: FeatureRow):
    if fr.return_63d is None:
        return None, "missing data", {}

    lag = clamp(-fr.return_63d / 0.3, 0.0, 1.0)

    if fr.price_percentile_252d is not None:
        undervalued = 1.0 - fr.price_percentile_252d
    else:
        undervalued = 0.5

    # percentile-based filtering (strongest approach)
    raw = clamp(0.6 * lag + 0.4 * undervalued, 0.0, 1.0)
    raw = raw ** 2.5

    # HARD cutoff
    if raw < 0.90:
        return None, "weak narrative lag", {}

    meta: dict[str, Any] = {}
    meta["drivers"] = _drivers([
        ("return_63d", lag, f"{fr.return_63d:.2%}"),
        ("price_percentile_252d", undervalued, f"{fr.price_percentile_252d:.2f}" if fr.price_percentile_252d is not None else "n/a"),
    ])

    return raw, "lagging performance + undervaluation (possible catch-up)", meta


def balance_sheet_survivor(fr: FeatureRow):
    if fr.return_63d is None or fr.volatility_20d is None:
        return None, "missing data", {}

    distress = clamp(-fr.return_63d / 0.5, 0.0, 1.0)
    stability = 1.0 - clamp(fr.volatility_20d / 0.1, 0.0, 1.0)

    raw = clamp(0.6 * distress + 0.4 * stability, 0.0, 1.0)
    raw = raw ** 1.7

    meta: dict[str, Any] = {}
    meta["drivers"] = _drivers([
        ("return_63d", distress, f"{fr.return_63d:.2%}"),
        ("volatility_20d", stability, f"{fr.volatility_20d:.4f}"),
    ])

    return raw, "drawdown + stabilization", meta


def ownership_vacuum(fr: FeatureRow):
    if fr.volume_zscore_20d is None or fr.dollar_volume is None:
        return None, "missing data", {}

    spike = clamp(fr.volume_zscore_20d / 5.0, 0.0, 1.0)
    low_liquidity = 1.0 - clamp(fr.dollar_volume / 10_000_000.0, 0.0, 1.0)

    raw = clamp(0.7 * spike + 0.3 * low_liquidity, 0.0, 1.0)
    raw = raw ** 1.8

    meta: dict[str, Any] = {}
    meta["drivers"] = _drivers([
        ("volume_zscore_20d", spike, f"{fr.volume_zscore_20d:.2f}"),
        ("dollar_volume", low_liquidity, f"{fr.dollar_volume:.0f}"),
    ])

    return raw, "volume spike in lower liquidity name", meta


# =========================
# REGISTRY
# =========================

STRATEGIES: dict[str, Callable[[FeatureRow], tuple[float | None, str, dict[str, Any]]]] = {
    # IC > 0 at 1-5d on pre-filter universe (weak: IC +0.011-0.012, below 0.02 threshold)
    "realness_repricer": realness_repricer,
    "narrative_lag": narrative_lag,
    # Experimental — IC negative on unfiltered universe; pending retest on price+ADV filtered universe
    "silent_compounder": silent_compounder,
    "ownership_vacuum": ownership_vacuum,
    "balance_sheet_survivor": balance_sheet_survivor,
}


# =========================
# SCORING PIPELINE
# =========================

THRESHOLDS: dict[str, float] = {
    "ownership_vacuum": 0.60,
    "realness_repricer": 0.60,
    "silent_compounder": 0.50,
    "narrative_lag": 0.70,
    "balance_sheet_survivor": 0.60,
}

# Universe filters applied before scoring (removes penny stocks and illiquid names).
# These are primary data quality gates — IC tests on unfiltered universe are not
# representative of actual operating universe.
MIN_CLOSE = 10.0          # exclude sub-$10 stocks (higher quality universe)
MIN_DOLLAR_VOLUME = 5_000_000.0  # exclude < $5M ADV (remove small-cap junk)
MIN_CONFIDENCE = 0.20      # minimum confidence to emit prediction


def score_candidates(
    features: dict[str, FeatureRow],
    *,
    strategy_type: str,
) -> list[DiscoveryCandidate]:
    fn = STRATEGIES[strategy_type]

    scored: list[tuple[str, float, str, dict[str, Any]]] = []
    threshold = THRESHOLDS.get(strategy_type, 0.6)

    # Pre-compute cross-sectional percentiles for behavior filtering
    all_vol = [fr.volatility_20d for fr in features.values() if fr.volatility_20d is not None]
    all_ret = [abs(fr.return_63d) for fr in features.values() if fr.return_63d is not None]
    
    vol_p30 = sorted(all_vol)[int(0.3 * len(all_vol))] if all_vol else 0.01
    ret_p40 = sorted(all_ret)[int(0.4 * len(all_ret))] if all_ret else 0.02

    for sym, fr in features.items():
        # Quality gates
        if fr.close is None or fr.close < MIN_CLOSE:
            continue
        if fr.dollar_volume is None or fr.dollar_volume < MIN_DOLLAR_VOLUME:
            continue
        # Critical: exclude market indices (not equities)
        if sym.startswith('^'):
            continue
        # Behavior filter: remove low-signal instruments (ETFs naturally cluster here)
        if fr.volatility_20d is not None and fr.volatility_20d < vol_p30:
            continue
        # Behavior filter: remove low-dispersion instruments (junk/ETFs)
        if fr.return_63d is not None and abs(fr.return_63d) < ret_p40:
            continue

        raw, reason, meta = fn(fr)
        if raw is None or raw < threshold:
            continue
        if raw < MIN_CONFIDENCE:
            continue
        scored.append((sym, float(raw), reason, meta))

    if not scored:
        return []

    raw_vals = [s[1] for s in scored]
    order = sorted(range(len(raw_vals)), key=lambda i: raw_vals[i])
    ranks = [0.0] * len(raw_vals)

    n = len(raw_vals)
    for r, idx in enumerate(order):
        ranks[idx] = (r / (n - 1)) if n > 1 else 1.0

    out: list[DiscoveryCandidate] = []
    for (sym, raw, reason, meta), score in zip(scored, ranks):
        fr = features[sym]

        md = {
            "raw_score": raw,
            "drivers": meta.get("drivers", []),
            "close": fr.close,
            "dollar_volume": fr.dollar_volume,
        }

        out.append(
            DiscoveryCandidate(
                symbol=sym,
                strategy_type=strategy_type,
                score=float(score),
                reason=str(reason),
                metadata=md,
            )
        )

    out.sort(key=lambda c: c.score, reverse=True)
    return out


def to_repo_rows(cands: list[DiscoveryCandidate]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for c in cands:
        rows.append(
            {
                "symbol": c.symbol,
                "strategy_type": c.strategy_type,
                "score": float(c.score),
                "reason": c.reason,
                "metadata_json": _as_json(dict(c.metadata)),
            }
        )
    return rows
