from __future__ import annotations

import json
import os
from typing import Any, Callable

from app.discovery.scoring import clamp, clamp01
from app.discovery.types import DiscoveryCandidate, FeatureRow


def _as_json(meta: dict[str, Any]) -> str:
    return json.dumps(meta, separators=(",", ":"), sort_keys=True)


def _require(*vals) -> bool:
    return all(v is not None for v in vals)

def _fmt_money(x: float | None) -> str:
    if x is None:
        return "n/a"
    v = float(x)
    if abs(v) >= 1_000_000_000:
        return f"${v/1_000_000_000:.2f}B"
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def _pct(x: float | None) -> str:
    if x is None:
        return "n/a"
    return f"{float(x)*100:.1f}%"


def _drivers(items: list[tuple[str, float, str]], *, top_k: int = 3) -> list[str]:
    """
    items: (label, impact, display_str). Higher impact sorts first.
    """
    items = sorted(items, key=lambda t: float(t[1]), reverse=True)
    out: list[str] = []
    for label, impact, display in items[: int(top_k)]:
        out.append(f"{label}: {display}")
    return out


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


# Conservative default gates (operators can override via env vars).
_REV_MIN_REALNESS = _env_float("DISC_REV_MIN_REALNESS", 100_000_000.0)
_REV_MIN_SURVIVOR = _env_float("DISC_REV_MIN_SURVIVOR", 50_000_000.0)
_PRICE_PCTL_MAX_REALNESS = _env_float("DISC_PRICE_PCTL_MAX_REALNESS", 0.20)
_MAX_DD_MIN_SURVIVOR = _env_float("DISC_MAX_DD_MIN_SURVIVOR", 0.60)
_VOL_MAX_COMPOUNDER = _env_float("DISC_VOL_MAX_COMPOUNDER", 0.06)
_ABS_RET_252_MAX_COMPOUNDER = _env_float("DISC_ABS_RET_252_MAX_COMPOUNDER", 0.30)
_ADV_MAX_VACUUM = _env_float("DISC_ADV_MAX_VACUUM", 5_000_000.0)
_VOLZ_MIN_VACUUM = _env_float("DISC_VOLZ_MIN_VACUUM", 3.0)
_ABS_RET_5D_MAX_VACUUM = _env_float("DISC_ABS_RET_5D_MAX_VACUUM", 0.12)


def realness_repricer(fr: FeatureRow) -> tuple[float | None, str, dict[str, Any]]:
    if fr.close is None or fr.price_percentile_252d is None or fr.revenue_ttm is None:
        return None, "missing required price/fundamentals", {}
    if float(fr.revenue_ttm) < _REV_MIN_REALNESS:
        return None, "revenue below realness threshold", {}
    if float(fr.price_percentile_252d) > _PRICE_PCTL_MAX_REALNESS:
        return None, "not sufficiently depressed vs 252d history", {}

    # saturate at ~$1B
    revenue_score = clamp(fr.revenue_ttm / 1_000_000_000.0, 0.0, 1.0)

    depressed = 1.0 - clamp01(fr.price_percentile_252d)
    dd = clamp((fr.max_drawdown_252d or 0.0) / 0.7, 0.0, 1.0)

    dilution_pen = 0.0
    if fr.shares_growth is not None:
        dilution_pen = clamp(fr.shares_growth / 0.25, 0.0, 1.0)

    raw = (0.35 * revenue_score) + (0.35 * depressed) + (0.30 * dd) - (0.25 * dilution_pen)
    raw = clamp(raw, 0.0, 1.0)
    reason = "real revenue + depressed price + drawdown (dilution penalized)"
    meta = {
        "sub": {
            "revenue_score": revenue_score,
            "depressed_score": depressed,
            "drawdown_score": dd,
            "dilution_penalty": dilution_pen,
        }
    }
    meta["drivers"] = _drivers(
        [
            ("price_percentile_252d", 0.35 * depressed, f"{clamp01(fr.price_percentile_252d):.2f} (low)"),
            ("revenue_ttm", 0.35 * revenue_score, _fmt_money(fr.revenue_ttm)),
            ("max_drawdown_252d", 0.30 * dd, _pct(fr.max_drawdown_252d)),
        ]
    )
    return raw, reason, meta


def silent_compounder(fr: FeatureRow) -> tuple[float | None, str, dict[str, Any]]:
    if fr.close is None or fr.return_252d is None or fr.volatility_20d is None or fr.revenue_growth is None:
        return None, "missing required return/volatility/fundamentals", {}
    if float(fr.revenue_growth) <= 0:
        return None, "revenue growth not positive", {}
    if float(fr.volatility_20d) > _VOL_MAX_COMPOUNDER:
        return None, "too volatile for compounder profile", {}
    if abs(float(fr.return_252d)) > _ABS_RET_252_MAX_COMPOUNDER:
        return None, "not sideways enough (already trending)", {}

    growth = clamp((fr.revenue_growth) / 0.25, 0.0, 1.0)

    # low realized vol is good
    vol_score = 1.0 - clamp((fr.volatility_20d or 0.0) / 0.08, 0.0, 1.0)
    sideways = 1.0 - clamp(abs(fr.return_252d or 0.0) / 0.25, 0.0, 1.0)

    raw = clamp((0.45 * growth) + (0.35 * vol_score) + (0.20 * sideways), 0.0, 1.0)
    reason = "positive growth + low volatility + sideways price"
    meta = {"sub": {"growth_score": growth, "vol_score": vol_score, "sideways_score": sideways}}
    meta["drivers"] = _drivers(
        [
            ("revenue_growth", 0.45 * growth, _pct(fr.revenue_growth)),
            ("volatility_20d", 0.35 * vol_score, f"{float(fr.volatility_20d):.4f} (low)"),
            ("return_252d", 0.20 * sideways, _pct(fr.return_252d)),
        ]
    )
    return raw, reason, meta


def narrative_lag(fr: FeatureRow) -> tuple[float | None, str, dict[str, Any]]:
    if fr.sector is None or fr.sector_return_63d is None or fr.peer_relative_return_63d is None:
        return None, "missing sector/peer data", {}

    if float(fr.sector_return_63d) <= 0:
        return None, "sector not positive (no hot narrative)", {}
    if float(fr.peer_relative_return_63d) >= 0:
        return None, "not lagging peers", {}

    sector_hot = clamp((fr.sector_return_63d or 0.0) / 0.20, 0.0, 1.0)
    lag = clamp((-(fr.peer_relative_return_63d or 0.0)) / 0.20, 0.0, 1.0)

    not_already_moved = 1.0
    if fr.return_20d is not None:
        not_already_moved = 1.0 - clamp((fr.return_20d or 0.0) / 0.15, 0.0, 1.0)

    raw = clamp((0.45 * sector_hot) + (0.40 * lag) + (0.15 * not_already_moved), 0.0, 1.0)
    reason = "hot sector + underperforming peer-relative return"
    meta = {"sub": {"sector_hot_score": sector_hot, "lag_score": lag, "not_moved_score": not_already_moved}}
    meta["drivers"] = _drivers(
        [
            ("sector_return_63d", 0.45 * sector_hot, _pct(fr.sector_return_63d)),
            ("peer_relative_return_63d", 0.40 * lag, _pct(fr.peer_relative_return_63d)),
            ("return_20d", 0.15 * not_already_moved, _pct(fr.return_20d)),
        ]
    )
    return raw, reason, meta


def balance_sheet_survivor(fr: FeatureRow) -> tuple[float | None, str, dict[str, Any]]:
    if fr.max_drawdown_252d is None or fr.return_63d is None or fr.revenue_growth is None:
        return None, "missing required drawdown/return/fundamentals", {}
    if float(fr.max_drawdown_252d) < _MAX_DD_MIN_SURVIVOR:
        return None, "not distressed enough (drawdown too small)", {}
    if fr.revenue_ttm is not None and float(fr.revenue_ttm) < _REV_MIN_SURVIVOR:
        return None, "revenue below survivor threshold", {}
    if float(fr.revenue_growth) < -0.10:
        return None, "revenue collapsing", {}

    distress = clamp((fr.max_drawdown_252d or 0.0) / 0.70, 0.0, 1.0)
    rebound = clamp((fr.return_63d or 0.0) / 0.30, 0.0, 1.0)

    # Penalize obvious dilution.
    dilution_pen = 0.0
    if fr.shares_growth is not None:
        dilution_pen = clamp(fr.shares_growth / 0.25, 0.0, 1.0)

    # If revenue is collapsing, reduce confidence.
    revenue_stable = clamp((fr.revenue_growth + 0.10) / 0.20, 0.0, 1.0)

    raw = (0.45 * distress) + (0.35 * rebound) + (0.20 * revenue_stable) - (0.20 * dilution_pen)
    raw = clamp(raw, 0.0, 1.0)
    reason = "large prior drawdown + signs of stabilization (dilution penalized)"
    meta = {
        "sub": {
            "distress_score": distress,
            "rebound_score": rebound,
            "revenue_stable_score": revenue_stable,
            "dilution_penalty": dilution_pen,
        }
    }
    meta["drivers"] = _drivers(
        [
            ("max_drawdown_252d", 0.45 * distress, _pct(fr.max_drawdown_252d)),
            ("return_63d", 0.35 * rebound, _pct(fr.return_63d)),
            ("revenue_growth", 0.20 * revenue_stable, _pct(fr.revenue_growth)),
        ]
    )
    return raw, reason, meta


def ownership_vacuum(fr: FeatureRow) -> tuple[float | None, str, dict[str, Any]]:
    if fr.avg_dollar_volume_20d is None or fr.volume_zscore_20d is None or fr.return_5d is None:
        return None, "missing liquidity/volume spike data", {}
    if float(fr.avg_dollar_volume_20d) > _ADV_MAX_VACUUM:
        return None, "too liquid for ownership vacuum profile", {}
    if float(fr.volume_zscore_20d) < _VOLZ_MIN_VACUUM:
        return None, "no strong volume spike", {}
    if abs(float(fr.return_5d)) > _ABS_RET_5D_MAX_VACUUM:
        return None, "already repriced in last 5d", {}

    # Long low liquidity is good for "vacuum", but avoid extreme illiquidity via caller universe filters.
    low_liquidity = 1.0 - clamp((fr.avg_dollar_volume_20d or 0.0) / 5_000_000.0, 0.0, 1.0)
    spike = clamp((fr.volume_zscore_20d or 0.0) / 5.0, 0.0, 1.0)
    not_repriced = 1.0 - clamp(abs(fr.return_5d or 0.0) / 0.12, 0.0, 1.0)

    raw = clamp((0.35 * low_liquidity) + (0.45 * spike) + (0.20 * not_repriced), 0.0, 1.0)
    reason = "low baseline liquidity + sudden volume spike (price not fully moved)"
    meta = {"sub": {"low_liquidity_score": low_liquidity, "spike_score": spike, "not_repriced_score": not_repriced}}
    meta["drivers"] = _drivers(
        [
            ("volume_zscore_20d", 0.45 * spike, f"{float(fr.volume_zscore_20d):.2f} (spike)"),
            ("avg_dollar_volume_20d", 0.35 * low_liquidity, _fmt_money(fr.avg_dollar_volume_20d)),
            ("return_5d", 0.20 * not_repriced, _pct(fr.return_5d)),
        ]
    )
    return raw, reason, meta


STRATEGIES: dict[str, Callable[[FeatureRow], tuple[float | None, str, dict[str, Any]]]] = {
    "realness_repricer": realness_repricer,
    "silent_compounder": silent_compounder,
    "narrative_lag": narrative_lag,
    "balance_sheet_survivor": balance_sheet_survivor,
    "ownership_vacuum": ownership_vacuum,
}


def score_candidates(
    features: dict[str, FeatureRow],
    *,
    strategy_type: str,
) -> list[DiscoveryCandidate]:
    fn = STRATEGIES[strategy_type]
    scored: list[tuple[str, float, str, dict[str, Any]]] = []
    for sym, fr in features.items():
        raw, reason, meta = fn(fr)
        if raw is None:
            continue
        scored.append((sym, float(raw), reason, meta))

    if not scored:
        return []

    # Normalize within-strategy via percentile rank (stable, 0..1).
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
            "sub": meta.get("sub", {}),
            "drivers": meta.get("drivers", []),
            "price_bucket": fr.price_bucket,
            "close": fr.close,
            "avg_dollar_volume_20d": fr.avg_dollar_volume_20d,
            "sector": fr.sector,
            "industry": fr.industry,
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
