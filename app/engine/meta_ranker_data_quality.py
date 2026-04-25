from __future__ import annotations

import os
from collections import Counter
from typing import Any

QUALITY_MIN_ROWS = int(os.getenv("META_RANKER_QUALITY_MIN_ROWS", "15"))
QUALITY_MAX_TOP_STRATEGY_SHARE = float(os.getenv("META_RANKER_QUALITY_MAX_TOP_STRATEGY_SHARE", "0.70"))
QUALITY_MIN_SECTOR_COUNT = int(os.getenv("META_RANKER_QUALITY_MIN_SECTOR_COUNT", "3"))
QUALITY_MIN_REGIME_COUNT = int(os.getenv("META_RANKER_QUALITY_MIN_REGIME_COUNT", "1"))
QUALITY_MAX_MISSING_RATE = float(os.getenv("META_RANKER_QUALITY_MAX_MISSING_RATE", "0.20"))


def _safe_float(v: Any, fallback: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(fallback)


def _missing_rate(rows: list[dict[str, Any]], key: str) -> float:
    if not rows:
        return 1.0
    missing = 0
    for r in rows:
        val = r.get(key)
        if val is None:
            missing += 1
            continue
        if isinstance(val, str) and not val.strip():
            missing += 1
    return missing / float(len(rows))


def evaluate_meta_ranker_data_quality(rows: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    strategy_counts = Counter(str(r.get("strategy") or "unknown") for r in rows)
    sector_counts = Counter(str(r.get("sector") or "unknown") for r in rows)
    regime_counts = Counter(str(r.get("regime") or "unknown") for r in rows)

    top_strategy_share = 0.0
    if strategy_counts and n > 0:
        top_strategy_share = max(strategy_counts.values()) / float(n)

    missing_rates = {
        "avg_score": _missing_rate(rows, "avg_score"),
        "momentum_5d": _missing_rate(rows, "momentum_5d"),
        "momentum_20d": _missing_rate(rows, "momentum_20d"),
        "volatility_20d": _missing_rate(rows, "volatility_20d"),
        "liquidity": _missing_rate(rows, "liquidity"),
        "claim_count": _missing_rate(rows, "claim_count"),
        "overlap_count": _missing_rate(rows, "overlap_count"),
        "days_seen": _missing_rate(rows, "days_seen"),
        "strategy_win_rate": _missing_rate(rows, "strategy_win_rate"),
        "strategy_decay": _missing_rate(rows, "strategy_decay"),
    }
    max_missing = max(missing_rates.values()) if missing_rates else 0.0
    avg_missing = (
        sum(float(v) for v in missing_rates.values()) / float(len(missing_rates))
        if missing_rates
        else 0.0
    )

    checks = {
        "min_rows": n >= QUALITY_MIN_ROWS,
        "strategy_diversity": top_strategy_share <= QUALITY_MAX_TOP_STRATEGY_SHARE,
        "sector_diversity": len(sector_counts) >= QUALITY_MIN_SECTOR_COUNT,
        "regime_coverage": len(regime_counts) >= QUALITY_MIN_REGIME_COUNT,
        "missing_rate": max_missing <= QUALITY_MAX_MISSING_RATE,
    }
    passed = all(bool(v) for v in checks.values())

    return {
        "passed": bool(passed),
        "checks": checks,
        "stats": {
            "row_count": n,
            "strategy_count": len(strategy_counts),
            "sector_count": len(sector_counts),
            "regime_count": len(regime_counts),
            "top_strategy_share": round(_safe_float(top_strategy_share), 6),
            "max_missing_rate": round(_safe_float(max_missing), 6),
            "avg_missing_rate": round(_safe_float(avg_missing), 6),
        },
        "distribution": {
            "strategy_mix": dict(strategy_counts),
            "sector_mix": dict(sector_counts),
            "regime_mix": dict(regime_counts),
        },
        "missing_rates": {k: round(_safe_float(v), 6) for k, v in missing_rates.items()},
        "thresholds": {
            "min_rows": int(QUALITY_MIN_ROWS),
            "max_top_strategy_share": float(QUALITY_MAX_TOP_STRATEGY_SHARE),
            "min_sector_count": int(QUALITY_MIN_SECTOR_COUNT),
            "min_regime_count": int(QUALITY_MIN_REGIME_COUNT),
            "max_missing_rate": float(QUALITY_MAX_MISSING_RATE),
        },
    }
