from __future__ import annotations

import os


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


META_RANKER_BASE_WEIGHT = float(os.getenv("META_RANKER_BASE_WEIGHT", "0.45"))
META_RANKER_OUTPERFORM_WEIGHT = float(os.getenv("META_RANKER_OUTPERFORM_WEIGHT", "0.40"))
META_RANKER_FAIL_WEIGHT = float(os.getenv("META_RANKER_FAIL_WEIGHT", "0.30"))
META_RANKER_REGIME_MISMATCH_PENALTY = float(os.getenv("META_RANKER_REGIME_MISMATCH_PENALTY", "0.05"))


def _crowding_penalty(row: dict) -> float:
    claim = float(row.get("claim_count") or 1.0)
    overlap = float(row.get("overlap_count") or 1.0)
    return max(0.0, ((claim - 3.0) * 0.03) + ((overlap - 3.0) * 0.02))


def _regime_mismatch_penalty(row: dict) -> float:
    regime = str(row.get("regime") or "neutral")
    strategy = str(row.get("strategy") or "")
    if regime == "risk_off" and strategy in {"silent_compounder", "volatility_breakout", "sniper_coil"}:
        return META_RANKER_REGIME_MISMATCH_PENALTY
    if regime == "risk_on" and strategy in {"balance_sheet_survivor"}:
        return META_RANKER_REGIME_MISMATCH_PENALTY * 0.5
    return 0.0


def combine_meta_ranker_score(*, row: dict, p_outperform: float, p_fail: float) -> dict:
    base = float(row.get("base_score") or 0.0)
    crowd = _crowding_penalty(row)
    regime_mismatch = _regime_mismatch_penalty(row)
    final = (
        (META_RANKER_BASE_WEIGHT * base)
        + (META_RANKER_OUTPERFORM_WEIGHT * float(p_outperform))
        - (META_RANKER_FAIL_WEIGHT * float(p_fail))
        - crowd
        - regime_mismatch
    )
    return {
        "final_rank_score": _clamp(final, 0.0, 1.0),
        "penalties": {
            "crowding": round(crowd, 6),
            "regime_mismatch": round(regime_mismatch, 6),
        },
    }
