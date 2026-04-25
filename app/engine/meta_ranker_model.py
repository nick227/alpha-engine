from __future__ import annotations

import math


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-float(x)))


def predict_meta_ranker_probs(row: dict) -> tuple[float, float]:
    """
    Shadow-model probabilities for the first challenger experiment.
    Returns (p_outperform, p_fail).
    """
    base = float(row.get("base_score") or 0.0)
    m5 = float(row.get("momentum_5d") or 0.0)
    m20 = float(row.get("momentum_20d") or 0.0)
    vol = float(row.get("volatility_20d") or 0.0)
    sector = float(row.get("sector_strength") or 0.0)
    liq = float(row.get("liquidity") or 0.0)
    liq_scaled = _clamp(math.log10(max(liq, 1.0)) / 8.0, 0.0, 2.0) - 1.0
    win = float(row.get("strategy_win_rate") or 0.5) - 0.5
    decay = float(row.get("strategy_decay") or 0.0)
    claims = _clamp(float(row.get("claim_count") or 1.0) / 5.0, 0.0, 1.0)
    overlap = _clamp(float(row.get("overlap_count") or 1.0) / 5.0, 0.0, 1.0)

    out_logit = (
        -0.25
        + (1.65 * base)
        + (0.90 * m20)
        + (0.45 * m5)
        + (0.35 * sector)
        + (0.25 * liq_scaled)
        + (0.60 * win)
        + (0.20 * decay)
        + (0.20 * claims)
        + (0.15 * overlap)
        - (1.10 * vol)
    )
    fail_logit = (
        -0.35
        - (1.10 * base)
        - (0.55 * m20)
        - (0.20 * m5)
        - (0.35 * sector)
        - (0.45 * win)
        + (1.25 * vol)
        + (0.25 * max(0.0, claims - 0.6))
    )
    p_out = _clamp(_sigmoid(out_logit), 0.0, 1.0)
    p_fail = _clamp(_sigmoid(fail_logit), 0.0, 1.0)
    return p_out, p_fail
