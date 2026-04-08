from __future__ import annotations


def derive_track_weights(
    sentiment_accuracy: float,
    quant_accuracy: float,
    sentiment_stability: float = 1.0,
    quant_stability: float = 1.0,
) -> dict[str, float]:
    """
    Calculate track weights based on accuracy and stability.
    
    Canonical implementation - replaces app/engine/weight_engine.py and app/intelligence/weight_engine.py
    """
    s = max(0.0, sentiment_accuracy * sentiment_stability)
    q = max(0.0, quant_accuracy * quant_stability)

    total = s + q
    if total <= 0:
        return {"ws": 0.5, "wq": 0.5}

    return {
        "ws": round(s / total, 4),
        "wq": round(q / total, 4),
    }


def derive_track_weights_from_stability(
    sentiment_stability: float | None,
    quant_stability: float | None,
) -> dict[str, float]:
    """
    Dynamic consensus weights using only stability signals.

    Ws = sentiment stability
    Wq = quant stability

    Falls back to 0.5/0.5 when missing or non-positive.
    """
    s = float(sentiment_stability or 0.0)
    q = float(quant_stability or 0.0)
    s = max(0.0, s)
    q = max(0.0, q)

    total = s + q
    if total <= 0:
        return {"ws": 0.5, "wq": 0.5}

    return {"ws": round(s / total, 4), "wq": round(q / total, 4)}
