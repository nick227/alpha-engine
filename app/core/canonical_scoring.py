from __future__ import annotations
import numpy as np
from typing import Any, Dict, List

def score_prediction(
    direction_correct: bool,
    return_pct: float,
    confidence: float,
    max_return_in_run: float = 0.10 # Used to normalize returns
) -> float:
    """
    Computes the Prediction Alpha Score for a single trade.
    Focuses on accuracy, yield, and model confidence.
    No clamping to [0, 1] to preserve ranking separation.
    """
    dir_score = 1.0 if direction_correct else 0.0
    
    # Simple normalization of return_pct relative to a baseline 'good' return
    # If return is 5% and baseline is 10%, return_score is 0.5.
    # Note: Using absolute value of return_pct for the score, as negative returns
    # with correct direction are rare but possible in some strategies.
    # However, usually we want to reward ROI relative to the direction.
    norm_return = return_pct / max(abs(max_return_in_run), 0.001)
    
    alpha_prediction = (0.4 * dir_score) + (0.3 * norm_return) + (0.3 * confidence)
    return float(alpha_prediction)

def score_strategy(
    prediction_scores: List[float],
    max_drawdown: float = 0.0,
    variance_penalty_weight: float = 0.5,
    drawdown_penalty_weight: float = 1.0
) -> Dict[str, float]:
    """
    Computes the Strategy Alpha Score from a rolling window of predictions.
    Applies penalties for instability and drawdown.
    """
    if not prediction_scores:
        return {
            "alpha_strategy": 0.0,
            "avg_prediction_alpha": 0.0,
            "drawdown_penalty": 0.0,
            "variance_penalty": 0.0
        }
    
    avg_alpha = float(np.mean(prediction_scores))
    
    # Variance penalty (instability)
    # Higher variance in prediction scores reduces the overall strategy score
    var_alpha = float(np.var(prediction_scores))
    var_penalty = var_alpha * variance_penalty_weight
    
    # Drawdown penalty
    # Direct penalty based on the maximum drawdown experienced in the window
    dd_penalty = max_drawdown * drawdown_penalty_weight
    
    alpha_strategy = avg_alpha - var_penalty - dd_penalty
    
    return {
        "alpha_strategy": float(alpha_strategy),
        "avg_prediction_alpha": avg_alpha,
        "drawdown_penalty": float(dd_penalty),
        "variance_penalty": float(var_penalty)
    }

def normalize_for_display(score: float, factor: float = 100.0) -> float:
    """
    Scales a raw alpha score for UI presentation.
    Typically used to turn scores like 0.85 into 85.0.
    """
    return score * factor
