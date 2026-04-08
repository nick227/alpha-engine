from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from app.core.types import Prediction, PredictionOutcome


def build_performance_table(predictions: List[Prediction], outcomes: List[PredictionOutcome]) -> pd.DataFrame:
    pred_df = pd.DataFrame([p.to_dict() for p in predictions])
    out_df = pd.DataFrame([o.to_dict() for o in outcomes])

    if pred_df.empty or out_df.empty:
        return pd.DataFrame(
            columns=[
                "strategy_name",
                "horizon",
                "total_predictions",
                "accuracy",
                "avg_return",
                "sharpe_proxy",
                "calibration_score",
            ]
        )

    merged = pred_df.merge(out_df, left_on="id", right_on="prediction_id", how="inner")

    rows = []
    for (strategy_name, horizon), group in merged.groupby(["strategy_name", "horizon"]):
        returns = group["return_pct"].astype(float)
        confidences = group["confidence"].astype(float)
        actuals = group["direction_correct"].astype(float)
        accuracy = float(actuals.mean())
        avg_return = float(returns.mean())
        return_std = float(returns.std(ddof=0)) if len(returns) > 1 else 0.0
        sharpe_proxy = float(avg_return / return_std) if return_std > 0 else 0.0
        calibration_score = float(1.0 - np.mean(np.abs(confidences - actuals)))
        rows.append(
            {
                "strategy_name": strategy_name,
                "horizon": horizon,
                "total_predictions": int(len(group)),
                "accuracy": accuracy,
                "avg_return": avg_return,
                "sharpe_proxy": sharpe_proxy,
                "calibration_score": calibration_score,
            }
        )

    return pd.DataFrame(rows).sort_values(["sharpe_proxy", "avg_return"], ascending=False)
