"""
Ridge model training with walk-forward timeboxing.

For each (horizon, TimeWindow), loads learning_rows from the DB, applies
winsorization + standardization, fits a Ridge model, runs the baseline gate,
and persists the result to ml_models.

Production safeguards implemented:
  - Dynamic min_rows = max(50, n_features * 10)
  - Winsorization at 1%/99% per feature
  - StandardScaler (mean/std stored for inference)
  - Directional accuracy gate > 52% vs baseline (predict sign = 0)
  - Feature importance = normalized abs(weights)
  - Information coefficient stored for debugging
  - score_std stored for confidence calibration
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Optional
from uuid import uuid4

import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from app.ml.timebox import TimeWindow, generate_windows

# ── Defaults ────────────────────────────────────────────────────────────────
RIDGE_ALPHA = 1.0
MIN_DIRECTIONAL_ACCURACY = 0.52   # baseline gate
WINSORIZE_LO = 1.0                # percentile
WINSORIZE_HI = 99.0


def train_model(
    window: TimeWindow,
    horizon: str,
    db_path: str | Path = "data/alpha.db",
    tenant_id: str = "default",
    ridge_alpha: float = RIDGE_ALPHA,
    factors_path: str = "config/factors.yaml",
) -> Optional[str]:
    """
    Train one Ridge model for (horizon, window) and persist to ml_models.

    Returns model_id if training succeeded AND passed the baseline accuracy gate.
    Returns None if data is insufficient or accuracy gate fails.
    """
    db_path = Path(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        rows = conn.execute(
            """
            SELECT features_json, future_return FROM ml_learning_rows
            WHERE tenant_id = ? AND horizon = ?
              AND future_return IS NOT NULL
              AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
              AND split = 'train'
            ORDER BY timestamp ASC
            """,
            (tenant_id, horizon,
             window.train_start.isoformat(),
             window.train_end.isoformat()),
        ).fetchall()

        if not rows:
            return None

        # Parse and align feature dicts
        parsed: list[tuple[dict[str, float], float]] = []
        for r in rows:
            try:
                feats = json.loads(r["features_json"])
                parsed.append((feats, float(r["future_return"])))
            except Exception:
                continue

        if not parsed:
            return None

        # Stable, sorted feature name list (union across all rows)
        all_keys: list[str] = sorted({k for feats, _ in parsed for k in feats})
        n_features = len(all_keys)
        if n_features == 0:
            return None

        # Dynamic minimum rows guard
        min_rows = max(50, n_features * 10)
        if len(parsed) < min_rows:
            return None

        # Build X (n_samples, n_features) filling missing values with NaN
        n = len(parsed)
        X_raw = np.full((n, n_features), np.nan, dtype=np.float64)
        y = np.array([ret for _, ret in parsed], dtype=np.float64)

        for i, (feats, _) in enumerate(parsed):
            for j, key in enumerate(all_keys):
                if key in feats:
                    X_raw[i, j] = feats[key]

        # Impute NaNs with column median (computed only from training data)
        col_medians = np.nanmedian(X_raw, axis=0)
        nan_mask = np.isnan(X_raw)
        for j in range(n_features):
            X_raw[nan_mask[:, j], j] = col_medians[j]

        # Load factor config for meta controls and group weights
        from app.ml.factor_spec import load_factor_config
        factor_config = load_factor_config(factors_path)
        meta = factor_config.meta
        gw_map = factor_config.factor_group_weights()  # {factor_name: group_weight}

        # Compute feature coverage (fraction of non-NaN before imputation)
        feature_coverage_used = float(1.0 - nan_mask.mean())

        # Winsorize each feature at 1%/99%
        clip_params: dict[str, dict[str, float]] = {}
        X_clipped = X_raw.copy()
        for j, key in enumerate(all_keys):
            lo = float(np.percentile(X_raw[:, j], WINSORIZE_LO))
            hi = float(np.percentile(X_raw[:, j], WINSORIZE_HI))
            X_clipped[:, j] = np.clip(X_raw[:, j], lo, hi)
            clip_params[key] = {"lo": lo, "hi": hi}

        # Standardize (fit on training data, store params for inference)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_clipped)

        scaler_json: dict[str, dict[str, float]] = {
            key: {"mean": float(scaler.mean_[j]), "std": float(scaler.scale_[j])}
            for j, key in enumerate(all_keys)
        }

        # max_corr: drop one of any highly correlated feature pair
        # Keep whichever is more correlated with the target y
        if meta.max_corr < 1.0 and n_features > 1:
            corr_mat = np.corrcoef(X_clipped.T)
            y_corr = np.array([
                abs(float(np.corrcoef(X_clipped[:, j], y)[0, 1])) for j in range(n_features)
            ])
            to_drop: set[int] = set()
            for i in range(n_features):
                for j in range(i + 1, n_features):
                    if i in to_drop or j in to_drop:
                        continue
                    if abs(corr_mat[i, j]) > meta.max_corr:
                        drop_idx = i if y_corr[i] < y_corr[j] else j
                        to_drop.add(drop_idx)
            if to_drop:
                keep = [j for j in range(n_features) if j not in to_drop]
                X_clipped = X_clipped[:, keep]
                all_keys = [all_keys[j] for j in keep]
                n_features = len(all_keys)
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X_clipped)
                scaler_json = {
                    key: {"mean": float(scaler.mean_[j]), "std": float(scaler.scale_[j])}
                    for j, key in enumerate(all_keys)
                }

        # Group weight scaling: multiply each column by its group weight before fitting.
        # This biases Ridge regularization — lower-weight groups receive more effective penalty.
        # The same weights are applied at inference so the scale is consistent.
        group_weights_array = np.array([gw_map.get(k, 1.0) for k in all_keys])
        group_weights_json: dict[str, float] = {k: gw_map.get(k, 1.0) for k in all_keys}
        X_for_fit = X_scaled * group_weights_array

        # Fit Ridge
        model = Ridge(alpha=ridge_alpha)
        model.fit(X_for_fit, y)

        weights = {key: float(model.coef_[j]) for j, key in enumerate(all_keys)}
        intercept = float(model.intercept_)

        # In-sample scores (using group-weighted features, matching inference path)
        scores = X_for_fit @ model.coef_ + model.intercept_
        score_std = float(np.std(scores)) or 0.01

        # Directional accuracy
        model_accuracy = float((np.sign(scores) == np.sign(y)).mean())

        # Baseline: predict sign = 0 (market-neutral); ties go to 0
        baseline_accuracy = float((np.sign(np.zeros_like(y)) == np.sign(y)).mean())

        # Information coefficient (Pearson correlation of score vs return)
        if np.std(scores) > 0 and np.std(y) > 0:
            ic_score = float(np.corrcoef(scores, y)[0, 1])
        else:
            ic_score = 0.0

        avg_return = float(np.mean(y))
        win_rate = float((y > 0).mean())

        # Feature importance = normalized absolute weights
        abs_w = {k: abs(v) for k, v in weights.items()}
        total_abs = sum(abs_w.values()) or 1.0
        importance: dict[str, float] = {
            k: round(v / total_abs, 4)
            for k, v in sorted(abs_w.items(), key=lambda x: -x[1])
        }

        passed_gate = model_accuracy > MIN_DIRECTIONAL_ACCURACY

        model_id = str(uuid4())
        conn.execute(
            """
            INSERT INTO ml_models
                (model_id, tenant_id, horizon, train_start, train_end,
                 weights_json, scaler_json, clip_params_json, feature_importance_json,
                 group_weights_json,
                 baseline_accuracy, model_accuracy, ic_score, avg_return, win_rate,
                 train_rows, feature_coverage_used, score_std, passed_gate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                model_id, tenant_id, horizon,
                window.train_start.isoformat(), window.train_end.isoformat(),
                json.dumps({"intercept": intercept, "weights": weights, "feature_names": all_keys}),
                json.dumps(scaler_json),
                json.dumps(clip_params),
                json.dumps(importance),
                json.dumps(group_weights_json),
                round(baseline_accuracy, 4),
                round(model_accuracy, 4),
                round(ic_score, 4),
                round(avg_return, 6),
                round(win_rate, 4),
                len(parsed),
                round(feature_coverage_used, 4),
                round(score_std, 6),
                1 if passed_gate else 0,
            ),
        )
        conn.commit()
        return model_id if passed_gate else None

    finally:
        conn.close()


def run_training_pipeline(
    symbols: list[str],
    horizons: list[str],
    db_path: str | Path = "data/alpha.db",
    dumps_root: str | Path = "data/raw_dumps",
    train_days: int = 180,
    predict_days: int = 30,
    step_days: int = 30,
    data_start: Optional[date] = None,
    data_end: Optional[date] = None,
    tenant_id: str = "default",
    factors_path: str = "config/factors.yaml",
    min_feature_coverage: float = 0.6,
) -> dict[str, list[str]]:
    """
    Full walk-forward training pipeline.

    1. Determines the available date range from price_bars.
    2. Builds learning_rows via dataset.build_dataset for each window.
    3. Trains one Ridge model per (horizon, window).
    4. Returns {horizon: [model_ids]} for models that passed the gate.
    """
    from app.ml.dataset import build_dataset, HORIZON_DAYS

    db_path = Path(db_path)

    # Determine date range from price_bars if not supplied
    if data_start is None or data_end is None:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT MIN(DATE(timestamp)) as mn, MAX(DATE(timestamp)) as mx FROM price_bars WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        conn.close()
        from datetime import date as _date
        data_start = data_start or (_date.fromisoformat(row["mn"]) if row and row["mn"] else _date(2020, 1, 1))
        data_end = data_end or (_date.fromisoformat(row["mx"]) if row and row["mx"] else _date.today())

    results: dict[str, list[str]] = {h: [] for h in horizons}

    for window in generate_windows(data_start, data_end, train_days, predict_days, step_days):
        # Build dataset for the train window
        build_dataset(
            symbols=symbols,
            date_range=(window.train_start, window.train_end),
            horizons=horizons,
            db_path=db_path,
            dumps_root=dumps_root,
            min_feature_coverage=min_feature_coverage,
            tenant_id=tenant_id,
            split="train",
            factors_path=factors_path,
        )

        for horizon in horizons:
            model_id = train_model(window, horizon, db_path, tenant_id)
            if model_id:
                results[horizon].append(model_id)
                print(f"  [OK] horizon={horizon} window={window.train_start}-{window.train_end} model_id={model_id}")
            else:
                print(f"  [--] horizon={horizon} window={window.train_start}-{window.train_end} gate failed or insufficient data")

    return results


if __name__ == "__main__":
    import sys
    from datetime import date

    symbols = ["AAPL", "MSFT", "NVDA", "SPY", "QQQ"]
    horizons = ["1d", "7d", "30d"]

    print("Running walk-forward ML training pipeline...")
    results = run_training_pipeline(symbols=symbols, horizons=horizons)
    total = sum(len(v) for v in results.values())
    print(f"\nDone. {total} models trained and passed gate.")
    for h, ids in results.items():
        print(f"  {h}: {len(ids)} model(s)")
