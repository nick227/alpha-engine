"""
Ridge model training with walk-forward timeboxing.

For each (horizon, TimeWindow), loads learning_rows from the DB, applies
winsorization + standardization, fits a Ridge model, runs the baseline gate,
and persists the result to ml_models.

Production safeguards implemented:
  - Dynamic min_rows = max(50, n_features * 10)
  - Chronological 80/20 train/validation split (no look-ahead)
  - Preprocessors (medians, winsorization bounds, StandardScaler) fitted strictly on training data
  - Majority-class and random walk baselines (no trivial 0.0000 baseline)
  - Directional accuracy gate > 52% AND > baseline_accuracy AND IC > 0.0
  - Feature importance = normalized abs(weights)
  - Information coefficient and uncertainty standard error stored for validation
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

    Enforces chronological separation:
    - 80% of rows used for fitting preprocessors and Ridge model.
    - 20% validation holdout used for out-of-sample evaluation.
    - Baseline accuracy benchmarked against majority-class direction and 50% random walk.

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

        # Parse and align feature dicts chronologically
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
        n_total = len(parsed)
        if n_total < min_rows:
            return None

        # Chronological Train / Validation split (80% train, 20% val)
        n_train = int(n_total * 0.80)
        if n_total - n_train < 10:
            n_train = max(n_total - 10, int(n_total * 0.70))
        if n_train < 30:
            return None

        parsed_train = parsed[:n_train]
        parsed_val = parsed[n_train:]

        # Build X_train and X_val
        X_train_raw = np.full((len(parsed_train), n_features), np.nan, dtype=np.float64)
        y_train = np.array([ret for _, ret in parsed_train], dtype=np.float64)

        for i, (feats, _) in enumerate(parsed_train):
            for j, key in enumerate(all_keys):
                if key in feats:
                    X_train_raw[i, j] = feats[key]

        X_val_raw = np.full((len(parsed_val), n_features), np.nan, dtype=np.float64)
        y_val = np.array([ret for _, ret in parsed_val], dtype=np.float64)

        for i, (feats, _) in enumerate(parsed_val):
            for j, key in enumerate(all_keys):
                if key in feats:
                    X_val_raw[i, j] = feats[key]

        # Impute NaNs with column median (computed strictly from training data)
        col_medians = np.nanmedian(X_train_raw, axis=0)
        # Handle all-NaN column fallback
        overall_median = float(np.nanmedian(col_medians)) if not np.isnan(np.nanmedian(col_medians)) else 0.0
        col_medians[np.isnan(col_medians)] = overall_median

        for j in range(n_features):
            X_train_raw[np.isnan(X_train_raw[:, j]), j] = col_medians[j]
            X_val_raw[np.isnan(X_val_raw[:, j]), j] = col_medians[j]

        # Load factor config for meta controls and group weights
        from app.ml.factor_spec import load_factor_config
        factor_config = load_factor_config(factors_path)
        meta = factor_config.meta
        gw_map = factor_config.factor_group_weights()

        # Winsorize each feature at 1%/99% using training percentiles
        clip_params: dict[str, dict[str, float]] = {}
        X_train_clipped = X_train_raw.copy()
        X_val_clipped = X_val_raw.copy()

        for j, key in enumerate(all_keys):
            lo = float(np.percentile(X_train_raw[:, j], WINSORIZE_LO))
            hi = float(np.percentile(X_train_raw[:, j], WINSORIZE_HI))
            if lo == hi:
                hi = lo + 1e-6
            X_train_clipped[:, j] = np.clip(X_train_raw[:, j], lo, hi)
            X_val_clipped[:, j] = np.clip(X_val_raw[:, j], lo, hi)
            clip_params[key] = {"lo": lo, "hi": hi}

        # Standardize (fitted strictly on training data)
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train_clipped)
        X_val_scaled = scaler.transform(X_val_clipped)

        scaler_json: dict[str, dict[str, float]] = {
            key: {"mean": float(scaler.mean_[j]), "std": float(scaler.scale_[j])}
            for j, key in enumerate(all_keys)
        }

        # Correlation pruning based strictly on training data
        if meta.max_corr < 1.0 and n_features > 1:
            corr_mat = np.corrcoef(X_train_clipped.T)
            y_corr = np.array([
                abs(float(np.corrcoef(X_train_clipped[:, j], y_train)[0, 1])) if np.std(X_train_clipped[:, j]) > 0 else 0.0
                for j in range(n_features)
            ])
            _exempt_prefixes = ("opt_", "short_", "earn_", "breadth_")
            to_drop: set[int] = set()
            for i in range(n_features):
                for j in range(i + 1, n_features):
                    if i in to_drop or j in to_drop:
                        continue
                    ki = all_keys[i]
                    kj = all_keys[j]
                    if ki.startswith(_exempt_prefixes) or kj.startswith(_exempt_prefixes):
                        continue
                    if abs(corr_mat[i, j]) > meta.max_corr:
                        drop_idx = i if y_corr[i] < y_corr[j] else j
                        to_drop.add(drop_idx)
            if to_drop:
                keep = [j for j in range(n_features) if j not in to_drop]
                X_train_clipped = X_train_clipped[:, keep]
                X_val_clipped = X_val_clipped[:, keep]
                all_keys = [all_keys[j] for j in keep]
                n_features = len(all_keys)
                scaler = StandardScaler()
                X_train_scaled = scaler.fit_transform(X_train_clipped)
                X_val_scaled = scaler.transform(X_val_clipped)
                scaler_json = {
                    key: {"mean": float(scaler.mean_[j]), "std": float(scaler.scale_[j])}
                    for j, key in enumerate(all_keys)
                }

        # Group weight scaling
        group_weights_array = np.array([gw_map.get(k, 1.0) for k in all_keys])
        group_weights_json: dict[str, float] = {k: gw_map.get(k, 1.0) for k in all_keys}
        X_train_for_fit = X_train_scaled * group_weights_array
        X_val_for_fit = X_val_scaled * group_weights_array

        # Fit Ridge on training slice
        model = Ridge(alpha=ridge_alpha)
        model.fit(X_train_for_fit, y_train)

        weights = {key: float(model.coef_[j]) for j, key in enumerate(all_keys)}
        intercept = float(model.intercept_)

        # In-sample scores and scale
        train_scores = X_train_for_fit @ model.coef_ + model.intercept_
        score_std = float(np.std(train_scores)) or 0.01

        # Out-of-sample evaluation on validation holdout
        val_scores = X_val_for_fit @ model.coef_ + model.intercept_
        val_accuracy = float((np.sign(val_scores) == np.sign(y_val)).mean())

        # Meaningful baselines on validation set
        # 1. Majority-class baseline: predicting always up or always down
        majority_baseline = float(max((y_val > 0).mean(), (y_val < 0).mean()))
        # 2. Strict benchmark: model must beat both 50% coin-flip AND majority class
        baseline_accuracy = max(0.50, majority_baseline)

        # Validation IC (Pearson correlation of score vs realized return)
        if np.std(val_scores) > 0 and np.std(y_val) > 0:
            val_ic = float(np.corrcoef(val_scores, y_val)[0, 1])
        else:
            val_ic = 0.0

        val_avg_return = float(np.mean(y_val))
        val_win_rate = float((y_val > 0).mean())

        # Statistical uncertainty: standard error of validation accuracy
        val_n = len(y_val)
        val_std_error = float(np.sqrt(val_accuracy * (1.0 - val_accuracy) / val_n)) if val_n > 0 else 0.0

        # Feature importance = normalized absolute weights
        abs_w = {k: abs(v) for k, v in weights.items()}
        total_abs = sum(abs_w.values()) or 1.0
        importance: dict[str, float] = {
            k: round(v / total_abs, 4)
            for k, v in sorted(abs_w.items(), key=lambda x: -x[1])
        }

        # Rigorous gate: must beat minimum threshold, beat majority baseline, and have positive IC
        passed_gate = (
            val_accuracy >= MIN_DIRECTIONAL_ACCURACY
            and val_accuracy > baseline_accuracy
            and val_ic > 0.0
        )

        validation_metadata = {
            "validation_accuracy": round(val_accuracy, 4),
            "baseline_accuracy": round(baseline_accuracy, 4),
            "majority_baseline": round(majority_baseline, 4),
            "validation_ic": round(val_ic, 4),
            "validation_samples": val_n,
            "train_samples": len(parsed_train),
            "accuracy_std_error": round(val_std_error, 4),
            "passed_gate": bool(passed_gate),
        }

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
                json.dumps({
                    "intercept": intercept,
                    "weights": weights,
                    "feature_names": all_keys,
                    "validation_metadata": validation_metadata,
                }),
                json.dumps(scaler_json),
                json.dumps(clip_params),
                json.dumps(importance),
                json.dumps(group_weights_json),
                round(baseline_accuracy, 4),
                round(val_accuracy, 4),
                round(val_ic, 4),
                round(val_avg_return, 6),
                round(val_win_rate, 4),
                len(parsed_train),
                round(float(1.0 - np.isnan(X_train_raw).mean()), 4),
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
    Full walk-forward training pipeline with chronological train and test windows.
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
        # 1. Build dataset for the train window
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

        # 2. Build dataset for the sequential out-of-sample predict window
        build_dataset(
            symbols=symbols,
            date_range=(window.predict_start, window.predict_end),
            horizons=horizons,
            db_path=db_path,
            dumps_root=dumps_root,
            min_feature_coverage=min_feature_coverage,
            tenant_id=tenant_id,
            split="test",
            factors_path=factors_path,
        )

        for horizon in horizons:
            model_id = train_model(window, horizon, db_path, tenant_id)
            if model_id:
                results[horizon].append(model_id)
                print(f"  [OK] horizon={horizon} window={window.train_start}-{window.train_end} model_id={model_id}")
            else:
                print(f"  [--] horizon={horizon} window={window.train_start}-{window.train_end} gate failed (below majority baseline or min accuracy)")

    return results


if __name__ == "__main__":
    symbols = ["AAPL", "MSFT", "NVDA", "SPY", "QQQ"]
    horizons = ["1d", "7d", "30d"]

    print("Running walk-forward ML training pipeline...")
    results = run_training_pipeline(symbols=symbols, horizons=horizons)
    total = sum(len(v) for v in results.values())
    print(f"\nDone. {total} models trained and passed gate.")
    for h, ids in results.items():
        print(f"  {h}: {len(ids)} model(s)")
