"""
MLPredictor — factor-weighted Ridge model inference.

Implements StrategyBase so it integrates naturally into the existing strategy
dispatch loop. Falls back gracefully (returns None) when:
  - no trained model exists for the configured horizon
  - the model is older than max_model_age_days
  - feature coverage for the ticker falls below min_feature_coverage

The rules engine remains active in parallel and handles all fallback cases.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

from app.core.types import MRAOutcome, Prediction, ScoredEvent, StrategyConfig
from app.ml.feature_builder import FeatureBuilder
from app.strategies.base import StrategyBase


HORIZON_DAYS: dict[str, float] = {
    "1h":  1 / 24,
    "4h":  4 / 24,
    "1d":  1.0,
    "7d":  7.0,
    "30d": 30.0,
}

# Switch to live score buffer for confidence once we have this many samples
_LIVE_STD_MIN_SAMPLES = 20


class MLPredictor(StrategyBase):
    """
    Factor-weighted prediction using a trained Ridge model.

    The model is loaded from ml_models (latest passing gate, within age limit).
    Feature vectors are built from price_bars and FRED dump parquets using
    FeatureBuilder with strict no-lookahead semantics.

    Confidence = |score| / std(recent_scores)
      - Uses training score_std until _LIVE_STD_MIN_SAMPLES live scores accumulate
      - Adapts to regime shifts as live scores update the buffer
    """

    def __init__(
        self,
        config: StrategyConfig,
        db_path: str | Path = "data/alpha.db",
        dumps_root: str | Path = "data/raw_dumps",
        min_feature_coverage: float = 0.8,
        max_model_age_days: int = 30,
        factors_path: str = "config/factors.yaml",
    ) -> None:
        super().__init__(config)
        cfg_dict = config.config or {}
        self._db_path = Path(db_path)
        # Training pipeline writes models/rows under `ml_train` by default; allow override per strategy.
        self._tenant_id = str(cfg_dict.get("tenant_id") or os.getenv("ML_TENANT_ID") or "ml_train")
        self._min_coverage = float(cfg_dict.get("min_feature_coverage", min_feature_coverage))
        self._max_age_days = int(cfg_dict.get("max_model_age_days", max_model_age_days))
        self._fb = FeatureBuilder(
            db_path=db_path,
            dumps_root=dumps_root,
            tenant_id=self._tenant_id,
            factors_path=factors_path,
        )
        # {horizon: (model_dict, loaded_at_datetime)}
        self._model_cache: dict[str, tuple[dict, datetime]] = {}
        # {horizon: deque of raw prediction scores for adaptive std}
        self._score_buffer: dict[str, deque[float]] = {}

    # ── StrategyBase interface ───────────────────────────────────────────────

    def maybe_predict(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        price_context: dict,
        event_timestamp: datetime,
    ) -> Optional[Prediction]:
        horizon = self.config.config.get("horizon", "1d")
        horizon_days = HORIZON_DAYS.get(horizon, 1.0)
        ticker = scored_event.primary_ticker
        as_of = event_timestamp.date()

        # 1. Load model (24h in-process cache, DB staleness check)
        model = self._get_model(horizon)
        if model is None:
            return None

        # 2. Build point-in-time feature vector
        features, coverage = self._fb.build(ticker, as_of, horizon)
        if coverage < self._min_coverage:
            return None

        # 3. Preprocess: clip → standardize using stored training params
        processed = self._preprocess(features, model)
        if not processed:
            return None

        # 4. Score
        w_block = model["weights_json"]
        intercept = w_block.get("intercept", 0.0)
        weights = w_block.get("weights", {})
        score = intercept + sum(weights.get(f, 0.0) * v for f, v in processed.items())

        # 5. Confidence = |score| / score_std (adaptive to live distribution)
        buf = self._score_buffer.setdefault(horizon, deque(maxlen=200))
        buf.append(score)
        if len(buf) >= _LIVE_STD_MIN_SAMPLES:
            score_std = _std_deque(buf) or model.get("score_std", 0.01)
        else:
            score_std = model.get("score_std", 0.01)

        raw_confidence = abs(score) / max(score_std, 1e-6)
        confidence = max(0.1, min(raw_confidence, 0.95))

        # 6. Direction
        direction = "up" if score > 0 else "down"

        return Prediction(
            id=str(uuid4()),
            strategy_id=self.config.id,
            scored_event_id=scored_event.id,
            ticker=ticker,
            timestamp=event_timestamp,
            prediction=direction,
            confidence=round(confidence, 4),
            horizon=horizon,
            entry_price=float(price_context.get("entry_price", 100.0)),
            mode=self.config.mode,
            feature_snapshot={
                "ml_score": round(score, 5),
                "coverage": round(coverage, 3),
                "top_factors": _top_factors(model["feature_importance_json"]),
                "model_accuracy": model.get("model_accuracy"),
                "ic_score": model.get("ic_score"),
                "train_rows": model.get("train_rows"),
            },
        )

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _get_model(self, horizon: str) -> Optional[dict]:
        """Return cached model or fetch from DB; None if stale/missing."""
        if horizon in self._model_cache:
            m, loaded_at = self._model_cache[horizon]
            age_hours = (datetime.utcnow() - loaded_at).total_seconds() / 3600
            if age_hours < 24:
                return m

        model = _load_latest_model(self._db_path, self._tenant_id, horizon, self._max_age_days)
        if model is not None:
            self._model_cache[horizon] = (model, datetime.utcnow())
        return model

    def _preprocess(self, features: dict, model: dict) -> Optional[dict]:
        """Apply stored clip → standardize → group-weight scaling to raw features."""
        clip_params: dict = model.get("clip_params_json", {})
        scaler: dict = model.get("scaler_json", {})
        group_weights: dict = model.get("group_weights_json", {})
        feature_names: list[str] = model.get("weights_json", {}).get("feature_names", [])

        result: dict[str, float] = {}
        for name in feature_names:
            if name not in features:
                continue
            val = float(features[name])
            # Winsorize (clip bounds from training distribution)
            if name in clip_params:
                val = max(clip_params[name]["lo"], min(val, clip_params[name]["hi"]))
            # Standardize
            if name in scaler:
                std = scaler[name]["std"]
                val = (val - scaler[name]["mean"]) / std if std > 0 else 0.0
            # Apply group weight (mirrors the pre-fit scaling done in train.py)
            val *= group_weights.get(name, 1.0)
            result[name] = val

        return result or None

    def close(self) -> None:
        self._fb.close()

    def __enter__(self) -> "MLPredictor":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ── Module-level helpers ─────────────────────────────────────────────────────

def _load_latest_model(
    db_path: Path,
    tenant_id: str,
    horizon: str,
    max_age_days: int,
) -> Optional[dict]:
    """Load the most recent passed-gate model for a horizon from the DB."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT * FROM ml_models
            WHERE tenant_id = ? AND horizon = ? AND passed_gate = 1
              AND CAST(JULIANDAY('now') - JULIANDAY(created_at) AS INTEGER) <= ?
            ORDER BY created_at DESC LIMIT 1
            """,
            (tenant_id, horizon, max_age_days),
        ).fetchone()
        if row is None:
            return None
        m = dict(row)
        for col in ("weights_json", "scaler_json", "clip_params_json", "feature_importance_json", "group_weights_json"):
            if isinstance(m.get(col), str):
                m[col] = json.loads(m[col])
        return m
    finally:
        conn.close()


def _std_deque(buf: deque) -> float:
    """Population standard deviation of a deque of floats."""
    vals = list(buf)
    n = len(vals)
    if n < 2:
        return 0.0
    mean = sum(vals) / n
    variance = sum((v - mean) ** 2 for v in vals) / n
    return math.sqrt(variance)


def _top_factors(importance: dict, n: int = 5) -> list[dict]:
    """Return the n highest-importance factors as [{factor, importance}]."""
    return [
        {"factor": k, "importance": round(v, 4)}
        for k, v in sorted(importance.items(), key=lambda x: -x[1])[:n]
    ]


def make_ml_strategy_config(
    horizon: str = "1d",
    strategy_id: str = "ml_factor_ridge",
    mode: str = "live",
    tenant_id: str = "ml_train",
) -> StrategyConfig:
    """Convenience factory for a StrategyConfig targeting the ML predictor."""
    return StrategyConfig(
        id=f"{strategy_id}_{horizon}",
        name=f"ML Factor Ridge ({horizon})",
        version="1.0",
        strategy_type="ml_factor",
        mode=mode,
        config={"horizon": horizon, "tenant_id": tenant_id},
        active=True,
    )
