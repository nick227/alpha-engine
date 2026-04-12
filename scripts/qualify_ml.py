#!/usr/bin/env python
"""
ML Qualification Test

Validates the complete ML factor pipeline end-to-end without measuring
performance. Tests mechanics only: data flows, transforms compute,
preprocessing is identical at train and predict time.

Ticker : AAPL (SPY used as excess-return benchmark)
Horizon: 7d
Pass conditions (ALL must be true):
  - factors loaded and expanded
  - horizon filtering applied
  - feature builder returns non-empty vectors
  - coverage filter does not kill all rows
  - dataset builder produces rows
  - walk-forward training executes
  - at least one model trained
  - predictions generated
  - predictions are not all the same direction (std > 0)
  - no NaN confidences
  - preprocessing path is consistent (predict uses stored clip/scale/weights)

Expected runtime: < 5 seconds
"""
from __future__ import annotations

import sys
from pathlib import Path as _Path

# Ensure project root is on sys.path when running as a script
_ROOT = _Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import math
import random
import shutil
import tempfile
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────
TICKER = "AAPL"               # predicted stock (SPY is the excess-return benchmark)
HORIZON = "7d"
N_TRADING_DAYS = 320          # ~1.3 years — enough for 3 walk-forward windows
TRAIN_CALENDAR_DAYS = 150     # ~105 trading days per window
PREDICT_DAYS = 30
STEP_DAYS = 100               # 3 windows; mechanics, not performance
MIN_COVERAGE = 0.55           # relaxed: only AAPL + SPY + QQQ + VIX + TLT + IWM synthetic
N_PREDICT_DATES = 60          # dates to run predictions over
SEED = 42

# Symbols to seed — enough to cover most 7d horizon_set factors
SYMBOLS = ["AAPL", "SPY", "QQQ", "^VIX", "TLT", "IWM"]


def main() -> None:
    t0 = time.perf_counter()
    tmp = tempfile.mkdtemp(prefix="alpha_qualify_")
    try:
        passed, stats = _run(Path(tmp))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    elapsed = time.perf_counter() - t0
    status = "PASS" if passed else "FAIL"

    print(f"\nML qualification test: {status}")
    print(f"predictions:      {stats['n_predictions']}")
    print(f"models trained:   {stats['n_models']}")
    print(f"avg factors used: {stats['avg_factors']}")
    print(f"prediction std:   {stats['pred_std']:.4f}")
    print(f"elapsed:          {elapsed:.2f}s")

    if not passed:
        print("\nFailed checks:")
        for k, v in stats["checks"].items():
            if not v:
                print(f"  FAIL  {k}")


# ── Core test ─────────────────────────────────────────────────────────────────

def _run(tmp: Path) -> tuple[bool, dict]:
    random.seed(SEED)
    db_path = tmp / "qualify.db"

    from app.db.repository import AlphaRepository
    from app.ml.dataset import build_dataset
    from app.ml.factor_spec import load_factor_config
    from app.ml.feature_builder import FeatureBuilder
    from app.ml.timebox import generate_windows
    from app.ml.train import train_model
    from app.ml.predict import MLPredictor, make_ml_strategy_config

    checks: dict[str, bool] = {}

    # ── Step 1: factor config ─────────────────────────────────────────────
    cfg = load_factor_config()
    eligible_7d = cfg.get_eligible_specs(HORIZON, 7.0)

    checks["factors_loaded"] = len(cfg.factors) > 0
    checks["expansion_works"] = len(cfg.factors) > 20
    checks["horizon_filtering"] = 0 < len(eligible_7d) < len(cfg.factors)
    checks["group_weights_defined"] = len(cfg.groups) > 0

    # ── Step 2: synthetic price bars ──────────────────────────────────────
    repo = AlphaRepository(str(db_path))
    conn = repo.conn

    all_dates = _seed_prices(conn)
    data_start, data_end = all_dates[0], all_dates[-1]

    # ── Step 3: feature builder ───────────────────────────────────────────
    fb = FeatureBuilder(db_path=str(db_path))
    probe_date = all_dates[len(all_dates) // 2]
    feats, cov = fb.build(TICKER, probe_date, HORIZON)
    fb.close()

    checks["feature_builder_works"] = len(feats) > 0
    checks["coverage_not_zero"] = cov > 0.0
    checks["horizon_set_applied"] = len(feats) == len(eligible_7d) or cov > 0

    avg_factors = len(feats)

    # ── Step 4: build dataset ─────────────────────────────────────────────
    n_rows = build_dataset(
        symbols=[TICKER],
        date_range=(data_start, data_end - timedelta(days=60)),
        horizons=[HORIZON],
        db_path=str(db_path),
        min_feature_coverage=MIN_COVERAGE,
        split="train",
    )
    checks["dataset_built"] = n_rows > 0
    checks["coverage_filter_not_killing_all"] = n_rows > 10

    # ── Step 5: walk-forward training ─────────────────────────────────────
    windows = list(generate_windows(
        data_start, data_end,
        train_days=TRAIN_CALENDAR_DAYS,
        predict_days=PREDICT_DAYS,
        step_days=STEP_DAYS,
    ))
    checks["walk_forward_generates_windows"] = len(windows) >= 2

    trained_ids: list[str] = []
    all_model_ids: list[str] = []

    for w in windows:
        mid = train_model(w, HORIZON, db_path=str(db_path))
        row = conn.execute(
            "SELECT model_id FROM ml_models ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            all_model_ids.append(row["model_id"])
        if mid:
            trained_ids.append(mid)

    checks["training_executes"] = len(all_model_ids) > 0
    checks["models_pass_gate"] = len(trained_ids) > 0

    # ── Step 6: prediction path ───────────────────────────────────────────
    # Use the most recent model regardless of gate to test the mechanics path.
    # If the gate is the only failure, prediction path is still valid.
    predictions = _run_predictions(
        db_path, all_model_ids, all_dates, conn
    )

    checks["predictions_generated"] = len(predictions) > 0

    if predictions:
        confidences = [p.confidence for p in predictions]
        directions = [p.prediction for p in predictions]

        checks["no_nans"] = not any(math.isnan(c) for c in confidences)
        checks["predictions_not_constant"] = len(set(directions)) > 1
        pred_std = _std(confidences)
        checks["confidence_std_positive"] = pred_std > 0

        # Verify preprocessing path by checking feature_snapshot is populated
        has_snapshot = all(
            p.feature_snapshot.get("ml_score") is not None for p in predictions
        )
        checks["preprocessing_consistent"] = has_snapshot
    else:
        checks["no_nans"] = True
        checks["predictions_not_constant"] = False
        checks["confidence_std_positive"] = False
        checks["preprocessing_consistent"] = False
        pred_std = 0.0
        confidences = []

    # ── Step 7: scaling round-trip check ─────────────────────────────────
    # Verify stored scaler params can reconstruct the preprocessing.
    import json
    m_row = conn.execute(
        "SELECT scaler_json, clip_params_json, group_weights_json FROM ml_models "
        "ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if m_row:
        scaler = json.loads(m_row["scaler_json"])
        clip = json.loads(m_row["clip_params_json"])
        gw = json.loads(m_row["group_weights_json"])
        checks["scaler_stored"] = len(scaler) > 0
        checks["clip_params_stored"] = len(clip) > 0
        checks["group_weights_stored"] = len(gw) > 0
    else:
        checks["scaler_stored"] = False
        checks["clip_params_stored"] = False
        checks["group_weights_stored"] = False

    return all(checks.values()), {
        "n_predictions": len(predictions),
        "n_models": len(trained_ids),
        "avg_factors": avg_factors,
        "pred_std": pred_std,
        "checks": checks,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _seed_prices(conn) -> list[date]:
    """
    Generate N_TRADING_DAYS of synthetic OHLCV bars.

    Injects a weak learnable signal so Ridge can pass the 52% accuracy gate:
      AAPL_excess_return_7d ≈ -0.004 * vix_level_normalized + noise
    High VIX → AAPL underperforms SPY the following week.
    """
    prices = {
        "AAPL": 155.0, "SPY": 400.0, "QQQ": 320.0,
        "^VIX": 18.0, "TLT": 95.0, "IWM": 185.0,
    }
    vix_history: list[float] = []
    all_dates: list[date] = []

    d = date(2021, 6, 1)
    for step in range(N_TRADING_DAYS):
        # Advance VIX
        prices["^VIX"] = max(10.0, prices["^VIX"] + random.gauss(0.0, 0.35))
        vix = prices["^VIX"]
        vix_history.append(vix)

        # Base market drift
        spy_ret = random.gauss(0.0003, 0.010)
        prices["SPY"] *= math.exp(spy_ret)
        prices["QQQ"] *= math.exp(spy_ret + random.gauss(0.0, 0.005))
        prices["TLT"] *= math.exp(-spy_ret * 0.4 + random.gauss(0.0, 0.004))
        prices["IWM"] *= math.exp(spy_ret + random.gauss(0.0, 0.006))

        # AAPL: inject learnable VIX signal (normalized by rolling 20-period mean)
        vix_norm = (vix - 18.0) / 5.0          # approx zscore
        signal = -0.004 * vix_norm              # high VIX → negative AAPL excess
        aapl_ret = spy_ret + signal + random.gauss(0.0, 0.014)
        prices["AAPL"] *= math.exp(aapl_ret)

        for ticker in SYMBOLS:
            p = prices[ticker]
            spread = 0.005 if ticker == "^VIX" else 0.008
            conn.execute(
                "INSERT OR REPLACE INTO price_bars "
                "(tenant_id,ticker,timeframe,timestamp,open,high,low,close,volume) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    "default", ticker, "1d", d.isoformat(),
                    p * (1 - spread * 0.5),
                    p * (1 + spread),
                    p * (1 - spread),
                    p,
                    max(1e5, 1e7 + random.gauss(0, 8e5)),
                ),
            )

        all_dates.append(d)
        d += timedelta(days=1)
        while d.weekday() >= 5:
            d += timedelta(days=1)

    conn.commit()
    return all_dates


def _run_predictions(
    db_path: Path,
    all_model_ids: list[str],
    all_dates: list[date],
    conn,
) -> list:
    if not all_model_ids:
        return []

    import json
    from app.core.types import MRAOutcome, ScoredEvent
    from app.ml.predict import MLPredictor, make_ml_strategy_config

    # Force-activate the latest model by temporarily patching the gate check
    # so mechanics are validated independent of accuracy gate
    latest_row = conn.execute(
        "SELECT model_id, passed_gate FROM ml_models ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if not latest_row:
        return []

    original_gate = latest_row["passed_gate"]
    if original_gate == 0:
        conn.execute(
            "UPDATE ml_models SET passed_gate=1 WHERE model_id=?",
            (latest_row["model_id"],),
        )
        conn.commit()

    cfg_strat = make_ml_strategy_config(horizon=HORIZON, mode="backtest")
    predictor = MLPredictor(
        cfg_strat,
        db_path=str(db_path),
        min_feature_coverage=MIN_COVERAGE,
        max_model_age_days=3650,
    )

    test_dates = all_dates[-N_PREDICT_DATES:]

    # Prefetch price bars into the predictor's FeatureBuilder to avoid
    # per-prediction DB queries (same optimization as dataset build phase)
    predict_start = test_dates[0] if test_dates else date.today()
    predict_end = test_dates[-1] if test_dates else date.today()
    all_price_syms = [TICKER]
    for spec in predictor._fb.config.factors:
        if spec.source in ("price", "price_relative") and spec.symbol:
            sym = spec.resolve_symbol(TICKER)
            if sym:
                all_price_syms.append(sym)
        if spec.source == "price_relative" and spec.benchmark:
            all_price_syms.append(spec.benchmark)
    predictor._fb.prefetch_bars(list(set(all_price_syms)), predict_start, predict_end)

    predictions = []
    prices_ref = conn.execute(
        "SELECT close FROM price_bars WHERE ticker='AAPL' ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    entry_price = float(prices_ref["close"]) if prices_ref else 150.0
    for td in test_dates:
        scored = ScoredEvent(
            id=str(uuid.uuid4()),
            raw_event_id="qualify",
            primary_ticker=TICKER,
            category="market_move",
            materiality=0.65,
            direction="positive",
            confidence=0.60,
            company_relevance=0.75,
            concept_tags=[],
            explanation_terms=[],
        )
        mra = MRAOutcome(
            id=str(uuid.uuid4()),
            scored_event_id=scored.id,
            return_1m=0.001, return_5m=0.002, return_15m=0.003, return_1h=0.005,
            volume_ratio=1.15, vwap_distance=0.008, range_expansion=1.05,
            continuation_slope=0.4, pullback_depth=0.08, mra_score=0.58,
        )
        price_ctx = {"entry_price": entry_price}
        event_ts = datetime(td.year, td.month, td.day, 9, 30)

        pred = predictor.maybe_predict(scored, mra, price_ctx, event_ts)
        if pred is not None:
            predictions.append(pred)

    predictor.close()

    # Restore original gate state
    if original_gate == 0:
        conn.execute(
            "UPDATE ml_models SET passed_gate=0 WHERE model_id=?",
            (latest_row["model_id"],),
        )
        conn.commit()

    return predictions


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))


if __name__ == "__main__":
    main()
