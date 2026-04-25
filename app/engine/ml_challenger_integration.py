from __future__ import annotations

import json
import os
import random
from typing import Any

from app.db.repository import AlphaRepository
from app.engine.alt_data_ingest import ingest_alt_data_snapshot
from app.engine.meta_ranker_runner import run_meta_ranker_shadow
from app.engine.meta_ranker_trade_intents import build_and_store_trade_intents

ML_CHALLENGER_ENABLED = str(os.getenv("ML_CHALLENGER_ENABLED", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
ML_CHALLENGER_EXPERIMENT_KEY = str(os.getenv("ML_CHALLENGER_EXPERIMENT_KEY", "ml_meta_ranker_v1")).strip()
ML_CHALLENGER_CLASS_KEY = "ml_model"
ML_PROMOTION_ENABLED = str(os.getenv("ML_PROMOTION_ENABLED", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
ML_PROMOTION_WINDOW_DAYS = int(os.getenv("ML_PROMOTION_WINDOW_DAYS", "30"))
ML_PROMOTION_SHARPE_DELTA = float(os.getenv("ML_PROMOTION_SHARPE_DELTA", "0.10"))
ML_PROMOTION_WINRATE_DELTA = float(os.getenv("ML_PROMOTION_WINRATE_DELTA", "0.00"))
ML_PROMOTION_DRAWDOWN_DELTA = float(os.getenv("ML_PROMOTION_DRAWDOWN_DELTA", "0.00"))
ML_PROMOTION_MIN_REALIZED_SAMPLES = int(os.getenv("ML_PROMOTION_MIN_REALIZED_SAMPLES", "25"))
ML_PROMOTION_SIGNIFICANCE_ENABLED = str(os.getenv("ML_PROMOTION_SIGNIFICANCE_ENABLED", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
ML_PROMOTION_BOOTSTRAP_ITERATIONS = int(os.getenv("ML_PROMOTION_BOOTSTRAP_ITERATIONS", "500"))
ML_PROMOTION_MIN_EFFECT_RETURN = float(os.getenv("ML_PROMOTION_MIN_EFFECT_RETURN", "0.0025"))
ML_PROMOTION_MIN_EFFECT_WINRATE = float(os.getenv("ML_PROMOTION_MIN_EFFECT_WINRATE", "0.01"))


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _load_json_dict(raw: Any) -> dict[str, Any]:
    try:
        payload = json.loads(str(raw or "{}"))
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _safe_float(v: Any, fallback: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(fallback)


def _compute_feature_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"top_features": [], "weights": {}}
    weights = {
        "avg_score": 0.70,
        "claim_count": 0.07,
        "overlap_count": 0.04,
        "days_seen": 0.02,
    }
    sums = {k: 0.0 for k in weights}
    weighted_sums = {k: 0.0 for k in weights}
    n = float(len(rows))
    for r in rows:
        md = _load_json_dict(r.get("metadata_json"))
        raw_vals = {
            "avg_score": _safe_float(md.get("avg_score", md.get("raw_score", 0.0))),
            "claim_count": min(_safe_float(md.get("claim_count", 1.0), 1.0), 5.0),
            "overlap_count": _safe_float(md.get("overlap_count", 1.0), 1.0),
            "days_seen": min(_safe_float(md.get("days_seen", 1.0), 1.0), 10.0),
        }
        for key, val in raw_vals.items():
            sums[key] += val
            weighted_sums[key] += val * weights[key]

    top_features = []
    for key, weight in weights.items():
        avg_raw = sums[key] / n
        avg_weighted = weighted_sums[key] / n
        top_features.append(
            {
                "feature": key,
                "avg_value": round(avg_raw, 6),
                "weight": round(weight, 4),
                "avg_weighted_contribution": round(avg_weighted, 6),
            }
        )
    top_features.sort(key=lambda x: -float(x["avg_weighted_contribution"]))
    return {
        "top_features": top_features,
        "weights": weights,
    }


def _sharpe_from_series(vals: list[float]) -> float | None:
    if len(vals) < 2:
        return None
    mean_val = sum(vals) / len(vals)
    var = sum((x - mean_val) ** 2 for x in vals) / max(1, len(vals) - 1)
    std = var ** 0.5
    if std <= 1e-12:
        return None
    return mean_val / std


def _bootstrap_delta_ci(
    *,
    a: list[float],
    b: list[float],
    iters: int = ML_PROMOTION_BOOTSTRAP_ITERATIONS,
    seed: int = 7,
) -> tuple[float, float] | None:
    if not a or not b:
        return None
    rng = random.Random(int(seed))
    deltas: list[float] = []
    n_a = len(a)
    n_b = len(b)
    for _ in range(max(100, int(iters))):
        sample_a = [a[rng.randrange(0, n_a)] for _ in range(n_a)]
        sample_b = [b[rng.randrange(0, n_b)] for _ in range(n_b)]
        deltas.append((sum(sample_a) / n_a) - (sum(sample_b) / n_b))
    deltas.sort()
    lo_idx = max(0, int(0.025 * len(deltas)) - 1)
    hi_idx = min(len(deltas) - 1, int(0.975 * len(deltas)) - 1)
    return float(deltas[lo_idx]), float(deltas[hi_idx])


def _wilson_ci(p: float, n: int, z: float = 1.96) -> tuple[float, float] | None:
    if n <= 0:
        return None
    p = max(0.0, min(1.0, float(p)))
    denom = 1.0 + (z * z) / n
    center = (p + ((z * z) / (2.0 * n))) / denom
    margin = (z / denom) * ((p * (1.0 - p) / n) + ((z * z) / (4.0 * n * n))) ** 0.5
    return float(center - margin), float(center + margin)


def _evaluate_significance(
    *,
    challenger_returns: list[float],
    baseline_returns: list[float],
    challenger_win_rate: float | None,
    baseline_win_rate: float | None,
    challenger_n: int,
    baseline_n: int,
) -> dict[str, Any]:
    if not ML_PROMOTION_SIGNIFICANCE_ENABLED:
        return {"enabled": False, "passed": True, "reason": "significance_disabled"}

    if not challenger_returns or not baseline_returns:
        return {"enabled": True, "passed": False, "reason": "insufficient_return_samples"}

    ret_ci = _bootstrap_delta_ci(a=challenger_returns, b=baseline_returns)
    if ret_ci is None:
        return {"enabled": True, "passed": False, "reason": "return_ci_unavailable"}
    ret_delta = (sum(challenger_returns) / len(challenger_returns)) - (sum(baseline_returns) / len(baseline_returns))

    if challenger_win_rate is None or baseline_win_rate is None:
        return {"enabled": True, "passed": False, "reason": "insufficient_winrate_samples"}

    win_delta = float(challenger_win_rate) - float(baseline_win_rate)
    win_ci_ch = _wilson_ci(float(challenger_win_rate), int(challenger_n))
    win_ci_bs = _wilson_ci(float(baseline_win_rate), int(baseline_n))
    if win_ci_ch is None or win_ci_bs is None:
        return {"enabled": True, "passed": False, "reason": "winrate_ci_unavailable"}
    win_delta_ci = (float(win_ci_ch[0] - win_ci_bs[1]), float(win_ci_ch[1] - win_ci_bs[0]))

    ret_sig = float(ret_ci[0]) > 0.0
    win_sig = float(win_delta_ci[0]) > 0.0
    ret_effect = float(ret_delta) >= float(ML_PROMOTION_MIN_EFFECT_RETURN)
    win_effect = float(win_delta) >= float(ML_PROMOTION_MIN_EFFECT_WINRATE)

    passed = bool(ret_sig and win_sig and ret_effect and win_effect)
    return {
        "enabled": True,
        "passed": passed,
        "reason": ("passed" if passed else "not_significant_or_small_effect"),
        "return_delta": float(ret_delta),
        "return_delta_ci": [float(ret_ci[0]), float(ret_ci[1])],
        "winrate_delta": float(win_delta),
        "winrate_delta_ci": [float(win_delta_ci[0]), float(win_delta_ci[1])],
        "checks": {
            "return_significant": bool(ret_sig),
            "winrate_significant": bool(win_sig),
            "return_effect": bool(ret_effect),
            "winrate_effect": bool(win_effect),
        },
        "thresholds": {
            "min_effect_return": float(ML_PROMOTION_MIN_EFFECT_RETURN),
            "min_effect_winrate": float(ML_PROMOTION_MIN_EFFECT_WINRATE),
            "bootstrap_iterations": int(ML_PROMOTION_BOOTSTRAP_ITERATIONS),
        },
    }


def _evaluate_promotion(
    *,
    repo: AlphaRepository,
    tenant_id: str,
    experiment_key: str,
    window_days: int = ML_PROMOTION_WINDOW_DAYS,
) -> dict[str, Any]:
    """
    Automatic promotion rule:
    promote when challenger beats baseline over trailing window:
    - sharpe > baseline + delta
    - drawdown < baseline + drawdown_delta
    - winrate > baseline + delta
    """
    ml_rows = repo.conn.execute(
        """
        SELECT return_pct, is_win
        FROM experiment_realized_labels
        WHERE tenant_id = ?
          AND class_key = ?
          AND experiment_key = ?
          AND horizon_days = 5
          AND as_of_date >= date('now', '-' || ? || ' days')
        ORDER BY as_of_date ASC
        """,
        (str(tenant_id), ML_CHALLENGER_CLASS_KEY, str(experiment_key), int(window_days)),
    ).fetchall()
    ml_returns = [float(r["return_pct"]) for r in ml_rows if r["return_pct"] is not None]
    ml_wins = [float(r["is_win"]) for r in ml_rows if r["is_win"] is not None]
    ml_dd = [float(x) for x in ml_returns]

    baseline_rows = repo.conn.execute(
        """
        SELECT return_pct
        FROM discovery_outcomes
        WHERE tenant_id = ?
          AND horizon_days = 5
          AND watchlist_date >= date('now', '-' || ? || ' days')
          AND return_pct IS NOT NULL
        ORDER BY watchlist_date ASC
        """,
        (str(tenant_id), int(window_days)),
    ).fetchall()
    base_returns = [float(r["return_pct"]) for r in baseline_rows if r["return_pct"] is not None]
    base_wins = [1.0 if x > 0.0 else 0.0 for x in base_returns]
    base_dd = [float(x) for x in base_returns]

    metrics = {
        "challenger": {
            "sample_count": len(ml_returns),
            "sharpe": _sharpe_from_series(ml_returns),
            "win_rate": (sum(ml_wins) / len(ml_wins)) if ml_wins else None,
            "drawdown": (sum(ml_dd) / len(ml_dd)) if ml_dd else None,
        },
        "baseline": {
            "sample_count": len(baseline_rows),
            "sharpe": _sharpe_from_series(base_returns),
            "win_rate": (sum(base_wins) / len(base_wins)) if base_wins else None,
            "drawdown": (sum(base_dd) / len(base_dd)) if base_dd else None,
        },
    }
    ch = metrics["challenger"]
    bs = metrics["baseline"]
    has_required = all(
        ch.get(k) is not None and bs.get(k) is not None
        for k in ("sharpe", "win_rate", "drawdown")
    ) and len(ml_returns) >= int(ML_PROMOTION_MIN_REALIZED_SAMPLES)
    if not has_required:
        return {
            "promoted": False,
            "reason": "insufficient_realized_metrics",
            "window_days": int(window_days),
            "min_realized_samples": int(ML_PROMOTION_MIN_REALIZED_SAMPLES),
            "metrics": metrics,
        }

    threshold_passed = (
        float(ch["sharpe"]) > float(bs["sharpe"]) + float(ML_PROMOTION_SHARPE_DELTA)
        and float(ch["drawdown"]) < float(bs["drawdown"]) + float(ML_PROMOTION_DRAWDOWN_DELTA)
        and float(ch["win_rate"]) > float(bs["win_rate"]) + float(ML_PROMOTION_WINRATE_DELTA)
    )
    significance = _evaluate_significance(
        challenger_returns=ml_returns,
        baseline_returns=base_returns,
        challenger_win_rate=ch.get("win_rate"),
        baseline_win_rate=bs.get("win_rate"),
        challenger_n=len(ml_returns),
        baseline_n=len(base_returns),
    )
    promoted = bool(threshold_passed and bool(significance.get("passed", False)))
    if promoted:
        repo.set_experiment_status(
            class_key=ML_CHALLENGER_CLASS_KEY,
            experiment_key=str(experiment_key),
            status="candidate_production_model",
            tenant_id=str(tenant_id),
        )
    return {
        "promoted": promoted,
        "reason": (
            "passed_thresholds_and_significance"
            if promoted
            else ("thresholds_not_met" if not threshold_passed else "significance_not_met")
        ),
        "window_days": int(window_days),
        "threshold_passed": bool(threshold_passed),
        "significance": significance,
        "thresholds": {
            "sharpe_delta": float(ML_PROMOTION_SHARPE_DELTA),
            "drawdown_delta": float(ML_PROMOTION_DRAWDOWN_DELTA),
            "winrate_delta": float(ML_PROMOTION_WINRATE_DELTA),
        },
        "min_realized_samples": int(ML_PROMOTION_MIN_REALIZED_SAMPLES),
        "metrics": metrics,
    }


def _compute_realized_metrics(
    *,
    repo: AlphaRepository,
    tenant_id: str,
    as_of_date: str,
    cohort_symbols: list[str],
) -> dict[str, Any]:
    syms = sorted({str(s).strip().upper() for s in cohort_symbols if str(s).strip()})
    if not syms:
        return {"h5": {}, "h20": {}}
    placeholders = ",".join(["?"] * len(syms))
    rows = repo.conn.execute(
        f"""
        SELECT horizon_days, return_pct
        FROM discovery_outcomes
        WHERE tenant_id = ?
          AND watchlist_date = ?
          AND symbol IN ({placeholders})
          AND horizon_days IN (5, 20)
          AND return_pct IS NOT NULL
        """,
        (str(tenant_id), str(as_of_date), *syms),
    ).fetchall()

    def summarize(vals: list[float]) -> dict[str, Any]:
        if not vals:
            return {"sample_count": 0, "avg_return": None, "win_rate": None, "drawdown": None, "sharpe": None}
        wins = [1.0 if v > 0.0 else 0.0 for v in vals]
        return {
            "sample_count": len(vals),
            "avg_return": (sum(vals) / len(vals)),
            "win_rate": (sum(wins) / len(wins)),
            "drawdown": min(vals),
            "sharpe": _sharpe_from_series(vals),
        }

    h5_vals = [float(r["return_pct"]) for r in rows if int(r["horizon_days"]) == 5]
    h20_vals = [float(r["return_pct"]) for r in rows if int(r["horizon_days"]) == 20]
    return {
        "h5": summarize(h5_vals),
        "h20": summarize(h20_vals),
    }


def _compute_realized_metrics_from_labels(
    *,
    repo: AlphaRepository,
    run_id: str,
    tenant_id: str,
    experiment_key: str,
) -> dict[str, Any]:
    rows = repo.conn.execute(
        """
        SELECT horizon_days, return_pct, is_win
        FROM experiment_realized_labels
        WHERE tenant_id = ? AND run_id = ? AND class_key = ? AND experiment_key = ?
        """,
        (str(tenant_id), str(run_id), ML_CHALLENGER_CLASS_KEY, str(experiment_key)),
    ).fetchall()
    h5_vals = [float(r["return_pct"]) for r in rows if int(r["horizon_days"]) == 5]
    h20_vals = [float(r["return_pct"]) for r in rows if int(r["horizon_days"]) == 20]

    def summarize(vals: list[float]) -> dict[str, Any]:
        if not vals:
            return {"sample_count": 0, "avg_return": None, "win_rate": None, "drawdown": None, "sharpe": None}
        wins = [1.0 if v > 0.0 else 0.0 for v in vals]
        return {
            "sample_count": len(vals),
            "avg_return": (sum(vals) / len(vals)),
            "win_rate": (sum(wins) / len(wins)),
            "drawdown": min(vals),
            "sharpe": _sharpe_from_series(vals),
        }

    return {
        "h5": summarize(h5_vals),
        "h20": summarize(h20_vals),
    }


def run_ml_challenger_meta_ranker(
    *,
    repo: AlphaRepository,
    as_of_date: str,
    tenant_id: str = "default",
    experiment_key: str = ML_CHALLENGER_EXPERIMENT_KEY,
) -> dict[str, Any]:
    if not ML_CHALLENGER_ENABLED:
        return {"enabled": False, "updated": 0}

    repo.upsert_experiment_class(
        class_key=ML_CHALLENGER_CLASS_KEY,
        display_name="ML Model",
        description="Machine-learning challenger family running parallel to deterministic strategies.",
        active=True,
        tenant_id=str(tenant_id),
    )
    repo.upsert_experiment(
        class_key=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
        display_name="ML Meta Ranker V1",
        status="sandbox",
        version="v1",
        config_json=json.dumps({"mode": "parallel_challenger", "writes": ["prediction_queue.metadata_json"]}, sort_keys=True),
        metadata_json=json.dumps({"non_replacing": True}, sort_keys=True),
        active=True,
        tenant_id=str(tenant_id),
    )

    rows = repo.list_prediction_queue(
        as_of_date=str(as_of_date),
        status="pending",
        limit=5000,
        tenant_id=str(tenant_id),
    )
    pre_symbols = [str(r.get("symbol") or "").strip().upper() for r in rows if str(r.get("symbol") or "").strip()]
    alt_data_summary = ingest_alt_data_snapshot(
        repo=repo,
        as_of_date=str(as_of_date),
        tenant_id=str(tenant_id),
        symbols=pre_symbols,
        source="proxy_free",
    )
    run_id = repo.start_experiment_run(
        class_key=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
        as_of_date=str(as_of_date),
        metadata_json=json.dumps({"input_rows": len(rows)}, sort_keys=True),
        tenant_id=str(tenant_id),
    )

    shadow = run_meta_ranker_shadow(
        repo=repo,
        as_of_date=str(as_of_date),
        tenant_id=str(tenant_id),
        experiment_class=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
    )
    updated_rows = list(shadow.get("updated_rows") or [])
    scores = [float(x) for x in (shadow.get("scores") or [])]
    cohort_symbols = [str(s) for s in (shadow.get("selected_symbols") or [])]
    cohort_count = repo.insert_experiment_cohort_items(
        run_id=str(run_id),
        class_key=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
        as_of_date=str(as_of_date),
        symbols=cohort_symbols,
        tenant_id=str(tenant_id),
    )
    intent_summary = build_and_store_trade_intents(
        repo=repo,
        run_id=str(run_id),
        class_key=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
        as_of_date=str(as_of_date),
        tenant_id=str(tenant_id),
        selected_symbols=cohort_symbols,
        score_map=dict(shadow.get("score_details") or {}),
        horizons=(5, 20),
    )
    labels_inserted = repo.refresh_experiment_realized_labels_for_run(
        run_id=str(run_id),
        class_key=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
        tenant_id=str(tenant_id),
    )

    feature_attribution = _compute_feature_attribution(rows)
    realized = _compute_realized_metrics_from_labels(
        repo=repo,
        run_id=str(run_id),
        tenant_id=str(tenant_id),
        experiment_key=str(experiment_key),
    )
    avg_score = (sum(scores) / len(scores)) if scores else None
    metric_5d = realized.get("h5", {}).get("avg_return")
    metric_20d = realized.get("h20", {}).get("avg_return")
    win_rate = realized.get("h5", {}).get("win_rate")
    drawdown = realized.get("h5", {}).get("drawdown")
    result_meta = {
        "updated_rows": len(updated_rows),
        "avg_challenger_score": avg_score,
        "non_replacing": True,
        "feature_attribution": feature_attribution,
        "cohort": {
            "as_of_date": str(as_of_date),
            "symbol_count": int(cohort_count),
        },
        "trade_intents": intent_summary,
        "labels": {
            "inserted": int(labels_inserted),
        },
        "realized": realized,
        "filters": dict(shadow.get("dropped") or {}),
        "alt_data": dict(shadow.get("alt_data") or {}),
        "alt_data_ingest": alt_data_summary,
        "data_quality": dict(shadow.get("quality") or {}),
    }
    quality_passed = bool((result_meta.get("data_quality") or {}).get("passed", True))
    promotion = (
        _evaluate_promotion(
            repo=repo,
            tenant_id=str(tenant_id),
            experiment_key=str(experiment_key),
            window_days=int(ML_PROMOTION_WINDOW_DAYS),
        )
        if ML_PROMOTION_ENABLED and quality_passed
        else {
            "promoted": False,
            "reason": ("promotion_disabled" if not ML_PROMOTION_ENABLED else "data_quality_gate_failed"),
            "data_quality_passed": bool(quality_passed),
        }
    )
    result_meta["promotion"] = promotion
    repo.insert_experiment_result(
        run_id=str(run_id),
        class_key=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
        metric_5d_return=metric_5d,
        metric_20d_return=metric_20d,
        win_rate=win_rate,
        drawdown=drawdown,
        turnover=None,
        regime_json=json.dumps({}),
        calibration_json=json.dumps({"avg_challenger_score": avg_score}),
        overlap_json=json.dumps({}),
        metadata_json=json.dumps(result_meta, sort_keys=True),
        tenant_id=str(tenant_id),
    )
    repo.finish_experiment_run(
        run_id=str(run_id),
        status="success",
        metadata_json=json.dumps(result_meta, sort_keys=True),
        tenant_id=str(tenant_id),
    )
    return {
        "enabled": True,
        "experiment_key": str(experiment_key),
        "run_id": str(run_id),
        "updated": len(updated_rows),
        "avg_score": avg_score,
        "promotion": promotion,
        "feature_attribution": feature_attribution,
    }
