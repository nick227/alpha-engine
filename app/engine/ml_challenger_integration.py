from __future__ import annotations

import json
import os
from typing import Any

from app.db.repository import AlphaRepository
from app.engine.meta_ranker_runner import run_meta_ranker_shadow

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
        SELECT metric_5d_return, win_rate, drawdown, metadata_json
        FROM experiment_results
        WHERE tenant_id = ?
          AND class_key = ?
          AND experiment_key = ?
          AND created_at >= datetime('now', '-' || ? || ' days')
        ORDER BY created_at ASC
        """,
        (str(tenant_id), ML_CHALLENGER_CLASS_KEY, str(experiment_key), int(window_days)),
    ).fetchall()
    ml_returns: list[float] = []
    ml_wins: list[float] = []
    ml_dd: list[float] = []
    for r in ml_rows:
        md = _load_json_dict(r["metadata_json"])
        realized = md.get("realized") if isinstance(md, dict) else {}
        h5 = realized.get("h5") if isinstance(realized, dict) else {}
        sample_n = int(h5.get("sample_count") or 0) if isinstance(h5, dict) else 0
        if sample_n < int(ML_PROMOTION_MIN_REALIZED_SAMPLES):
            continue
        ret = h5.get("avg_return")
        win = h5.get("win_rate")
        dd = h5.get("drawdown")
        if ret is not None:
            ml_returns.append(float(ret))
        if win is not None:
            ml_wins.append(float(win))
        if dd is not None:
            ml_dd.append(float(dd))

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
    )
    if not has_required:
        return {
            "promoted": False,
            "reason": "insufficient_realized_metrics",
            "window_days": int(window_days),
            "min_realized_samples": int(ML_PROMOTION_MIN_REALIZED_SAMPLES),
            "metrics": metrics,
        }

    passed = (
        float(ch["sharpe"]) > float(bs["sharpe"]) + float(ML_PROMOTION_SHARPE_DELTA)
        and float(ch["drawdown"]) < float(bs["drawdown"]) + float(ML_PROMOTION_DRAWDOWN_DELTA)
        and float(ch["win_rate"]) > float(bs["win_rate"]) + float(ML_PROMOTION_WINRATE_DELTA)
    )
    if passed:
        repo.set_experiment_status(
            class_key=ML_CHALLENGER_CLASS_KEY,
            experiment_key=str(experiment_key),
            status="candidate_production_model",
            tenant_id=str(tenant_id),
        )
    return {
        "promoted": bool(passed),
        "reason": ("passed_thresholds" if passed else "thresholds_not_met"),
        "window_days": int(window_days),
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

    feature_attribution = _compute_feature_attribution(rows)
    realized = _compute_realized_metrics(
        repo=repo,
        tenant_id=str(tenant_id),
        as_of_date=str(as_of_date),
        cohort_symbols=cohort_symbols,
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
        "realized": realized,
        "filters": dict(shadow.get("dropped") or {}),
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
