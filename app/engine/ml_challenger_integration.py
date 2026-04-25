from __future__ import annotations

import json
import os
from typing import Any

from app.db.repository import AlphaRepository

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


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _load_json_dict(raw: Any) -> dict[str, Any]:
    try:
        payload = json.loads(str(raw or "{}"))
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _score_row(md: dict[str, Any]) -> float:
    avg_score = float(md.get("avg_score") or md.get("raw_score") or 0.0)
    claim_count = float(md.get("claim_count") or 1.0)
    overlap = float(md.get("overlap_count") or 1.0)
    days_seen = float(md.get("days_seen") or 1.0)
    # Lightweight challenger score: blends existing deterministic evidence.
    return _clamp((avg_score * 0.7) + (min(claim_count, 5.0) * 0.07) + (overlap * 0.04) + (min(days_seen, 10.0) * 0.02), 0.0, 1.0)


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
        SELECT metric_5d_return, win_rate, drawdown
        FROM experiment_results
        WHERE tenant_id = ?
          AND class_key = ?
          AND experiment_key = ?
          AND created_at >= datetime('now', '-' || ? || ' days')
          AND metric_5d_return IS NOT NULL
        ORDER BY created_at ASC
        """,
        (str(tenant_id), ML_CHALLENGER_CLASS_KEY, str(experiment_key), int(window_days)),
    ).fetchall()
    ml_returns = [float(r["metric_5d_return"]) for r in ml_rows if r["metric_5d_return"] is not None]
    ml_wins = [float(r["win_rate"]) for r in ml_rows if r["win_rate"] is not None]
    ml_dd = [float(r["drawdown"]) for r in ml_rows if r["drawdown"] is not None]

    baseline_rows = repo.conn.execute(
        """
        SELECT total_return_actual, direction_hit_rate, magnitude_error
        FROM prediction_scores
        WHERE tenant_id = ?
          AND forecast_days = 5
          AND created_at >= datetime('now', '-' || ? || ' days')
        ORDER BY created_at ASC
        """,
        (str(tenant_id), int(window_days)),
    ).fetchall()
    base_returns = [float(r["total_return_actual"]) for r in baseline_rows if r["total_return_actual"] is not None]
    base_wins = [float(r["direction_hit_rate"]) for r in baseline_rows if r["direction_hit_rate"] is not None]
    base_dd = [float(r["magnitude_error"]) for r in baseline_rows if r["magnitude_error"] is not None]

    metrics = {
        "challenger": {
            "sample_count": len(ml_rows),
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
            "reason": "insufficient_metrics",
            "window_days": int(window_days),
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
        "metrics": metrics,
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

    updated_rows: list[dict[str, Any]] = []
    scores: list[float] = []
    for r in rows:
        md = _load_json_dict(r.get("metadata_json"))
        score = _score_row(md)
        scores.append(score)
        ml_block = {
            "experiment_class": ML_CHALLENGER_CLASS_KEY,
            "experiment_key": str(experiment_key),
            "score": score,
            "mode": "parallel_challenger",
            "non_replacing": True,
            "as_of_date": str(as_of_date),
        }
        md["ml_challenger"] = ml_block
        updated_rows.append(
            {
                "as_of_date": str(r["as_of_date"]),
                "symbol": str(r["symbol"]),
                "source": str(r.get("source") or "discovery"),
                "metadata_json": json.dumps(md, sort_keys=True),
            }
        )

    if updated_rows:
        repo.update_prediction_queue_metadata_many(rows=updated_rows, tenant_id=str(tenant_id))

    feature_attribution = _compute_feature_attribution(rows)
    avg_score = (sum(scores) / len(scores)) if scores else None
    proxy_ret = ((avg_score - 0.5) * 0.1) if avg_score is not None else None
    proxy_win_rate = avg_score if avg_score is not None else None
    proxy_drawdown = (1.0 - avg_score) if avg_score is not None else None
    result_meta = {
        "updated_rows": len(updated_rows),
        "avg_challenger_score": avg_score,
        "non_replacing": True,
        "feature_attribution": feature_attribution,
    }
    promotion = (
        _evaluate_promotion(
            repo=repo,
            tenant_id=str(tenant_id),
            experiment_key=str(experiment_key),
            window_days=int(ML_PROMOTION_WINDOW_DAYS),
        )
        if ML_PROMOTION_ENABLED
        else {"promoted": False, "reason": "promotion_disabled"}
    )
    result_meta["promotion"] = promotion
    repo.insert_experiment_result(
        run_id=str(run_id),
        class_key=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
        metric_5d_return=proxy_ret,
        metric_20d_return=proxy_ret,
        win_rate=proxy_win_rate,
        drawdown=proxy_drawdown,
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
