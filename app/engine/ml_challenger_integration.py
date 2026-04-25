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

    avg_score = (sum(scores) / len(scores)) if scores else None
    result_meta = {
        "updated_rows": len(updated_rows),
        "avg_challenger_score": avg_score,
        "non_replacing": True,
    }
    repo.insert_experiment_result(
        run_id=str(run_id),
        class_key=ML_CHALLENGER_CLASS_KEY,
        experiment_key=str(experiment_key),
        win_rate=None,
        drawdown=None,
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
    }
