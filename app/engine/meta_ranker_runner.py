from __future__ import annotations

import json
import os
from typing import Any

from app.db.repository import AlphaRepository
from app.engine.meta_ranker_data_quality import evaluate_meta_ranker_data_quality
from app.engine.meta_ranker_features import build_meta_ranker_feature_rows
from app.engine.meta_ranker_fusion import combine_meta_ranker_score
from app.engine.meta_ranker_model import predict_meta_ranker_probs

META_RANKER_TOP_N = int(os.getenv("META_RANKER_TOP_N", "50"))


def _load_json_dict(raw: Any) -> dict[str, Any]:
    try:
        payload = json.loads(str(raw or "{}"))
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def run_meta_ranker_shadow(
    *,
    repo: AlphaRepository,
    as_of_date: str,
    tenant_id: str,
    experiment_class: str,
    experiment_key: str,
) -> dict[str, Any]:
    rows, dropped = build_meta_ranker_feature_rows(
        repo=repo,
        as_of_date=str(as_of_date),
        tenant_id=str(tenant_id),
    )
    if not rows:
        return {
            "updated_rows": [],
            "selected_symbols": [],
            "scores": [],
            "dropped": dropped,
            "quality": evaluate_meta_ranker_data_quality([]),
        }

    quality = evaluate_meta_ranker_data_quality(rows)

    updates: list[dict[str, Any]] = []
    scored_rows: list[dict[str, Any]] = []
    for row in rows:
        p_out, p_fail = predict_meta_ranker_probs(row)
        fused = combine_meta_ranker_score(row=row, p_outperform=p_out, p_fail=p_fail)
        final_score = float(fused["final_rank_score"])
        md = dict(_load_json_dict(row.get("metadata")))
        md["ml_challenger"] = {
            "experiment_class": str(experiment_class),
            "experiment_key": str(experiment_key),
            "mode": "parallel_challenger",
            "non_replacing": True,
            "as_of_date": str(as_of_date),
            "base_score": float(row.get("base_score") or 0.0),
            "p_outperform": float(p_out),
            "p_fail": float(p_fail),
            "final_rank_score": final_score,
            "penalties": fused["penalties"],
            "feature_version": "meta_ranker_features_v1",
            "normalization_version": "batch_norm_v1",
        }
        updates.append(
            {
                "as_of_date": str(row["as_of_date"]),
                "symbol": str(row["symbol"]),
                "source": str(row["source"]),
                "metadata_json": json.dumps(md, sort_keys=True),
            }
        )
        scored_rows.append(
            {
                "symbol": str(row["symbol"]),
                "final_rank_score": final_score,
            }
        )

    repo.update_prediction_queue_metadata_many(rows=updates, tenant_id=str(tenant_id))
    scored_rows.sort(key=lambda r: -float(r["final_rank_score"]))
    selected = [str(r["symbol"]) for r in scored_rows[: max(1, int(META_RANKER_TOP_N))]]
    return {
        "updated_rows": updates,
        "selected_symbols": selected,
        "scores": [float(r["final_rank_score"]) for r in scored_rows],
        "dropped": dropped,
        "quality": quality,
    }
