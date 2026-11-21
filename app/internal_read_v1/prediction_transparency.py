"""Read-only, provenance-aware prediction evaluation and model evidence.

All metrics come from persisted, non-quarantined observations. No synthetic
probabilities, unverified SPY comparisons, or reconstructed model attributions.
"""
from __future__ import annotations

import json
import math
import sqlite3
from collections import defaultdict
from typing import Any

from app.internal_read_v1.intelligence_read import get_prediction_context


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute("PRAGMA table_info(" + table + ")")}
    except sqlite3.Error:
        return set()


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _round(value: float | None, places: int = 6) -> float | None:
    return round(value, places) if value is not None else None


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def prediction_scoreboard(
    conn: sqlite3.Connection, *, tenant_id: str = "default",
    ticker: str | None = None, horizon: str | None = None,
    limit: int = 5000,
) -> dict[str, Any]:
    """Compare observed outcomes by strategy and horizon without look-ahead claims.

    Rows with invalid or unverified outcomes, missing quarantine metadata, or
    ambiguous directions do not contribute to accuracy. The always-up baseline
    uses the *same* observed sample; it is not SPY or prospective performance.
    """
    pcols = _columns(conn, "predictions")
    ocols = _columns(conn, "prediction_outcomes")
    required_p = {"id", "tenant_id", "strategy_id", "ticker", "timestamp", "prediction", "horizon"}
    required_o = {"id", "tenant_id", "prediction_id", "evaluated_at", "is_quarantined", "direction_correct"}
    return_col = "actual_return" if "actual_return" in ocols else "return_pct" if "return_pct" in ocols else None
    result: dict[str, Any] = {
        "tenant_id": tenant_id, "status": "unavailable", "reason": None,
        "methodology": {
            "source": "prediction_outcomes_joined_to_predictions",
            "quarantinedExcluded": True,
            "deduplication": "latest_evaluated_outcome_per_prediction",
            "benchmark": "always_up_on_same_observed_sample",
            "benchmarkIsProspective": False,
            "outOfSampleVerified": False,
            "returnUnits": "fraction",
            "isInvestmentAdvice": False,
        },
        "sampleCount": 0, "groups": [],
    }
    if not required_p <= pcols or not required_o <= ocols or return_col is None:
        result["reason"] = "required_outcome_schema_unavailable"
        return result

    filters = ["p.tenant_id = ?", "o.tenant_id = ?", "COALESCE(o.is_quarantined, 1) = 0",
               "julianday(o.evaluated_at) >= julianday(p.timestamp)"]
    args: list[Any] = [tenant_id, tenant_id]
    if ticker:
        filters.append("UPPER(p.ticker) = ?")
        args.append(ticker.strip().upper())
    if horizon:
        filters.append("p.horizon = ?")
        args.append(horizon)
    # The bounded recent sample avoids scanning the entire outcome history in a
    # web request and is explicitly reported; results are not lifetime metrics.
    sql = f"""
        SELECT p.id, p.strategy_id, p.ticker, p.horizon, p.prediction,
               o.direction_correct, o.{return_col} AS realized_return,
               o.evaluated_at, o.id AS outcome_id
        FROM predictions p
        JOIN prediction_outcomes o
          ON o.tenant_id = p.tenant_id AND o.prediction_id = p.id
        WHERE {" AND ".join(filters)}
        ORDER BY o.evaluated_at DESC, o.id DESC
        LIMIT ?
    """
    requested = max(1, min(int(limit), 10000))
    args.append(requested + 1)
    try:
        rows = conn.execute(sql, args).fetchall()
    except sqlite3.Error:
        result["reason"] = "outcome_query_unavailable"
        return result
    truncated = len(rows) > requested
    rows = rows[:requested]
    seen: set[str] = set()
    grouped: dict[tuple[str, str], list[tuple[bool, float]]] = defaultdict(list)
    for row in rows:
        pred_id = str(row["id"])
        if pred_id in seen:
            continue
        seen.add(pred_id)
        direction = str(row["prediction"] or "").lower()
        if direction not in {"up", "down", "buy", "sell"}:
            continue
        value = _number(row["realized_return"])
        if value is None or row["direction_correct"] not in (0, 1):
            continue
        key = (str(row["strategy_id"] or "unknown"), str(row["horizon"] or "unknown"))
        grouped[key].append((bool(row["direction_correct"]), value))

    items = []
    for (strategy, period), observations in sorted(grouped.items()):
        n = len(observations)
        accuracy = sum(correct for correct, _ in observations) / n
        returns = [ret for _, ret in observations]
        baseline = sum(ret > 0 for ret in returns) / n
        items.append({
            "strategyId": strategy, "horizon": period, "sampleCount": n,
            "directionalAccuracy": _round(accuracy),
            "alwaysUpBaselineAccuracy": _round(baseline),
            "accuracyDifferenceFromBaseline": _round(accuracy - baseline),
            "averageUnderlyingReturn": _round(_mean(returns)),
            "validationStatus": "observed_not_verified_out_of_sample",
        })
    result.update({
        "status": "available" if items else "unavailable",
        "reason": None if items else "no_verified_observed_outcomes",
        "sampleCount": sum(item["sampleCount"] for item in items),
        "groups": items, "truncated": truncated,
        "maxRecentOutcomeRows": requested,
    })
    return result


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def prediction_evidence(
    conn: sqlite3.Connection, *, tenant_id: str, prediction_id: str
) -> dict[str, Any] | None:
    """Return original prediction context plus *linked* model evidence only.

    Never attach a current model to an old prediction based solely on horizon.
    Coefficients/feature importance are not presented as local contributions.
    """
    context = get_prediction_context(conn, tenant_id=tenant_id, prediction_id=prediction_id)
    if context is None:
        return None
    features = context.get("featureSnapshot") or {}
    model_id = features.get("model_id") or features.get("modelId")
    model: dict[str, Any] | None = None
    status = "unlinked"
    if model_id and {"model_id", "tenant_id", "horizon", "train_start", "train_end",
                     "model_accuracy", "baseline_accuracy", "ic_score", "passed_gate"} <= _columns(conn, "ml_models"):
        try:
            row = conn.execute(
                """SELECT model_id, train_start, train_end, horizon, model_accuracy,
                          baseline_accuracy, ic_score, passed_gate, train_rows,
                          feature_importance_json
                   FROM ml_models WHERE tenant_id = ? AND model_id = ? LIMIT 1""",
                (tenant_id, str(model_id)),
            ).fetchone()
            if row is not None and str(row["horizon"]) == str(context["horizon"]) and str(row["train_end"]) <= str(context["timestamp"])[:10]:
                model = {
                    "modelId": str(row["model_id"]), "modelFamily": "ridge_regression",
                    "trainStart": row["train_start"], "trainEnd": row["train_end"],
                    "horizon": row["horizon"], "validationAccuracy": _number(row["model_accuracy"]),
                    "baselineAccuracy": _number(row["baseline_accuracy"]),
                    "informationCoefficient": _number(row["ic_score"]),
                    "trainingRows": row["train_rows"],
                    "passedTrainingGate": bool(row["passed_gate"]),
                    "globalFeatureImportance": _object(row["feature_importance_json"]),
                    "localFeatureContributions": None,
                }
                status = "linked"
        except sqlite3.Error:
            status = "unavailable"
    return {
        "predictionId": prediction_id, "ticker": context["ticker"],
        "timestamp": context["timestamp"], "horizon": context["horizon"],
        "prediction": context["prediction"], "strategyId": context["strategyId"],
        "sourceContext": context,
        "modelEvidenceStatus": status,
        "modelEvidence": model,
        "confidenceMethodology": {
            "method": "strategy_defined_uncalibrated",
            "isProbability": False,
            "successProbability": None,
        },
        "explanation": {
            "modelAssociation": "explicit_model_id_only",
            "featureImportanceIsLocalAttribution": False,
            "pointInTimeFeatureAuditVerified": False,
        },
    }
