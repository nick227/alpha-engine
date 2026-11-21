"""Versioned, read-only research references for bot decision audit trails.

This contract is a handoff to the trading service. A decision writer must
persist both the returned reference and evidence snapshot *at decision time*.
The digest detects changes; it is not a signature or an immutable archive.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from typing import Any

from app.internal_read_v1.intelligence_read import get_prediction_context

TRACE_VERSION = "research-trace/v1"


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _stable_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _snapshot(context: dict[str, Any]) -> dict[str, Any]:
    """Only data persisted with the prediction, never live quote/model joins."""
    return {
        "predictionId": context["predictionId"],
        "ticker": context["ticker"],
        "observedAt": context["timestamp"],
        "strategyId": context["strategyId"],
        "direction": context["prediction"],
        "horizon": context["horizon"],
        "mode": context["mode"],
        "entryPrice": _stable_number(context["entryPrice"]),
        "rankScore": _stable_number(context["rankScore"]),
        "confidence": _stable_number(context["confidence"]),
        "confidenceIsProbability": False,
        "successProbability": None,
        "features": context.get("featureSnapshot") or {},
        "ranking": context.get("rankingContext") or {},
    }


def build_prediction_trace(
    conn: sqlite3.Connection, *, tenant_id: str, prediction_id: str,
) -> dict[str, Any] | None:
    """Produce a deterministic decision-time evidence snapshot and reference.

    Returns None when the tenant-scoped prediction is not found. Does not infer
    suitability, risk approval, or trading eligibility.
    """
    context = get_prediction_context(
        conn, tenant_id=tenant_id, prediction_id=prediction_id
    )
    if context is None:
        return None
    snapshot = _snapshot(context)
    evidence_digest = hashlib.sha256(_canonical(snapshot)).hexdigest()
    # Include tenant in the stable reference, without embedding the tenant ID
    # itself into the public identifier.
    identity = hashlib.sha256(
        _canonical({"tenantId": tenant_id, "predictionId": prediction_id})
    ).hexdigest()
    trace_id = f"research:{identity}"
    feature = snapshot["features"]
    model_id = feature.get("model_id") or feature.get("modelId")
    available = {
        "features": bool(feature),
        "ranking": bool(snapshot["ranking"]),
        "modelId": bool(model_id),
    }
    return {
        "schemaVersion": TRACE_VERSION,
        "traceId": trace_id,
        "tenant_id": tenant_id,
        "source": {
            "system": "alpha-engine",
            "kind": "stored_prediction",
            "predictionId": prediction_id,
            "strategyId": context["strategyId"],
            "ticker": context["ticker"],
            "observedAt": context["timestamp"],
            "horizon": context["horizon"],
        },
        "decisionReference": {
            "schemaVersion": TRACE_VERSION,
            "traceId": trace_id,
            "predictionId": prediction_id,
            "observedAt": context["timestamp"],
            "evidenceSha256": evidence_digest,
        },
        "evidence": {
            "sha256": evidence_digest,
            "snapshot": snapshot,
            "availability": available,
            "snapshotOrigin": "stored_prediction_fields",
            "completeness": (
                "partial" if not all(available.values()) else "stored_fields_present"
            ),
        },
        "integrity": {
            "digestAlgorithm": "SHA-256",
            "canonicalization": "JSON sorted keys, compact UTF-8, non-finite rejected",
            "digitallySigned": False,
            "immutableStorageVerified": False,
            "pointInTimeSourceAuditVerified": False,
            "limitation": (
                "Hash detects changes only if the consuming service persists "
                "the original snapshot and digest at decision time."
            ),
        },
        "trading": {
            "actionable": False,
            "executionPermission": False,
            "botDecisionId": None,
            "brokerOrderId": None,
            "reason": "research_reference_only",
        },
    }


def verify_evidence_digest(snapshot: dict[str, Any], expected_sha256: str) -> bool:
    """Verify captured evidence locally; this is not an authenticity signature."""
    if not isinstance(snapshot, dict) or not isinstance(expected_sha256, str):
        return False
    if len(expected_sha256) != 64:
        return False
    try:
        return hashlib.sha256(_canonical(snapshot)).hexdigest() == expected_sha256.lower()
    except (TypeError, ValueError, OverflowError):
        return False
