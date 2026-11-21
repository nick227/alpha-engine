"""Research trace contract: deterministic identity and honest evidence boundaries."""
import json
import sqlite3

from app.internal_read_v1.research_trace import (
    TRACE_VERSION, build_prediction_trace, verify_evidence_digest,
)


def _db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE predictions (
        id TEXT, tenant_id TEXT, strategy_id TEXT, ticker TEXT,
        timestamp TEXT, prediction TEXT, confidence REAL, horizon TEXT,
        mode TEXT, entry_price REAL, rank_score REAL,
        feature_snapshot_json TEXT, ranking_context_json TEXT
    )""")
    return db


def _add(db, tenant="a", feature=None):
    db.execute("INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
        "p1", tenant, "strat1", "SPY", "2026-04-21T13:00:00Z", "up",
        0.73, "7d", "paper", 540.0, 0.45,
        json.dumps(feature if feature is not None else {"momentum": 0.12, "model_id": "m7"}),
        json.dumps({"discovery": "eligible"}),
    ))


def test_trace_is_stable_and_verifiable():
    db = _db()
    _add(db)
    a = build_prediction_trace(db, tenant_id="a", prediction_id="p1")
    b = build_prediction_trace(db, tenant_id="a", prediction_id="p1")
    assert a == b
    assert a["schemaVersion"] == TRACE_VERSION
    assert verify_evidence_digest(a["evidence"]["snapshot"], a["evidence"]["sha256"])
    assert a["evidence"]["canonicalJson"] == json.dumps(a["evidence"]["snapshot"], ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    assert a["decisionReference"]["evidenceSha256"] == a["evidence"]["sha256"]
    assert a["trading"]["actionable"] is False
    assert a["integrity"]["immutableStorageVerified"] is False


def test_tamper_changes_digest_not_identity():
    db = _db()
    _add(db)
    original = build_prediction_trace(db, tenant_id="a", prediction_id="p1")
    db.execute("UPDATE predictions SET rank_score = 0.99 WHERE tenant_id = 'a'")
    updated = build_prediction_trace(db, tenant_id="a", prediction_id="p1")
    assert updated["traceId"] == original["traceId"]
    assert updated["evidence"]["sha256"] != original["evidence"]["sha256"]
    assert not verify_evidence_digest(updated["evidence"]["snapshot"], original["evidence"]["sha256"])


def test_tenant_scoping_and_missing_records():
    db = _db()
    _add(db, tenant="a")
    assert build_prediction_trace(db, tenant_id="b", prediction_id="p1") is None
    assert build_prediction_trace(db, tenant_id="a", prediction_id="missing") is None


def test_unavailable_model_is_not_fabricated():
    db = _db()
    _add(db, feature={"rsi": 45})
    result = build_prediction_trace(db, tenant_id="a", prediction_id="p1")
    assert result["evidence"]["availability"]["modelId"] is False
    assert result["evidence"]["completeness"] == "partial"
    assert result["evidence"]["snapshot"]["successProbability"] is None
    assert result["trading"]["brokerOrderId"] is None


def test_invalid_hash_and_snapshot_fail():
    assert not verify_evidence_digest({"bad": float("nan")}, "a" * 64)
    assert not verify_evidence_digest({}, "broken")
