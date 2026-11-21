"""Prediction transparency contracts: no fabricated accuracy or model linkage."""
import json
import sqlite3

from app.internal_read_v1.prediction_transparency import prediction_evidence, prediction_scoreboard


def _database():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript("""
        CREATE TABLE predictions (
          id TEXT, tenant_id TEXT, strategy_id TEXT, ticker TEXT,
          timestamp TEXT, prediction TEXT, horizon TEXT, confidence REAL,
          mode TEXT, entry_price REAL, rank_score REAL,
          feature_snapshot_json TEXT, ranking_context_json TEXT
        );
        CREATE TABLE prediction_outcomes (
          id TEXT, tenant_id TEXT, prediction_id TEXT, evaluated_at TEXT,
          is_quarantined INTEGER, direction_correct INTEGER, actual_return REAL
        );
        CREATE TABLE ml_models (
          model_id TEXT, tenant_id TEXT, train_start TEXT, train_end TEXT,
          horizon TEXT, model_accuracy REAL, baseline_accuracy REAL,
          ic_score REAL, passed_gate INTEGER, train_rows INTEGER,
          feature_importance_json TEXT
        );
    """)
    return db


def test_scoreboard_excludes_quarantine_and_other_tenants():
    db = _database()
    for identifier, tenant in [("p1", "a"), ("p2", "a"), ("p3", "a"), ("p4", "b")]:
        db.execute(
            "INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (identifier, tenant, "ml_factor_ridge", "SPY", "2026-01-01T00:00:00Z",
             "up", "7d", .8, "paper", 100, None, "{}", "{}"))
    for identifier, pred, tenant, quarantined, correct, ret in [
        ("o1", "p1", "a", 0, 1, .05),
        ("o2", "p2", "a", 0, 0, -.02),
        ("o3", "p3", "a", 1, 1, .9),
        ("o4", "p4", "b", 0, 1, .9),
    ]:
        db.execute("INSERT INTO prediction_outcomes VALUES (?,?,?,?,?,?,?)",
                   (identifier, tenant, pred, "2026-01-09T00:00:00Z", quarantined, correct, ret))
    output = prediction_scoreboard(db, tenant_id="a")
    assert output["sampleCount"] == 2
    assert output["groups"][0]["directionalAccuracy"] == 0.5
    assert output["groups"][0]["alwaysUpBaselineAccuracy"] == 0.5
    assert output["methodology"]["outOfSampleVerified"] is False


def test_missing_schema_fails_closed():
    db = sqlite3.connect(":memory:")
    assert prediction_scoreboard(db)["status"] == "unavailable"


def test_evidence_never_attaches_unlinked_model():
    db = _database()
    db.execute(
        "INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("p1", "a", "ml_factor_ridge", "SPY", "2026-01-01T00:00:00Z",
         "up", "7d", 0.91, "paper", 100, None, "{}", "{}"))
    db.execute(
        "INSERT INTO ml_models VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("model1", "a", "2025-01-01", "2025-12-31", "7d", .7, .6, .1, 1, 200, "{}"))
    evidence = prediction_evidence(db, tenant_id="a", prediction_id="p1")
    assert evidence["modelEvidenceStatus"] == "unlinked"
    assert evidence["modelEvidence"] is None
    assert evidence["confidenceMethodology"]["isProbability"] is False


def test_linked_model_exposes_validation_but_not_probability():
    db = _database()
    db.execute(
        "INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("p1", "a", "ml_factor_ridge", "SPY", "2026-01-01T00:00:00Z",
         "up", "7d", .8, "paper", 100, None,
         json.dumps({"model_id": "m1"}), "{}"))
    db.execute(
        "INSERT INTO ml_models VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("m1", "a", "2025-01-01", "2025-12-31", "7d", .58, .53, .1, 1, 200, '{"mom": 0.7}'))
    out = prediction_evidence(db, tenant_id="a", prediction_id="p1")
    assert out["modelEvidenceStatus"] == "linked"
    assert out["modelEvidence"]["validationAccuracy"] == .58
    assert out["modelEvidence"]["localFeatureContributions"] is None
