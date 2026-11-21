"""Matched benchmark guards against fabricated comparisons."""
import sqlite3
from app.internal_read_v1.pairwise_benchmark import compare_prediction_strategies


def _db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript("""
      CREATE TABLE predictions(id TEXT, tenant_id TEXT, strategy_id TEXT, ticker TEXT,
        timestamp TEXT, horizon TEXT, prediction TEXT);
      CREATE TABLE prediction_outcomes(id TEXT, tenant_id TEXT, prediction_id TEXT,
        is_quarantined INTEGER, evaluated_at TEXT, target_time TEXT,
        direction_correct INTEGER, actual_return REAL);
    """)
    return db


def _add(db, id, strategy, date, target, correct, ret, tenant="a", quarantine=0, direction="up"):
    db.execute("INSERT INTO predictions VALUES (?,?,?,?,?,?,?)",
       (id, tenant, strategy, "SPY", date, "7d", direction))
    db.execute("INSERT INTO prediction_outcomes VALUES (?,?,?,?,?,?,?,?)",
       ("o" + id, tenant, id, quarantine, target, target, correct, ret))


def test_matched_comparison_and_baseline():
    db = _db()
    _add(db, "m1", "ml", "2026-01-01T12:00:00Z", "2026-01-08T12:00:00Z", 1, .03)
    _add(db, "r1", "rules", "2026-01-01T15:00:00Z", "2026-01-08T12:00:00Z", 0, .03, direction="down")
    _add(db, "m2", "ml", "2026-01-02T12:00:00Z", "2026-01-09T12:00:00Z", 0, -.04)
    _add(db, "r2", "rules", "2026-01-02T15:00:00Z", "2026-01-09T12:00:00Z", 1, -.04, direction="down")
    out = compare_prediction_strategies(db, tenant_id="a", ml_strategy_id="ml", rules_strategy_id="rules")
    assert out["matchedSamples"] == 2
    assert out["mlDirectionalAccuracy"] == .5
    assert out["rulesDirectionalAccuracy"] == .5
    assert out["alwaysUpBaselineAccuracy"] == .5
    assert out["methodology"]["outOfSampleVerified"] is False


def test_quarantine_tenants_and_incompatible_returns_excluded():
    db = _db()
    for i in range(4):
        dt, target = f"2026-01-0{i+1}T12:00:00Z", f"2026-01-{i+8:02d}T12:00:00Z"
        _add(db, "m" + str(i), "ml", dt, target, 1, .02, quarantine=1 if i == 0 else 0)
        _add(db, "r" + str(i), "rules", dt, target, 1, .03 if i == 1 else .02,
             tenant="other" if i == 2 else "a")
    out = compare_prediction_strategies(db, tenant_id="a", ml_strategy_id="ml", rules_strategy_id="rules")
    assert out["matchedSamples"] == 1
    assert out["status"] == "unavailable"


def test_missing_schema_returns_unavailable():
    out = compare_prediction_strategies(sqlite3.connect(":memory:"), tenant_id="a",
                                        ml_strategy_id="ml", rules_strategy_id="rules")
    assert out["reason"] == "required_point_in_time_outcomes_unavailable"


def test_truncation_never_reports_partial_accuracy():
    db = _db()
    for i in range(3):
        dt, target = f"2026-01-0{i+1}T12:00:00Z", f"2026-01-{i+8:02d}T12:00:00Z"
        _add(db, "m" + str(i), "ml", dt, target, 1, .02)
        _add(db, "r" + str(i), "rules", dt, target, 1, .02)
    out = compare_prediction_strategies(db, tenant_id="a", ml_strategy_id="ml",
                                        rules_strategy_id="rules", max_recent_rows=3)
    assert out["reason"] == "sample_window_truncated"
    assert out["status"] == "unavailable"
