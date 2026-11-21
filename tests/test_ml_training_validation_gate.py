import json
import sqlite3
import pytest
from datetime import date
from unittest.mock import patch
from app.ml.timebox import TimeWindow
from app.ml.train import train_model, MIN_DIRECTIONAL_ACCURACY


@pytest.fixture
def test_db(tmp_path):
    db_file = tmp_path / "test_ml.db"
    conn = sqlite3.connect(str(db_file))
    conn.executescript("""
        CREATE TABLE ml_learning_rows (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            horizon TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            symbol TEXT NOT NULL,
            features_json TEXT NOT NULL,
            future_return REAL,
            split TEXT NOT NULL
        );

        CREATE TABLE ml_models (
            model_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            horizon TEXT NOT NULL,
            train_start TEXT NOT NULL,
            train_end TEXT NOT NULL,
            weights_json TEXT NOT NULL,
            scaler_json TEXT NOT NULL,
            clip_params_json TEXT NOT NULL,
            feature_importance_json TEXT NOT NULL,
            group_weights_json TEXT NOT NULL,
            baseline_accuracy REAL NOT NULL,
            model_accuracy REAL NOT NULL,
            ic_score REAL NOT NULL,
            avg_return REAL NOT NULL,
            win_rate REAL NOT NULL,
            train_rows INTEGER NOT NULL,
            feature_coverage_used REAL NOT NULL,
            score_std REAL NOT NULL,
            passed_gate INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.close()
    return db_file


def test_baseline_accuracy_not_zero_and_rejects_sub_majority(test_db):
    """
    Simulate a bull-market scenario where 70% of days are positive.
    A naive model with 55% accuracy should FAIL the gate because 55% < 70% majority baseline.
    """
    conn = sqlite3.connect(str(test_db))
    # Populate 100 rows with 70 positive returns, 30 negative returns
    for i in range(100):
        # 70% positive returns
        ret = 0.02 if (i % 10 < 7) else -0.015
        feats = json.dumps({"f_momentum": (i % 5) * 0.1, "f_vol": 0.02})
        day = f"2026-01-{(i % 25) + 1:02d}"
        conn.execute("""
            INSERT INTO ml_learning_rows (id, tenant_id, horizon, timestamp, symbol, features_json, future_return, split)
            VALUES (?, 'default', '7d', ?, 'AAPL', ?, ?, 'train')
        """, (f"row_{i}", f"{day}T00:00:00Z", feats, ret))
    conn.commit()
    conn.close()

    window = TimeWindow(
        train_start=date(2026, 1, 1),
        train_end=date(2026, 1, 28),
        predict_start=date(2026, 1, 29),
        predict_end=date(2026, 2, 28),
    )

    model_id = train_model(window, "7d", db_path=test_db, tenant_id="default")

    conn = sqlite3.connect(str(test_db))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM ml_models ORDER BY created_at DESC LIMIT 1").fetchone()
    conn.close()

    assert row is not None
    # Baseline accuracy must NOT be 0.0000; it must reflect the true majority class (> 0.50)
    assert row["baseline_accuracy"] >= 0.50
    # Because validation baseline was high and noisy synthetic features don't beat it, passed_gate should be 0
    assert row["passed_gate"] == 0
    assert model_id is None  # Model rejected from actionable intelligence


def test_chronological_split_fits_preprocessors_on_train_only(test_db):
    """
    Ensure scaling parameters in scaler_json reflect training slice, not validation slice.
    """
    conn = sqlite3.connect(str(test_db))
    # Add training slice with mean feature = 10.0
    for i in range(80):
        feats = json.dumps({"f1": 10.0 + (i % 3)})
        conn.execute("""
            INSERT INTO ml_learning_rows (id, tenant_id, horizon, timestamp, symbol, features_json, future_return, split)
            VALUES (?, 'default', '1d', ?, 'AAPL', ?, 0.01, 'train')
        """, (f"train_{i}", f"2026-01-{(i % 20) + 1:02d}T00:00:00Z", feats))

    # Add validation slice with extreme feature = 500.0 (distribution shift)
    for i in range(20):
        feats = json.dumps({"f1": 500.0})
        conn.execute("""
            INSERT INTO ml_learning_rows (id, tenant_id, horizon, timestamp, symbol, features_json, future_return, split)
            VALUES (?, 'default', '1d', ?, 'AAPL', ?, 0.01, 'train')
        """, (f"val_{i}", f"2026-02-{(i % 20) + 1:02d}T00:00:00Z", feats))
    conn.commit()
    conn.close()

    window = TimeWindow(
        train_start=date(2026, 1, 1),
        train_end=date(2026, 2, 25),
        predict_start=date(2026, 2, 26),
        predict_end=date(2026, 3, 25),
    )

    train_model(window, "1d", db_path=test_db, tenant_id="default")

    conn = sqlite3.connect(str(test_db))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM ml_models ORDER BY created_at DESC LIMIT 1").fetchone()
    conn.close()

    assert row is not None
    scaler_params = json.loads(row["scaler_json"])
    # Mean of f1 in training partition was ~11.0, not 100+
    assert scaler_params["f1"]["mean"] < 25.0
