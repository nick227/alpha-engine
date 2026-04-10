from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta

from app.engine.trust_engine import TrustEngine


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def test_trust_engine_deterministic_for_fixed_evidence() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE predictions (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          strategy_id TEXT NOT NULL,
          horizon TEXT NOT NULL,
          timestamp TEXT NOT NULL,
          confidence REAL NOT NULL
        );
        CREATE TABLE prediction_outcomes (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          prediction_id TEXT NOT NULL,
          return_pct REAL NOT NULL,
          max_drawdown REAL NOT NULL,
          direction_correct INTEGER NOT NULL,
          evaluated_at TEXT NOT NULL
        );
        """
    )

    tenant_id = "t1"
    strategy_id = "s1"
    horizon = "1d"
    base = datetime(2026, 4, 1, tzinfo=timezone.utc)

    for i in range(40):
        pred_id = f"p{i}"
        conf = 0.6 if (i % 2 == 0) else 0.4
        correct = 1 if (i % 2 == 0) else 0
        ev = base + timedelta(days=i)
        conn.execute(
            "INSERT INTO predictions VALUES (?,?,?,?,?,?)",
            (pred_id, tenant_id, strategy_id, horizon, _isoz(ev - timedelta(days=1)), conf),
        )
        conn.execute(
            "INSERT INTO prediction_outcomes VALUES (?,?,?,?,?,?,?)",
            (f"o{i}", tenant_id, pred_id, 0.01 if correct else -0.01, -0.02, correct, _isoz(ev)),
        )

    te = TrustEngine(half_life_days=30.0)
    as_of = base + timedelta(days=39)
    r1 = te.compute_strategy_trust(conn, tenant_id=tenant_id, strategy_id=strategy_id, horizon=horizon, as_of=as_of)
    r2 = te.compute_strategy_trust(conn, tenant_id=tenant_id, strategy_id=strategy_id, horizon=horizon, as_of=as_of)

    assert r1.trust_score == r2.trust_score
    assert r1.trust_conservative == r2.trust_conservative
    assert r1.trust_exploratory == r2.trust_exploratory
    assert r1.sample_size == 40
    assert 0.0 <= r1.trust_score <= 1.0
    assert 0.0 <= r1.trust_conservative <= 1.0
    assert 0.0 <= r1.trust_exploratory <= 1.0
