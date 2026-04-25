from __future__ import annotations

import sqlite3

from app.services.engine_read_store import EngineReadStore


def _mk_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def test_engine_read_store_latest_consensus_filters_by_horizon(tmp_path) -> None:
    db = str(tmp_path / "alpha.db")
    con = _mk_db(db)
    con.execute(
        """
        CREATE TABLE consensus_signals (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            horizon TEXT,
            regime TEXT,
            direction TEXT,
            confidence REAL,
            total_weight REAL,
            participating_strategies INTEGER,
            created_at TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        INSERT INTO consensus_signals
          (id, tenant_id, ticker, horizon, regime, direction, confidence, total_weight, participating_strategies, created_at)
        VALUES
          ('a', 'default', 'AAPL', '1d', 'HIGH', 'BUY', 0.7, 1.0, 2, '2026-01-01T00:00:00Z'),
          ('b', 'default', 'AAPL', '7d', 'LOW',  'SELL', 0.9, 1.0, 2, '2026-01-01T00:00:00Z')
        """
    )
    con.commit()
    con.close()

    store = EngineReadStore(db_path=db)
    c1 = store.get_latest_consensus(tenant_id="default", ticker="AAPL", horizon="1d")
    assert c1 is not None
    assert c1.direction == "BUY"

    c7 = store.get_latest_consensus(tenant_id="default", ticker="AAPL", horizon="7d")
    assert c7 is not None
    assert c7.direction == "SELL"


def test_engine_read_store_adds_horizon_column_if_missing(tmp_path) -> None:
    db = str(tmp_path / "alpha.db")
    con = _mk_db(db)
    con.execute(
        """
        CREATE TABLE consensus_signals (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            regime TEXT,
            direction TEXT,
            confidence REAL,
            total_weight REAL,
            participating_strategies INTEGER,
            created_at TEXT NOT NULL
        )
        """
    )
    con.commit()
    con.close()

    store = EngineReadStore(db_path=db)
    # Should not crash, and should now be able to query with a horizon filter (returns None).
    assert store.get_latest_consensus(tenant_id="default", ticker="AAPL", horizon="1d") is None

