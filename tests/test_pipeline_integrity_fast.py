from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest


REQUIRED_TABLES = [
    "raw_events",
    "scored_events",
    "mra_outcomes",
    "predictions",
    "prediction_outcomes",
    "signals",
    "consensus_signals",
    "loop_heartbeats",
]

EXPECTED_HORIZONS = {"1d", "7d", "30d"}


def _db_path() -> Path:
    return Path(os.getenv("ALPHA_DB_PATH", "data/alpha.db"))


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _has_unique_index_on(conn: sqlite3.Connection, table: str, required_cols: set[str]) -> bool:
    """
    Returns True if the table has a UNIQUE index whose indexed columns are a superset of required_cols.
    """
    for idx in conn.execute(f"PRAGMA index_list({table})").fetchall():
        # Row shape: (seq, name, unique, origin, partial)
        idx_name = str(idx[1])
        is_unique = int(idx[2] or 0) == 1
        if not is_unique:
            continue
        cols = {str(r[2]) for r in conn.execute(f"PRAGMA index_info({idx_name})").fetchall()}
        if required_cols.issubset(cols):
            return True
    return False


def _sample_has_null_idempotency(conn: sqlite3.Connection, table: str, *, tenant_id: str) -> bool:
    cols = _cols(conn, table)
    if "idempotency_key" not in cols:
        return True
    if "tenant_id" in cols:
        row = conn.execute(
            f"""
            SELECT 1
            FROM {table}
            WHERE tenant_id = ?
              AND (idempotency_key IS NULL OR idempotency_key = '')
            LIMIT 1
            """,
            (tenant_id,),
        ).fetchone()
        return row is not None
    row = conn.execute(
        f"""
        SELECT 1
        FROM {table}
        WHERE (idempotency_key IS NULL OR idempotency_key = '')
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def _sample_horizons(conn: sqlite3.Connection, table: str, *, tenant_id: str) -> set[str]:
    cols = _cols(conn, table)
    if "horizon" not in cols:
        return set()
    if "tenant_id" in cols:
        rows = conn.execute(
            f"SELECT horizon FROM {table} WHERE tenant_id = ? AND horizon IS NOT NULL LIMIT 500",
            (tenant_id,),
        ).fetchall()
    else:
        rows = conn.execute(f"SELECT horizon FROM {table} WHERE horizon IS NOT NULL LIMIT 500").fetchall()
    return {str(r[0]).strip().lower() for r in rows if str(r[0] or "").strip()}


def _fast_count(conn: sqlite3.Connection, table: str, *, tenant_id: str) -> int:
    cols = _cols(conn, table)
    if "tenant_id" in cols:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE tenant_id = ?", (tenant_id,)).fetchone()[0] or 0)
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)


def test_pipeline_integrity_fast() -> None:
    """
    Near-instant integrity check.

    Constraints:
    - Does not run backfill or replay.
    - Inspects schema metadata and a small bounded sample of rows.
    """
    if str(os.getenv("RUN_PIPELINE_INTEGRITY_TEST", "")).strip() not in {"1", "true", "True"}:
        pytest.skip("Set RUN_PIPELINE_INTEGRITY_TEST=1 to enable this DB integrity check.")

    db_path = _db_path()
    if not db_path.exists():
        pytest.skip(f"DB not present: {db_path}")

    tenant_id = os.getenv("ALPHA_TENANT_ID", "backfill").strip() or "backfill"

    with _connect(db_path) as conn:
        # 1) Schema presence
        missing_tables = [t for t in REQUIRED_TABLES if not _table_exists(conn, t)]
        assert not missing_tables, f"Missing required tables: {missing_tables}"

        # 2) Idempotency schema safety (metadata)
        for table in REQUIRED_TABLES:
            cols = _cols(conn, table)
            for required_col in ("tenant_id", "run_id", "idempotency_key"):
                assert required_col in cols, f"{table} missing required column: {required_col}"
            assert _has_unique_index_on(conn, table, {"tenant_id", "idempotency_key"}), (
                f"{table} missing UNIQUE index on (tenant_id,idempotency_key)"
            )
            assert not _sample_has_null_idempotency(conn, table, tenant_id=tenant_id), (
                f"{table} has NULL/empty idempotency_key rows (tenant_id={tenant_id})"
            )

        # 3) Horizon coverage (bounded sample)
        for table in ("predictions", "signals", "consensus_signals"):
            if _fast_count(conn, table, tenant_id=tenant_id) == 0:
                continue
            horizons = _sample_horizons(conn, table, tenant_id=tenant_id)
            assert EXPECTED_HORIZONS.issubset(horizons), f"{table} missing horizons in sample: got={sorted(horizons)}"

        # 4) Consensus uniqueness (metadata + bounded guard)
        assert _has_unique_index_on(conn, "consensus_signals", {"tenant_id", "idempotency_key"})
        # Horizon-scoped key is enforced by the idempotency key and schema; double-check column exists.
        assert "horizon" in _cols(conn, "consensus_signals")
        assert "timestamp" in _cols(conn, "consensus_signals")
