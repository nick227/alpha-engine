#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import sqlite3
import sys
from datetime import datetime, timezone


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


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _parse_isoz(ts: str) -> datetime:
    s = str(ts).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def predicted_return_expected(*, ticker: str, strategy_id: str, horizon: str, timestamp: str, direction: str) -> float:
    h = str(horizon).strip().lower()
    d = str(direction).strip().lower()
    if h not in {"1d", "7d", "30d"}:
        return 0.0
    if d == "flat":
        return 0.0
    sign = 1.0 if d == "up" else (-1.0 if d == "down" else 0.0)
    if sign == 0.0:
        return 0.0
    mag_max = {"1d": 0.02, "7d": 0.05, "30d": 0.10}[h]
    key = f"{ticker}|{strategy_id}|{h}|{_parse_isoz(timestamp).isoformat().replace('+00:00','Z')}"
    u = int(_sha256_hex(key)[:16], 16) / float(16**16)
    return sign * (u * mag_max)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _count(conn: sqlite3.Connection, table: str, *, tenant_id: str) -> int:
    cols = _cols(conn, table)
    if "tenant_id" in cols:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE tenant_id=?", (tenant_id,)).fetchone()[0] or 0)
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)


def _check_duplicates(conn: sqlite3.Connection, table: str, *, tenant_id: str) -> list[tuple[str, int]]:
    cols = _cols(conn, table)
    if "idempotency_key" not in cols:
        return []
    if "tenant_id" in cols:
        rows = conn.execute(
            f"""
            SELECT idempotency_key, COUNT(*) AS n
            FROM {table}
            WHERE tenant_id = ?
            GROUP BY idempotency_key
            HAVING n > 1
            LIMIT 5
            """,
            (tenant_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT idempotency_key, COUNT(*) AS n
            FROM {table}
            GROUP BY idempotency_key
            HAVING n > 1
            LIMIT 5
            """
        ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


def main() -> int:
    p = argparse.ArgumentParser(description="Verify backfill+replay seed-parity invariants")
    p.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")
    p.add_argument("--tenant-id", default="backfill", help="Tenant to validate (default: backfill)")
    p.add_argument("--limit", type=int, default=2000, help="Row limit for expensive checks")
    args = p.parse_args()

    db = str(args.db)
    tenant_id = str(args.tenant_id)
    limit = int(args.limit)

    conn = sqlite3.connect(db)
    try:
        conn.row_factory = sqlite3.Row

        missing = [t for t in REQUIRED_TABLES if not _table_exists(conn, t)]
        if missing:
            print("FAIL missing tables:", ", ".join(missing))
            return 2

        # Acceptance counts.
        counts = {t: _count(conn, t, tenant_id=tenant_id) for t in REQUIRED_TABLES}
        print("Counts:", counts)

        required_nonzero = ["predictions", "prediction_outcomes", "signals", "consensus_signals", "loop_heartbeats"]
        for t in required_nonzero:
            if int(counts.get(t, 0)) <= 0:
                print(f"FAIL {t} has 0 rows for tenant_id={tenant_id}")
                return 2

        # Horizon coverage.
        for table in ("predictions", "signals", "consensus_signals"):
            cols = _cols(conn, table)
            if "horizon" not in cols:
                print(f"FAIL {table} missing horizon column")
                return 2
            hs = {
                str(r[0]).strip().lower()
                for r in conn.execute(
                    f"SELECT DISTINCT horizon FROM {table} WHERE tenant_id=?",
                    (tenant_id,),
                ).fetchall()
            }
            if not {"1d", "7d", "30d"}.issubset(hs):
                print(f"FAIL {table} missing horizons. got={sorted(hs)}")
                return 2

        # Duplicate idempotency keys.
        dup_fail = False
        for table in REQUIRED_TABLES:
            dups = _check_duplicates(conn, table, tenant_id=tenant_id)
            if dups:
                dup_fail = True
                print(f"FAIL duplicates in {table}:", dups)
        if dup_fail:
            return 2

        # Placeholder horizon rule: never emit multiple placeholders for same (scored_event_id,horizon).
        if "strategy_id" in _cols(conn, "predictions") and "scored_event_id" in _cols(conn, "predictions"):
            rows = conn.execute(
                """
                SELECT scored_event_id, horizon, COUNT(*) AS n
                FROM predictions
                WHERE tenant_id = ?
                  AND strategy_id LIKE 'placeholder-backfill-%'
                GROUP BY scored_event_id, horizon
                HAVING n > 1
                LIMIT 5
                """,
                (tenant_id,),
            ).fetchall()
            if rows:
                print("FAIL duplicate placeholder predictions:", [(str(r[0]), str(r[1]), int(r[2])) for r in rows])
                return 2

        # consensus_signals horizon-scoped key.
        cs_cols = _cols(conn, "consensus_signals")
        if "timestamp" not in cs_cols:
            print("FAIL consensus_signals missing timestamp column")
            return 2

        # Verify predicted_return determinism (sample).
        pred_cols = _cols(conn, "predictions")
        need = {"ticker", "strategy_id", "horizon", "timestamp", "predicted_return"}
        if need.issubset(pred_cols):
            rows = conn.execute(
                """
                SELECT ticker, strategy_id, horizon, timestamp,
                       COALESCE(direction, prediction) AS direction,
                       predicted_return
                FROM predictions
                WHERE tenant_id = ?
                ORDER BY timestamp ASC
                LIMIT ?
                """,
                (tenant_id, limit),
            ).fetchall()
            mismatches = 0
            for r in rows:
                exp = predicted_return_expected(
                    ticker=str(r["ticker"]),
                    strategy_id=str(r["strategy_id"]),
                    horizon=str(r["horizon"]),
                    timestamp=str(r["timestamp"]),
                    direction=str(r["direction"]),
                )
                got = float(r["predicted_return"] or 0.0)
                if abs(exp - got) > 1e-12:
                    mismatches += 1
                    if mismatches <= 5:
                        print("FAIL predicted_return mismatch:", dict(r), "expected=", exp)
            if mismatches:
                return 2

        # Verify prediction_outcomes idempotency key includes strategy_id by recomputing from joined prediction inputs.
        out_cols = _cols(conn, "prediction_outcomes")
        if {"idempotency_key", "prediction_id", "horizon"}.issubset(out_cols) and {"id", "ticker", "strategy_id", "timestamp"}.issubset(pred_cols):
            rows = conn.execute(
                """
                SELECT o.idempotency_key AS ok,
                       p.ticker AS ticker,
                       p.strategy_id AS strategy_id,
                       o.horizon AS horizon,
                       p.timestamp AS ts
                FROM prediction_outcomes o
                JOIN predictions p
                  ON p.id = o.prediction_id
                 AND p.tenant_id = o.tenant_id
                WHERE o.tenant_id = ?
                ORDER BY p.timestamp ASC
                LIMIT ?
                """,
                (tenant_id, limit),
            ).fetchall()
            for r in rows:
                exp = _sha256_hex(f"{r['ticker']}|{r['strategy_id']}|{r['horizon']}|{r['ts']}")
                if str(r["ok"]) != exp:
                    print("FAIL prediction_outcomes idempotency mismatch:", dict(r), "expected=", exp)
                    return 2

        # loop_heartbeats minimal schema.
        hb_cols = _cols(conn, "loop_heartbeats")
        if "run_id" not in hb_cols or "status" not in hb_cols:
            print("FAIL loop_heartbeats missing run_id/status")
            return 2

        print("OK")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

