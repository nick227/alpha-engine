from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _isoz(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def prediction_idempotency_key(*, ticker: str, strategy_id: str, horizon: str, timestamp: str) -> str:
    return _sha256_hex(f"{ticker}|{strategy_id}|{horizon}|{timestamp}")


def stable_prediction_id(*, tenant_id: str, idempotency_key: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}:{idempotency_key}".encode("utf-8")).hexdigest()[:16]
    return f"pred_{digest}"


def ensure_predictions_schema(conn: sqlite3.Connection) -> None:
    """
    Ensure additive columns required by backfill replay.

    We do not attempt to own the full predictions schema; we only add the
    columns needed for seed-parity + idempotent writes.
    """
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(predictions)").fetchall()}
    if not cols:
        # A minimal compatible schema (existing repos also create a richer version).
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS predictions (
              id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              strategy_id TEXT NOT NULL,
              scored_event_id TEXT NOT NULL,
              ticker TEXT NOT NULL,
              timestamp TEXT NOT NULL,
              prediction TEXT NOT NULL,
              confidence REAL NOT NULL,
              horizon TEXT NOT NULL,
              entry_price REAL NOT NULL,
              predicted_return REAL NOT NULL DEFAULT 0.0,
              mode TEXT NOT NULL DEFAULT 'backfill',
              feature_snapshot_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_idem
              ON predictions(tenant_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_predictions_ticker_ts
              ON predictions(tenant_id, ticker, timestamp);
            """
        )
        return

    additions: list[tuple[str, str]] = [
        ("run_id", "ALTER TABLE predictions ADD COLUMN run_id TEXT;"),
        ("idempotency_key", "ALTER TABLE predictions ADD COLUMN idempotency_key TEXT;"),
        ("predicted_return", "ALTER TABLE predictions ADD COLUMN predicted_return REAL NOT NULL DEFAULT 0.0;"),
        ("direction", "ALTER TABLE predictions ADD COLUMN direction TEXT;"),
        ("prediction_id", "ALTER TABLE predictions ADD COLUMN prediction_id TEXT;"),
    ]
    for col, ddl in additions:
        if col not in cols:
            conn.execute(ddl)

    # Unique idempotency index (some schemas already have it).
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_idempotency_key ON predictions(tenant_id, idempotency_key);"
    )


@dataclass(frozen=True, slots=True)
class PredictionWrite:
    tenant_id: str
    run_id: str
    strategy_id: str
    scored_event_id: str
    ticker: str
    timestamp: datetime
    horizon: str
    direction: str  # up|down|flat
    confidence: float
    entry_price: float
    predicted_return: float
    feature_snapshot: dict[str, Any]
    mode: str = "backfill"


def upsert_prediction(conn: sqlite3.Connection, w: PredictionWrite) -> str:
    ensure_predictions_schema(conn)
    ts = _isoz(w.timestamp)
    idem = prediction_idempotency_key(
        ticker=str(w.ticker), strategy_id=str(w.strategy_id), horizon=str(w.horizon), timestamp=str(ts)
    )
    pred_id = stable_prediction_id(tenant_id=str(w.tenant_id), idempotency_key=idem)

    feature_json = json.dumps(dict(w.feature_snapshot or {}), sort_keys=True, separators=(",", ":"))

    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(predictions)").fetchall()}

    insert_cols = [
        "id",
        "tenant_id",
        "run_id",
        "idempotency_key",
        "strategy_id",
        "scored_event_id",
        "ticker",
        "timestamp",
        "prediction",
        "confidence",
        "horizon",
        "entry_price",
        "predicted_return",
        "mode",
        "feature_snapshot_json",
    ]
    values: list[Any] = [
        pred_id,
        str(w.tenant_id),
        str(w.run_id),
        idem,
        str(w.strategy_id),
        str(w.scored_event_id),
        str(w.ticker),
        ts,
        str(w.direction),
        float(w.confidence),
        str(w.horizon),
        float(w.entry_price),
        float(w.predicted_return),
        str(w.mode),
        feature_json,
    ]

    # Optional alias columns for the MVP contract.
    if "direction" in cols and "direction" not in insert_cols:
        insert_cols.append("direction")
        values.append(str(w.direction))
    if "prediction_id" in cols and "prediction_id" not in insert_cols:
        insert_cols.append("prediction_id")
        values.append(pred_id)

    placeholders = ",".join(["?"] * len(insert_cols))
    conn.execute(
        """
        INSERT OR REPLACE INTO predictions ({cols})
        VALUES ({ph})
        """.format(cols=",".join(insert_cols), ph=placeholders),
        tuple(values),
    )

    return pred_id
