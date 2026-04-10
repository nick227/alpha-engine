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


def consensus_idempotency_key(*, ticker: str, horizon: str, timestamp: str) -> str:
    # Match global idempotency contract: hash(ticker + strategy + horizon + timestamp)
    return _sha256_hex(f"{ticker}|consensus|{horizon}|{timestamp}")


def stable_consensus_id(*, tenant_id: str, idempotency_key: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}:{idempotency_key}".encode("utf-8")).hexdigest()[:16]
    return f"cs_{digest}"


def ensure_consensus_schema(conn: sqlite3.Connection) -> None:
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(consensus_signals)").fetchall()}
    if not cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS consensus_signals (
              id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              ticker TEXT NOT NULL,
              horizon TEXT NOT NULL,
              timestamp TEXT NOT NULL,
              score REAL NOT NULL,
              direction TEXT NOT NULL,
              confidence REAL NOT NULL,
              regime TEXT,
              strategies_json TEXT NOT NULL DEFAULT '[]',
              weights_json TEXT NOT NULL DEFAULT '{}',
              trust_score REAL,
              trust_conservative REAL,
              trust_exploratory REAL,
              trust_json TEXT,
              trust_updated_at TEXT,
              created_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_consensus_idem
              ON consensus_signals(tenant_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_consensus_rank
              ON consensus_signals(tenant_id, ticker, horizon, timestamp, score);
            """
        )
        return

    additions: list[tuple[str, str]] = [
        ("run_id", "ALTER TABLE consensus_signals ADD COLUMN run_id TEXT;"),
        ("idempotency_key", "ALTER TABLE consensus_signals ADD COLUMN idempotency_key TEXT;"),
        ("timestamp", "ALTER TABLE consensus_signals ADD COLUMN timestamp TEXT;"),
        ("horizon", "ALTER TABLE consensus_signals ADD COLUMN horizon TEXT;"),
        ("score", "ALTER TABLE consensus_signals ADD COLUMN score REAL;"),
        ("direction", "ALTER TABLE consensus_signals ADD COLUMN direction TEXT;"),
        ("confidence", "ALTER TABLE consensus_signals ADD COLUMN confidence REAL;"),
        ("strategies_json", "ALTER TABLE consensus_signals ADD COLUMN strategies_json TEXT NOT NULL DEFAULT '[]';"),
        ("weights_json", "ALTER TABLE consensus_signals ADD COLUMN weights_json TEXT NOT NULL DEFAULT '{}';"),
        ("trust_score", "ALTER TABLE consensus_signals ADD COLUMN trust_score REAL;"),
        ("trust_conservative", "ALTER TABLE consensus_signals ADD COLUMN trust_conservative REAL;"),
        ("trust_exploratory", "ALTER TABLE consensus_signals ADD COLUMN trust_exploratory REAL;"),
        ("trust_json", "ALTER TABLE consensus_signals ADD COLUMN trust_json TEXT;"),
        ("trust_updated_at", "ALTER TABLE consensus_signals ADD COLUMN trust_updated_at TEXT;"),
    ]
    for col, ddl in additions:
        if col not in cols:
            conn.execute(ddl)

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_consensus_idempotency_key ON consensus_signals(tenant_id, idempotency_key);"
    )


def _sign_direction(score: float, eps: float = 1e-12) -> str:
    if score > eps:
        return "up"
    if score < -eps:
        return "down"
    return "flat"


@dataclass(frozen=True, slots=True)
class ConsensusWrite:
    tenant_id: str
    run_id: str
    ticker: str
    horizon: str
    timestamp: datetime
    score: float
    confidence: float
    regime: str | None
    strategies: list[dict[str, Any]]
    weights: dict[str, float]


def upsert_consensus(conn: sqlite3.Connection, w: ConsensusWrite) -> str:
    ensure_consensus_schema(conn)
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(consensus_signals)").fetchall()}
    ts = _isoz(w.timestamp)
    idem = consensus_idempotency_key(ticker=str(w.ticker), horizon=str(w.horizon), timestamp=ts)
    cid = stable_consensus_id(tenant_id=str(w.tenant_id), idempotency_key=idem)
    created_at = _isoz(datetime.now(timezone.utc))

    strategies_json = json.dumps(list(w.strategies or []), sort_keys=True, separators=(",", ":"))
    weights_json = json.dumps(dict(w.weights or {}), sort_keys=True, separators=(",", ":"))
    direction = _sign_direction(float(w.score))

    insert_cols = ["id", "tenant_id", "ticker"]
    values: list[Any] = [cid, str(w.tenant_id), str(w.ticker)]

    # Additive MVP columns (only if present in the active schema).
    if "run_id" in cols:
        insert_cols.append("run_id")
        values.append(str(w.run_id))
    if "idempotency_key" in cols:
        insert_cols.append("idempotency_key")
        values.append(idem)
    if "horizon" in cols:
        insert_cols.append("horizon")
        values.append(str(w.horizon))
    if "timestamp" in cols:
        insert_cols.append("timestamp")
        values.append(ts)
    if "score" in cols:
        insert_cols.append("score")
        values.append(float(w.score))
    if "direction" in cols:
        insert_cols.append("direction")
        values.append(direction)
    if "confidence" in cols:
        insert_cols.append("confidence")
        values.append(float(w.confidence))
    if "regime" in cols:
        values_regime = str(w.regime) if w.regime is not None else "UNKNOWN"
        insert_cols.append("regime")
        values.append(values_regime)
    if "strategies_json" in cols:
        insert_cols.append("strategies_json")
        values.append(strategies_json)
    if "weights_json" in cols:
        insert_cols.append("weights_json")
        values.append(str(weights_json))

    # Backwards compatible columns expected by older UI schemas.
    # Fill with placeholder values when present.
    defaults: dict[str, Any] = {
        "total_weight": 1.0,
        "participating_strategies": int(len(w.strategies or [])),
        "sentiment_strategy_id": None,
        "quant_strategy_id": None,
        "sentiment_score": 0.0,
        "quant_score": 0.0,
        "ws": 0.5,
        "wq": 0.5,
        "agreement_bonus": 0.0,
        "p_final": float(w.confidence),
        "stability_score": 0.0,
        "created_at": created_at,
    }
    for col, val in defaults.items():
        if col in cols and col not in insert_cols:
            insert_cols.append(col)
            values.append(val)

    placeholders = ",".join(["?"] * len(insert_cols))
    conn.execute(
        f"INSERT OR REPLACE INTO consensus_signals ({','.join(insert_cols)}) VALUES ({placeholders})",
        tuple(values),
    )

    return cid
