from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone


def _isoz(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def signal_idempotency_key(*, ticker: str, strategy_id: str, horizon: str, timestamp: str) -> str:
    return _sha256_hex(f"{ticker}|{strategy_id}|{horizon}|{timestamp}")


def stable_signal_id(*, tenant_id: str, prediction_id: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}:{prediction_id}".encode("utf-8")).hexdigest()[:16]
    return f"sig_{digest}"


def ensure_signals_schema(conn: sqlite3.Connection) -> None:
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(signals)").fetchall()}
    if not cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS signals (
              id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              prediction_id TEXT NOT NULL,
              ticker TEXT NOT NULL,
              strategy_id TEXT NOT NULL,
              horizon TEXT NOT NULL,
              timestamp TEXT NOT NULL,
              direction TEXT NOT NULL,
              confidence REAL NOT NULL,
              predicted_return REAL NOT NULL DEFAULT 0.0,
              trust_score REAL,
              trust_conservative REAL,
              trust_exploratory REAL,
              trust_json TEXT,
              trust_updated_at TEXT,
              created_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_idem
              ON signals(tenant_id, idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_signals_ticker_ts
              ON signals(tenant_id, ticker, timestamp);
            """
        )
        return

    additions: list[tuple[str, str]] = [
        ("run_id", "ALTER TABLE signals ADD COLUMN run_id TEXT;"),
        ("idempotency_key", "ALTER TABLE signals ADD COLUMN idempotency_key TEXT;"),
        ("horizon", "ALTER TABLE signals ADD COLUMN horizon TEXT;"),
        ("predicted_return", "ALTER TABLE signals ADD COLUMN predicted_return REAL NOT NULL DEFAULT 0.0;"),
        ("trust_score", "ALTER TABLE signals ADD COLUMN trust_score REAL;"),
        ("trust_conservative", "ALTER TABLE signals ADD COLUMN trust_conservative REAL;"),
        ("trust_exploratory", "ALTER TABLE signals ADD COLUMN trust_exploratory REAL;"),
        ("trust_json", "ALTER TABLE signals ADD COLUMN trust_json TEXT;"),
        ("trust_updated_at", "ALTER TABLE signals ADD COLUMN trust_updated_at TEXT;"),
    ]
    for col, ddl in additions:
        if col not in cols:
            conn.execute(ddl)

    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_idempotency_key ON signals(tenant_id, idempotency_key);")


@dataclass(frozen=True, slots=True)
class SignalWrite:
    tenant_id: str
    run_id: str
    prediction_id: str
    ticker: str
    strategy_id: str
    horizon: str
    timestamp: datetime
    direction: str
    confidence: float
    predicted_return: float


def upsert_signal(conn: sqlite3.Connection, w: SignalWrite) -> str:
    ensure_signals_schema(conn)
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(signals)").fetchall()}
    ts = _isoz(w.timestamp)
    idem = signal_idempotency_key(ticker=str(w.ticker), strategy_id=str(w.strategy_id), horizon=str(w.horizon), timestamp=ts)
    sig_id = stable_signal_id(tenant_id=str(w.tenant_id), prediction_id=str(w.prediction_id))
    now = _isoz(datetime.now(timezone.utc))

    insert_cols = [
        "id",
        "tenant_id",
        "run_id",
        "idempotency_key",
        "prediction_id",
        "ticker",
        "strategy_id",
        "horizon",
        "timestamp",
        "direction",
        "confidence",
        "predicted_return",
        "created_at",
    ]
    values = [
        sig_id,
        str(w.tenant_id),
        str(w.run_id),
        idem,
        str(w.prediction_id),
        str(w.ticker),
        str(w.strategy_id),
        str(w.horizon),
        ts,
        str(w.direction),
        float(w.confidence),
        float(w.predicted_return),
        now,
    ]

    # Backwards compatible schemas may still require these fields.
    if "track" in cols and "track" not in insert_cols:
        insert_cols.append("track")
        values.append("unknown")
    if "regime" in cols and "regime" not in insert_cols:
        insert_cols.append("regime")
        values.append(None)

    placeholders = ",".join(["?"] * len(insert_cols))
    conn.execute(
        f"INSERT OR REPLACE INTO signals ({','.join(insert_cols)}) VALUES ({placeholders})",
        tuple(values),
    )

    return sig_id
