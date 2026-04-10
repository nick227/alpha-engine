from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


def _isoz(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def outcome_idempotency_key(*, prediction_id: str) -> str:
    return _sha256_hex(f"outcome|{prediction_id}")


def stable_outcome_id(*, tenant_id: str, prediction_id: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}:{prediction_id}".encode("utf-8")).hexdigest()[:16]
    return f"out_{digest}"


def ensure_prediction_outcomes_schema(conn: sqlite3.Connection) -> None:
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(prediction_outcomes)").fetchall()}
    if not cols:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS prediction_outcomes (
              id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              idempotency_key TEXT NOT NULL,
              prediction_id TEXT NOT NULL,
              horizon TEXT NOT NULL,
              target_time TEXT NOT NULL,
              exit_price REAL NOT NULL,
              actual_return REAL NOT NULL,
              return_pct REAL NOT NULL,
              direction_correct INTEGER NOT NULL,
              max_runup REAL NOT NULL DEFAULT 0.0,
              max_drawdown REAL NOT NULL DEFAULT 0.0,
              evaluated_at TEXT NOT NULL,
              exit_reason TEXT NOT NULL DEFAULT 'horizon',
              residual_alpha REAL NOT NULL DEFAULT 0.0
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_outcomes_idem
              ON prediction_outcomes(tenant_id, idempotency_key);
            """
        )
        return

    additions: list[tuple[str, str]] = [
        ("run_id", "ALTER TABLE prediction_outcomes ADD COLUMN run_id TEXT;"),
        ("idempotency_key", "ALTER TABLE prediction_outcomes ADD COLUMN idempotency_key TEXT;"),
        ("horizon", "ALTER TABLE prediction_outcomes ADD COLUMN horizon TEXT;"),
        ("target_time", "ALTER TABLE prediction_outcomes ADD COLUMN target_time TEXT;"),
        ("actual_return", "ALTER TABLE prediction_outcomes ADD COLUMN actual_return REAL;"),
        ("max_runup", "ALTER TABLE prediction_outcomes ADD COLUMN max_runup REAL NOT NULL DEFAULT 0.0;"),
        ("max_drawdown", "ALTER TABLE prediction_outcomes ADD COLUMN max_drawdown REAL NOT NULL DEFAULT 0.0;"),
    ]
    for col, ddl in additions:
        if col not in cols:
            conn.execute(ddl)

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_outcomes_idempotency_key ON prediction_outcomes(tenant_id, idempotency_key);"
    )


@dataclass(frozen=True, slots=True)
class OutcomeWrite:
    tenant_id: str
    run_id: str
    ticker: str
    strategy_id: str
    prediction_id: str
    prediction_time: datetime
    horizon: str
    direction: str
    entry_price: float
    # If provided, use this to resolve actual_return quickly (e.g. from price_context future_return_*).
    actual_return_hint: float | None = None
    exit_price_hint: float | None = None
    evaluated_at: datetime | None = None
    exit_reason: str = "horizon"


def _horizon_to_days(horizon: str) -> int:
    h = str(horizon).strip().lower()
    if h == "1d":
        return 1
    if h == "7d":
        return 7
    if h == "30d":
        return 30
    raise ValueError(f"Unsupported horizon: {horizon}")


def _direction_correct(*, direction: str, actual_return: float) -> bool:
    d = str(direction).strip().lower()
    if d == "up":
        return actual_return > 0
    if d == "down":
        return actual_return < 0
    # flat
    return actual_return == 0.0


def resolve_outcome(conn: sqlite3.Connection, w: OutcomeWrite) -> str:
    """
    Resolve outcomes without lookahead.

    For MVP seed-parity, we allow a best-effort `actual_return_hint` (e.g. computed
    from cached bars as future_return_*). When absent, the caller should provide
    it (we intentionally do not own bars access here).
    """
    ensure_prediction_outcomes_schema(conn)

    pred_ts = w.prediction_time.astimezone(timezone.utc).replace(microsecond=0)
    target_time = pred_ts + timedelta(days=_horizon_to_days(w.horizon))

    if w.actual_return_hint is None:
        raise ValueError("OutcomeWrite.actual_return_hint is required (bars access is owned by replay_engine).")

    actual_return = float(w.actual_return_hint)
    exit_price = float(w.exit_price_hint) if w.exit_price_hint is not None else float(w.entry_price) * (1.0 + actual_return)
    correct = _direction_correct(direction=w.direction, actual_return=actual_return)

    evaluated_at = (w.evaluated_at or target_time).astimezone(timezone.utc).replace(microsecond=0)

    # Idempotency contract: hash(ticker + strategy + horizon + timestamp)
    pred_ts = w.prediction_time.astimezone(timezone.utc).replace(microsecond=0)
    idem = _sha256_hex(f"{w.ticker}|{w.strategy_id}|{w.horizon}|{_isoz(pred_ts)}")
    out_id = stable_outcome_id(tenant_id=str(w.tenant_id), prediction_id=str(w.prediction_id))

    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(prediction_outcomes)").fetchall()}

    insert_cols: list[str] = [
        "id",
        "tenant_id",
        "run_id",
        "idempotency_key",
        "prediction_id",
        "horizon",
        "target_time",
        "exit_price",
        "actual_return",
        "return_pct",
        "direction_correct",
        "evaluated_at",
        "exit_reason",
    ]
    values: list[Any] = [
        out_id,
        str(w.tenant_id),
        str(w.run_id),
        idem,
        str(w.prediction_id),
        str(w.horizon),
        _isoz(target_time),
        float(exit_price),
        float(actual_return),
        float(actual_return),
        1 if bool(correct) else 0,
        _isoz(evaluated_at),
        str(w.exit_reason),
    ]

    if "max_runup" in cols:
        insert_cols.append("max_runup")
        values.append(0.0)
    if "max_drawdown" in cols:
        insert_cols.append("max_drawdown")
        values.append(0.0)
    if "residual_alpha" in cols:
        insert_cols.append("residual_alpha")
        values.append(0.0)

    placeholders = ",".join(["?"] * len(insert_cols))
    col_sql = ",".join(insert_cols)
    conn.execute(
        f"INSERT OR REPLACE INTO prediction_outcomes ({col_sql}) VALUES ({placeholders})",
        tuple(values),
    )

    return out_id
