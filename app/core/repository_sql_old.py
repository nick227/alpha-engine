from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from uuid import uuid4

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    source TEXT NOT NULL,
    text TEXT NOT NULL,
    tickers_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scored_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    raw_event_id TEXT NOT NULL,
    primary_ticker TEXT NOT NULL,
    category TEXT NOT NULL,
    materiality REAL NOT NULL,
    direction TEXT NOT NULL,
    confidence REAL NOT NULL,
    company_relevance REAL NOT NULL,
    concept_tags_json TEXT NOT NULL,
    explanation_terms_json TEXT NOT NULL,
    scorer_version TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mra_outcomes (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    scored_event_id TEXT NOT NULL,
    return_1m REAL NOT NULL,
    return_5m REAL NOT NULL,
    return_15m REAL NOT NULL,
    return_1h REAL NOT NULL,
    volume_ratio REAL NOT NULL,
    vwap_distance REAL NOT NULL,
    range_expansion REAL NOT NULL,
    continuation_slope REAL NOT NULL,
    pullback_depth REAL NOT NULL,
    mra_score REAL NOT NULL,
    market_context_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategies (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    strategy_type TEXT NOT NULL,
    mode TEXT NOT NULL,
    active INTEGER NOT NULL,
    config_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS predictions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    scored_event_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    prediction TEXT NOT NULL,
    confidence REAL NOT NULL,
    horizon TEXT NOT NULL,
    entry_price REAL NOT NULL,
    mode TEXT NOT NULL,
    feature_snapshot_json TEXT NOT NULL,
    regime TEXT,
    trend_strength TEXT
);

CREATE TABLE IF NOT EXISTS prediction_outcomes (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    prediction_id TEXT NOT NULL,
    exit_price REAL NOT NULL,
    return_pct REAL NOT NULL,
    direction_correct INTEGER NOT NULL,
    max_runup REAL NOT NULL,
    max_drawdown REAL NOT NULL,
    evaluated_at TEXT NOT NULL,
    exit_reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS price_bars (
    tenant_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    PRIMARY KEY (tenant_id, ticker, timestamp)
);

CREATE TABLE IF NOT EXISTS strategy_performance (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    horizon TEXT NOT NULL,
    prediction_count INTEGER NOT NULL,
    accuracy REAL NOT NULL,
    avg_return REAL NOT NULL,
    avg_residual_alpha REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS regime_performance (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    regime TEXT NOT NULL,
    prediction_count INTEGER NOT NULL,
    accuracy REAL NOT NULL,
    avg_return REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_stability (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    backtest_accuracy REAL NOT NULL,
    live_accuracy REAL NOT NULL,
    stability_score REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_state (
    strategy_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    track TEXT NOT NULL,
    status TEXT NOT NULL,
    parent_id TEXT,
    version TEXT,
    consecutive_bad_windows INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS promotion_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    parent_id TEXT,
    track TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT,
    gate_logs_json TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_kv (
    tenant_id TEXT NOT NULL,
    k TEXT NOT NULL,
    v TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, k)
);

CREATE TABLE IF NOT EXISTS loop_heartbeats (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    loop_type TEXT NOT NULL,
    status TEXT NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_event_queue (
    tenant_id TEXT NOT NULL,
    raw_event_id TEXT NOT NULL,
    status TEXT NOT NULL,         -- NEW | DEFERRED | PROCESSED
    next_retry_at TEXT,           -- ISO8601, only for DEFERRED
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    processed_at TEXT,
    PRIMARY KEY (tenant_id, raw_event_id)
);
"""


class Repository:
    def __init__(self, db_path: str | Path = "data/alpha.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        # Concurrency/robustness for multi-loop runtime:
        # - WAL reduces writer/reader contention
        # - busy_timeout prevents spurious "database is locked" errors under light contention
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.execute("PRAGMA busy_timeout=5000;")
        self.conn.executescript(DB_SCHEMA)
        self._migrate()
        self.conn.commit()
        self._in_tx = False

    @contextmanager
    def transaction(self):
        """
        Groups many small writes into one commit. Critical for runtime loop performance.
        """
        if self._in_tx:
            # Nested transaction: just reuse outer boundary.
            yield self
            return

        self._in_tx = True
        try:
            self.conn.execute("BEGIN")
            yield self
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self._in_tx = False

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.conn.execute(sql, params)
        if not self._in_tx:
            self.conn.commit()

    def executemany(self, sql: str, params: Iterable[tuple]) -> None:
        self.conn.executemany(sql, params)
        if not self._in_tx:
            self.conn.commit()

    def query_df(self, sql: str, params: tuple = ()):
        import pandas as pd
        return pd.read_sql_query(sql, self.conn, params=params)

    def _table_columns(self, table: str) -> set[str]:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r["name"]) for r in rows}

    def _migrate(self) -> None:
        """
        Lightweight SQLite migrations for additive schema changes.
        Safe to run on every startup.
        """
        pred_cols = self._table_columns("predictions")
        if "scored_outcome_id" not in pred_cols:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN scored_outcome_id TEXT")
        if "scored_at" not in pred_cols:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN scored_at TEXT")
        if "regime" not in pred_cols:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN regime TEXT")
        if "trend_strength" not in pred_cols:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN trend_strength TEXT")

        out_cols = self._table_columns("prediction_outcomes")
        if "residual_alpha" not in out_cols:
            self.conn.execute("ALTER TABLE prediction_outcomes ADD COLUMN residual_alpha REAL NOT NULL DEFAULT 0.0")

        # raw_events additive columns (cursoring/processing support)
        raw_cols = self._table_columns("raw_events")
        if "ingested_at" not in raw_cols:
            self.conn.execute("ALTER TABLE raw_events ADD COLUMN ingested_at TEXT")

        # raw_event_queue might not exist in older DBs created before this table.
        # executescript already creates it for new DBs; this is just a safety net.
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_event_queue (
                tenant_id TEXT NOT NULL,
                raw_event_id TEXT NOT NULL,
                status TEXT NOT NULL,
                next_retry_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                processed_at TEXT,
                PRIMARY KEY (tenant_id, raw_event_id)
            )
            """
        )

    def _payload(self, obj: Any) -> Mapping[str, Any]:
        if is_dataclass(obj):
            return asdict(obj)
        if isinstance(obj, Mapping):
            return obj
        raise TypeError(f"Unsupported payload type: {type(obj)!r}")

    def persist_strategy(self, strategy: Any, tenant_id: str = "default") -> None:
        payload = self._payload(strategy)
        self.execute(
            """
            INSERT OR REPLACE INTO strategies (id, tenant_id, name, version, strategy_type, mode, active, config_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"], tenant_id, payload["name"], payload["version"], payload["strategy_type"],
                payload["mode"], 1 if payload.get("active", True) else 0, json.dumps(payload["config"]),
            ),
        )

    def set_strategy_active(self, strategy_id: str, active: bool, tenant_id: str = "default") -> None:
        self.execute(
            "UPDATE strategies SET active = ? WHERE tenant_id = ? AND id = ?",
            (1 if active else 0, tenant_id, strategy_id),
        )

    def persist_prediction(self, prediction: Any, tenant_id: str = "default") -> None:
        payload = self._payload(prediction)
        feature_snapshot = payload.get("feature_snapshot", {})
        if not isinstance(feature_snapshot, Mapping):
            feature_snapshot = {}
        regime = feature_snapshot.get("regime")
        trend_strength = feature_snapshot.get("trend_strength")
        self.execute(
            """
            INSERT OR REPLACE INTO predictions (id, tenant_id, strategy_id, scored_event_id, ticker, timestamp, prediction, confidence, horizon, entry_price, mode, feature_snapshot_json, regime, trend_strength)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"], tenant_id, payload["strategy_id"], payload["scored_event_id"], payload["ticker"],
                str(payload["timestamp"]), payload["prediction"], payload["confidence"], payload["horizon"],
                payload["entry_price"], payload["mode"], json.dumps(payload.get("feature_snapshot", {})),
                None if regime is None else str(regime),
                None if trend_strength is None else str(trend_strength),
            ),
        )

    def persist_raw_event(self, raw: Any, tenant_id: str = "default") -> None:
        payload = self._payload(raw)
        now = self.now_iso().replace("+00:00", "Z")
        ts = payload.get("timestamp")
        if hasattr(ts, "isoformat"):
            ts = ts.isoformat()
        ts = str(ts).replace("+00:00", "Z")
        self.execute(
            """
            INSERT OR REPLACE INTO raw_events (id, tenant_id, timestamp, source, text, tickers_json, metadata_json, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"],
                tenant_id,
                ts,
                payload["source"],
                payload["text"],
                json.dumps(payload.get("tickers", [])),
                json.dumps(payload.get("metadata", {})),
                now,
            ),
        )

        # Ensure the event enters the processing queue (idempotent).
        self.enqueue_raw_event(payload["id"], tenant_id=tenant_id)

    def persist_scored_event(self, scored: Any, tenant_id: str = "default", raw_event_id: str | None = None) -> None:
        payload = self._payload(scored)
        self.execute(
            """
            INSERT OR REPLACE INTO scored_events (id, tenant_id, raw_event_id, primary_ticker, category, materiality, direction, confidence, company_relevance, concept_tags_json, explanation_terms_json, scorer_version, taxonomy_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"],
                tenant_id,
                raw_event_id or payload.get("raw_event_id"),
                payload["primary_ticker"],
                payload["category"],
                payload["materiality"],
                payload["direction"],
                payload["confidence"],
                payload["company_relevance"],
                json.dumps(payload.get("concept_tags", [])),
                json.dumps(payload.get("explanation_terms", [])),
                payload.get("scorer_version", "v2"),
                payload.get("taxonomy_version", "v1"),
            ),
        )

    def persist_mra_outcome(self, mra: Any, tenant_id: str = "default", scored_event_id: str | None = None) -> None:
        payload = self._payload(mra)
        self.execute(
            """
            INSERT OR REPLACE INTO mra_outcomes (id, tenant_id, scored_event_id, return_1m, return_5m, return_15m, return_1h, volume_ratio, vwap_distance, range_expansion, continuation_slope, pullback_depth, mra_score, market_context_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"],
                tenant_id,
                scored_event_id or payload.get("scored_event_id"),
                payload["return_1m"],
                payload["return_5m"],
                payload["return_15m"],
                payload["return_1h"],
                payload["volume_ratio"],
                payload["vwap_distance"],
                payload["range_expansion"],
                payload["continuation_slope"],
                payload["pullback_depth"],
                payload["mra_score"],
                json.dumps(payload.get("market_context", {})),
            ),
        )

    def persist_outcome(self, outcome: Any, tenant_id: str = "default") -> None:
        payload = self._payload(outcome)
        self.execute(
            """
            INSERT OR REPLACE INTO prediction_outcomes (id, tenant_id, prediction_id, exit_price, return_pct, direction_correct, max_runup, max_drawdown, evaluated_at, exit_reason, residual_alpha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["id"], tenant_id, payload["prediction_id"], payload["exit_price"], payload["return_pct"],
                1 if payload["direction_correct"] else 0, payload["max_runup"], payload["max_drawdown"],
                str(payload["evaluated_at"]), payload.get("exit_reason", "horizon"), float(payload.get("residual_alpha", 0.0)),
            ),
        )

    def upsert_price_bar(
        self,
        ticker: str,
        timestamp: str,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        tenant_id: str = "default",
    ) -> None:
        self.execute(
            """
            INSERT OR REPLACE INTO price_bars (tenant_id, ticker, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (tenant_id, ticker, timestamp, open_price, high, low, close, volume),
        )

    def get_kv(self, key: str, tenant_id: str = "default") -> str | None:
        row = self.conn.execute(
            "SELECT v FROM system_kv WHERE tenant_id = ? AND k = ?",
            (tenant_id, key),
        ).fetchone()
        return None if row is None else str(row["v"])

    def set_kv(self, key: str, value: str, tenant_id: str = "default") -> None:
        now = self.now_iso().replace("+00:00", "Z")
        self.execute(
            "INSERT OR REPLACE INTO system_kv (tenant_id, k, v, updated_at) VALUES (?, ?, ?, ?)",
            (tenant_id, key, value, now),
        )

    def add_heartbeat(self, loop_type: str, status: str, notes: str | None = None, tenant_id: str = "default") -> str:
        hb_id = str(uuid4())
        now = self.now_iso().replace("+00:00", "Z")
        self.execute(
            "INSERT INTO loop_heartbeats (id, tenant_id, loop_type, status, notes, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (hb_id, tenant_id, loop_type, status, notes, now),
        )
        return hb_id

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def enqueue_raw_event(self, raw_event_id: str, tenant_id: str = "default") -> None:
        now = self.now_iso().replace("+00:00", "Z")
        self.execute(
            """
            INSERT OR IGNORE INTO raw_event_queue (tenant_id, raw_event_id, status, next_retry_at, last_error, created_at, updated_at, processed_at)
            VALUES (?, ?, 'NEW', NULL, NULL, ?, ?, NULL)
            """,
            (tenant_id, raw_event_id, now, now),
        )

    def set_raw_event_queue_status(
        self,
        raw_event_id: str,
        status: str,
        *,
        next_retry_at: str | None = None,
        last_error: str | None = None,
        processed_at: str | None = None,
        tenant_id: str = "default",
    ) -> None:
        now = self.now_iso().replace("+00:00", "Z")
        self.execute(
            """
            UPDATE raw_event_queue
               SET status = ?,
                   next_retry_at = ?,
                   last_error = ?,
                   processed_at = COALESCE(?, processed_at),
                   updated_at = ?
             WHERE tenant_id = ? AND raw_event_id = ?
            """,
            (status, next_retry_at, last_error, processed_at, now, tenant_id, raw_event_id),
        )

    def get_strategy_stability_score(self, strategy_id: str, tenant_id: str = "default") -> float | None:
        row = self.conn.execute(
            """
            SELECT stability_score
            FROM strategy_stability
            WHERE tenant_id = ? AND strategy_id = ?
            """,
            (tenant_id, strategy_id),
        ).fetchone()
        if row is None:
            return None
        try:
            return float(row["stability_score"])
        except (TypeError, ValueError):
            return None

    def upsert_strategy_state(
        self,
        *,
        strategy_id: str,
        track: str,
        status: str,
        parent_id: str | None = None,
        version: str | None = None,
        consecutive_bad_windows: int = 0,
        notes: str | None = None,
        tenant_id: str = "default",
    ) -> None:
        now = self.now_iso().replace("+00:00", "Z")
        existing = self.conn.execute(
            "SELECT created_at FROM strategy_state WHERE tenant_id = ? AND strategy_id = ?",
            (tenant_id, strategy_id),
        ).fetchone()
        created_at = str(existing["created_at"]) if existing is not None else now

        self.execute(
            """
            INSERT OR REPLACE INTO strategy_state
              (strategy_id, tenant_id, track, status, parent_id, version, consecutive_bad_windows, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                strategy_id,
                tenant_id,
                track,
                status,
                parent_id,
                version,
                int(consecutive_bad_windows),
                notes,
                created_at,
                now,
            ),
        )

    def add_promotion_event(
        self,
        *,
        strategy_id: str,
        track: str,
        action: str,
        parent_id: str | None = None,
        reason: str | None = None,
        gate_logs: dict | None = None,
        tenant_id: str = "default",
    ) -> str:
        event_id = str(uuid4())
        now = self.now_iso().replace("+00:00", "Z")
        self.execute(
            """
            INSERT INTO promotion_events
              (id, tenant_id, strategy_id, parent_id, track, action, reason, gate_logs_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                tenant_id,
                strategy_id,
                parent_id,
                track,
                action,
                reason,
                json.dumps(gate_logs or {}),
                now,
            ),
        )
        return event_id

    def close(self) -> None:
        self.conn.close()
