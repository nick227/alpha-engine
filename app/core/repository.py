"""
Repository facade for the engine runtime and unit tests.

`app.db.repository.AlphaRepository` provides the bulk of the SQLite schema and a large
set of persistence helpers. The engine runtime (and unit tests) expect a smaller,
opinionated API surface (transaction handling, KV store, raw event queue, etc.).

This module defines that runtime `Repository` by extending `AlphaRepository` with
the missing primitives.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from app.core.repository_interface import PriceRepository, SignalRepository
from app.core.types import MRAOutcome, Prediction, PredictionOutcome, RawEvent, ScoredEvent, StrategyConfig
from app.db.repository import AlphaRepository
from app.ingest.replay_engine import upsert_loop_heartbeat

__all__ = ["Repository", "SignalRepository", "PriceRepository"]


def _isoz(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def _strategy_track(strategy_type: str) -> str:
    st = str(strategy_type).lower()
    if st.startswith("text_") or st.startswith("sentiment"):
        return "sentiment"
    if st.startswith("technical_") or st.startswith("baseline_") or st.startswith("quant"):
        return "quant"
    if st == "consensus":
        return "consensus"
    return "unknown"


class Repository(AlphaRepository):
    """
    Engine/runtime repository wrapper around the SQLite database.

    Supports:
    - nested transactions via SAVEPOINTs
    - raw event queue helpers (for live loop)
    - simple KV store (for champion selection, cursors, etc.)
    """

    def __init__(self, db_path: str | Path = "data/alpha.db") -> None:
        super().__init__(db_path=db_path)
        self._tx_depth = 0
        self._ensure_runtime_schema()

    def _ensure_runtime_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
              tenant_id TEXT NOT NULL DEFAULT 'default',
              k TEXT NOT NULL,
              v TEXT,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, k)
            );

            CREATE TABLE IF NOT EXISTS raw_event_queue (
              tenant_id TEXT NOT NULL DEFAULT 'default',
              raw_event_id TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'NEW',
              enqueued_at TEXT NOT NULL,
              processed_at TEXT,
              next_retry_at TEXT,
              last_error TEXT,
              PRIMARY KEY (tenant_id, raw_event_id)
            );

            CREATE INDEX IF NOT EXISTS idx_raw_event_queue_status
              ON raw_event_queue(tenant_id, status, COALESCE(next_retry_at, ''), enqueued_at);

            CREATE TABLE IF NOT EXISTS strategy_state (
              tenant_id TEXT NOT NULL DEFAULT 'default',
              strategy_id TEXT NOT NULL,
              track TEXT NOT NULL,
              status TEXT NOT NULL,
              parent_id TEXT,
              version TEXT,
              notes TEXT,
              consecutive_bad_windows INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, strategy_id)
            );

            CREATE INDEX IF NOT EXISTS idx_strategy_state_status
              ON strategy_state(tenant_id, status, track);

            CREATE TABLE IF NOT EXISTS feature_snapshot (
              symbol TEXT NOT NULL,
              as_of_date TEXT NOT NULL,
              close REAL,
              return_63d REAL,
              volatility_20d REAL,
              price_percentile_252d REAL,
              dollar_volume REAL,
              volume_zscore_20d REAL,
              PRIMARY KEY (symbol, as_of_date)
            );

            CREATE TABLE IF NOT EXISTS missing_price_context_events (
              tenant_id TEXT NOT NULL,
              raw_event_id TEXT NOT NULL,
              ticker TEXT,
              event_timestamp TEXT NOT NULL,
              reason TEXT NOT NULL,
              observed_at TEXT NOT NULL,
              PRIMARY KEY (tenant_id, raw_event_id, reason)
            );
            """
        )
        self.conn.commit()

    def execute(self, sql: str, params: tuple | list | None = None):
        if params is None:
            return self.conn.execute(sql)
        return self.conn.execute(sql, params)

    def query_df(self, sql: str, params: tuple | list | None = None):
        import pandas as pd

        if params is None:
            return pd.read_sql_query(sql, self.conn)
        return pd.read_sql_query(sql, self.conn, params=params)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Transaction context manager.

        Supports nested usage via SAVEPOINTs so engine components can be composed
        without worrying about outer transaction ownership.
        """
        if self._tx_depth == 0 and not self.conn.in_transaction:
            self._tx_depth = 1
            try:
                self.conn.execute("BEGIN")
                yield
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            finally:
                self._tx_depth = 0
            return

        self._tx_depth += 1
        sp = f"sp_{self._tx_depth}"
        try:
            self.conn.execute(f"SAVEPOINT {sp}")
            yield
            self.conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            self.conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            self.conn.execute(f"RELEASE SAVEPOINT {sp}")
            raise
        finally:
            self._tx_depth -= 1

    # -----------------
    # KV store
    # -----------------

    def get_kv(self, key: str, *, tenant_id: str = "default") -> str | None:
        row = self.conn.execute(
            "SELECT v FROM kv_store WHERE tenant_id = ? AND k = ?",
            (str(tenant_id), str(key)),
        ).fetchone()
        if row is None:
            return None
        v = row["v"]
        return None if v is None else str(v)

    def set_kv(self, key: str, value: str, *, tenant_id: str = "default") -> None:
        now = _isoz(datetime.now(timezone.utc))
        self.execute(
            "INSERT OR REPLACE INTO kv_store (tenant_id, k, v, updated_at) VALUES (?,?,?,?)",
            (str(tenant_id), str(key), str(value), now),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    # -----------------
    # Queue helpers
    # -----------------

    def enqueue_raw_event(self, raw_event_id: str, *, tenant_id: str = "default") -> None:
        now = _isoz(datetime.now(timezone.utc))
        self.execute(
            """
            INSERT OR IGNORE INTO raw_event_queue
              (tenant_id, raw_event_id, status, enqueued_at)
            VALUES (?, ?, 'NEW', ?)
            """,
            (str(tenant_id), str(raw_event_id), now),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def set_raw_event_queue_status(
        self,
        raw_event_id: str,
        status: str,
        *,
        tenant_id: str = "default",
        processed_at: str | None = None,
        next_retry_at: str | None = None,
        last_error: str | None = None,
    ) -> None:
        self.execute(
            """
            UPDATE raw_event_queue
            SET status = ?,
                processed_at = ?,
                next_retry_at = ?,
                last_error = ?
            WHERE tenant_id = ? AND raw_event_id = ?
            """,
            (str(status), processed_at, next_retry_at, last_error, str(tenant_id), str(raw_event_id)),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    # -----------------
    # Engine persistence helpers
    # -----------------

    def persist_strategy(self, cfg: StrategyConfig, *, tenant_id: str = "default") -> None:
        track = _strategy_track(cfg.strategy_type)
        self.execute(
            """
            INSERT OR REPLACE INTO strategies
              (id, tenant_id, track, parent_id, name, version, strategy_type, mode, active, config_json, status)
            VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, COALESCE((SELECT status FROM strategies WHERE tenant_id=? AND id=?), 'CANDIDATE'))
            """,
            (
                str(cfg.id),
                str(tenant_id),
                track,
                str(cfg.name),
                str(cfg.version),
                str(cfg.strategy_type),
                str(cfg.mode),
                1 if bool(cfg.active) else 0,
                json.dumps(dict(cfg.config or {}), sort_keys=True),
                str(tenant_id),
                str(cfg.id),
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def persist_raw_event(self, raw: RawEvent, *, tenant_id: str = "default") -> None:
        self.execute(
            """
            INSERT OR REPLACE INTO raw_events
              (id, tenant_id, timestamp, source, text, tickers_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(raw.id),
                str(tenant_id),
                _isoz(raw.timestamp),
                str(raw.source),
                str(raw.text),
                json.dumps(list(raw.tickers or []), sort_keys=True),
                json.dumps(dict(raw.metadata or {}), sort_keys=True),
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def persist_scored_event(self, scored: ScoredEvent, *, raw_event_id: str, tenant_id: str = "default") -> None:
        self.execute(
            """
            INSERT OR REPLACE INTO scored_events
              (id, tenant_id, raw_event_id, primary_ticker, category, materiality, direction, confidence, company_relevance,
               concept_tags_json, explanation_terms_json, scorer_version, taxonomy_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(scored.id),
                str(tenant_id),
                str(raw_event_id),
                str(scored.primary_ticker),
                str(scored.category),
                float(scored.materiality),
                str(scored.direction),
                float(scored.confidence),
                float(scored.company_relevance),
                json.dumps(list(scored.concept_tags or []), sort_keys=True),
                json.dumps(list(scored.explanation_terms or []), sort_keys=True),
                str(scored.scorer_version),
                str(scored.taxonomy_version),
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def persist_mra_outcome(self, mra: MRAOutcome, *, scored_event_id: str, tenant_id: str = "default") -> None:
        self.execute(
            """
            INSERT OR REPLACE INTO mra_outcomes
              (id, tenant_id, scored_event_id, return_1m, return_5m, return_15m, return_1h,
               volume_ratio, vwap_distance, range_expansion, continuation_slope, pullback_depth, mra_score, market_context_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(mra.id),
                str(tenant_id),
                str(scored_event_id),
                float(mra.return_1m),
                float(mra.return_5m),
                float(mra.return_15m),
                float(mra.return_1h),
                float(mra.volume_ratio),
                float(mra.vwap_distance),
                float(mra.range_expansion),
                float(mra.continuation_slope),
                float(mra.pullback_depth),
                float(mra.mra_score),
                json.dumps(dict(mra.market_context or {}), sort_keys=True),
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def persist_prediction(self, pred: Prediction, *, tenant_id: str = "default") -> None:
        self.execute(
            """
            INSERT OR REPLACE INTO predictions
              (id, tenant_id, strategy_id, scored_event_id, ticker, timestamp, prediction, confidence, horizon, entry_price,
               mode, feature_snapshot_json, regime, trend_strength)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(pred.id),
                str(tenant_id),
                str(pred.strategy_id),
                str(pred.scored_event_id),
                str(pred.ticker),
                _isoz(pred.timestamp),
                str(pred.prediction),
                float(pred.confidence),
                str(pred.horizon),
                float(pred.entry_price),
                str(pred.mode),
                json.dumps(dict(pred.feature_snapshot or {}), sort_keys=True),
                str(pred.regime) if pred.regime else None,
                str(pred.trend_strength) if pred.trend_strength else None,
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def persist_outcome(self, out: PredictionOutcome, *, tenant_id: str = "default") -> None:
        self.execute(
            """
            INSERT OR REPLACE INTO prediction_outcomes
              (id, tenant_id, prediction_id, exit_price, return_pct, direction_correct, max_runup, max_drawdown, evaluated_at, exit_reason, residual_alpha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(out.id),
                str(tenant_id),
                str(out.prediction_id),
                float(out.exit_price),
                float(out.return_pct),
                1 if bool(out.direction_correct) else 0,
                float(out.max_runup),
                float(out.max_drawdown),
                _isoz(out.evaluated_at),
                str(out.exit_reason),
                0.0,
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def get_strategy_stability_score(self, strategy_id: str, *, tenant_id: str = "default") -> float | None:
        row = self.conn.execute(
            """
            SELECT stability_score
            FROM strategy_stability
            WHERE tenant_id = ? AND strategy_id = ?
            """,
            (str(tenant_id), str(strategy_id)),
        ).fetchone()
        if row is None:
            return None
        try:
            return float(row["stability_score"])
        except Exception:
            return None

    def upsert_strategy_state(
        self,
        *,
        strategy_id: str,
        track: str,
        status: str,
        parent_id: str | None,
        consecutive_bad_windows: int = 0,
        version: str | None = None,
        notes: str | None = None,
        tenant_id: str = "default",
    ) -> None:
        now = _isoz(datetime.now(timezone.utc))
        self.execute(
            """
            INSERT OR REPLACE INTO strategy_state
              (tenant_id, strategy_id, track, status, parent_id, version, notes, consecutive_bad_windows, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(tenant_id),
                str(strategy_id),
                str(track),
                str(status),
                None if parent_id is None else str(parent_id),
                None if version is None else str(version),
                None if notes is None else str(notes),
                int(consecutive_bad_windows),
                now,
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def add_promotion_event(
        self,
        *,
        strategy_id: str,
        parent_id: str | None,
        track: str,
        action: str,
        reason: str,
        gate_logs: dict | None = None,
        tenant_id: str = "default",
    ) -> str:
        event_id = str(uuid4())
        meta: dict[str, Any] = {
            "parent_id": parent_id,
            "track": track,
            "action": action,
            "reason": reason,
        }
        if gate_logs:
            meta["gate_logs"] = gate_logs
        self.execute(
            """
            INSERT INTO promotion_events
              (id, tenant_id, strategy_id, prev_status, new_status, event_type, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                str(tenant_id),
                str(strategy_id),
                None if parent_id is None else str(parent_id),
                str(action),
                str(reason),
                json.dumps(meta, sort_keys=True),
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()
        return event_id

    # -----------------
    # Market data helper
    # -----------------

    def upsert_price_bar(
        self,
        ticker: str,
        timestamp: str,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        *,
        timeframe: str = "1m",
        tenant_id: str = "default",
    ) -> None:
        self.execute(
            """
            INSERT OR REPLACE INTO price_bars
              (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(tenant_id),
                str(ticker),
                str(timeframe),
                str(timestamp),
                float(open_price),
                float(high),
                float(low),
                float(close),
                float(volume),
            ),
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def persist_price_bars(self, rows: list[tuple]) -> None:
        """
        Bulk insert/replace into `price_bars`.

        Expected row shape:
          (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
        """
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO price_bars
              (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        if self._tx_depth == 0:
            self.conn.commit()

    def persist_missing_price_context_events(self, rows: list[tuple]) -> None:
        """
        Bulk insert/replace missing price context rows.

        Expected row shape:
          (tenant_id, raw_event_id, ticker, event_timestamp, reason, observed_at)
        """
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO missing_price_context_events
              (tenant_id, raw_event_id, ticker, event_timestamp, reason, observed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        if self._tx_depth == 0:
            self.conn.commit()

    # -----------------
    # Ops helper
    # -----------------

    def add_heartbeat(self, loop_type: str, status: str, notes: str | None = None, *, tenant_id: str = "default") -> str:
        run_id = f"{loop_type}:{datetime.now(timezone.utc).date().isoformat()}"
        hb_id = upsert_loop_heartbeat(
            self.conn,
            tenant_id=str(tenant_id),
            run_id=run_id,
            loop_type=str(loop_type),
            status=str(status),
            notes=notes,
        )
        if self._tx_depth == 0:
            self.conn.commit()
        return hb_id
