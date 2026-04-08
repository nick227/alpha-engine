from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

import pandas as pd

from app.core.types import MRAOutcome, Prediction, PredictionOutcome, RawEvent, ScoredEvent, StrategyConfig


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_json(value: Any, default: Any) -> Any:
    try:
        parsed = json.loads(str(value))
        return parsed
    except Exception:
        return default


DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    source TEXT NOT NULL,
    text TEXT NOT NULL,
    tickers_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_event_queue (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    raw_event_id TEXT NOT NULL,
    status TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    processed_at TEXT,
    next_retry_at TEXT,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_event_queue_status
  ON raw_event_queue(tenant_id, status, next_retry_at, enqueued_at);

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

CREATE INDEX IF NOT EXISTS idx_scored_events_raw
  ON scored_events(tenant_id, raw_event_id);

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
    trend_strength TEXT,
    idempotency_key TEXT,
    scored_outcome_id TEXT,
    scored_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_idempotency
  ON predictions(tenant_id, idempotency_key);

CREATE INDEX IF NOT EXISTS idx_predictions_ticker_ts
  ON predictions(tenant_id, ticker, timestamp);

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
    exit_reason TEXT NOT NULL,
    residual_alpha REAL NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS price_bars (
    tenant_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    PRIMARY KEY (tenant_id, ticker, timeframe, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_price_bars_ticker_ts
  ON price_bars(tenant_id, ticker, timeframe, timestamp);

CREATE TABLE IF NOT EXISTS missing_price_context_events (
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    ticker TEXT,
    timestamp TEXT NOT NULL,
    reason TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_missing_price_context_ts
  ON missing_price_context_events(tenant_id, timestamp, reason);

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

CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    prediction_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    direction TEXT NOT NULL,
    confidence REAL NOT NULL,
    track TEXT NOT NULL,
    regime TEXT,
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_prediction_id
  ON signals(tenant_id, prediction_id);

CREATE INDEX IF NOT EXISTS idx_signals_ticker_ts
  ON signals(tenant_id, ticker, timestamp);

CREATE INDEX IF NOT EXISTS idx_signals_track_ts
  ON signals(tenant_id, track, timestamp);

CREATE TABLE IF NOT EXISTS consensus_signals (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    ticker TEXT NOT NULL,
    regime TEXT,
    direction TEXT,
    confidence REAL,
    total_weight REAL,
    participating_strategies INTEGER,
    sentiment_strategy_id TEXT,
    quant_strategy_id TEXT,
    sentiment_score REAL,
    quant_score REAL,
    ws REAL,
    wq REAL,
    agreement_bonus REAL,
    p_final REAL,
    stability_score REAL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS strategy_weights (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    win_rate REAL NOT NULL,
    alpha REAL NOT NULL,
    stability REAL NOT NULL,
    confidence_weight REAL NOT NULL,
    regime_strength_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_weights_strategy
  ON strategy_weights(tenant_id, strategy_id);
"""


class Repository:
    """
    SQLite repository used by engine loops and tests.

    This is intentionally lightweight and uses JSON blobs for nested structures.
    """

    def __init__(self, db_path: str | Path = "data/alpha.db") -> None:
        self.db_path = str(db_path)
        if self.db_path not in (":memory:", "") and not self.db_path.startswith("file:"):
            p = Path(self.db_path)
            if p.parent and str(p.parent) not in (".", ""):
                p.parent.mkdir(parents=True, exist_ok=True)

        # autocommit by default; `transaction()` provides explicit boundaries.
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._configure()
        self._ensure_schema()

    def _configure(self) -> None:
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=OFF;")
        self.conn.execute("PRAGMA busy_timeout=3000;")

    def _ensure_schema(self) -> None:
        # Preflight: if a legacy `price_bars` table exists without `timeframe`, migrate it before applying DB_SCHEMA.
        # DB_SCHEMA creates an index that references `timeframe`, so applying it first would crash on older DBs.
        try:
            bar_cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(price_bars)").fetchall()}
            if bar_cols and "timeframe" not in bar_cols:
                with self.transaction():
                    self.conn.execute("ALTER TABLE price_bars RENAME TO price_bars_legacy;")
                    self.conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS price_bars (
                            tenant_id TEXT NOT NULL,
                            ticker TEXT NOT NULL,
                            timeframe TEXT NOT NULL,
                            timestamp TEXT NOT NULL,
                            open REAL NOT NULL,
                            high REAL NOT NULL,
                            low REAL NOT NULL,
                            close REAL NOT NULL,
                            volume REAL NOT NULL,
                            PRIMARY KEY (tenant_id, ticker, timeframe, timestamp)
                        );
                        """
                    )
                    self.conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_price_bars_ticker_ts ON price_bars(tenant_id, ticker, timeframe, timestamp);"
                    )
                    self.conn.execute(
                        """
                        INSERT OR REPLACE INTO price_bars
                          (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
                        SELECT
                          tenant_id, ticker, '1m' as timeframe, timestamp, open, high, low, close, volume
                        FROM price_bars_legacy
                        """
                    )
                    self.conn.execute("DROP TABLE price_bars_legacy;")
        except Exception:
            pass

        self.conn.executescript(DB_SCHEMA)

        # Additive schema guards for older DBs.

        pred_cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(predictions)").fetchall()}
        if "idempotency_key" not in pred_cols:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN idempotency_key TEXT;")
        if "scored_outcome_id" not in pred_cols:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN scored_outcome_id TEXT;")
        if "scored_at" not in pred_cols:
            self.conn.execute("ALTER TABLE predictions ADD COLUMN scored_at TEXT;")

        out_cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(prediction_outcomes)").fetchall()}
        if "residual_alpha" not in out_cols:
            self.conn.execute("ALTER TABLE prediction_outcomes ADD COLUMN residual_alpha REAL NOT NULL DEFAULT 0.0;")

        # consensus_signals may exist with a smaller schema; ensure additive columns exist.
        try:
            cs_cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(consensus_signals)").fetchall()}
            for col, ddl in (
                ("direction", "ALTER TABLE consensus_signals ADD COLUMN direction TEXT;"),
                ("confidence", "ALTER TABLE consensus_signals ADD COLUMN confidence REAL;"),
                ("total_weight", "ALTER TABLE consensus_signals ADD COLUMN total_weight REAL;"),
                ("participating_strategies", "ALTER TABLE consensus_signals ADD COLUMN participating_strategies INTEGER;"),
            ):
                if col not in cs_cols:
                    self.conn.execute(ddl)
        except Exception:
            pass

        # signals additive columns
        try:
            sig_cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(signals)").fetchall()}
            for col, ddl in (
                ("track", "ALTER TABLE signals ADD COLUMN track TEXT NOT NULL DEFAULT 'unknown';"),
                ("regime", "ALTER TABLE signals ADD COLUMN regime TEXT;"),
            ):
                if sig_cols and col not in sig_cols:
                    self.conn.execute(ddl)
        except Exception:
            pass

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def execute(self, sql: str, params: tuple | Mapping[str, Any] | None = None) -> None:
        if params is None:
            self.conn.execute(sql)
        else:
            self.conn.execute(sql, params)

    @contextmanager
    def transaction(self):
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def query_df(self, sql: str, params: tuple | None = None) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.conn, params=params or ())

    # --- Persistence helpers ---

    def persist_raw_event(self, raw: RawEvent, tenant_id: str = "default") -> None:
        tickers = raw.tickers if isinstance(raw.tickers, list) else list(raw.tickers or [])
        meta = raw.metadata if hasattr(raw, "metadata") else {}
        self.conn.execute(
            """
            INSERT OR REPLACE INTO raw_events
              (id, tenant_id, timestamp, source, text, tickers_json, metadata_json, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(raw.id),
                tenant_id,
                _isoz(raw.timestamp),
                str(raw.source),
                str(raw.text),
                json.dumps(list(tickers), sort_keys=True),
                json.dumps(dict(meta or {}), sort_keys=True),
                _isoz(datetime.now(timezone.utc)),
            ),
        )

    def persist_scored_event(self, scored: ScoredEvent, *, raw_event_id: str, tenant_id: str = "default") -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO scored_events
              (id, tenant_id, raw_event_id, primary_ticker, category, materiality, direction, confidence, company_relevance,
               concept_tags_json, explanation_terms_json, scorer_version, taxonomy_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(scored.id),
                tenant_id,
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

    def persist_mra_outcome(self, mra: MRAOutcome, *, scored_event_id: str, tenant_id: str = "default") -> None:
        mc = mra.market_context if hasattr(mra, "market_context") else {}
        self.conn.execute(
            """
            INSERT OR REPLACE INTO mra_outcomes
              (id, tenant_id, scored_event_id, return_1m, return_5m, return_15m, return_1h, volume_ratio,
               vwap_distance, range_expansion, continuation_slope, pullback_depth, mra_score, market_context_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(mra.id),
                tenant_id,
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
                json.dumps(dict(mc or {}), sort_keys=True),
            ),
        )

    def persist_strategy(self, cfg: StrategyConfig, tenant_id: str = "default") -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO strategies
              (id, tenant_id, name, version, strategy_type, mode, active, config_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(cfg.id),
                tenant_id,
                str(cfg.name),
                str(cfg.version),
                str(cfg.strategy_type),
                str(cfg.mode),
                1 if cfg.active else 0,
                json.dumps(cfg.config or {}, sort_keys=True),
            ),
        )

    def persist_prediction(self, pred: Prediction, tenant_id: str = "default") -> None:
        snap = pred.feature_snapshot
        if is_dataclass(snap):
            snap = asdict(snap)
        if snap is None:
            snap = {}
        idempotency_key = getattr(pred, "idempotency_key", None)
        try:
            idempotency_key_s = str(idempotency_key) if idempotency_key else None
        except Exception:
            idempotency_key_s = None

        self.conn.execute(
            """
            INSERT OR REPLACE INTO predictions
              (id, tenant_id, strategy_id, scored_event_id, ticker, timestamp, prediction, confidence, horizon,
               entry_price, mode, feature_snapshot_json, regime, trend_strength, idempotency_key, scored_outcome_id, scored_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(pred.id),
                tenant_id,
                str(pred.strategy_id),
                str(pred.scored_event_id),
                str(pred.ticker),
                _isoz(pred.timestamp),
                str(pred.prediction),
                float(pred.confidence),
                str(pred.horizon),
                float(pred.entry_price),
                str(pred.mode),
                json.dumps(dict(snap or {}), sort_keys=True),
                str(getattr(pred, "regime", None)) if getattr(pred, "regime", None) is not None else None,
                str(getattr(pred, "trend_strength", None)) if getattr(pred, "trend_strength", None) is not None else None,
                idempotency_key_s,
                str(getattr(pred, "scored_outcome_id", None)) if getattr(pred, "scored_outcome_id", None) is not None else None,
                str(getattr(pred, "scored_at", None)) if getattr(pred, "scored_at", None) is not None else None,
            ),
        )

    def persist_signal(
        self,
        *,
        prediction_id: str,
        strategy_id: str,
        ticker: str,
        timestamp: datetime,
        direction: str,
        confidence: float,
        track: str,
        regime: str | None,
        tenant_id: str = "default",
    ) -> None:
        """
        Persist a first-class signal row at emission time.

        This is a UI read-model convenience so the dashboard doesn't have to infer
        "signals" from the predictions table.
        """
        now = _isoz(datetime.now(timezone.utc))
        self.conn.execute(
            """
            INSERT OR REPLACE INTO signals
              (id, tenant_id, prediction_id, strategy_id, ticker, timestamp, direction, confidence, track, regime, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"sig_{prediction_id}",
                tenant_id,
                str(prediction_id),
                str(strategy_id),
                str(ticker),
                _isoz(timestamp),
                str(direction),
                float(confidence),
                str(track),
                regime,
                now,
            ),
        )

    def upsert_strategy_weight(
        self,
        *,
        strategy_id: str,
        win_rate: float,
        alpha: float,
        stability: float,
        confidence_weight: float,
        regime_strength_json: str,
        tenant_id: str = "default",
        updated_at: datetime | None = None,
    ) -> None:
        updated_at = updated_at or datetime.now(timezone.utc)
        self.conn.execute(
            """
            INSERT OR REPLACE INTO strategy_weights
              (id, tenant_id, strategy_id, win_rate, alpha, stability, confidence_weight, regime_strength_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"w_{strategy_id}",
                tenant_id,
                str(strategy_id),
                float(win_rate),
                float(alpha),
                float(stability),
                float(confidence_weight),
                str(regime_strength_json),
                _isoz(updated_at),
            ),
        )

    def persist_consensus_signal(
        self,
        *,
        prediction_id: str,
        ticker: str,
        timestamp: datetime,
        direction: str,
        confidence: float,
        regime: str | None,
        sentiment_strategy_id: str | None,
        quant_strategy_id: str | None,
        sentiment_score: float | None,
        quant_score: float | None,
        ws: float | None,
        wq: float | None,
        agreement_bonus: float | None,
        p_final: float | None,
        stability_score: float | None,
        tenant_id: str = "default",
    ) -> None:
        """
        Materialize a consensus read row for the UI.

        This is a pure write-through of the engine's consensus output, so the dashboard
        doesn't need to parse `predictions.feature_snapshot_json`.
        """
        total_weight = None
        if ws is not None and wq is not None:
            total_weight = float(ws) + float(wq)

        participating = 2
        self.conn.execute(
            """
            INSERT OR REPLACE INTO consensus_signals
              (id, tenant_id, ticker, regime, direction, confidence, total_weight, participating_strategies,
               sentiment_strategy_id, quant_strategy_id, sentiment_score, quant_score,
               ws, wq, agreement_bonus, p_final, stability_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"cs_{prediction_id}",
                tenant_id,
                str(ticker),
                regime,
                str(direction),
                float(confidence),
                float(total_weight) if total_weight is not None else None,
                int(participating),
                sentiment_strategy_id,
                quant_strategy_id,
                float(sentiment_score) if sentiment_score is not None else None,
                float(quant_score) if quant_score is not None else None,
                float(ws) if ws is not None else None,
                float(wq) if wq is not None else None,
                float(agreement_bonus) if agreement_bonus is not None else None,
                float(p_final) if p_final is not None else None,
                float(stability_score) if stability_score is not None else None,
                _isoz(timestamp),
            ),
        )

    def persist_outcome(self, out: PredictionOutcome, tenant_id: str = "default") -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO prediction_outcomes
              (id, tenant_id, prediction_id, exit_price, return_pct, direction_correct, max_runup, max_drawdown, evaluated_at, exit_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(out.id),
                tenant_id,
                str(out.prediction_id),
                float(out.exit_price),
                float(out.return_pct),
                1 if out.direction_correct else 0,
                float(out.max_runup),
                float(out.max_drawdown),
                _isoz(out.evaluated_at),
                str(out.exit_reason),
            ),
        )

    # --- Queue + bars ---

    def enqueue_raw_event(self, raw_event_id: str, tenant_id: str = "default") -> str:
        qid = str(uuid4())
        now = _isoz(datetime.now(timezone.utc))
        self.conn.execute(
            """
            INSERT OR REPLACE INTO raw_event_queue
              (id, tenant_id, raw_event_id, status, enqueued_at, processed_at, next_retry_at, last_error)
            VALUES (?, ?, ?, 'NEW', ?, NULL, NULL, NULL)
            """,
            (qid, tenant_id, str(raw_event_id), now),
        )
        return qid

    def set_raw_event_queue_status(
        self,
        raw_event_id: str,
        status: str,
        *,
        processed_at: str | None = None,
        next_retry_at: str | None = None,
        last_error: str | None = None,
        tenant_id: str = "default",
    ) -> None:
        row = self.conn.execute(
            "SELECT id, enqueued_at FROM raw_event_queue WHERE tenant_id = ? AND raw_event_id = ? LIMIT 1",
            (tenant_id, str(raw_event_id)),
        ).fetchone()
        qid = str(row["id"]) if row is not None else str(uuid4())
        enqueued_at = str(row["enqueued_at"]) if row is not None else _isoz(datetime.now(timezone.utc))
        self.conn.execute(
            """
            INSERT OR REPLACE INTO raw_event_queue
              (id, tenant_id, raw_event_id, status, enqueued_at, processed_at, next_retry_at, last_error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (qid, tenant_id, str(raw_event_id), str(status), enqueued_at, processed_at, next_retry_at, last_error),
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
        *,
        timeframe: str = "1m",
        tenant_id: str = "default",
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                str(ticker),
                str(timeframe).strip().lower(),
                str(timestamp),
                float(open_price),
                float(high),
                float(low),
                float(close),
                float(volume),
            ),
        )

    def persist_price_bars(self, rows: list[tuple]) -> None:
        """
        Bulk upsert into price_bars.

        Row shape:
          (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
        """
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def persist_missing_price_context_events(self, rows: list[tuple]) -> None:
        """
        Bulk upsert into missing_price_context_events.

        Row shape:
          (tenant_id, event_id, ticker, timestamp, reason, observed_at)
        """
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO missing_price_context_events
              (tenant_id, event_id, ticker, timestamp, reason, observed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    # --- KV + heartbeats + strategy state ---

    def get_kv(self, key: str, tenant_id: str = "default") -> str | None:
        row = self.conn.execute(
            "SELECT v FROM system_kv WHERE tenant_id = ? AND k = ?",
            (tenant_id, str(key)),
        ).fetchone()
        return None if row is None else str(row["v"])

    def set_kv(self, key: str, value: str, tenant_id: str = "default") -> None:
        now = _isoz(datetime.now(timezone.utc))
        self.conn.execute(
            "INSERT OR REPLACE INTO system_kv (tenant_id, k, v, updated_at) VALUES (?, ?, ?, ?)",
            (tenant_id, str(key), str(value), now),
        )

    def add_heartbeat(self, loop_type: str, status: str, notes: str | None = None, tenant_id: str = "default") -> str:
        hb_id = str(uuid4())
        now = _isoz(datetime.now(timezone.utc))
        self.conn.execute(
            "INSERT INTO loop_heartbeats (id, tenant_id, loop_type, status, notes, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (hb_id, tenant_id, str(loop_type), str(status), notes, now),
        )
        return hb_id

    def get_strategy_stability_score(self, strategy_id: str, tenant_id: str = "default") -> float | None:
        row = self.conn.execute(
            "SELECT stability_score FROM strategy_stability WHERE tenant_id = ? AND strategy_id = ?",
            (tenant_id, str(strategy_id)),
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
        now = _isoz(datetime.now(timezone.utc))
        existing = self.conn.execute(
            "SELECT created_at FROM strategy_state WHERE tenant_id = ? AND strategy_id = ?",
            (tenant_id, str(strategy_id)),
        ).fetchone()
        created_at = str(existing["created_at"]) if existing is not None else now
        self.conn.execute(
            """
            INSERT OR REPLACE INTO strategy_state
              (strategy_id, tenant_id, track, status, parent_id, version, consecutive_bad_windows, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(strategy_id),
                tenant_id,
                str(track),
                str(status),
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
        parent_id: str | None,
        track: str,
        action: str,
        reason: str | None = None,
        gate_logs: Mapping[str, Any] | None = None,
        tenant_id: str = "default",
    ) -> str:
        event_id = str(uuid4())
        now = _isoz(datetime.now(timezone.utc))
        self.conn.execute(
            """
            INSERT INTO promotion_events (id, tenant_id, strategy_id, parent_id, track, action, reason, gate_logs_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                tenant_id,
                str(strategy_id),
                parent_id,
                str(track),
                str(action),
                reason,
                json.dumps(dict(gate_logs or {}), sort_keys=True) if gate_logs is not None else None,
                now,
            ),
        )
        return event_id

    def set_strategy_active(self, strategy_id: str, active: bool, tenant_id: str = "default") -> None:
        self.conn.execute(
            "UPDATE strategies SET active = ? WHERE tenant_id = ? AND id = ?",
            (1 if active else 0, tenant_id, str(strategy_id)),
        )
