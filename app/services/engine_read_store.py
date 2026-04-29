from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChampionRow:
    strategy_id: str
    strategy_type: str
    version: str
    mode: str
    track: str
    win_rate: float
    alpha: float
    stability: float
    confidence_weight: float
    prediction_count: int

    regime_strength: dict[str, float] | None = None

@dataclass(frozen=True)
class ConsensusRow:
    ticker: str
    timestamp: str
    direction: str
    confidence: float
    total_weight: float
    participating_strategies: int
    active_regime: str | None
    high_vol_strength: float | None
    low_vol_strength: float | None
    trust: float | None = None  # exploratory (if present), else conservative


@dataclass(frozen=True)
class SignalRow:
    time: str
    ticker: str
    direction: str
    strategy: str
    regime: str | None
    confidence: float
    trust: float | None = None  # exploratory (if present), else conservative


@dataclass(frozen=True)
class RankingSnapshotRow:
    ticker: str
    score: float
    conviction: float
    attribution: dict[str, float]
    regime: str
    timestamp: str


@dataclass(frozen=True)
class StrategyEfficiencyRow:
    strategy_id: str
    strategy_version: str | None
    forecast_days: int | None
    samples: int
    total_forecast_days: int
    avg_efficiency_rating: float
    alpha_strategy: float = 0.0
    win_rate: float = 0.0
    avg_return: float = 0.0
    drawdown: float = 0.0
    stability: float = 0.0


@dataclass(frozen=True)
class PredictionRunRow:
    id: str
    ingress_start: str
    ingress_end: str
    prediction_start: str
    prediction_end: str
    timeframe: str
    regime: str | None
    created_at: str


@dataclass(frozen=True)
class SeriesPointRow:
    timestamp: str
    value: float


@dataclass(frozen=True)
class PredictionScoreRow:
    strategy_id: str
    strategy_version: str | None
    ticker: str
    timeframe: str
    regime: str | None
    forecast_days: int
    direction_hit_rate: float
    sync_rate: float
    total_return_actual: float
    total_return_pred: float
    total_return_error: float
    magnitude_error: float
    efficiency_rating: float
    alpha_prediction: float = 0.0
    attribution_json: str = "{}"


@dataclass(frozen=True)
class ChampionMatrixRow:
    """One cell in the strategy × ticker comparison matrix."""
    ticker: str
    timeframe: str
    forecast_days: int
    strategy_id: str
    regime: str
    alpha_strategy: float
    avg_pred_return: float
    avg_actual_return: float
    direction_accuracy: float
    entry_price: float | None
    samples: int


@dataclass(frozen=True)
class StrategyTimelineRow:
    """One historical run snapshot for a strategy/ticker — used for the autopsy timeline."""
    run_date: str
    ticker: str
    strategy_id: str
    prediction_start: str
    prediction_end: str
    forecast_days: int
    alpha_prediction: float
    total_return_pred: float
    total_return_actual: float
    direction_hit_rate: float
    entry_price: float | None
    target_price: float | None


@dataclass(frozen=True)
class LoopHealthRow:
    loop_type: str
    status: str
    last_heartbeat_at: str
    notes: str | None = None


@dataclass(frozen=True)
class LoopHealthSummary:
    last_write_at: str | None
    signal_rate_per_min: float | None
    consensus_rate_per_min: float | None
    learner_update_rate_per_min: float | None
    heartbeats: list[LoopHealthRow]


@dataclass(frozen=True)
class TradeRow:
    id: str
    tenant_id: str
    ticker: str
    direction: str
    quantity: float
    entry_price: float
    exit_price: float | None
    pnl: float | None
    status: str
    mode: str
    strategy_id: str | None
    timestamp: str
    analysis: str | None = None
    llm_prediction: str | None = None
    engine_decision: str | None = None
    llm_status: str | None = None
    llm_agrees: int | None = None


@dataclass(frozen=True)
class PositionRow:
    ticker: str
    tenant_id: str
    direction: str
    quantity: float
    average_entry_price: float
    mode: str


@dataclass(frozen=True)
class IngestRunSummaryRow:
    source_id: str
    windows: int
    complete_windows: int
    running_windows: int
    failed_windows: int
    empty_windows: int
    retries_total: int
    fetched_rows: int
    emitted_rows: int
    skipped_windows: int


@dataclass(frozen=True)
class IngestRunRow:
    source_id: str
    start_ts: str
    end_ts: str
    spec_hash: str
    provider: str
    status: str
    ok: int
    retry_count: int
    fetched_count: int
    emitted_count: int
    empty_count: int
    oldest_event_ts: str | None
    newest_event_ts: str | None
    last_error: str | None
    started_at: str
    completed_at: str | None
    updated_at: str


@dataclass(frozen=True)
class BackfillHorizonRow:
    source_id: str
    spec_hash: str
    backfilled_until_ts: str
    updated_at: str


@dataclass(frozen=True)
class MLTickerReadinessRow:
    symbol: str
    horizon: str
    train_rows_total: int
    train_rows_labeled: int
    label_null_rate: float
    coverage_p10: float | None
    coverage_median: float | None
    pct_coverage_ge_min: float | None
    n_features_est: int | None
    min_rows_required: int | None
    ready: bool
    top_blocker: str
    suggested_action: str
    suggested_action_kind: str


@dataclass(frozen=True)
class MLCoverageWindow:
    symbol: str
    horizon: str
    min_bad_date: str | None
    max_bad_date: str | None
    bad_rows: int
    total_rows: int


def _infer_track(strategy_type: str) -> str:
    st = str(strategy_type).strip().lower()
    if st.startswith("text_") or st.startswith("sentiment"):
        return "sentiment"
    if st.startswith("technical_") or st.startswith("baseline_") or st.startswith("quant"):
        return "quant"
    if st == "consensus":
        return "consensus"
    return "unknown"


def _confidence_weight(win_rate: float, stability: float) -> float:
    # Mirrors the intent of ContinuousLearner: win_rate * clamp(stability).
    stab = max(0.1, min(1.0, float(stability)))
    return float(win_rate) * stab


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


class EngineReadStore:
    """
    Read-only query layer over `alpha.db`.

    DashboardService should call into this store; Streamlit should call only DashboardService.
    """

    def __init__(self, db_path: str | Path = "data/alpha.db") -> None:
        self.db_path = str(db_path)
        self._local = threading.local()
        self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._local.conn.row_factory = sqlite3.Row
        self._consensus_has_horizon = False
        self._ensure_read_model_contract()

    @property
    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.db_path, check_same_thread=False)
            c.row_factory = sqlite3.Row
            self._local.conn = c
        return c

    def _ensure_read_model_contract(self) -> None:
        """
        Best-effort, additive schema alignment for UI read models.

        This does not create mock data; it only ensures optional columns exist so
        the UI can rely on a stable contract during rolling upgrades.
        """
        try:
            cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(consensus_signals)").fetchall()}
            for col, ddl in (
                ("horizon", "ALTER TABLE consensus_signals ADD COLUMN horizon TEXT;"),
                ("direction", "ALTER TABLE consensus_signals ADD COLUMN direction TEXT;"),
                ("confidence", "ALTER TABLE consensus_signals ADD COLUMN confidence REAL;"),
                ("total_weight", "ALTER TABLE consensus_signals ADD COLUMN total_weight REAL;"),
                ("participating_strategies", "ALTER TABLE consensus_signals ADD COLUMN participating_strategies INTEGER;"),
                ("weights_json", "ALTER TABLE consensus_signals ADD COLUMN weights_json TEXT;"),
                ("strategies_json", "ALTER TABLE consensus_signals ADD COLUMN strategies_json TEXT;"),
                ("trust_score", "ALTER TABLE consensus_signals ADD COLUMN trust_score REAL;"),
                ("trust_conservative", "ALTER TABLE consensus_signals ADD COLUMN trust_conservative REAL;"),
                ("trust_exploratory", "ALTER TABLE consensus_signals ADD COLUMN trust_exploratory REAL;"),
                ("trust_json", "ALTER TABLE consensus_signals ADD COLUMN trust_json TEXT;"),
                ("trust_updated_at", "ALTER TABLE consensus_signals ADD COLUMN trust_updated_at TEXT;"),
            ):
                if cols and col not in cols:
                    self.conn.execute(ddl)
            # Refresh after any ALTERs.
            cols2 = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(consensus_signals)").fetchall()}
            self._consensus_has_horizon = "horizon" in cols2
        except Exception:
            pass

        # Ensure the read-model tables exist (empty is fine; writers populate them).
        try:
            self.conn.execute(
                """
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
                """
            )
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_weights_strategy ON strategy_weights(tenant_id, strategy_id);"
            )
        except Exception:
            pass

        # Optional: prediction scoring read model (writers populate this).
        try:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS prediction_scores (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    alpha_version TEXT NOT NULL DEFAULT 'canonical_v1',
                    strategy_id TEXT NOT NULL,
                    strategy_version TEXT,
                    ticker TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    regime TEXT,
                    forecast_days INTEGER NOT NULL,
                    direction_hit_rate REAL NOT NULL,
                    sync_rate REAL NOT NULL,
                    total_return_actual REAL NOT NULL,
                    total_return_pred REAL NOT NULL,
                    total_return_error REAL NOT NULL,
                    magnitude_error REAL NOT NULL,
                    horizon_weight REAL NOT NULL,
                    efficiency_rating REAL NOT NULL,
                    attribution_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prediction_scores_rank ON prediction_scores(tenant_id, ticker, timeframe, efficiency_rating);"
            )
            
            # Additional prediction analytics tables
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS prediction_runs (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    ingress_start TEXT NOT NULL,
                    ingress_end TEXT NOT NULL,
                    prediction_start TEXT NOT NULL,
                    prediction_end TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    regime TEXT,
                    created_at TEXT NOT NULL
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS predicted_series_points (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    strategy_version TEXT,
                    ticker TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    value REAL NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS actual_series_points (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    value REAL NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        except Exception:
            pass

        # Additive alignment for scoring tables (if they pre-existed without new cols).
        try:
            cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(prediction_scores)").fetchall()}
            for col, ddl in (
                ("regime", "ALTER TABLE prediction_scores ADD COLUMN regime TEXT;"),
                ("alpha_version", "ALTER TABLE prediction_scores ADD COLUMN alpha_version TEXT NOT NULL DEFAULT 'canonical_v1';"),
                ("attribution_json", "ALTER TABLE prediction_scores ADD COLUMN attribution_json TEXT NOT NULL DEFAULT '{}';"),
            ):
                if cols and col not in cols:
                    self.conn.execute(ddl)
        except Exception:
            pass

        try:
            cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(prediction_runs)").fetchall()}
            if cols and "regime" not in cols:
                self.conn.execute("ALTER TABLE prediction_runs ADD COLUMN regime TEXT;")
        except Exception:
            pass

        try:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    prediction_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    horizon TEXT,
                    timestamp TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    predicted_return REAL NOT NULL DEFAULT 0.0,
                    trust_score REAL,
                    trust_json TEXT,
                    trust_updated_at TEXT,
                    track TEXT NOT NULL,
                    regime TEXT,
                    created_at TEXT NOT NULL
                );
                """
            )
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_ticker_ts ON signals(tenant_id, ticker, timestamp);")
        except Exception:
            pass

        try:
            cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(signals)").fetchall()}
            for col, ddl in (
                ("track", "ALTER TABLE signals ADD COLUMN track TEXT NOT NULL DEFAULT 'unknown';"),
                ("regime", "ALTER TABLE signals ADD COLUMN regime TEXT;"),
                ("horizon", "ALTER TABLE signals ADD COLUMN horizon TEXT;"),
                ("predicted_return", "ALTER TABLE signals ADD COLUMN predicted_return REAL NOT NULL DEFAULT 0.0;"),
                ("trust_score", "ALTER TABLE signals ADD COLUMN trust_score REAL;"),
                ("trust_conservative", "ALTER TABLE signals ADD COLUMN trust_conservative REAL;"),
                ("trust_exploratory", "ALTER TABLE signals ADD COLUMN trust_exploratory REAL;"),
                ("trust_json", "ALTER TABLE signals ADD COLUMN trust_json TEXT;"),
                ("trust_updated_at", "ALTER TABLE signals ADD COLUMN trust_updated_at TEXT;"),
            ):
                if cols and col not in cols:
                    self.conn.execute(ddl)
        except Exception:
            pass

        # Trades / positions are written by AlphaRepository (paper trading + sims).
        # Ensure tables exist so the UI can query them even on fresh DBs.
        try:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    ticker TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_price REAL,
                    pnl REAL,
                    status TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    strategy_id TEXT,
                    timestamp TEXT NOT NULL,
                    analysis TEXT,
                    llm_prediction TEXT,
                    engine_decision TEXT,
                    llm_status TEXT,
                    llm_agrees INTEGER,
                    prediction_id TEXT,
                    broker_order_id TEXT,
                    source TEXT
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    ticker TEXT NOT NULL,
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    direction TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    average_entry_price REAL NOT NULL,
                    mode TEXT NOT NULL,
                    PRIMARY KEY (ticker, tenant_id, mode)
                );
                """
            )

            cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(trades)").fetchall()}
            for col, ddl in (
                ("analysis", "ALTER TABLE trades ADD COLUMN analysis TEXT;"),
                ("llm_prediction", "ALTER TABLE trades ADD COLUMN llm_prediction TEXT;"),
                ("engine_decision", "ALTER TABLE trades ADD COLUMN engine_decision TEXT;"),
                ("llm_status", "ALTER TABLE trades ADD COLUMN llm_status TEXT;"),
                ("llm_agrees", "ALTER TABLE trades ADD COLUMN llm_agrees INTEGER;"),
                ("prediction_id", "ALTER TABLE trades ADD COLUMN prediction_id TEXT;"),
                ("broker_order_id", "ALTER TABLE trades ADD COLUMN broker_order_id TEXT;"),
                ("source", "ALTER TABLE trades ADD COLUMN source TEXT;"),
            ):
                if cols and col not in cols:
                    self.conn.execute(ddl)
        except Exception:
            pass

        # Ingestion tables are created by EventStore; ensure they exist for monitoring UIs.
        # This is safe and keeps the UI robust when alpha.db is newly created.
        try:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingest_runs (
                    source_id TEXT NOT NULL,
                    start_ts TEXT NOT NULL,
                    end_ts TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    status TEXT NOT NULL,
                    ok INTEGER NOT NULL,
                    retry_count INTEGER NOT NULL,
                    fetched_count INTEGER NOT NULL,
                    emitted_count INTEGER NOT NULL,
                    empty_count INTEGER NOT NULL,
                    oldest_event_ts TEXT,
                    newest_event_ts TEXT,
                    last_error TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_id, start_ts, end_ts, spec_hash)
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingest_run_stats (
                    source_id TEXT NOT NULL,
                    start_ts TEXT NOT NULL,
                    end_ts TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    request_hash TEXT,
                    request_cache_hit INTEGER NOT NULL,
                    response_fingerprint TEXT,
                    fetch_time_s REAL,
                    total_time_s REAL,
                    raw_rows_count INTEGER NOT NULL,
                    normalized_count INTEGER NOT NULL,
                    valid_count INTEGER NOT NULL,
                    bounded_count INTEGER NOT NULL,
                    dropped_empty_text INTEGER NOT NULL,
                    dropped_bad_timestamp INTEGER NOT NULL,
                    dropped_invalid_shape INTEGER NOT NULL,
                    dropped_out_of_bounds INTEGER NOT NULL,
                    dropped_duplicate INTEGER NOT NULL,
                    warnings_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (source_id, start_ts, end_ts, spec_hash)
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_horizons (
                    source_id TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    backfilled_until_ts TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_id, spec_hash)
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    ticker TEXT,
                    text TEXT,
                    tags TEXT,
                    weight REAL,
                    numeric_json TEXT,
                    created_at TEXT NOT NULL
                );
                """
            )
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ingest_runs_source_range ON ingest_runs(source_id, start_ts, end_ts);")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);")
        except Exception:
            pass

        # ML tables are created by AlphaRepository. Ensure the learning rows table exists so Ops can
        # report ML readiness even before any training run has been executed.
        try:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ml_learning_rows (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    features_json TEXT NOT NULL,
                    future_return REAL,
                    coverage_ratio REAL NOT NULL,
                    split TEXT NOT NULL DEFAULT 'train',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ml_rows_symbol_ts ON ml_learning_rows(tenant_id, symbol, horizon, timestamp);"
            )
        except Exception:
            pass

    def close(self) -> None:
        try:
            c = getattr(self._local, "conn", None)
            if c is not None:
                c.close()
                self._local.conn = None
        except Exception:
            pass

    def list_discovery_dates(self, *, tenant_id: str = "default", limit: int = 120) -> list[str]:
        try:
            rows = self.conn.execute(
                """
                SELECT DISTINCT as_of_date
                FROM discovery_candidates
                WHERE tenant_id = ?
                ORDER BY as_of_date DESC
                LIMIT ?
                """,
                (str(tenant_id), int(limit)),
            ).fetchall()
            return [str(r["as_of_date"]) for r in rows if r and r["as_of_date"] is not None]
        except Exception:
            return []

    def list_discovery_strategy_types(self, *, tenant_id: str = "default") -> list[str]:
        try:
            rows = self.conn.execute(
                """
                SELECT DISTINCT strategy_type
                FROM discovery_candidates
                WHERE tenant_id = ?
                ORDER BY strategy_type ASC
                """,
                (str(tenant_id),),
            ).fetchall()
            return [str(r["strategy_type"]) for r in rows if r and r["strategy_type"] is not None]
        except Exception:
            return []

    def list_discovery_candidates(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str,
        strategy_type: str | None = None,
        price_bucket: str | None = None,
        min_score: float = 0.0,
        symbol: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        try:
            where = ["tenant_id = ?", "as_of_date = ?", "score >= ?"]
            params: list[Any] = [str(tenant_id), str(as_of_date), float(min_score)]
            if strategy_type:
                where.append("strategy_type = ?")
                params.append(str(strategy_type))
            if symbol:
                where.append("symbol = ?")
                params.append(str(symbol).upper())
            sql = (
                "SELECT as_of_date, symbol, strategy_type, score, reason, metadata_json "
                "FROM discovery_candidates "
                f"WHERE {' AND '.join(where)} "
                "ORDER BY score DESC "
                "LIMIT ?"
            )
            params.append(int(limit))
            rows = self.conn.execute(sql, params).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                md_raw = str(r["metadata_json"] or "{}")
                md: dict[str, Any] = {}
                try:
                    md0 = json.loads(md_raw)
                    md = md0 if isinstance(md0, dict) else {}
                except Exception:
                    md = {}
                row = {
                    "as_of_date": str(r["as_of_date"]),
                    "symbol": str(r["symbol"]),
                    "strategy_type": str(r["strategy_type"]),
                    "score": float(r["score"] or 0.0),
                    "reason": str(r["reason"]),
                    "metadata_json": md_raw,
                    # Common metadata fields for easy table rendering
                    "close": md.get("close"),
                    "price_bucket": md.get("price_bucket"),
                    "avg_dollar_volume_20d": md.get("avg_dollar_volume_20d"),
                    "sector": md.get("sector"),
                    "industry": md.get("industry"),
                    "raw_score": md.get("raw_score"),
                }
                out.append(row)

            if price_bucket:
                pb = str(price_bucket)
                out = [x for x in out if str(x.get("price_bucket") or "") == pb]
            return out
        except Exception:
            return []

    def list_discovery_overlap(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str,
        min_strategies: int = 2,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """
        Symbols that appear in multiple discovery strategies on the same as_of_date.
        """
        try:
            rows = self.conn.execute(
                """
                SELECT
                  symbol,
                  COUNT(*) as strategy_count,
                  group_concat(strategy_type, ',') as strategies,
                  MAX(score) as max_score,
                  AVG(score) as avg_score
                FROM discovery_candidates
                WHERE tenant_id = ? AND as_of_date = ?
                GROUP BY symbol
                HAVING COUNT(*) >= ?
                ORDER BY strategy_count DESC, max_score DESC
                LIMIT ?
                """,
                (str(tenant_id), str(as_of_date), int(min_strategies), int(limit)),
            ).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "as_of_date": str(as_of_date),
                        "symbol": str(r["symbol"]),
                        "strategy_count": int(r["strategy_count"]),
                        "strategies": str(r["strategies"] or ""),
                        "max_score": float(r["max_score"] or 0.0),
                        "avg_score": float(r["avg_score"] or 0.0),
                    }
                )
            return out
        except Exception:
            return []

    def list_discovery_momentum(
        self,
        *,
        tenant_id: str = "default",
        end_date: str,
        window_days: int = 10,
        strategy_type: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """
        Candidate lifecycle stats over a trailing window ending at end_date.
        """
        try:
            end_d = str(end_date)
            rows = self.conn.execute(
                """
                SELECT
                  symbol,
                  COUNT(*) as appearances,
                  COUNT(DISTINCT as_of_date) as days_seen,
                  COUNT(DISTINCT strategy_type) as strategies_seen,
                  MIN(as_of_date) as first_seen,
                  MAX(as_of_date) as last_seen,
                  MAX(score) as max_score,
                  AVG(score) as avg_score
                FROM discovery_candidates
                WHERE tenant_id = ?
                  AND as_of_date <= ?
                  AND as_of_date >= date(?, '-' || (? - 1) || ' day')
                  AND (? IS NULL OR strategy_type = ?)
                GROUP BY symbol
                ORDER BY appearances DESC, max_score DESC
                LIMIT ?
                """,
                (
                    str(tenant_id),
                    end_d,
                    end_d,
                    int(window_days),
                    strategy_type,
                    strategy_type,
                    int(limit),
                ),
            ).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "end_date": end_d,
                        "symbol": str(r["symbol"]),
                        "appearances": int(r["appearances"]),
                        "days_seen": int(r["days_seen"]),
                        "strategies_seen": int(r["strategies_seen"]),
                        "first_seen": str(r["first_seen"]),
                        "last_seen": str(r["last_seen"]),
                        "max_score": float(r["max_score"] or 0.0),
                        "avg_score": float(r["avg_score"] or 0.0),
                    }
                )
            return out
        except Exception:
            return []

    def list_watchlist_dates(self, *, tenant_id: str = "default", limit: int = 120) -> list[str]:
        try:
            rows = self.conn.execute(
                """
                SELECT DISTINCT as_of_date
                FROM discovery_watchlist
                WHERE tenant_id = ?
                ORDER BY as_of_date DESC
                LIMIT ?
                """,
                (str(tenant_id), int(limit)),
            ).fetchall()
            return [str(r["as_of_date"]) for r in rows if r and r["as_of_date"] is not None]
        except Exception:
            return []

    def list_watchlist(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        try:
            rows = self.conn.execute(
                """
                SELECT as_of_date, symbol, overlap_count, days_seen, avg_score, playbook_id, prediction_plan_json, strategies_json, created_at
                FROM discovery_watchlist
                WHERE tenant_id = ? AND as_of_date = ?
                ORDER BY overlap_count DESC, days_seen DESC, avg_score DESC
                LIMIT ?
                """,
                (str(tenant_id), str(as_of_date), int(limit)),
            ).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append({k: r[k] for k in r.keys()})
            return out
        except Exception:
            return []

    def list_daily_top_picks(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str,
        limit: int = 15,
    ) -> list[dict[str, Any]]:
        """
        Human-consumable daily top picks derived from discovery_watchlist + discovery_candidates metadata.
        """
        try:
            wl = self.conn.execute(
                """
                SELECT symbol, overlap_count, days_seen, avg_score, playbook_id, prediction_plan_json, strategies_json
                FROM discovery_watchlist
                WHERE tenant_id = ? AND as_of_date = ?
                ORDER BY overlap_count DESC, days_seen DESC, avg_score DESC
                LIMIT ?
                """,
                (str(tenant_id), str(as_of_date), int(limit)),
            ).fetchall()
            if not wl:
                return []

            out: list[dict[str, Any]] = []
            for r in wl:
                symbol = str(r["symbol"]).upper()
                overlap_count = int(r["overlap_count"])
                days_seen = int(r["days_seen"])
                avg_score = float(r["avg_score"] or 0.0)
                playbook_id = str(r["playbook_id"] or "")

                strategies: list[str] = []
                try:
                    payload = json.loads(str(r["strategies_json"] or "[]"))
                    if isinstance(payload, list):
                        strategies = [str(x) for x in payload if str(x).strip()]
                except Exception:
                    strategies = []

                # Pull candidate rows for drivers/explanations.
                cand = self.conn.execute(
                    """
                    SELECT strategy_type, score, reason, metadata_json
                    FROM discovery_candidates
                    WHERE tenant_id = ? AND as_of_date = ? AND symbol = ?
                    ORDER BY score DESC
                    """,
                    (str(tenant_id), str(as_of_date), symbol),
                ).fetchall()

                driver_pool: list[str] = []
                reasons: list[str] = []
                for c in cand:
                    reasons.append(str(c["reason"] or ""))
                    try:
                        md = json.loads(str(c["metadata_json"] or "{}"))
                        if isinstance(md, dict):
                            drivers = md.get("drivers")
                            if isinstance(drivers, list):
                                for d in drivers:
                                    ds = str(d).strip()
                                    if ds:
                                        driver_pool.append(ds)
                    except Exception:
                        continue

                # Unique drivers, keep order.
                seen: set[str] = set()
                drivers_unique: list[str] = []
                for d in driver_pool:
                    if d in seen:
                        continue
                    seen.add(d)
                    drivers_unique.append(d)
                drivers = drivers_unique[:3]

                # Conviction buckets (simple, transparent).
                if overlap_count >= 3 and days_seen >= 3 and avg_score >= 0.90:
                    conviction = "HIGH"
                elif overlap_count >= 2 and days_seen >= 2:
                    conviction = "MEDIUM"
                else:
                    conviction = "LOW"

                # Action heuristic (keep simple; refine later).
                if playbook_id in {"distressed_repricer", "silent_compounder_trend_adoption", "narrative_lag_catchup"}:
                    side = "LONG"
                elif playbook_id in {"early_accumulation_breakout"}:
                    side = "WATCH"
                else:
                    side = "WATCH"

                why = " | ".join(drivers) if drivers else (reasons[0] if reasons and reasons[0] else "")
                out.append(
                    {
                        "as_of_date": str(as_of_date),
                        "symbol": symbol,
                        "side": side,
                        "conviction": conviction,
                        "playbook_id": playbook_id,
                        "overlap_count": overlap_count,
                        "days_seen": days_seen,
                        "avg_score": avg_score,
                        "strategies": ", ".join(strategies),
                        "why": why,
                    }
                )

            return out
        except Exception:
            return []

    def get_last_discovery_job(
        self,
        *,
        tenant_id: str = "default",
        job_type: str,
    ) -> dict[str, Any] | None:
        try:
            r = self.conn.execute(
                """
                SELECT id, job_type, status, started_at, completed_at, message
                FROM discovery_jobs
                WHERE tenant_id = ? AND job_type = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (str(tenant_id), str(job_type)),
            ).fetchone()
            return dict(r) if r else None
        except Exception:
            return None

    def list_watchlist_outcomes(
        self,
        *,
        tenant_id: str = "default",
        watchlist_date: str,
        horizon_days: int = 5,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        try:
            rows = self.conn.execute(
                """
                SELECT watchlist_date, symbol, horizon_days, entry_date, exit_date, entry_close, exit_close, return_pct,
                       overlap_count, days_seen, strategies_json, created_at
                FROM discovery_outcomes
                WHERE tenant_id = ? AND watchlist_date = ? AND horizon_days = ?
                ORDER BY return_pct DESC
                LIMIT ?
                """,
                (str(tenant_id), str(watchlist_date), int(horizon_days), int(limit)),
            ).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append({k: r[k] for k in r.keys()})
            return out
        except Exception:
            return []

    def list_discovery_stats(
        self,
        *,
        tenant_id: str = "default",
        end_date: str,
        window_days: int = 30,
        horizon_days: int = 5,
        group_type: str | None = None,
        latest_only: bool = True,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        try:
            where = ["tenant_id = ?", "end_date = ?", "window_days = ?", "horizon_days = ?"]
            params: list[Any] = [str(tenant_id), str(end_date), int(window_days), int(horizon_days)]
            if group_type:
                where.append("group_type = ?")
                params.append(str(group_type))
            if latest_only:
                where.append(
                    "computed_at = (SELECT MAX(computed_at) FROM discovery_stats WHERE tenant_id = ? AND end_date = ? AND window_days = ? AND horizon_days = ?)"
                )
                params.extend([str(tenant_id), str(end_date), int(window_days), int(horizon_days)])
            rows = self.conn.execute(
                f"""
                SELECT computed_at, end_date, window_days, horizon_days, group_type, group_value, n, avg_return, win_rate, lift, status
                FROM discovery_stats
                WHERE {' AND '.join(where)}
                ORDER BY computed_at DESC, group_type ASC, group_value ASC
                LIMIT ?
                """,
                (*params, int(limit)),
            ).fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append({k: r[k] for k in r.keys()})
            return out
        except Exception:
            return []

    def list_tenants(self) -> list[str]:
        for table in ("predictions", "strategies", "loop_heartbeats", "regime_performance", "trades", "positions"):
            try:
                rows = self.conn.execute(f"SELECT DISTINCT tenant_id FROM {table} ORDER BY tenant_id").fetchall()
                tenants = [str(r["tenant_id"]) for r in rows if str(r["tenant_id"]).strip()]
                if tenants:
                    return tenants
            except Exception:
                continue
        return ["default"]

    def list_tickers(self, *, tenant_id: str = "default") -> list[str]:
        try:
            rows = self.conn.execute(
                "SELECT DISTINCT ticker FROM predictions WHERE tenant_id = ? ORDER BY ticker",
                (tenant_id,),
            ).fetchall()
            tickers = [str(r["ticker"]) for r in rows if str(r["ticker"]).strip()]
            if tickers:
                return tickers
        except Exception:
            pass

        for table in ("trades", "positions"):
            try:
                rows = self.conn.execute(
                    f"SELECT DISTINCT ticker FROM {table} WHERE tenant_id = ? ORDER BY ticker",
                    (tenant_id,),
                ).fetchall()
                tickers = [str(r["ticker"]) for r in rows if str(r["ticker"]).strip()]
                if tickers:
                    return tickers
            except Exception:
                continue
        return []

    def list_positions(
        self,
        *,
        tenant_id: str = "default",
        mode: str | None = "paper",
        ticker: str | None = None,
    ) -> list[PositionRow]:
        where: list[str] = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if mode:
            where.append("mode = ?")
            params.append(mode)
        if ticker:
            where.append("ticker = ?")
            params.append(ticker)

        sql = (
            "SELECT ticker, tenant_id, direction, quantity, average_entry_price, mode "
            "FROM positions "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY ticker"
        )
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            return [
                PositionRow(
                    ticker=str(r["ticker"]),
                    tenant_id=str(r["tenant_id"]),
                    direction=str(r["direction"]),
                    quantity=float(r["quantity"]),
                    average_entry_price=float(r["average_entry_price"]),
                    mode=str(r["mode"]),
                )
                for r in rows
            ]
        except Exception:
            return []

    def list_trades(
        self,
        *,
        tenant_id: str = "default",
        mode: str | None = "paper",
        ticker: str | None = None,
        strategy_id: str | None = None,
        status: str | None = None,
        since_iso: str | None = None,
        limit: int = 250,
    ) -> list[TradeRow]:
        where: list[str] = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if mode:
            where.append("mode = ?")
            params.append(mode)
        if ticker:
            where.append("ticker = ?")
            params.append(ticker)
        if strategy_id:
            where.append("strategy_id = ?")
            params.append(strategy_id)
        if status:
            where.append("status = ?")
            params.append(status)
        if since_iso:
            where.append("timestamp >= ?")
            params.append(since_iso)

        sql = (
            "SELECT id, tenant_id, ticker, direction, quantity, entry_price, exit_price, pnl, "
            "status, mode, strategy_id, timestamp, analysis, llm_prediction, engine_decision, "
            "llm_status, llm_agrees "
            "FROM trades "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY timestamp DESC "
            "LIMIT ?"
        )
        params.append(int(limit))
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            out: list[TradeRow] = []
            for r in rows:
                out.append(
                    TradeRow(
                        id=str(r["id"]),
                        tenant_id=str(r["tenant_id"]),
                        ticker=str(r["ticker"]),
                        direction=str(r["direction"]),
                        quantity=float(r["quantity"]),
                        entry_price=float(r["entry_price"]),
                        exit_price=(float(r["exit_price"]) if r["exit_price"] is not None else None),
                        pnl=(float(r["pnl"]) if r["pnl"] is not None else None),
                        status=str(r["status"]),
                        mode=str(r["mode"]),
                        strategy_id=(str(r["strategy_id"]) if r["strategy_id"] is not None else None),
                        timestamp=str(r["timestamp"]),
                        analysis=(str(r["analysis"]) if r["analysis"] is not None else None),
                        llm_prediction=(str(r["llm_prediction"]) if r["llm_prediction"] is not None else None),
                        engine_decision=(str(r["engine_decision"]) if r["engine_decision"] is not None else None),
                        llm_status=(str(r["llm_status"]) if r["llm_status"] is not None else None),
                        llm_agrees=(int(r["llm_agrees"]) if r["llm_agrees"] is not None else None),
                    )
                )
            return out
        except Exception:
            return []

    def get_latest_close_prices(
        self,
        *,
        tickers: list[str],
        tenant_id: str = "default",
        timeframe: str = "1d",
    ) -> dict[str, float]:
        if not tickers:
            return {}
        # sqlite doesn't support array binding; build placeholders safely.
        placeholders = ",".join(["?"] * len(tickers))
        params: list[Any] = [tenant_id, timeframe, *tickers]
        sql = f"""
        SELECT b.ticker as ticker, b.close as close
        FROM price_bars b
        JOIN (
          SELECT ticker, MAX(timestamp) as max_ts
          FROM price_bars
          WHERE tenant_id = ? AND timeframe = ? AND ticker IN ({placeholders})
          GROUP BY ticker
        ) m
        ON b.ticker = m.ticker AND b.timestamp = m.max_ts AND b.tenant_id = ? AND b.timeframe = ?
        """
        # Duplicate tenant/timeframe for the JOIN predicate params
        params2: list[Any] = [tenant_id, timeframe, *tickers, tenant_id, timeframe]
        try:
            rows = self.conn.execute(sql, tuple(params2)).fetchall()
            return {str(r["ticker"]): float(r["close"]) for r in rows if r["ticker"] is not None and r["close"] is not None}
        except Exception:
            return {}

    def summarize_ingest_runs(
        self,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
        source_id: str | None = None,
    ) -> list[IngestRunSummaryRow]:
        where: list[str] = []
        params: list[Any] = []
        if source_id:
            where.append("source_id = ?")
            params.append(str(source_id))
        if start_ts:
            where.append("end_ts > ?")
            params.append(str(start_ts))
        if end_ts:
            where.append("start_ts < ?")
            params.append(str(end_ts))

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
        SELECT
          source_id,
          COUNT(*) as windows,
          SUM(CASE WHEN status = 'complete' THEN 1 ELSE 0 END) as complete_windows,
          SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running_windows,
          SUM(CASE WHEN status LIKE 'failed%' THEN 1 ELSE 0 END) as failed_windows,
          SUM(empty_count) as empty_windows,
          SUM(retry_count) as retries_total,
          SUM(fetched_count) as fetched_rows,
          SUM(emitted_count) as emitted_rows,
          SUM(CASE WHEN status = 'complete' AND fetched_count = 0 AND last_error IS NOT NULL THEN 1 ELSE 0 END) as skipped_windows
        FROM ingest_runs
        {where_sql}
        GROUP BY source_id
        ORDER BY windows DESC, source_id ASC
        """
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            out: list[IngestRunSummaryRow] = []
            for r in rows:
                out.append(
                    IngestRunSummaryRow(
                        source_id=str(r["source_id"]),
                        windows=int(r["windows"] or 0),
                        complete_windows=int(r["complete_windows"] or 0),
                        running_windows=int(r["running_windows"] or 0),
                        failed_windows=int(r["failed_windows"] or 0),
                        empty_windows=int(r["empty_windows"] or 0),
                        retries_total=int(r["retries_total"] or 0),
                        fetched_rows=int(r["fetched_rows"] or 0),
                        emitted_rows=int(r["emitted_rows"] or 0),
                        skipped_windows=int(r["skipped_windows"] or 0),
                    )
                )
            return out
        except Exception:
            return []

    def list_recent_ingest_runs(
        self,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
        source_id: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[IngestRunRow]:
        where: list[str] = []
        params: list[Any] = []
        if source_id:
            where.append("source_id = ?")
            params.append(str(source_id))
        if status:
            where.append("status = ?")
            params.append(str(status))
        if start_ts:
            where.append("end_ts > ?")
            params.append(str(start_ts))
        if end_ts:
            where.append("start_ts < ?")
            params.append(str(end_ts))
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
        SELECT
          source_id, start_ts, end_ts, spec_hash, provider, status, ok,
          retry_count, fetched_count, emitted_count, empty_count,
          oldest_event_ts, newest_event_ts, last_error,
          started_at, completed_at, updated_at
        FROM ingest_runs
        {where_sql}
        ORDER BY updated_at DESC
        LIMIT ?
        """
        params.append(int(limit))
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            return [
                IngestRunRow(
                    source_id=str(r["source_id"]),
                    start_ts=str(r["start_ts"]),
                    end_ts=str(r["end_ts"]),
                    spec_hash=str(r["spec_hash"]),
                    provider=str(r["provider"]),
                    status=str(r["status"]),
                    ok=int(r["ok"] or 0),
                    retry_count=int(r["retry_count"] or 0),
                    fetched_count=int(r["fetched_count"] or 0),
                    emitted_count=int(r["emitted_count"] or 0),
                    empty_count=int(r["empty_count"] or 0),
                    oldest_event_ts=(str(r["oldest_event_ts"]) if r["oldest_event_ts"] is not None else None),
                    newest_event_ts=(str(r["newest_event_ts"]) if r["newest_event_ts"] is not None else None),
                    last_error=(str(r["last_error"]) if r["last_error"] is not None else None),
                    started_at=str(r["started_at"]),
                    completed_at=(str(r["completed_at"]) if r["completed_at"] is not None else None),
                    updated_at=str(r["updated_at"]),
                )
                for r in rows
            ]
        except Exception:
            return []

    def list_backfill_horizons(self, *, limit: int = 200) -> list[BackfillHorizonRow]:
        try:
            rows = self.conn.execute(
                """
                SELECT source_id, spec_hash, backfilled_until_ts, updated_at
                FROM backfill_horizons
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            return [
                BackfillHorizonRow(
                    source_id=str(r["source_id"]),
                    spec_hash=str(r["spec_hash"]),
                    backfilled_until_ts=str(r["backfilled_until_ts"]),
                    updated_at=str(r["updated_at"]),
                )
                for r in rows
            ]
        except Exception:
            return []

    def summarize_events_freshness(
        self,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if start_ts:
            where.append("timestamp >= ?")
            params.append(str(start_ts))
        if end_ts:
            where.append("timestamp < ?")
            params.append(str(end_ts))
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
        SELECT
          source as source_id,
          COUNT(*) as rows_count,
          MIN(timestamp) as min_ts,
          MAX(timestamp) as max_ts
        FROM events
        {where_sql}
        GROUP BY source
        ORDER BY max_ts DESC, rows_count DESC
        LIMIT ?
        """
        params.append(int(limit))
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            return [
                {
                    "source_id": str(r["source_id"]),
                    "rows": int(r["rows_count"] or 0),
                    "min_ts": (str(r["min_ts"]) if r["min_ts"] is not None else None),
                    "max_ts": (str(r["max_ts"]) if r["max_ts"] is not None else None),
                }
                for r in rows
            ]
        except Exception:
            return []

    def monthly_counts_price_bars(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str = "1d",
        start_ym: str,
        end_ym: str,
    ) -> dict[str, int]:
        where: list[str] = ["tenant_id = ?", "timeframe = ?"]
        params: list[Any] = [tenant_id, str(timeframe)]
        if ticker:
            where.append("ticker = ?")
            params.append(str(ticker))

        # YYYY-MM extracted from ISO strings (fast + works for Z/offset variants).
        where.append("substr(timestamp, 1, 7) >= ?")
        params.append(str(start_ym))
        where.append("substr(timestamp, 1, 7) <= ?")
        params.append(str(end_ym))

        sql = f"""
        SELECT substr(timestamp, 1, 7) as ym, COUNT(*) as n
        FROM price_bars
        WHERE {' AND '.join(where)}
        GROUP BY ym
        ORDER BY ym
        """
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            return {str(r["ym"]): int(r["n"] or 0) for r in rows if r["ym"] is not None}
        except Exception:
            return {}

    def monthly_counts_events(
        self,
        *,
        source_id: str | None = None,
        start_ym: str,
        end_ym: str,
    ) -> dict[str, int]:
        where: list[str] = []
        params: list[Any] = []
        if source_id:
            where.append("source = ?")
            params.append(str(source_id))
        where.append("substr(timestamp, 1, 7) >= ?")
        params.append(str(start_ym))
        where.append("substr(timestamp, 1, 7) <= ?")
        params.append(str(end_ym))

        sql = f"""
        SELECT substr(timestamp, 1, 7) as ym, COUNT(*) as n
        FROM events
        WHERE {' AND '.join(where)}
        GROUP BY ym
        ORDER BY ym
        """
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            return {str(r["ym"]): int(r["n"] or 0) for r in rows if r["ym"] is not None}
        except Exception:
            return {}

    def monthly_counts_ingest_runs(
        self,
        *,
        source_id: str | None = None,
        status: str | None = "complete",
        start_ym: str,
        end_ym: str,
    ) -> dict[str, int]:
        where: list[str] = []
        params: list[Any] = []
        if source_id:
            where.append("source_id = ?")
            params.append(str(source_id))
        if status:
            where.append("status = ?")
            params.append(str(status))
        where.append("substr(start_ts, 1, 7) >= ?")
        params.append(str(start_ym))
        where.append("substr(start_ts, 1, 7) <= ?")
        params.append(str(end_ym))

        sql = f"""
        SELECT substr(start_ts, 1, 7) as ym, COUNT(*) as n
        FROM ingest_runs
        WHERE {' AND '.join(where)}
        GROUP BY ym
        ORDER BY ym
        """
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            return {str(r["ym"]): int(r["n"] or 0) for r in rows if r["ym"] is not None}
        except Exception:
            return {}

    def list_ml_symbols(
        self,
        *,
        tenant_id: str = "default",
        horizon: str = "7d",
        limit: int = 400,
    ) -> list[str]:
        try:
            rows = self.conn.execute(
                """
                SELECT symbol, COUNT(*) as n
                FROM ml_learning_rows
                WHERE tenant_id = ? AND horizon = ? AND split = 'train'
                GROUP BY symbol
                ORDER BY n DESC, symbol ASC
                LIMIT ?
                """,
                (tenant_id, str(horizon), int(limit)),
            ).fetchall()
            return [str(r["symbol"]) for r in rows if r["symbol"]]
        except Exception:
            return []

    def summarize_ml_readiness_per_ticker(
        self,
        *,
        tenant_id: str = "default",
        horizon: str = "7d",
        start_date: str,
        end_date: str,
        min_feature_coverage: float = 0.8,
        sample_rows_for_feature_union: int = 1500,
        limit_symbols: int = 80,
        symbol_filter: str | None = None,
    ) -> list[MLTickerReadinessRow]:
        """
        ML readiness summary designed for Ops UI.

        This is "per-ticker readiness" even if current training is pooled.
        It answers: do we have enough labeled + covered rows to train a per-ticker model?
        """
        where = ["tenant_id = ?", "horizon = ?", "split = 'train'", "DATE(timestamp) >= ?", "DATE(timestamp) <= ?"]
        params: list[Any] = [tenant_id, str(horizon), str(start_date), str(end_date)]
        if symbol_filter:
            where.append("symbol = ?")
            params.append(str(symbol_filter))
            limit_symbols = 1

        # Aggregate counts + coverage stats.
        sql = f"""
        SELECT
          symbol,
          COUNT(*) as total_rows,
          SUM(CASE WHEN future_return IS NOT NULL THEN 1 ELSE 0 END) as labeled_rows,
          AVG(coverage_ratio) as avg_cov,
          MIN(coverage_ratio) as min_cov,
          MAX(coverage_ratio) as max_cov,
          SUM(CASE WHEN coverage_ratio >= ? THEN 1 ELSE 0 END) as cov_ge_min
        FROM ml_learning_rows
        WHERE {' AND '.join(where)}
        GROUP BY symbol
        ORDER BY total_rows DESC, symbol ASC
        LIMIT ?
        """
        params_cov = [*params, float(min_feature_coverage), int(limit_symbols)]
        try:
            rows = self.conn.execute(sql, tuple(params_cov)).fetchall()
        except Exception:
            return []

        out: list[MLTickerReadinessRow] = []
        for r in rows:
            symbol = str(r["symbol"])
            total_rows = int(r["total_rows"] or 0)
            labeled_rows = int(r["labeled_rows"] or 0)
            cov_ge_min = int(r["cov_ge_min"] or 0)

            label_null_rate = 0.0
            if total_rows > 0:
                label_null_rate = float(1.0 - (labeled_rows / total_rows))

            # Coverage quantiles (p10/median) for interpretability.
            cov_p10 = None
            cov_med = None
            pct_cov_ge_min = None
            try:
                cov_rows = self.conn.execute(
                    """
                    SELECT coverage_ratio
                    FROM ml_learning_rows
                    WHERE tenant_id = ? AND symbol = ? AND horizon = ? AND split = 'train'
                      AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
                    ORDER BY coverage_ratio ASC
                    """,
                    (tenant_id, symbol, str(horizon), str(start_date), str(end_date)),
                ).fetchall()
                cov_vals = [float(x["coverage_ratio"]) for x in cov_rows if x["coverage_ratio"] is not None]
                if cov_vals:
                    cov_vals.sort()
                    cov_p10 = float(cov_vals[int(0.10 * (len(cov_vals) - 1))])
                    cov_med = float(cov_vals[int(0.50 * (len(cov_vals) - 1))])
                    pct_cov_ge_min = float(cov_ge_min / len(cov_vals))
            except Exception:
                pass

            # Estimate feature dimensionality by union of keys in recent feature snapshots.
            n_features_est: int | None = None
            min_rows_required: int | None = None
            try:
                feat_rows = self.conn.execute(
                    """
                    SELECT features_json
                    FROM ml_learning_rows
                    WHERE tenant_id = ? AND symbol = ? AND horizon = ? AND split = 'train'
                      AND future_return IS NOT NULL
                      AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (
                        tenant_id,
                        symbol,
                        str(horizon),
                        str(start_date),
                        str(end_date),
                        int(sample_rows_for_feature_union),
                    ),
                ).fetchall()
                import json as _json

                keys: set[str] = set()
                for fr in feat_rows:
                    try:
                        payload = _json.loads(fr["features_json"])
                        if isinstance(payload, dict):
                            keys.update([str(k) for k in payload.keys()])
                    except Exception:
                        continue
                n_features_est = int(len(keys))
                if n_features_est > 0:
                    min_rows_required = int(max(50, n_features_est * 10))
            except Exception:
                pass

            # Determine readiness + top blocker (simple, actionable).
            blocker = ""
            ready = True
            suggested_action = "OK"
            suggested_kind = "none"

            if labeled_rows <= 0:
                ready = False
                blocker = "labels_missing (need price_bars for symbol+SPY entry/exit)"
                if total_rows > 0:
                    suggested_action = "Backfill missing price/bars data, then rebuild ML dataset"
                    suggested_kind = "backfill_missing_data"
                else:
                    suggested_action = "Build ML dataset (compute labels/features)"
                    suggested_kind = "build_ml_dataset"
            elif pct_cov_ge_min is not None and pct_cov_ge_min < 0.80:
                ready = False
                blocker = "low_feature_coverage (missing factor inputs)"
                suggested_action = "Backfill missing data (then rebuild ML dataset)"
                suggested_kind = "backfill_missing_data"
            elif min_rows_required is not None and labeled_rows < min_rows_required:
                ready = False
                blocker = f"insufficient_rows (need {min_rows_required}, have {labeled_rows})"
                suggested_action = "Extend history (older backfill), then rebuild ML dataset"
                suggested_kind = "extend_history"

            if not blocker:
                blocker = "ok"

            out.append(
                MLTickerReadinessRow(
                    symbol=symbol,
                    horizon=str(horizon),
                    train_rows_total=total_rows,
                    train_rows_labeled=labeled_rows,
                    label_null_rate=float(label_null_rate),
                    coverage_p10=cov_p10,
                    coverage_median=cov_med,
                    pct_coverage_ge_min=pct_cov_ge_min,
                    n_features_est=n_features_est,
                    min_rows_required=min_rows_required,
                    ready=bool(ready),
                    top_blocker=str(blocker),
                    suggested_action=str(suggested_action),
                    suggested_action_kind=str(suggested_kind),
                )
            )

        return out

    def monthly_counts_ml_learning_rows(
        self,
        *,
        tenant_id: str = "default",
        symbol: str | None = None,
        horizon: str = "7d",
        labeled_only: bool = True,
        start_ym: str,
        end_ym: str,
    ) -> dict[str, int]:
        where: list[str] = ["tenant_id = ?", "horizon = ?", "split = 'train'"]
        params: list[Any] = [tenant_id, str(horizon)]
        if symbol:
            where.append("symbol = ?")
            params.append(str(symbol))
        if labeled_only:
            where.append("future_return IS NOT NULL")
        where.append("substr(timestamp, 1, 7) >= ?")
        params.append(str(start_ym))
        where.append("substr(timestamp, 1, 7) <= ?")
        params.append(str(end_ym))

        sql = f"""
        SELECT substr(timestamp, 1, 7) as ym, COUNT(*) as n
        FROM ml_learning_rows
        WHERE {' AND '.join(where)}
        GROUP BY ym
        ORDER BY ym
        """
        try:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
            return {str(r["ym"]): int(r["n"] or 0) for r in rows if r["ym"] is not None}
        except Exception:
            return {}

    def ml_dataset_state(
        self,
        *,
        tenant_id: str = "backfill",
        horizon: str = "7d",
        start_date: str,
        end_date: str,
        symbol: str | None = None,
    ) -> dict[str, int]:
        where = ["tenant_id = ?", "horizon = ?", "split = 'train'", "DATE(timestamp) >= ?", "DATE(timestamp) <= ?"]
        params: list[Any] = [tenant_id, str(horizon), str(start_date), str(end_date)]
        if symbol:
            where.append("symbol = ?")
            params.append(str(symbol))
        sql = f"""
        SELECT
          COUNT(*) as total_rows,
          SUM(CASE WHEN future_return IS NOT NULL THEN 1 ELSE 0 END) as labeled_rows
        FROM ml_learning_rows
        WHERE {' AND '.join(where)}
        """
        try:
            r = self.conn.execute(sql, tuple(params)).fetchone()
            if not r:
                return {"total_rows": 0, "labeled_rows": 0}
            return {"total_rows": int(r["total_rows"] or 0), "labeled_rows": int(r["labeled_rows"] or 0)}
        except Exception:
            return {"total_rows": 0, "labeled_rows": 0}

    def ml_low_coverage_window(
        self,
        *,
        tenant_id: str = "backfill",
        symbol: str,
        horizon: str = "7d",
        start_date: str,
        end_date: str,
        min_feature_coverage: float = 0.8,
    ) -> MLCoverageWindow:
        """
        Identify the date window where coverage is below threshold.

        This is used to propose a targeted backfill-range fix.
        """
        try:
            r = self.conn.execute(
                """
                SELECT
                  MIN(DATE(timestamp)) as min_bad,
                  MAX(DATE(timestamp)) as max_bad,
                  COUNT(*) as bad_rows
                FROM ml_learning_rows
                WHERE tenant_id = ? AND symbol = ? AND horizon = ? AND split = 'train'
                  AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
                  AND coverage_ratio < ?
                """,
                (
                    str(tenant_id),
                    str(symbol),
                    str(horizon),
                    str(start_date),
                    str(end_date),
                    float(min_feature_coverage),
                ),
            ).fetchone()
            t = self.conn.execute(
                """
                SELECT COUNT(*) as total_rows
                FROM ml_learning_rows
                WHERE tenant_id = ? AND symbol = ? AND horizon = ? AND split = 'train'
                  AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
                """,
                (str(tenant_id), str(symbol), str(horizon), str(start_date), str(end_date)),
            ).fetchone()
            min_bad = (str(r["min_bad"]) if r and r["min_bad"] is not None else None)
            max_bad = (str(r["max_bad"]) if r and r["max_bad"] is not None else None)
            bad_rows = int(r["bad_rows"] or 0) if r else 0
            total_rows = int(t["total_rows"] or 0) if t else 0
            return MLCoverageWindow(
                symbol=str(symbol),
                horizon=str(horizon),
                min_bad_date=min_bad,
                max_bad_date=max_bad,
                bad_rows=bad_rows,
                total_rows=total_rows,
            )
        except Exception:
            return MLCoverageWindow(
                symbol=str(symbol),
                horizon=str(horizon),
                min_bad_date=None,
                max_bad_date=None,
                bad_rows=0,
                total_rows=0,
            )

    def _strategy_metrics(self, *, tenant_id: str) -> list[ChampionRow]:
        """
        Produces one row per active strategy with joined performance + stability metrics.
        """
        rows = self.conn.execute(
            """
            SELECT
              s.id,
              s.strategy_type,
              s.version,
              s.mode,
              COALESCE(sp.prediction_count, 0) as prediction_count,
              COALESCE(sp.accuracy, 0.0) as win_rate,
              COALESCE(sp.avg_return, 0.0) as alpha,
              COALESCE(ss.stability_score, 0.0) as stability,
              sw.win_rate as sw_win_rate,
              sw.alpha as sw_alpha,
              sw.stability as sw_stability,
              sw.confidence_weight as sw_confidence_weight,
              sw.regime_strength_json as sw_regime_strength_json
            FROM strategies s
            LEFT JOIN strategy_performance sp
              ON sp.tenant_id = s.tenant_id
             AND sp.strategy_id = s.id
             AND sp.horizon = 'ALL'
            LEFT JOIN strategy_stability ss
              ON ss.tenant_id = s.tenant_id
             AND ss.strategy_id = s.id
            LEFT JOIN strategy_weights sw
              ON sw.tenant_id = s.tenant_id
             AND sw.strategy_id = s.id
            WHERE s.tenant_id = ?
              AND s.active = 1
            """,
            (tenant_id,),
        ).fetchall()

        out: list[ChampionRow] = []
        for r in rows:
            stype = str(r["strategy_type"])
            track = _infer_track(stype)
            # Prefer ContinuousLearner-derived weights if present.
            win_rate = float(r["sw_win_rate"]) if r["sw_win_rate"] is not None else float(r["win_rate"])
            alpha = float(r["sw_alpha"]) if r["sw_alpha"] is not None else float(r["alpha"])
            stability = float(r["sw_stability"]) if r["sw_stability"] is not None else float(r["stability"])
            confidence_weight = (
                float(r["sw_confidence_weight"])
                if r["sw_confidence_weight"] is not None
                else _confidence_weight(win_rate, stability)
            )
            regime_strength = None
            if r["sw_regime_strength_json"] is not None:
                try:
                    parsed = json.loads(str(r["sw_regime_strength_json"]))
                    regime_strength = dict(parsed) if isinstance(parsed, dict) else None
                except Exception:
                    regime_strength = None

            out.append(
                ChampionRow(
                    strategy_id=str(r["id"]),
                    strategy_type=stype,
                    version=str(r["version"]),
                    mode=str(r["mode"]),
                    track=track,
                    win_rate=win_rate,
                    alpha=alpha,
                    stability=stability,
                    confidence_weight=confidence_weight,
                    prediction_count=int(r["prediction_count"]),
                    regime_strength=regime_strength,
                )
            )
        return out

    @staticmethod
    def _rank_key(r: ChampionRow) -> tuple[float, float, float, int]:
        return (float(r.confidence_weight), float(r.stability), float(r.alpha), float(r.win_rate), int(r.prediction_count))

    def get_champions(self, *, tenant_id: str = "default", min_predictions: int = 5) -> dict[str, ChampionRow]:
        rows = [r for r in self._strategy_metrics(tenant_id=tenant_id) if r.track in ("sentiment", "quant")]
        out: dict[str, ChampionRow] = {}
        for track in ("sentiment", "quant"):
            pool = [r for r in rows if r.track == track]
            if not pool:
                continue
            eligible = [r for r in pool if r.prediction_count >= int(min_predictions)]
            ranked_pool = eligible if eligible else pool
            out[track] = max(ranked_pool, key=self._rank_key)
        return out

    def get_challengers(self, *, tenant_id: str = "default", min_predictions: int = 5) -> dict[str, ChampionRow]:
        rows = [r for r in self._strategy_metrics(tenant_id=tenant_id) if r.track in ("sentiment", "quant")]
        out: dict[str, ChampionRow] = {}
        for track in ("sentiment", "quant"):
            pool = [r for r in rows if r.track == track]
            if not pool:
                continue
            eligible = [r for r in pool if r.prediction_count >= int(min_predictions)]
            ranked_pool = eligible if eligible else pool
            ranked = sorted(ranked_pool, key=self._rank_key, reverse=True)
            if len(ranked) >= 2:
                out[track] = ranked[1]
        return out

    def _regime_strengths(self, *, tenant_id: str) -> dict[str, float]:
        """
        Best-effort regime strength map from `regime_performance.accuracy` (win rate).
        """
        try:
            rows = self.conn.execute(
                """
                SELECT regime, accuracy
                FROM regime_performance
                WHERE tenant_id = ?
                """,
                (tenant_id,),
            ).fetchall()
        except Exception:
            return {}
        out: dict[str, float] = {}
        for r in rows:
            out[str(r["regime"])] = float(r["accuracy"] or 0.0)
        return out

    def _champion_regime_strength(self, *, tenant_id: str) -> tuple[float | None, float | None]: 
        try:
            champs = self.get_champions(tenant_id=tenant_id, min_predictions=0)
        except Exception:
            return None, None

        strengths: list[dict[str, float]] = []
        for r in champs.values():
            if r.regime_strength:
                strengths.append(r.regime_strength)

        def pick(keys: list[str]) -> float | None:
            vals: list[float] = []
            for s in strengths:
                for k in keys:
                    if k in s:
                        try:
                            vals.append(float(s[k]))
                        except Exception:
                            continue
            if not vals:
                return None
            return max(vals)

        high = pick(["HIGH", "VolatilityRegime.HIGH", "HIGH_VOL"])
        low = pick(["LOW", "VolatilityRegime.LOW", "LOW_VOL"])
        return high, low

    def get_latest_consensus(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        horizon: str | None = None,
    ) -> ConsensusRow | None:
        # Prefer materialized consensus_signals (real output fields), fallback to predictions.
        row = None
        try:
            if self._has_column("consensus_signals", "trust_exploratory"):
                trust_sql = "trust_exploratory as trust_score"
            elif self._has_column("consensus_signals", "trust_score"):
                trust_sql = "trust_score as trust_score"
            else:
                trust_sql = "NULL as trust_score"
            where = "WHERE tenant_id = ? AND ticker = ?"
            params: list[Any] = [tenant_id, str(ticker)]
            if horizon and self._consensus_has_horizon:
                where += " AND horizon = ?"
                params.append(str(horizon))
            row = self.conn.execute(
                f"""
                SELECT ticker, created_at as timestamp, direction, confidence, total_weight, participating_strategies, regime, {trust_sql}
                FROM consensus_signals
                {where}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
        except Exception:
            row = None

        if row is None:
            try:
                sids = [
                    str(r["id"])
                    for r in self.conn.execute(
                        """
                        SELECT id FROM strategies
                        WHERE tenant_id = ? AND active = 1 AND LOWER(strategy_type) = 'consensus'
                        """,
                        (tenant_id,),
                    ).fetchall()
                ]
            except Exception:
                sids = []
            if not sids:
                return None

            placeholders = ",".join(["?"] * len(sids))
            row = self.conn.execute(
                f"""
                SELECT ticker, timestamp, prediction as direction, confidence, NULL as total_weight, NULL as participating_strategies, regime
                FROM predictions
                WHERE tenant_id = ?
                  AND ticker = ?
                  AND strategy_id IN ({placeholders})
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (tenant_id, str(ticker), *sids),
            ).fetchone()
            if row is None:
                return None

        # Row exists but has no meaningful content — treat as missing.
        if row["direction"] is None and row["confidence"] is None:
            return None

        # Prefer continuous-learning-derived regime strengths from champions, fallback to global regime_performance.
        high_strength, low_strength = self._champion_regime_strength(tenant_id=tenant_id)
        if high_strength is None or low_strength is None:
            strengths = self._regime_strengths(tenant_id=tenant_id)
            high_strength = high_strength if high_strength is not None else (strengths.get("HIGH") or strengths.get("VolatilityRegime.HIGH"))
            low_strength = low_strength if low_strength is not None else (strengths.get("LOW") or strengths.get("VolatilityRegime.LOW"))

        # Current engine consensus is dual-track, so weights sum to ~1 and participating strategies ~= 2.
        return ConsensusRow(
            ticker=str(row["ticker"]),
            timestamp=str(row["timestamp"]),
            direction=str(row["direction"]),
            confidence=float(row["confidence"] or 0.0),
            total_weight=float(row["total_weight"]) if row["total_weight"] is not None else 1.0,
            participating_strategies=int(row["participating_strategies"]) if row["participating_strategies"] is not None else 2,
            active_regime=str(row["regime"]) if row["regime"] is not None else None,
            high_vol_strength=float(high_strength) if high_strength is not None else None,
            low_vol_strength=float(low_strength) if low_strength is not None else None,
            trust=float(row["trust_score"]) if ("trust_score" in row.keys() and row["trust_score"] is not None) else None,
        )

    def get_latest_consensus_by_horizon(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        horizons: list[str],
    ) -> dict[str, ConsensusRow | None]:
        out: dict[str, ConsensusRow | None] = {}
        for h in horizons:
            out[str(h)] = self.get_latest_consensus(tenant_id=tenant_id, ticker=ticker, horizon=str(h))
        return out

    def get_recent_signals(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
        limit: int = 25,
    ) -> list[SignalRow]:
        limit = max(1, min(int(limit), 500))
        # Prefer first-class signals table; fallback to predictions if it's missing/empty.
        rows = []
        try:
            params: list[Any] = [tenant_id]
            where = ["sig.tenant_id = ?"]
            if ticker:
                where.append("sig.ticker = ?")
                params.append(str(ticker))

            if self._has_column("signals", "trust_exploratory"):
                trust_sql = "sig.trust_exploratory as trust_score"
            elif self._has_column("signals", "trust_score"):
                trust_sql = "sig.trust_score as trust_score"
            else:
                trust_sql = "NULL as trust_score"
            rows = self.conn.execute(
                f"""
                SELECT
                  sig.timestamp as timestamp,
                  sig.ticker as ticker,
                  sig.direction as prediction,
                  sig.confidence as confidence,
                  sig.regime as regime,
                  {trust_sql},
                  s.strategy_type as strategy_type,
                  s.version as version
                FROM signals sig
                LEFT JOIN strategies s
                  ON s.tenant_id = sig.tenant_id AND s.id = sig.strategy_id
                WHERE {' AND '.join(where)}
                ORDER BY sig.timestamp DESC
                LIMIT {limit}
                """,
                tuple(params),
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            params = [tenant_id]
            where = ["p.tenant_id = ?"]
            if ticker:
                where.append("p.ticker = ?")
                params.append(str(ticker))

            rows = self.conn.execute(
                f"""
                SELECT
                  p.timestamp,
                  p.ticker,
                  p.prediction,
                  p.confidence,
                  p.regime,
                  s.strategy_type,
                  s.version
                FROM predictions p
                LEFT JOIN strategies s
                  ON s.tenant_id = p.tenant_id AND s.id = p.strategy_id
                WHERE {' AND '.join(where)}
                ORDER BY p.timestamp DESC
                LIMIT {limit}
                """,
                tuple(params),
            ).fetchall()

        out: list[SignalRow] = []
        for r in rows:
            stype = str(r["strategy_type"] or "unknown")
            ver = str(r["version"] or "")
            out.append(
                SignalRow(
                    time=str(r["timestamp"]),
                    ticker=str(r["ticker"]),
                    direction=str(r["prediction"]),
                    confidence=float(r["confidence"] or 0.0),
                    strategy=f"{stype}:{ver}" if ver else stype,
                    regime=str(r["regime"]) if r["regime"] is not None else None,
                    trust=float(r["trust_score"]) if ("trust_score" in r.keys() and r["trust_score"] is not None) else None,
                )
            )
        return out

    def _has_column(self, table: str, column: str) -> bool:
        try:
            cols = {str(r["name"]) for r in self.conn.execute(f"PRAGMA table_info({table})").fetchall()}
            return column in cols
        except Exception:
            return False

    def _latest_write_ts(self, *, tenant_id: str) -> str | None:
        # Prefer system_kv if present (engine writes keys like `live:last_ts`).
        try:
            if self._has_column("system_kv", "k") and self._has_column("system_kv", "v"):
                row = self.conn.execute(
                    "SELECT v FROM system_kv WHERE tenant_id = ? AND k = 'live:last_ts' LIMIT 1",
                    (tenant_id,),
                ).fetchone()
                if row is not None and row["v"] is not None:
                    return str(row["v"])
        except Exception:
            pass

        try:
            row = self.conn.execute(
                "SELECT MAX(timestamp) AS ts FROM predictions WHERE tenant_id = ?",
                (tenant_id,),
            ).fetchone()
            return None if row is None or row["ts"] is None else str(row["ts"])
        except Exception:
            return None

    def _rate_per_min(self, timestamps: list[str], *, window_seconds: int = 300) -> float | None:
        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() - float(window_seconds)
        count = 0
        for ts in timestamps:
            dt = _parse_ts(ts)
            if dt is None:
                continue
            if dt.timestamp() >= cutoff:
                count += 1
        return count / (window_seconds / 60.0)

    def get_loop_health(self, *, tenant_id: str = "default") -> LoopHealthSummary:
        """
        Loop health read model:
        - last write timestamp
        - signal / consensus / learner update rates (best-effort)
        - latest heartbeat per loop type
        """
        try:
            rows = self.conn.execute(
                """
                SELECT loop_type, status, notes, created_at
                FROM loop_heartbeats
                WHERE tenant_id = ?
                ORDER BY created_at DESC
                LIMIT 200
                """,
                (tenant_id,),
            ).fetchall()
        except Exception:
            rows = []

        heartbeats: list[LoopHealthRow] = []
        seen: set[str] = set()
        for r in rows:
            lt = str(r["loop_type"])
            if lt in seen:
                continue
            seen.add(lt)
            heartbeats.append(
                LoopHealthRow(
                    loop_type=lt,
                    status=str(r["status"]),
                    last_heartbeat_at=str(r["created_at"]),
                    notes=str(r["notes"]) if r["notes"] is not None else None,
                )
            )

        # Rates are best-effort; if tables/joins aren't available, return None.
        signal_rate = None
        consensus_rate = None
        learner_rate = None

        # Signals: non-consensus predictions.
        try:
            pred_rows = self.conn.execute(
                """
                SELECT p.timestamp, COALESCE(s.strategy_type, '') as strategy_type
                FROM predictions p
                LEFT JOIN strategies s ON s.tenant_id = p.tenant_id AND s.id = p.strategy_id
                WHERE p.tenant_id = ?
                ORDER BY p.timestamp DESC
                LIMIT 1000
                """,
                (tenant_id,),
            ).fetchall()
            signal_ts = [str(r["timestamp"]) for r in pred_rows if str(r["strategy_type"]).strip().lower() != "consensus"]
            consensus_ts = [str(r["timestamp"]) for r in pred_rows if str(r["strategy_type"]).strip().lower() == "consensus"]
            signal_rate = self._rate_per_min(signal_ts)
            consensus_rate = self._rate_per_min(consensus_ts)
        except Exception:
            pass

        # Learner updates: approximate via strategy_performance.updated_at if available.
        try:
            if self._has_column("strategy_performance", "updated_at"):
                perf_rows = self.conn.execute(
                    """
                    SELECT updated_at
                    FROM strategy_performance
                    WHERE tenant_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 500
                    """,
                    (tenant_id,),
                ).fetchall()
                learner_rate = self._rate_per_min([str(r["updated_at"]) for r in perf_rows])
        except Exception:
            pass

        return LoopHealthSummary(
            last_write_at=self._latest_write_ts(tenant_id=tenant_id),
            signal_rate_per_min=signal_rate,
            consensus_rate_per_min=consensus_rate,
            learner_update_rate_per_min=learner_rate,
            heartbeats=heartbeats,
        )

    def get_latest_rankings(self, *, tenant_id: str = "default", limit: int = 20) -> list[RankingSnapshotRow]:
        try:
            # Find the latest sync timestamp.
            latest = self.conn.execute(
                "SELECT MAX(timestamp) as ts FROM ranking_snapshots WHERE tenant_id = ?",
                (tenant_id,),
            ).fetchone()
            if not latest or not latest["ts"]:
                return []
            
            ts = str(latest["ts"])
            rows = self.conn.execute(
                """
                SELECT ticker, score, conviction, attribution_json, regime, timestamp
                FROM ranking_snapshots
                WHERE tenant_id = ? AND timestamp = ?
                ORDER BY score DESC
                LIMIT ?
                """,
                (tenant_id, ts, limit),
            ).fetchall()
            
            out: list[RankingSnapshotRow] = []
            for r in rows:
                attr = {}
                try:
                    attr = json.loads(str(r["attribution_json"]))
                except Exception:
                    pass
                
                out.append(RankingSnapshotRow(
                    ticker=str(r["ticker"]),
                    score=float(r["score"] or 0.0),
                    conviction=float(r["conviction"] or 0.0),
                    attribution=attr,
                    regime=str(r["regime"] or ""),
                    timestamp=str(r["timestamp"]),
                ))
            return out
        except Exception:
            return []

    def rank_strategies_by_efficiency(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str | None = None,
        forecast_days: int | None = None,
        regime: str | None = None,
        min_samples: int | None = None,
        min_total_forecast_days: int | None = None,
        alpha_version: str | None = None,
        limit: int = 20,
    ) -> list[StrategyEfficiencyRow]:
        where = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if ticker:
            where.append("ticker = ?")
            params.append(str(ticker))
        if timeframe:
            where.append("timeframe = ?")
            params.append(str(timeframe))
        if forecast_days is not None:
            where.append("forecast_days = ?")
            params.append(int(forecast_days))
        if regime:
            where.append("regime = ?")
            params.append(str(regime))
        if alpha_version:
            where.append("alpha_version = ?")
            params.append(str(alpha_version))

        try:
            rows = self.conn.execute(
                f"""
                SELECT
                  strategy_id,
                  COALESCE(strategy_version, '') as strategy_version,
                  COUNT(*) as samples,
                  SUM(forecast_days) as total_forecast_days,
                  AVG(efficiency_rating) as avg_efficiency_rating,
                  AVG(efficiency_rating) as avg_alpha,
                  AVG(total_return_actual) as avg_total_return_actual,
                  MIN(total_return_actual) as min_total_return_actual,
                  AVG(CASE WHEN direction_hit_rate >= 0.5 THEN 1.0 ELSE 0.0 END) as win_rate
                FROM prediction_scores
                WHERE {' AND '.join(where)}
                GROUP BY strategy_id, COALESCE(strategy_version, '')
                HAVING (? IS NULL OR COUNT(*) >= ?)
                   AND (? IS NULL OR SUM(forecast_days) >= ?)
                ORDER BY AVG(efficiency_rating) DESC
                LIMIT ?
                """,
                (*params, min_samples, min_samples, min_total_forecast_days, min_total_forecast_days, int(limit)),
            ).fetchall()
        except Exception as e:
            log.error(f"Ranking error: {e}")
            return []

        out: list[StrategyEfficiencyRow] = []
        for row in rows:
            # Calculate Risk-Adjusted Alpha (Strategy Alpha Score)
            # alpha_strategy = avg(alpha_prediction) - drawdown_penalty - variance_penalty
            
            avg_alpha = float(row["avg_alpha"] or 0.0)
            drawdown = abs(min(0.0, float(row["min_total_return_actual"] or 0.0)))
            
            # Fetch last N predictions for this strategy to get variance
            # Only use versioned predictions for stability calc
            preds = self.conn.execute(
                "SELECT efficiency_rating FROM prediction_scores WHERE tenant_id = ? AND strategy_id = ? ORDER BY created_at DESC LIMIT 50",
                (tenant_id, row["strategy_id"])
            ).fetchall()
            
            alpha_vals = [float(p["efficiency_rating"]) for p in preds if p["efficiency_rating"] is not None]
            
            variance = 0.0
            if len(alpha_vals) > 1:
                mean = sum(alpha_vals) / len(alpha_vals)
                variance = sum((x - mean) ** 2 for x in alpha_vals) / len(alpha_vals)
            
            drawdown_penalty = drawdown * 0.5
            variance_penalty = variance * 2.0
            
            alpha_strategy = avg_alpha - drawdown_penalty - variance_penalty
            
            sv = str(row["strategy_version"])
            out.append(
                StrategyEfficiencyRow(
                    strategy_id=str(row["strategy_id"]),
                    strategy_version=(sv if sv else None),
                    forecast_days=(int(forecast_days) if forecast_days is not None else None),
                    samples=int(row["samples"]),
                    total_forecast_days=int(row["total_forecast_days"] or 0),
                    avg_efficiency_rating=float(row["avg_efficiency_rating"] or 0.0),
                    alpha_strategy=alpha_strategy,
                    win_rate=float(row["win_rate"] or 0.0),
                    avg_return=float(row["avg_total_return_actual"] or 0.0),
                    drawdown=drawdown,
                    stability=(1.0 - min(1.0, variance * 10.0))
                )
            )
            
        return sorted(out, key=lambda x: x.alpha_strategy, reverse=True)

    def get_efficiency_champion(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        timeframe: str = "1d",
        forecast_days: int | None = None,
        regime: str | None = None,
        min_samples: int = 20,
        min_total_forecast_days: int = 200,
    ) -> StrategyEfficiencyRow | None:
        rows = self.rank_strategies_by_efficiency(
            tenant_id=tenant_id,
            ticker=str(ticker),
            timeframe=str(timeframe),
            forecast_days=forecast_days,
            regime=regime,
            min_samples=int(min_samples) if min_samples is not None else None,
            min_total_forecast_days=int(min_total_forecast_days) if min_total_forecast_days is not None else None,
            limit=1,
        )
        return rows[0] if rows else None

    def list_prediction_runs(self, *, tenant_id: str = "default", limit: int = 50) -> list[PredictionRunRow]:
        try:
            rows = self.conn.execute(
                """
                SELECT id, ingress_start, ingress_end, prediction_start, prediction_end, timeframe, regime, created_at
                FROM prediction_runs
                WHERE tenant_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (tenant_id, limit),
            ).fetchall()
            return [
                PredictionRunRow(
                    id=str(r["id"]),
                    ingress_start=str(r["ingress_start"]),
                    ingress_end=str(r["ingress_end"]),
                    prediction_start=str(r["prediction_start"]),
                    prediction_end=str(r["prediction_end"]),
                    timeframe=str(r["timeframe"]),
                    regime=(str(r["regime"]) if r["regime"] is not None else None),
                    created_at=str(r["created_at"]),
                )
                for r in rows
            ]
        except Exception:
            return []

    def list_run_tickers(self, *, run_id: str, tenant_id: str = "default") -> list[str]:
        try:
            rows = self.conn.execute(
                """
                SELECT DISTINCT ticker
                FROM predicted_series_points
                WHERE tenant_id = ? AND run_id = ?
                ORDER BY ticker ASC
                """,
                (tenant_id, str(run_id)),
            ).fetchall()
            return [str(r["ticker"]) for r in rows]
        except Exception:
            return []

    def list_run_strategies(self, *, run_id: str, tenant_id: str = "default") -> list[str]:
        try:
            rows = self.conn.execute(
                """
                SELECT DISTINCT strategy_id
                FROM predicted_series_points
                WHERE tenant_id = ? AND run_id = ?
                ORDER BY strategy_id ASC
                """,
                (tenant_id, str(run_id)),
            ).fetchall()
            return [str(r["strategy_id"]) for r in rows]
        except Exception:
            return []

    def get_series_comparison(
        self,
        *,
        run_id: str,
        strategy_id: str,
        ticker: str,
        tenant_id: str = "default",
    ) -> dict[str, list[SeriesPointRow]]:
        """Fetch both predicted and actual series for comparison."""
        try:
            # Predicted
            p_rows = self.conn.execute(
                """
                SELECT timestamp, value
                FROM predicted_series_points
                WHERE tenant_id = ? AND run_id = ? AND strategy_id = ? AND ticker = ?
                ORDER BY timestamp ASC
                """,
                (tenant_id, str(run_id), str(strategy_id), str(ticker)),
            ).fetchall()
            
            # Actual
            a_rows = self.conn.execute(
                """
                SELECT timestamp, value
                FROM actual_series_points
                WHERE tenant_id = ? AND run_id = ? AND ticker = ?
                ORDER BY timestamp ASC
                """,
                (tenant_id, str(run_id), str(ticker)),
            ).fetchall()
            
            return {
                "predicted": [SeriesPointRow(str(r["timestamp"]), float(r["value"])) for r in p_rows],
                "actual": [SeriesPointRow(str(r["timestamp"]), float(r["value"])) for r in a_rows],
            }
        except Exception:
            return {"predicted": [], "actual": []}

    def get_strategy_scores_for_run(
        self,
        *,
        run_id: str,
        tenant_id: str = "default",
    ) -> list[PredictionScoreRow]:
        try:
            rows = self.conn.execute(
                """
                SELECT *
                FROM prediction_scores
                WHERE tenant_id = ? AND run_id = ?
                ORDER BY efficiency_rating DESC
                """,
                (tenant_id, str(run_id)),
            ).fetchall()
            return [
                PredictionScoreRow(
                    strategy_id=str(r["strategy_id"]),
                    strategy_version=str(r["strategy_version"]) if r["strategy_version"] else None,
                    ticker=str(r["ticker"]),
                    timeframe=str(r["timeframe"]),
                    regime=(str(r["regime"]) if ("regime" in r.keys() and r["regime"] is not None) else None),
                    forecast_days=int(r["forecast_days"]),
                    direction_hit_rate=float(r["direction_hit_rate"]),
                    sync_rate=float(r["sync_rate"]),
                    total_return_actual=float(r["total_return_actual"]),
                    total_return_pred=float(r["total_return_pred"]),
                    total_return_error=float(r["total_return_error"]),
                    magnitude_error=float(r["magnitude_error"]),
                    efficiency_rating=float(r["efficiency_rating"]),
                    alpha_prediction=float(r["alpha_prediction"] if "alpha_prediction" in r.keys() else 0.0),
                    attribution_json=str(r["attribution_json"] if "attribution_json" in r.keys() else "{}"),
                )
                for r in rows
            ]
        except Exception:
            return []

    def get_champion_comparison_matrix(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str | None = None,
        alpha_version: str = "canonical_v1",
    ) -> list[ChampionMatrixRow]:
        """
        Return all (ticker × strategy) combinations ranked by alpha_strategy.

        Joins efficiency_champions with prediction_scores to produce:
          - alpha_strategy  (risk-adjusted canonical alpha)
          - avg_pred_return / avg_actual_return
          - direction_accuracy
          - entry_price from price_bars at prediction_start

        This is the data source for the cross-strategy heatmap in the Intelligence Hub.
        """
        where = ["ps.tenant_id = ?", f"ps.alpha_version = '{alpha_version}'"]
        params: list[Any] = [tenant_id]
        if ticker:
            where.append("ps.ticker = ?")
            params.append(str(ticker))
        if timeframe:
            where.append("ps.timeframe = ?")
            params.append(str(timeframe))

        try:
            rows = self.conn.execute(
                f"""
                SELECT
                    ps.ticker,
                    ps.timeframe,
                    ps.forecast_days,
                    ps.strategy_id,
                    COALESCE(ps.regime, '') AS regime,
                    AVG(ps.alpha_prediction)            AS alpha_strategy,
                    AVG(ps.total_return_pred)           AS avg_pred_return,
                    AVG(ps.total_return_actual)         AS avg_actual_return,
                    AVG(ps.direction_hit_rate)          AS direction_accuracy,
                    COUNT(*)                            AS samples,
                    -- entry price: latest price bar close at or before the most recent prediction_start
                    -- Note: no timeframe filter — price_bars may be stored at any granularity (e.g. 1min)
                    (
                        SELECT pb.close
                        FROM price_bars pb
                        WHERE pb.ticker = ps.ticker
                          AND pb.timestamp <= (
                              SELECT MAX(pr2.prediction_start)
                              FROM prediction_runs pr2
                              JOIN prediction_scores ps2
                                ON ps2.run_id = pr2.id
                              WHERE ps2.tenant_id = ps.tenant_id
                                AND ps2.strategy_id = ps.strategy_id
                                AND ps2.ticker = ps.ticker
                          )
                        ORDER BY pb.timestamp DESC
                        LIMIT 1
                    ) AS entry_price
                FROM prediction_scores ps
                WHERE {' AND '.join(where)}
                GROUP BY ps.ticker, ps.strategy_id, ps.timeframe, ps.forecast_days, COALESCE(ps.regime, '')
                ORDER BY ps.ticker ASC, alpha_strategy DESC
                """,
                params,
            ).fetchall()
        except Exception as e:
            log.error(f"get_champion_comparison_matrix error: {e}")
            return []

        out: list[ChampionMatrixRow] = []
        for r in rows:
            avg_alpha = float(r["alpha_strategy"] or 0.0)
            entry = float(r["entry_price"]) if r["entry_price"] is not None else None
            out.append(
                ChampionMatrixRow(
                    ticker=str(r["ticker"]),
                    timeframe=str(r["timeframe"]),
                    forecast_days=int(r["forecast_days"]),
                    strategy_id=str(r["strategy_id"]),
                    regime=str(r["regime"]),
                    alpha_strategy=avg_alpha,
                    avg_pred_return=float(r["avg_pred_return"] or 0.0),
                    avg_actual_return=float(r["avg_actual_return"] or 0.0),
                    direction_accuracy=float(r["direction_accuracy"] or 0.0),
                    entry_price=entry,
                    samples=int(r["samples"]),
                )
            )
        return out

    def get_strategy_timeline(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        strategy_id: str,
        alpha_version: str = "canonical_v1",
        limit: int = 90,
    ) -> list[StrategyTimelineRow]:
        """
        Per-run prediction history for a strategy+ticker, ordered chronologically.

        Enables the 'strategy autopsy' chart:
          - Rolling alpha over time
          - Predicted vs Actual return comparison
          - Entry price and derived target price per run

        Entry price joins price_bars at the run's prediction_start.
        Target price = entry_price * (1 + total_return_pred).
        """
        try:
            rows = self.conn.execute(
                """
                SELECT
                    pr.prediction_start,
                    pr.prediction_end,
                    ps.ticker,
                    ps.strategy_id,
                    ps.forecast_days,
                    ps.alpha_prediction,
                    ps.total_return_pred,
                    ps.total_return_actual,
                    ps.direction_hit_rate,
                    (
                        SELECT pb.close
                        FROM price_bars pb
                        WHERE pb.ticker = ps.ticker
                          AND pb.timestamp <= pr.prediction_start
                        ORDER BY pb.timestamp DESC
                        LIMIT 1
                    ) AS entry_price
                FROM prediction_scores ps
                JOIN prediction_runs pr ON pr.id = ps.run_id AND pr.tenant_id = ps.tenant_id
                WHERE ps.tenant_id = ?
                  AND ps.ticker = ?
                  AND ps.strategy_id = ?
                  AND ps.alpha_version = ?
                ORDER BY pr.prediction_start ASC
                LIMIT ?
                """,
                (tenant_id, str(ticker), str(strategy_id), str(alpha_version), int(limit)),
            ).fetchall()
        except Exception as e:
            log.error(f"get_strategy_timeline error: {e}")
            return []

        out: list[StrategyTimelineRow] = []
        for r in rows:
            entry = float(r["entry_price"]) if r["entry_price"] is not None else None
            # Derive target price from predicted return
            target = (entry * (1.0 + float(r["total_return_pred"]))) if entry is not None else None
            out.append(
                StrategyTimelineRow(
                    run_date=str(r["prediction_start"])[:10],  # YYYY-MM-DD
                    ticker=str(r["ticker"]),
                    strategy_id=str(r["strategy_id"]),
                    prediction_start=str(r["prediction_start"]),
                    prediction_end=str(r["prediction_end"]),
                    forecast_days=int(r["forecast_days"]),
                    alpha_prediction=float(r["alpha_prediction"] or 0.0),
                    total_return_pred=float(r["total_return_pred"] or 0.0),
                    total_return_actual=float(r["total_return_actual"] or 0.0),
                    direction_hit_rate=float(r["direction_hit_rate"] or 0.0),
                    entry_price=entry,
                    target_price=target,
                )
            )
        return out

    def get_last_prediction_write(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
    ) -> str:
        """
        Return a stable timestamp-like cache invalidation key for recent prediction writes.

        Prefer the newest `prediction_scores.created_at` for the tenant/ticker slice and
        fall back to the latest prediction run creation time when score rows are absent.
        """
        where = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if ticker:
            where.append("ticker = ?")
            params.append(str(ticker))

        try:
            row = self.conn.execute(
                f"""
                SELECT MAX(created_at) AS last_write
                FROM prediction_scores
                WHERE {' AND '.join(where)}
                """,
                tuple(params),
            ).fetchone()
            if row and row["last_write"] is not None:
                return str(row["last_write"])
        except Exception:
            pass

        try:
            row = self.conn.execute(
                """
                SELECT MAX(created_at) AS last_write
                FROM prediction_runs
                WHERE tenant_id = ?
                """,
                (tenant_id,),
            ).fetchone()
            if row and row["last_write"] is not None:
                return str(row["last_write"])
        except Exception:
            pass

        return ""
