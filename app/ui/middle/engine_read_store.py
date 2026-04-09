from __future__ import annotations

import logging
import sqlite3
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


@dataclass(frozen=True)
class SignalRow:
    time: str
    ticker: str
    direction: str
    strategy: str
    regime: str | None
    confidence: float


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
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._ensure_read_model_contract()

    def _ensure_read_model_contract(self) -> None:
        """
        Best-effort, additive schema alignment for UI read models.

        This does not create mock data; it only ensures optional columns exist so
        the UI can rely on a stable contract during rolling upgrades.
        """
        try:
            cols = {str(r["name"]) for r in self.conn.execute("PRAGMA table_info(consensus_signals)").fetchall()}
            for col, ddl in (
                ("direction", "ALTER TABLE consensus_signals ADD COLUMN direction TEXT;"),
                ("confidence", "ALTER TABLE consensus_signals ADD COLUMN confidence REAL;"),
                ("total_weight", "ALTER TABLE consensus_signals ADD COLUMN total_weight REAL;"),
                ("participating_strategies", "ALTER TABLE consensus_signals ADD COLUMN participating_strategies INTEGER;"),
            ):
                if cols and col not in cols:
                    self.conn.execute(ddl)
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
                    timestamp TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    confidence REAL NOT NULL,
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
            ):
                if cols and col not in cols:
                    self.conn.execute(ddl)
        except Exception:
            pass

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def list_tenants(self) -> list[str]:
        for table in ("predictions", "strategies", "loop_heartbeats", "regime_performance"):
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
            return [str(r["ticker"]) for r in rows if str(r["ticker"]).strip()]
        except Exception:
            return []

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
            out[str(r["regime"])] = float(r["accuracy"])
        return out

    def _champion_regime_strength(self, *, tenant_id: str) -> tuple[float | None, float | None]:
        champs = self.get_champions(tenant_id=tenant_id, min_predictions=0)
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

    def get_latest_consensus(self, *, tenant_id: str = "default", ticker: str) -> ConsensusRow | None:
        # Prefer materialized consensus_signals (real output fields), fallback to predictions.
        row = None
        try:
            row = self.conn.execute(
                """
                SELECT ticker, created_at as timestamp, direction, confidence, total_weight, participating_strategies, regime
                FROM consensus_signals
                WHERE tenant_id = ? AND ticker = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (tenant_id, str(ticker)),
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
            confidence=float(row["confidence"]),
            total_weight=float(row["total_weight"]) if row["total_weight"] is not None else 1.0,
            participating_strategies=int(row["participating_strategies"]) if row["participating_strategies"] is not None else 2,
            active_regime=str(row["regime"]) if row["regime"] is not None else None,
            high_vol_strength=float(high_strength) if high_strength is not None else None,
            low_vol_strength=float(low_strength) if low_strength is not None else None,
        )

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

            rows = self.conn.execute(
                f"""
                SELECT
                  sig.timestamp as timestamp,
                  sig.ticker as ticker,
                  sig.direction as prediction,
                  sig.confidence as confidence,
                  sig.regime as regime,
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
                    confidence=float(r["confidence"]),
                    strategy=f"{stype}:{ver}" if ver else stype,
                    regime=str(r["regime"]) if r["regime"] is not None else None,
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
                    score=float(r["score"]),
                    conviction=float(r["conviction"]),
                    attribution=attr,
                    regime=str(r["regime"]),
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
                  AVG(alpha_prediction) as avg_alpha,
                  AVG(total_return_actual) as avg_total_return_actual,
                  MIN(total_return_actual) as min_total_return_actual,
                  AVG(CASE WHEN direction_hit_rate >= 0.5 THEN 1.0 ELSE 0.0 END) as win_rate
                FROM prediction_scores
                WHERE {' AND '.join(where)}
                GROUP BY strategy_id, COALESCE(strategy_version, '')
                HAVING (? IS NULL OR COUNT(*) >= ?)
                   AND (? IS NULL OR SUM(forecast_days) >= ?)
                ORDER BY COALESCE(AVG(alpha_prediction), AVG(efficiency_rating)) DESC
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
                "SELECT alpha_prediction FROM prediction_scores WHERE tenant_id = ? AND strategy_id = ? AND alpha_version = 'canonical_v1' ORDER BY created_at DESC LIMIT 50",
                (tenant_id, row["strategy_id"])
            ).fetchall()
            
            alpha_vals = [float(p["alpha_prediction"]) for p in preds if p["alpha_prediction"] is not None]
            
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
