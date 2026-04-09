"""
Unified Alpha Repository
Consolidates all repository operations into a single class
"""
from __future__ import annotations

import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from app.core.types import TargetRanking


class AlphaRepository:
    """
    Unified repository for all Alpha Engine data operations.
    Consolidates PredictionRepository, StrategyRepository, PerformanceRepository.
    """

    def __init__(self, db_path: str | Path = "data/alpha.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()

    def _create_schema(self) -> None:
        """Create unified schema matching Prisma"""
        schema = """
        CREATE TABLE IF NOT EXISTS raw_events (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            timestamp TEXT NOT NULL,
            source TEXT NOT NULL,
            text TEXT NOT NULL,
            tickers_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scored_events (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
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
            tenant_id TEXT NOT NULL DEFAULT 'default',
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
            tenant_id TEXT NOT NULL DEFAULT 'default',
            track TEXT NOT NULL,
            parent_id TEXT,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            mode TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            config_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'CANDIDATE',
            regime_focus TEXT,
            gate_logs TEXT,
            is_champion INTEGER NOT NULL DEFAULT 0,
            backtest_score REAL NOT NULL DEFAULT 0,
            forward_score REAL NOT NULL DEFAULT 0,
            live_score REAL NOT NULL DEFAULT 0,
            stability_score REAL NOT NULL DEFAULT 0,
            sample_size INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            activated_at TEXT,
            deactivated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS promotion_events (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            strategy_id TEXT NOT NULL,
            prev_status TEXT,
            new_status TEXT NOT NULL,
            event_type TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS predictions (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
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
            scored_outcome_id TEXT,
            scored_at TEXT
        );

        CREATE TABLE IF NOT EXISTS prediction_outcomes (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
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

        CREATE TABLE IF NOT EXISTS strategy_performance (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            strategy_id TEXT NOT NULL,
            horizon TEXT NOT NULL,
            prediction_count INTEGER NOT NULL,
            accuracy REAL NOT NULL,
            avg_return REAL NOT NULL,
            avg_residual_alpha REAL NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, strategy_id, horizon)
        );

        CREATE TABLE IF NOT EXISTS regime_performance (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            regime TEXT NOT NULL,
            prediction_count INTEGER NOT NULL,
            accuracy REAL NOT NULL,
            avg_return REAL NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, regime)
        );

        CREATE TABLE IF NOT EXISTS strategy_stability (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            strategy_id TEXT NOT NULL,
            backtest_accuracy REAL NOT NULL,
            live_accuracy REAL NOT NULL,
            stability_score REAL NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, strategy_id)
        );

        CREATE TABLE IF NOT EXISTS consensus_signals (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            ticker TEXT NOT NULL,
            regime TEXT NOT NULL,
            sentiment_strategy_id TEXT,
            quant_strategy_id TEXT,
            sentiment_score REAL NOT NULL,
            quant_score REAL NOT NULL,
            ws REAL NOT NULL,
            wq REAL NOT NULL,
            agreement_bonus REAL NOT NULL,
            p_final REAL NOT NULL,
            stability_score REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ranking_snapshots (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            ticker TEXT NOT NULL,
            score REAL NOT NULL,
            conviction REAL NOT NULL,
            attribution_json TEXT NOT NULL,
            regime TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS prediction_runs (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            ingress_start TEXT NOT NULL,
            ingress_end TEXT NOT NULL,
            prediction_start TEXT NOT NULL,
            prediction_end TEXT NOT NULL,
            timeframe TEXT NOT NULL DEFAULT '1d',
            regime TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_prediction_runs_windows
          ON prediction_runs(tenant_id, prediction_start, prediction_end, timeframe);

        CREATE TABLE IF NOT EXISTS predicted_series_points (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            run_id TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            strategy_version TEXT,
            ticker TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            value REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, run_id, strategy_id, ticker, timeframe, timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_predicted_series_by_run
          ON predicted_series_points(tenant_id, run_id, ticker, timeframe, timestamp);

        CREATE INDEX IF NOT EXISTS idx_predicted_series_by_strategy
          ON predicted_series_points(tenant_id, strategy_id, ticker, timeframe, timestamp);

        CREATE TABLE IF NOT EXISTS actual_series_points (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            run_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            value REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, run_id, ticker, timeframe, timestamp)
        );

        CREATE INDEX IF NOT EXISTS idx_actual_series_by_run
          ON actual_series_points(tenant_id, run_id, ticker, timeframe, timestamp);

        CREATE TABLE IF NOT EXISTS prediction_scores (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
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
            alpha_prediction REAL NOT NULL DEFAULT 0.0,
            alpha_version TEXT NOT NULL DEFAULT 'legacy',
            alpha_sample_count INTEGER DEFAULT 0,
            alpha_window_days INTEGER DEFAULT 0,
            alpha_prev REAL,
            attribution_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, run_id, strategy_id, ticker, timeframe)
        );

        CREATE INDEX IF NOT EXISTS idx_prediction_scores_rank
          ON prediction_scores(tenant_id, ticker, timeframe, efficiency_rating);

        CREATE TABLE IF NOT EXISTS efficiency_champions (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            ticker TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            forecast_days INTEGER NOT NULL DEFAULT -1,
            regime TEXT NOT NULL DEFAULT '',
            strategy_id TEXT NOT NULL,
            strategy_version TEXT,
            avg_efficiency_rating REAL NOT NULL,
            alpha_strategy REAL DEFAULT 0.0,
            samples INTEGER NOT NULL,
            total_forecast_days INTEGER NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, ticker, timeframe, forecast_days, regime)
        );

        CREATE INDEX IF NOT EXISTS idx_efficiency_champions_lookup
          ON efficiency_champions(tenant_id, ticker, timeframe, forecast_days, regime);
        """
        self.conn.executescript(schema)
        self._ensure_additive_schema()

    def _ensure_additive_schema(self) -> None:
        """
        Best-effort additive migration for optional/new columns.

        SQLite can't do full migrations easily here; we only add missing columns
        so existing DBs can roll forward without rebuilds.
        """
        def cols(table: str) -> set[str]:
            try:
                return {str(r["name"]) for r in self.conn.execute(f"PRAGMA table_info({table})").fetchall()}
            except Exception:
                return set()

        # prediction_runs additions
        pr_cols = cols("prediction_runs")
        if pr_cols and "regime" not in pr_cols:
            try:
                self.conn.execute("ALTER TABLE prediction_runs ADD COLUMN regime TEXT;")
            except Exception:
                pass

        # prediction_scores additions
        ps_cols = cols("prediction_scores")
        if ps_cols and "regime" not in ps_cols:
            try:
                self.conn.execute("ALTER TABLE prediction_scores ADD COLUMN regime TEXT;")
            except Exception:
                pass
        if ps_cols and "attribution_json" not in ps_cols:
            try:
                self.conn.execute("ALTER TABLE prediction_scores ADD COLUMN attribution_json TEXT NOT NULL DEFAULT '{}';")
            except Exception:
                pass
        if ps_cols and "alpha_prediction" not in ps_cols:
            try:
                self.conn.execute("ALTER TABLE prediction_scores ADD COLUMN alpha_prediction REAL NOT NULL DEFAULT 0.0;")
            except Exception:
                pass
        for col, col_type in [
            ("alpha_version", "TEXT NOT NULL DEFAULT 'legacy'"),
            ("alpha_sample_count", "INTEGER DEFAULT 0"),
            ("alpha_window_days", "INTEGER DEFAULT 0"),
            ("alpha_prev", "REAL"),
        ]:
            if ps_cols and col not in ps_cols:
                try:
                    self.conn.execute(f"ALTER TABLE prediction_scores ADD COLUMN {col} {col_type};")
                except Exception:
                    pass

        # strategies additions
        s_cols = cols("strategies")
        for col, col_type in [
            ("status", "TEXT NOT NULL DEFAULT 'CANDIDATE'"),
            ("track", "TEXT NOT NULL DEFAULT 'ALPHA'"),
            ("is_champion", "INTEGER NOT NULL DEFAULT 0"),
            ("alpha_strategy", "REAL DEFAULT 0.0"),
            ("regime_focus", "TEXT"),
            ("gate_logs", "TEXT"),
            ("sample_size", "INTEGER DEFAULT 0"),
            ("activated_at", "TEXT"),
            ("deactivated_at", "TEXT")
        ]:
            if s_cols and col not in s_cols:
                try:
                    self.conn.execute(f"ALTER TABLE strategies ADD COLUMN {col} {col_type};")
                except Exception:
                    pass

        # efficiency_champions additions
        ec_cols = cols("efficiency_champions")
        if ec_cols and "alpha_strategy" not in ec_cols:
            try:
                self.conn.execute("ALTER TABLE efficiency_champions ADD COLUMN alpha_strategy REAL DEFAULT 0.0;")
            except Exception:
                pass

        try:
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_prediction_scores_rank ON prediction_scores(tenant_id, ticker, timeframe, efficiency_rating);"
            )
        except Exception:
            pass

        try:
            self.conn.commit()
        except Exception:
            pass

    def create_prediction_run(
        self,
        *,
        ingress_start: str,
        ingress_end: str,
        prediction_start: str,
        prediction_end: str,
        timeframe: str = "1d",
        regime: str | None = None,
        tenant_id: str = "default",
        run_id: str | None = None,
    ) -> str:
        rid = run_id or str(uuid4())
        self.conn.execute(
            """
            INSERT OR REPLACE INTO prediction_runs
              (id, tenant_id, ingress_start, ingress_end, prediction_start, prediction_end, timeframe, regime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (rid, tenant_id, ingress_start, ingress_end, prediction_start, prediction_end, str(timeframe), regime),
        )
        self.conn.commit()
        return rid

    def upsert_predicted_series_points(
        self,
        *,
        run_id: str,
        strategy_id: str,
        ticker: str,
        timeframe: str,
        points: list[tuple[str, float]],
        tenant_id: str = "default",
        strategy_version: str | None = None,
    ) -> int:
        """
        Upsert predicted series points for a strategy/ticker/run.

        `points` is a list of (timestamp_iso, value).
        """
        if not points:
            return 0
        rows = [
            (
                str(uuid4()),
                tenant_id,
                run_id,
                strategy_id,
                strategy_version,
                str(ticker),
                str(timeframe),
                ts,
                float(val),
            )
            for ts, val in points
        ]
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO predicted_series_points
              (id, tenant_id, run_id, strategy_id, strategy_version, ticker, timeframe, timestamp, value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    def upsert_actual_series_points(
        self,
        *,
        run_id: str,
        ticker: str,
        timeframe: str,
        points: list[tuple[str, float]],
        tenant_id: str = "default",
    ) -> int:
        """
        Upsert actual series points for a ticker/run.

        `points` is a list of (timestamp_iso, value).
        """
        if not points:
            return 0
        rows = [
            (
                str(uuid4()),
                tenant_id,
                run_id,
                str(ticker),
                str(timeframe),
                ts,
                float(val),
            )
            for ts, val in points
        ]
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO actual_series_points
              (id, tenant_id, run_id, ticker, timeframe, timestamp, value)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    def get_prediction_run(self, *, run_id: str, tenant_id: str = "default") -> Dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM prediction_runs WHERE tenant_id = ? AND id = ?",
            (tenant_id, str(run_id)),
        ).fetchone()
        return None if row is None else dict(row)

    def list_prediction_runs(self, *, tenant_id: str = "default", limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM prediction_runs
            WHERE tenant_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (tenant_id, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def fetch_predicted_series(
        self,
        *,
        run_id: str,
        strategy_id: str,
        ticker: str,
        timeframe: str,
        tenant_id: str = "default",
    ) -> list[tuple[str, float]]:
        rows = self.conn.execute(
            """
            SELECT timestamp, value
            FROM predicted_series_points
            WHERE tenant_id = ? AND run_id = ? AND strategy_id = ? AND ticker = ? AND timeframe = ?
            ORDER BY timestamp ASC
            """,
            (tenant_id, str(run_id), str(strategy_id), str(ticker), str(timeframe)),
        ).fetchall()
        return [(str(r["timestamp"]), float(r["value"])) for r in rows]

    def fetch_actual_series(
        self,
        *,
        run_id: str,
        ticker: str,
        timeframe: str,
        tenant_id: str = "default",
    ) -> list[tuple[str, float]]:
        rows = self.conn.execute(
            """
            SELECT timestamp, value
            FROM actual_series_points
            WHERE tenant_id = ? AND run_id = ? AND ticker = ? AND timeframe = ?
            ORDER BY timestamp ASC
            """,
            (tenant_id, str(run_id), str(ticker), str(timeframe)),
        ).fetchall()
        return [(str(r["timestamp"]), float(r["value"])) for r in rows]

    def save_prediction_score(self, score_data: Dict[str, Any], tenant_id: str = "default") -> str:
        sid = score_data.get("id") or str(uuid4())
        attribution_json = score_data.get("attribution_json")
        if attribution_json is None:
            try:
                import json
                attribution_json = json.dumps(score_data.get("attribution") or {})
            except Exception:
                attribution_json = "{}"
        
        # Calculate Canonical Prediction Alpha Score
        from app.core.canonical_scoring import score_prediction
        alpha_prediction = score_prediction(
            direction_correct=bool(score_data.get("direction_hit_rate", 0) >= 0.5), # Simplified mapping
            return_pct=float(score_data.get("total_return_actual", 0.0)),
            confidence=float(score_data.get("sync_rate", 0.5)) # Using sync_rate as a proxy if confidence not present
        )
        if "alpha_prediction" in score_data:
            alpha_prediction = float(score_data["alpha_prediction"])

        self.conn.execute(
            """
            INSERT OR REPLACE INTO prediction_scores
              (id, tenant_id, run_id, strategy_id, strategy_version, ticker, timeframe, regime, forecast_days,
               direction_hit_rate, sync_rate, total_return_actual, total_return_pred, total_return_error,
               magnitude_error, horizon_weight, efficiency_rating, alpha_prediction, 
               alpha_version, alpha_sample_count, alpha_window_days, alpha_prev, attribution_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sid,
                tenant_id,
                score_data["run_id"],
                score_data["strategy_id"],
                score_data.get("strategy_version"),
                score_data["ticker"],
                score_data["timeframe"],
                score_data.get("regime"),
                int(score_data["forecast_days"]),
                float(score_data["direction_hit_rate"]),
                float(score_data["sync_rate"]),
                float(score_data["total_return_actual"]),
                float(score_data["total_return_pred"]),
                float(score_data["total_return_error"]),
                float(score_data["magnitude_error"]),
                float(score_data["horizon_weight"]),
                float(score_data["efficiency_rating"]),
                alpha_prediction,
                score_data.get("alpha_version", "legacy"),
                int(score_data.get("alpha_sample_count", 0)),
                int(score_data.get("alpha_window_days", 0)),
                score_data.get("alpha_prev"),
                str(attribution_json),
            ),
        )
        self.conn.commit()
        return sid

    def list_score_targets_for_run(
        self,
        *,
        run_id: str,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str | None = None,
        strategy_id: str | None = None,
    ) -> list[dict[str, Any]]:
        where = ["tenant_id = ?", "run_id = ?"]
        params: list[Any] = [tenant_id, str(run_id)]
        if ticker:
            where.append("ticker = ?")
            params.append(str(ticker))
        if timeframe:
            where.append("timeframe = ?")
            params.append(str(timeframe))
        if strategy_id:
            where.append("strategy_id = ?")
            params.append(str(strategy_id))
        rows = self.conn.execute(
            f"""
            SELECT DISTINCT strategy_id, COALESCE(strategy_version, '') as strategy_version, ticker, timeframe
            FROM predicted_series_points
            WHERE {' AND '.join(where)}
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def list_unscored_runs(self, *, tenant_id: str = "default", limit: int = 200) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT pr.*
            FROM prediction_runs pr
            LEFT JOIN prediction_scores ps
              ON ps.tenant_id = pr.tenant_id AND ps.run_id = pr.id
            WHERE pr.tenant_id = ?
            GROUP BY pr.id
            HAVING COUNT(ps.id) = 0
            ORDER BY pr.created_at DESC
            LIMIT ?
            """,
            (tenant_id, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def rank_strategies(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str | None = None,
        forecast_days: int | None = None,
        regime: str | None = None,
        min_samples: int | None = None,
        min_total_forecast_days: int | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
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
        rows = self.conn.execute(
            f"""
            SELECT
              strategy_id,
              COALESCE(strategy_version, '') as strategy_version,
              COUNT(*) as samples,
              SUM(forecast_days) as total_forecast_days,
              AVG(efficiency_rating) as avg_efficiency_rating
            FROM prediction_scores
            WHERE {' AND '.join(where)}
            GROUP BY strategy_id, COALESCE(strategy_version, '')
            HAVING (? IS NULL OR COUNT(*) >= ?)
               AND (? IS NULL OR SUM(forecast_days) >= ?)
            ORDER BY avg_efficiency_rating DESC
            LIMIT ?
            """,
            (*params, min_samples, min_samples, min_total_forecast_days, min_total_forecast_days, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def best_strategy_per_ticker(
        self,
        *,
        tenant_id: str = "default",
        timeframe: str = "1d",
        forecast_days: int | None = None,
        regime: str | None = None,
        min_samples: int | None = None,
    ) -> list[dict[str, Any]]:
        where = ["tenant_id = ?", "timeframe = ?"]
        params: list[Any] = [tenant_id, str(timeframe)]
        if forecast_days is not None:
            where.append("forecast_days = ?")
            params.append(int(forecast_days))
        if regime:
            where.append("regime = ?")
            params.append(str(regime))
        rows = self.conn.execute(
            f"""
            WITH ranked AS (
              SELECT
                ticker,
                strategy_id,
                COALESCE(strategy_version, '') as strategy_version,
                AVG(efficiency_rating) as avg_efficiency_rating,
                COUNT(*) as samples,
                ROW_NUMBER() OVER (
                  PARTITION BY ticker
                  ORDER BY AVG(efficiency_rating) DESC
                ) as rn
              FROM prediction_scores
              WHERE {' AND '.join(where)}
              GROUP BY ticker, strategy_id, COALESCE(strategy_version, '')
            )
            SELECT ticker, strategy_id, strategy_version, avg_efficiency_rating, samples
            FROM ranked
            WHERE rn = 1
              AND (? IS NULL OR samples >= ?)
            ORDER BY avg_efficiency_rating DESC
            """,
            (*params, min_samples, min_samples),
        ).fetchall()
        return [dict(r) for r in rows]

    def best_strategy_per_horizon(
        self,
        *,
        tenant_id: str = "default",
        timeframe: str = "1d",
        ticker: str | None = None,
        regime: str | None = None,
        min_samples: int | None = None,
    ) -> list[dict[str, Any]]:
        where = ["tenant_id = ?", "timeframe = ?"]
        params: list[Any] = [tenant_id, str(timeframe)]
        if ticker:
            where.append("ticker = ?")
            params.append(str(ticker))
        if regime:
            where.append("regime = ?")
            params.append(str(regime))
        rows = self.conn.execute(
            f"""
            WITH ranked AS (
              SELECT
                forecast_days,
                strategy_id,
                COALESCE(strategy_version, '') as strategy_version,
                AVG(efficiency_rating) as avg_efficiency_rating,
                COUNT(*) as samples,
                ROW_NUMBER() OVER (
                  PARTITION BY forecast_days
                  ORDER BY AVG(efficiency_rating) DESC
                ) as rn
              FROM prediction_scores
              WHERE {' AND '.join(where)}
              GROUP BY forecast_days, strategy_id, COALESCE(strategy_version, '')
            )
            SELECT forecast_days, strategy_id, strategy_version, avg_efficiency_rating, samples
            FROM ranked
            WHERE rn = 1
              AND (? IS NULL OR samples >= ?)
            ORDER BY forecast_days ASC
            """,
            (*params, min_samples, min_samples),
        ).fetchall()
        return [dict(r) for r in rows]

    def best_overall_strategy(
        self,
        *,
        tenant_id: str = "default",
        timeframe: str = "1d",
        regime: str | None = None,
        min_samples: int | None = None,
        min_total_forecast_days: int | None = None,
    ) -> dict[str, Any] | None:
        where = ["tenant_id = ?", "timeframe = ?"]
        params: list[Any] = [tenant_id, str(timeframe)]
        if regime:
            where.append("regime = ?")
            params.append(str(regime))
        row = self.conn.execute(
            f"""
            SELECT
              strategy_id,
              COALESCE(strategy_version, '') as strategy_version,
              COUNT(*) as samples,
              SUM(forecast_days) as total_forecast_days,
              AVG(efficiency_rating) as avg_efficiency_rating
            FROM prediction_scores
            WHERE {' AND '.join(where)}
            GROUP BY strategy_id, COALESCE(strategy_version, '')
            HAVING (? IS NULL OR COUNT(*) >= ?)
               AND (? IS NULL OR SUM(forecast_days) >= ?)
            ORDER BY avg_efficiency_rating DESC
            LIMIT 1
            """,
            (*params, min_samples, min_samples, min_total_forecast_days, min_total_forecast_days),
        ).fetchone()
        return None if row is None else dict(row)

    def rolling_efficiency(
        self,
        *,
        tenant_id: str = "default",
        strategy_id: str,
        ticker: str | None = None,
        timeframe: str = "1d",
        regime: str | None = None,
        lookback_days: int = 60,
    ) -> list[dict[str, Any]]:
        """
        Rolling average efficiency by day bucket (UTC) for one strategy.
        """
        where = ["ps.tenant_id = ?", "ps.strategy_id = ?", "ps.timeframe = ?"]
        params: list[Any] = [tenant_id, str(strategy_id), str(timeframe)]
        if ticker:
            where.append("ps.ticker = ?")
            params.append(str(ticker))
        if regime:
            where.append("ps.regime = ?")
            params.append(str(regime))
        params.append(int(lookback_days))
        rows = self.conn.execute(
            f"""
            SELECT
              substr(ps.created_at, 1, 10) as day,
              COUNT(*) as samples,
              AVG(ps.efficiency_rating) as avg_efficiency_rating
            FROM prediction_scores ps
            WHERE {' AND '.join(where)}
              AND ps.created_at >= datetime('now', '-' || ? || ' days')
            GROUP BY substr(ps.created_at, 1, 10)
            ORDER BY day ASC
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    def select_efficiency_champion(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        timeframe: str = "1d",
        forecast_days: int | None = None,
        regime: str | None = None,
        min_samples: int = 20,
        min_total_forecast_days: int = 200,
    ) -> dict[str, Any] | None:
        """
        Choose the best strategy for a context using efficiency_rating with guardrails.

        Defaults intentionally enforce a minimum sample threshold to avoid small-window bias.
        """
        ranked = self.rank_strategies(
            tenant_id=tenant_id,
            ticker=str(ticker),
            timeframe=str(timeframe),
            forecast_days=forecast_days,
            regime=regime,
            min_samples=int(min_samples) if min_samples is not None else None,
            min_total_forecast_days=int(min_total_forecast_days) if min_total_forecast_days is not None else None,
            limit=1,
        )
        return ranked[0] if ranked else None

    def get_efficiency_champion_record(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        timeframe: str = "1d",
        forecast_days: int | None = None,
        regime: str | None = None,
    ) -> dict[str, Any] | None:
        fd = int(forecast_days) if forecast_days is not None else -1
        rg = str(regime) if regime else ""
        row = self.conn.execute(
            """
            SELECT *
            FROM efficiency_champions
            WHERE tenant_id = ?
              AND ticker = ?
              AND timeframe = ?
              AND forecast_days = ?
              AND regime = ?
            """,
            (tenant_id, str(ticker), str(timeframe), fd, rg),
        ).fetchone()
        return None if row is None else dict(row)

    def upsert_efficiency_champion_record(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        timeframe: str = "1d",
        forecast_days: int | None = None,
        regime: str | None = None,
        strategy_id: str,
        strategy_version: str | None = None,
        avg_efficiency_rating: float,
        alpha_strategy: float = 0.0,
        samples: int,
        total_forecast_days: int,
    ) -> str:
        """
        Persist the currently-active efficiency champion for a context.

        This is intentionally separate from `strategies.is_champion` because
        efficiency champions are context-specific (ticker/timeframe/horizon/regime).
        """
        fd = int(forecast_days) if forecast_days is not None else -1
        rg = str(regime) if regime else ""
        row = self.conn.execute(
            """
            SELECT id
            FROM efficiency_champions
            WHERE tenant_id = ?
              AND ticker = ?
              AND timeframe = ?
              AND forecast_days = ?
              AND regime = ?
            """,
            (tenant_id, str(ticker), str(timeframe), fd, rg),
        ).fetchone()
        rid = str(row["id"]) if row is not None else str(uuid4())
        self.conn.execute(
            """
            INSERT OR REPLACE INTO efficiency_champions
              (id, tenant_id, ticker, timeframe, forecast_days, regime, strategy_id, strategy_version,
               avg_efficiency_rating, alpha_strategy, samples, total_forecast_days, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                rid,
                tenant_id,
                str(ticker),
                str(timeframe),
                fd,
                rg,
                str(strategy_id),
                (str(strategy_version) if strategy_version else None),
                float(avg_efficiency_rating),
                float(alpha_strategy),
                int(samples),
                int(total_forecast_days),
            ),
        )
        self.conn.commit()
        return rid

    def list_scored_tickers(
        self,
        *,
        tenant_id: str = "default",
        timeframe: str | None = None,
        forecast_days: int | None = None,
        regime: str | None = None,
        limit: int = 5000,
    ) -> list[str]:
        where = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if timeframe:
            where.append("timeframe = ?")
            params.append(str(timeframe))
        if forecast_days is not None:
            where.append("forecast_days = ?")
            params.append(int(forecast_days))
        if regime:
            where.append("regime = ?")
            params.append(str(regime))
        rows = self.conn.execute(
            f"""
            SELECT DISTINCT ticker
            FROM prediction_scores
            WHERE {' AND '.join(where)}
            ORDER BY ticker ASC
            LIMIT ?
            """,
            (*params, int(limit)),
        ).fetchall()
        return [str(r["ticker"]) for r in rows]

    def get_rolling_predictions(
        self,
        *,
        tenant_id: str = "default",
        strategy_id: str,
        ticker: str | None = None,
        timeframe: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Fetch a window of prediction scores for a strategy to compute rolling metrics.
        Returns fields needed for Strategy Alpha score (alpha_prediction, max_drawdown).
        """
        where = ["tenant_id = ?", "strategy_id = ?"]
        params: list[Any] = [tenant_id, str(strategy_id)]
        
        if ticker:
            where.append("ticker = ?")
            params.append(str(ticker))
        if timeframe:
            where.append("timeframe = ?")
            params.append(str(timeframe))
            
        rows = self.conn.execute(
            f"""
            SELECT 
                alpha_prediction,
                magnitude_error as max_drawdown, -- Using magnitude_error as a placeholder if drawdown not stored separately
                direction_hit_rate,
                total_return_actual,
                created_at
            FROM prediction_scores
            WHERE {' AND '.join(where)}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*params, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def save_prediction(self, prediction_data: Dict[str, Any], tenant_id: str = "default") -> str:
        """Save prediction using unified schema"""
        prediction_id = prediction_data.get("id") or str(uuid4())
        
        self.conn.execute("""
            INSERT OR REPLACE INTO predictions 
            (id, tenant_id, strategy_id, scored_event_id, ticker, timestamp,
             prediction, confidence, horizon, entry_price, mode, feature_snapshot_json,
             regime, trend_strength)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            prediction_id,
            tenant_id,
            prediction_data["strategy_id"],
            prediction_data["scored_event_id"],
            prediction_data["ticker"],
            prediction_data["timestamp"],
            prediction_data["prediction"],
            prediction_data["confidence"],
            prediction_data["horizon"],
            prediction_data["entry_price"],
            prediction_data["mode"],
            prediction_data["feature_snapshot_json"],
            prediction_data.get("regime"),
            prediction_data.get("trend_strength")
        ))
        
        self.conn.commit()
        return prediction_id

    def save_outcome(self, outcome_data: Dict[str, Any], tenant_id: str = "default") -> str:
        """Save prediction outcome using unified schema"""
        outcome_id = outcome_data.get("id") or str(uuid4())
        
        self.conn.execute("""
            INSERT OR REPLACE INTO prediction_outcomes 
            (id, tenant_id, prediction_id, exit_price, return_pct, 
             direction_correct, max_runup, max_drawdown, evaluated_at, exit_reason, residual_alpha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outcome_id,
            tenant_id,
            outcome_data["prediction_id"],
            outcome_data["exit_price"],
            outcome_data["return_pct"],
            1 if outcome_data["direction_correct"] else 0,
            outcome_data["max_runup"],
            outcome_data["max_drawdown"],
            outcome_data["evaluated_at"],
            outcome_data["exit_reason"],
            outcome_data.get("residual_alpha", 0.0)
        ))
        
        self.conn.commit()
        return outcome_id

    def get_performance(self, strategy_id: str, tenant_id: str = "default") -> Optional[Dict[str, Any]]:
        """Get strategy performance metrics"""
        row = self.conn.execute("""
            SELECT * FROM strategy_performance 
            WHERE tenant_id = ? AND strategy_id = ?
        """, (tenant_id, strategy_id)).fetchone()
        
        return dict(row) if row else None

    def get_unscored(self, tenant_id: str = "default", limit: int = 100) -> List[Dict[str, Any]]:
        """Get unscored predictions for evaluation"""
        rows = self.conn.execute("""
            SELECT p.*, s.strategy_type
            FROM predictions p
            JOIN strategies s ON s.id = p.strategy_id
            LEFT JOIN prediction_outcomes po ON po.prediction_id = p.id
            WHERE p.tenant_id = ? AND po.id IS NULL
            ORDER BY p.timestamp ASC
            LIMIT ?
        """, (tenant_id, limit)).fetchall()
        
        return [dict(row) for row in rows]

    def update_metrics(self, strategy_id: str, metrics: Dict[str, Any], tenant_id: str = "default") -> None:
        """Update strategy performance metrics"""
        self.conn.execute("""
            INSERT OR REPLACE INTO strategy_performance 
            (id, tenant_id, strategy_id, horizon, prediction_count,
             accuracy, avg_return, avg_residual_alpha, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"perf_{strategy_id}_{metrics.get('horizon', 'ALL')}",
            tenant_id,
            strategy_id,
            metrics.get("horizon", "ALL"),
            metrics.get("prediction_count", 0),
            metrics.get("accuracy", 0.0),
            metrics.get("avg_return", 0.0),
            metrics.get("avg_residual_alpha", 0.0),
            datetime.now(timezone.utc).isoformat()
        ))
        
        self.conn.commit()

    def save_strategy(self, strategy_data: Dict[str, Any], tenant_id: str = "default") -> str:
        """Save strategy using unified schema"""
        strategy_id = strategy_data.get("id") or str(uuid4())
        
        self.conn.execute("""
            INSERT OR REPLACE INTO strategies 
            (id, tenant_id, track, parent_id, name, version, strategy_type,
             mode, active, config_json, status, regime_focus, gate_logs,
             is_champion, backtest_score, forward_score, live_score,
             stability_score, sample_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            strategy_id,
            tenant_id,
            strategy_data.get("track"),
            strategy_data.get("parent_id"),
            strategy_data["name"],
            strategy_data["version"],
            strategy_data["strategy_type"],
            strategy_data["mode"],
            strategy_data.get("active", True),
            strategy_data["config_json"],
            strategy_data.get("status", "CANDIDATE"),
            strategy_data.get("regime_focus"),
            strategy_data.get("gate_logs"),
            strategy_data.get("is_champion", False),
            strategy_data.get("backtest_score", 0.0),
            strategy_data.get("forward_score", 0.0),
            strategy_data.get("live_score", 0.0),
            strategy_data.get("stability_score", 0.0),
            strategy_data.get("sample_size", 0)
        ))
        
        self.conn.commit()
        return strategy_id

    def get_strategies(self, tenant_id: str = "default", status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get strategies with optional status filter"""
        query = "SELECT * FROM strategies WHERE tenant_id = ?"
        params = [tenant_id]
        
        if status:
            query += " AND status = ?"
            params.append(status)
            
        rows = self.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def save_consensus_signal(self, consensus_data: Dict[str, Any], tenant_id: str = "default") -> str:
        """Save consensus signal using unified schema"""
        consensus_id = str(uuid4())
        
        self.conn.execute("""
            INSERT INTO consensus_signals 
            (id, tenant_id, ticker, regime, sentiment_strategy_id, quant_strategy_id,
             sentiment_score, quant_score, ws, wq, agreement_bonus, p_final, stability_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            consensus_id,
            tenant_id,
            consensus_data["ticker"],
            consensus_data["regime"],
            consensus_data.get("sentiment_strategy_id"),
            consensus_data.get("quant_strategy_id"),
            consensus_data["sentiment_score"],
            consensus_data["quant_score"],
            consensus_data["ws"],
            consensus_data["wq"],
            consensus_data["agreement_bonus"],
            consensus_data["p_final"],
            consensus_data["stability_score"]
        ))
        
        self.conn.commit()
        return consensus_id

    def save_target_ranking(self, rankings: List[TargetRanking], tenant_id: str = "default") -> None:
        """Save a batch of target rankings as a snapshot"""
        import json
        
        # We can use a shared snapshot ID or just individual rows.
        # Let's just save them individually for now, maybe add a snapshot_id if needed later.
        
        for r in rankings:
            ranking_id = str(uuid4())
            self.conn.execute("""
                INSERT INTO ranking_snapshots 
                (id, tenant_id, ticker, score, conviction, attribution_json, regime, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ranking_id,
                tenant_id,
                r.ticker,
                r.score,
                r.conviction,
                json.dumps(r.attribution),
                r.regime,
                r.timestamp.isoformat()
            ))
            
        self.conn.commit()

    def get_latest_rankings(self, tenant_id: str = "default", limit: int = 20) -> List[Dict[str, Any]]:
        """Get the most recent ranking snapshot for a tenant"""
        # First, find the latest timestamp in the table
        latest_row = self.conn.execute("""
            SELECT MAX(timestamp) as latest FROM ranking_snapshots 
            WHERE tenant_id = ?
        """, (tenant_id,)).fetchone()
        
        if not latest_row or not latest_row["latest"]:
            return []
            
        latest_ts = latest_row["latest"]
        
        # Then get all rankings for that timestamp
        rows = self.conn.execute("""
            SELECT * FROM ranking_snapshots 
            WHERE tenant_id = ? AND timestamp = ?
            ORDER BY score DESC
            LIMIT ?
        """, (tenant_id, latest_ts, limit)).fetchall()
        
        return [dict(row) for row in rows]

    def now_iso(self) -> str:
        """Get current UTC timestamp as ISO string"""
        return datetime.now(timezone.utc).isoformat()

    def close(self) -> None:
        """Close database connection"""
        self.conn.close()
