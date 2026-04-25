"""
Unified Alpha Repository
Consolidates all repository operations into a single class
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from app.core.types import TargetRanking

logger = logging.getLogger(__name__)


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
            scored_at TEXT,
            rank_score REAL,
            ranking_context_json TEXT
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

        CREATE TABLE IF NOT EXISTS positions (
            ticker TEXT NOT NULL,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            direction TEXT NOT NULL,
            quantity REAL NOT NULL,
            average_entry_price REAL NOT NULL,
            mode TEXT NOT NULL,
            PRIMARY KEY (ticker, tenant_id, mode)
        );

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

        CREATE INDEX IF NOT EXISTS idx_ml_rows_symbol_ts
          ON ml_learning_rows(tenant_id, symbol, horizon, timestamp);

        CREATE TABLE IF NOT EXISTS ml_models (
            model_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            horizon TEXT NOT NULL,
            train_start TEXT NOT NULL,
            train_end TEXT NOT NULL,
            weights_json TEXT NOT NULL,
            scaler_json TEXT NOT NULL,
            clip_params_json TEXT NOT NULL,
            feature_importance_json TEXT NOT NULL,
            baseline_accuracy REAL NOT NULL,
            model_accuracy REAL NOT NULL,
            ic_score REAL NOT NULL,
            avg_return REAL NOT NULL,
            win_rate REAL NOT NULL,
            train_rows INTEGER NOT NULL,
            feature_coverage_used REAL NOT NULL,
            score_std REAL NOT NULL DEFAULT 0.01,
            passed_gate INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_ml_models_horizon_date
          ON ml_models(horizon, passed_gate, created_at DESC);

        CREATE TABLE IF NOT EXISTS fundamentals_snapshot (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            revenue_ttm REAL,
            revenue_growth REAL,
            shares_outstanding REAL,
            shares_growth REAL,
            sector TEXT,
            industry TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, ticker, as_of_date)
        );

        CREATE INDEX IF NOT EXISTS idx_fundamentals_latest
          ON fundamentals_snapshot(tenant_id, ticker, as_of_date DESC);

        CREATE TABLE IF NOT EXISTS discovery_candidates (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            as_of_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            score REAL NOT NULL,
            reason TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, as_of_date, symbol, strategy_type)
        );

        CREATE INDEX IF NOT EXISTS idx_discovery_by_strategy
          ON discovery_candidates(tenant_id, as_of_date, strategy_type, score DESC);

        CREATE TABLE IF NOT EXISTS discovery_watchlist (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            as_of_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            overlap_count INTEGER NOT NULL,
            days_seen INTEGER NOT NULL,
            avg_score REAL NOT NULL,
            playbook_id TEXT NOT NULL DEFAULT '',
            prediction_plan_json TEXT NOT NULL DEFAULT '{}',
            strategies_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, as_of_date, symbol)
        );

        CREATE INDEX IF NOT EXISTS idx_discovery_watchlist_rank
          ON discovery_watchlist(tenant_id, as_of_date, overlap_count DESC, days_seen DESC, avg_score DESC);

        CREATE TABLE IF NOT EXISTS prediction_queue (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            as_of_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'discovery',
            priority INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, as_of_date, symbol, source)
        );

        CREATE INDEX IF NOT EXISTS idx_prediction_queue_pending
          ON prediction_queue(tenant_id, status, priority DESC, created_at DESC);

        CREATE TABLE IF NOT EXISTS discovery_outcomes (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            watchlist_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            entry_date TEXT NOT NULL,
            exit_date TEXT,
            entry_close REAL NOT NULL,
            exit_close REAL,
            return_pct REAL,
            overlap_count INTEGER,
            days_seen INTEGER,
            strategies_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, watchlist_date, symbol, horizon_days)
        );

        CREATE INDEX IF NOT EXISTS idx_discovery_outcomes_lookup
          ON discovery_outcomes(tenant_id, watchlist_date, horizon_days, return_pct);

        CREATE TABLE IF NOT EXISTS discovery_candidate_outcomes (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            as_of_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            strategy_type TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            entry_date TEXT NOT NULL,
            exit_date TEXT,
            entry_close REAL NOT NULL,
            exit_close REAL,
            return_pct REAL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tenant_id, as_of_date, symbol, strategy_type, horizon_days)
        );

        CREATE INDEX IF NOT EXISTS idx_discovery_candidate_outcomes_lookup
          ON discovery_candidate_outcomes(tenant_id, as_of_date, horizon_days, strategy_type, return_pct);

        CREATE TABLE IF NOT EXISTS discovery_stats (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            end_date TEXT NOT NULL,
            window_days INTEGER NOT NULL,
            horizon_days INTEGER NOT NULL,
            group_type TEXT NOT NULL,
            group_value TEXT NOT NULL,
            n INTEGER NOT NULL,
            avg_return REAL NOT NULL,
            win_rate REAL NOT NULL,
            lift REAL NOT NULL DEFAULT 0.0,
            status TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (tenant_id, computed_at, end_date, window_days, horizon_days, group_type, group_value)
        );

        CREATE INDEX IF NOT EXISTS idx_discovery_stats_recent
          ON discovery_stats(tenant_id, computed_at DESC, group_type);

        CREATE TABLE IF NOT EXISTS sniper_near_misses (
            symbol          TEXT NOT NULL,
            as_of_date      TEXT NOT NULL,
            score           REAL,
            price_extreme   REAL,
            vol_extreme     REAL,
            spike_extreme   REAL,
            trend_extreme   REAL,
            fear_regime     INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (symbol, as_of_date)
        );

        CREATE INDEX IF NOT EXISTS idx_sniper_near_misses_date
          ON sniper_near_misses(as_of_date);

        CREATE TABLE IF NOT EXISTS discovery_jobs (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_discovery_jobs_type
          ON discovery_jobs(tenant_id, job_type, started_at DESC);

        CREATE TABLE IF NOT EXISTS prediction_jobs (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            as_of_date TEXT,
            run_id TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_prediction_jobs_recent
          ON prediction_jobs(tenant_id, started_at DESC);

        CREATE TABLE IF NOT EXISTS experiment_classes (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            class_key TEXT NOT NULL,
            display_name TEXT NOT NULL,
            description TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, class_key)
        );

        CREATE TABLE IF NOT EXISTS experiments (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            class_key TEXT NOT NULL,
            experiment_key TEXT NOT NULL,
            display_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'sandbox',
            version TEXT NOT NULL DEFAULT 'v1',
            config_json TEXT NOT NULL DEFAULT '{}',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenant_id, class_key, experiment_key)
        );

        CREATE TABLE IF NOT EXISTS experiment_runs (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            class_key TEXT NOT NULL,
            experiment_key TEXT NOT NULL,
            as_of_date TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS experiment_results (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            run_id TEXT NOT NULL,
            class_key TEXT NOT NULL,
            experiment_key TEXT NOT NULL,
            metric_5d_return REAL,
            metric_20d_return REAL,
            win_rate REAL,
            drawdown REAL,
            turnover REAL,
            regime_json TEXT NOT NULL DEFAULT '{}',
            calibration_json TEXT NOT NULL DEFAULT '{}',
            overlap_json TEXT NOT NULL DEFAULT '{}',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_experiments_lookup
          ON experiments(tenant_id, class_key, experiment_key, active);

        CREATE INDEX IF NOT EXISTS idx_experiment_runs_lookup
          ON experiment_runs(tenant_id, class_key, experiment_key, started_at DESC);

        CREATE INDEX IF NOT EXISTS idx_experiment_results_lookup
          ON experiment_results(tenant_id, class_key, experiment_key, created_at DESC);

        CREATE TABLE IF NOT EXISTS candidate_queue (
            tenant_id TEXT NOT NULL DEFAULT 'default',
            ticker TEXT NOT NULL,
            status TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            signal_count INTEGER NOT NULL DEFAULT 0,
            rejection_reason TEXT,
            primary_strategy TEXT,
            strategy_tags_json TEXT NOT NULL DEFAULT '[]',
            discovery_lens TEXT,
            discovery_score REAL,
            price_bucket TEXT,
            market_cap_bucket TEXT,
            sector TEXT,
            industry TEXT,
            multiplier_score REAL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (tenant_id, ticker),
            CHECK (status IN ('seen', 'recurring', 'shortlisted', 'admitted', 'rejected'))
        );

        CREATE INDEX IF NOT EXISTS idx_candidate_queue_status
          ON candidate_queue(tenant_id, status);

        CREATE TABLE IF NOT EXISTS admission_metrics (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            run_at TEXT NOT NULL,
            newly_admitted_count INTEGER NOT NULL DEFAULT 0,
            overrule_swap_count INTEGER NOT NULL DEFAULT 0,
            overrule_detail_json TEXT NOT NULL DEFAULT '[]',
            thresholds_json TEXT NOT NULL DEFAULT '{}',
            admitted_total INTEGER NOT NULL DEFAULT 0,
            avg_multiplier_admitted REAL,
            avg_multiplier_recurring REAL,
            avg_multiplier_rejected REAL,
            lens_admitted_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE INDEX IF NOT EXISTS idx_admission_metrics_tenant_run
          ON admission_metrics(tenant_id, run_at DESC);
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

        # ml_models additions
        ml_cols = cols("ml_models")
        if ml_cols and "group_weights_json" not in ml_cols:
            try:
                self.conn.execute("ALTER TABLE ml_models ADD COLUMN group_weights_json TEXT NOT NULL DEFAULT '{}';")
            except Exception:
                pass

        # discovery_watchlist additions
        wl_cols = cols("discovery_watchlist")
        if wl_cols and "playbook_id" not in wl_cols:
            try:
                self.conn.execute("ALTER TABLE discovery_watchlist ADD COLUMN playbook_id TEXT NOT NULL DEFAULT '';")
            except Exception:
                pass
        if wl_cols and "prediction_plan_json" not in wl_cols:
            try:
                self.conn.execute("ALTER TABLE discovery_watchlist ADD COLUMN prediction_plan_json TEXT NOT NULL DEFAULT '{}';")
            except Exception:
                pass

        # discovery_stats additions
        ds_cols = cols("discovery_stats")
        if ds_cols and "lift" not in ds_cols:
            try:
                self.conn.execute("ALTER TABLE discovery_stats ADD COLUMN lift REAL NOT NULL DEFAULT 0.0;")
            except Exception:
                pass
        if ds_cols and "status" not in ds_cols:
            try:
                self.conn.execute("ALTER TABLE discovery_stats ADD COLUMN status TEXT NOT NULL DEFAULT '';")
            except Exception:
                pass

        # discovery_jobs additions (table is new; safe if missing)
        try:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS discovery_jobs (
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    message TEXT
                );
                """
            )
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_jobs_type ON discovery_jobs(tenant_id, job_type, started_at DESC);")
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

        # trades additions
        t_cols = cols("trades")
        if t_cols and "analysis" not in t_cols:
            try:
                self.conn.execute("ALTER TABLE trades ADD COLUMN analysis TEXT;")
            except Exception:
                pass
        
        if t_cols and "llm_prediction" not in t_cols:
            try:
                self.conn.execute("ALTER TABLE trades ADD COLUMN llm_prediction TEXT;")
            except Exception:
                pass
        
        for col, col_type in [
            ("engine_decision", "TEXT"),
            ("llm_status", "TEXT"),
            ("llm_agrees", "INTEGER"),
            ("prediction_id", "TEXT"),
            ("broker_order_id", "TEXT"),
            ("source", "TEXT"),
        ]:
            if t_cols and col not in t_cols:
                try:
                    self.conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {col_type};")
                except Exception:
                    pass

        try:
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_prediction_id ON trades(tenant_id, prediction_id);"
            )
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

        pred_cols = cols("predictions")
        if pred_cols and "rank_score" not in pred_cols:
            try:
                self.conn.execute("ALTER TABLE predictions ADD COLUMN rank_score REAL;")
            except Exception:
                pass
        if pred_cols and "ranking_context_json" not in pred_cols:
            try:
                self.conn.execute("ALTER TABLE predictions ADD COLUMN ranking_context_json TEXT;")
            except Exception:
                pass

        try:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admission_metrics (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL DEFAULT 'default',
                    run_at TEXT NOT NULL,
                    newly_admitted_count INTEGER NOT NULL DEFAULT 0,
                    overrule_swap_count INTEGER NOT NULL DEFAULT 0,
                    overrule_detail_json TEXT NOT NULL DEFAULT '[]',
                    thresholds_json TEXT NOT NULL DEFAULT '{}',
                    admitted_total INTEGER NOT NULL DEFAULT 0,
                    avg_multiplier_admitted REAL,
                    avg_multiplier_recurring REAL,
                    avg_multiplier_rejected REAL,
                    lens_admitted_json TEXT NOT NULL DEFAULT '{}'
                );
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_admission_metrics_tenant_run ON admission_metrics(tenant_id, run_at DESC);"
            )
        except Exception:
            pass

        cq_cols = cols("candidate_queue")
        if cq_cols:
            for col, col_type in [
                ("primary_strategy", "TEXT"),
                ("strategy_tags_json", "TEXT NOT NULL DEFAULT '[]'"),
                ("discovery_lens", "TEXT"),
                ("discovery_score", "REAL"),
                ("price_bucket", "TEXT"),
                ("market_cap_bucket", "TEXT"),
                ("sector", "TEXT"),
                ("industry", "TEXT"),
                ("multiplier_score", "REAL"),
                ("metadata_json", "TEXT NOT NULL DEFAULT '{}'"),
            ]:
                if col not in cq_cols:
                    try:
                        self.conn.execute(f"ALTER TABLE candidate_queue ADD COLUMN {col} {col_type};")
                    except Exception:
                        pass

        # trades and positions (handled by CREATE TABLE IF NOT EXISTS in _create_schema, 
        # but if we need new columns later we add them here)

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

    def list_admitted_candidate_tickers(self, tenant_id: str = "default") -> list[str]:
        """Tickers in candidate_queue with status admitted (dynamic universe)."""
        rows = self.conn.execute(
            """
            SELECT ticker FROM candidate_queue
            WHERE tenant_id = ? AND status = ?
            ORDER BY ticker ASC
            """,
            (tenant_id, "admitted"),
        ).fetchall()
        return sorted({str(r["ticker"]).strip().upper() for r in rows if r["ticker"]})

    def merge_discovery_into_candidate_queue(
        self,
        *,
        tenant_id: str,
        ticker: str,
        strategy_type: str,
        discovery_score: float,
        discovery_lens: str,
        as_of_date: str,
        price_bucket: str | None,
        market_cap_bucket: str | None,
        sector: str | None,
        industry: str | None,
        price_percentile_252d: float | None,
        volatility_20d: float | None,
    ) -> None:
        """
        Upsert discovery hits into candidate_queue (gated pipeline; not ranking).

        Preserves admitted/shortlisted; bumps seen→recurring after repeated contact.
        multiplier_score is promotion-side only (see app.discovery.candidate_queue_tags).
        """
        from app.core.candidate_scoring import compute_multiplier_score, merge_strategy_tags_json

        sym = str(ticker).strip().upper()
        if not sym:
            return
        now = self.now_iso()
        row = self.conn.execute(
            """
            SELECT status, signal_count, strategy_tags_json, multiplier_score, discovery_score,
                   sector, industry, price_bucket, market_cap_bucket
            FROM candidate_queue
            WHERE tenant_id = ? AND ticker = ?
            """,
            (tenant_id, sym),
        ).fetchone()
        if row and str(row["status"] or "") == "rejected":
            return

        prev_tags = str(row["strategy_tags_json"]) if row and row["strategy_tags_json"] is not None else None
        tags_json = merge_strategy_tags_json(
            prev_tags,
            strategy_type=str(strategy_type),
            score=float(discovery_score),
            discovery_lens=str(discovery_lens),
            as_of_date=str(as_of_date),
        )
        next_count = int((row["signal_count"] if row else 0) or 0) + 1

        mult = compute_multiplier_score(
            price_percentile_252d=price_percentile_252d,
            volatility_20d=volatility_20d,
            signal_count=next_count,
        )
        if row and row["multiplier_score"] is not None:
            mult = max(float(row["multiplier_score"]), mult)

        best_score = float(discovery_score)
        if row and row["discovery_score"] is not None:
            best_score = max(best_score, float(row["discovery_score"]))

        pb = price_bucket if price_bucket is not None else (str(row["price_bucket"]) if row and row["price_bucket"] else None)
        mcb = market_cap_bucket if market_cap_bucket is not None else (
            str(row["market_cap_bucket"]) if row and row["market_cap_bucket"] else None
        )
        sec = sector if sector is not None else (str(row["sector"]) if row and row["sector"] else None)
        ind = industry if industry is not None else (str(row["industry"]) if row and row["industry"] else None)

        cur_status = str(row["status"] or "seen") if row else "seen"
        if cur_status in ("admitted", "shortlisted"):
            new_status = cur_status
        elif next_count >= 3:
            new_status = "recurring"
        else:
            new_status = "seen"

        if not row:
            self.conn.execute(
                """
                INSERT INTO candidate_queue (
                  tenant_id, ticker, status, first_seen_at, last_seen_at, signal_count,
                  rejection_reason, primary_strategy, strategy_tags_json, discovery_lens, discovery_score,
                  price_bucket, market_cap_bucket, sector, industry, multiplier_score, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    sym,
                    new_status,
                    now,
                    now,
                    next_count,
                    None,
                    str(strategy_type),
                    tags_json,
                    str(discovery_lens),
                    best_score,
                    pb,
                    mcb,
                    sec,
                    ind,
                    mult,
                    "{}",
                ),
            )
        else:
            self.conn.execute(
                """
                UPDATE candidate_queue SET
                  last_seen_at = ?,
                  signal_count = ?,
                  status = ?,
                  primary_strategy = ?,
                  strategy_tags_json = ?,
                  discovery_lens = ?,
                  discovery_score = ?,
                  price_bucket = ?,
                  market_cap_bucket = ?,
                  sector = ?,
                  industry = ?,
                  multiplier_score = ?
                WHERE tenant_id = ? AND ticker = ?
                """,
                (
                    now,
                    next_count,
                    new_status,
                    str(strategy_type),
                    tags_json,
                    str(discovery_lens),
                    best_score,
                    pb,
                    mcb,
                    sec,
                    ind,
                    mult,
                    tenant_id,
                    sym,
                ),
            )
        self.conn.commit()

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
        """Save prediction using unified schema with playbook gating"""
        prediction_id = prediction_data.get("id") or str(uuid4())

        # Determine playbook_id based on strategy_id
        strategy_id = prediction_data.get("strategy_id", "")
        playbook_id = self._get_playbook_for_strategy(strategy_id)

        # Check if symbol is in structural_candidates
        ticker = prediction_data.get("ticker", "")
        structural_filter = self._check_structural_candidate(ticker, tenant_id)

        self.conn.execute("""
            INSERT OR REPLACE INTO predictions
            (id, tenant_id, strategy_id, scored_event_id, ticker, timestamp,
             prediction, confidence, horizon, entry_price, mode, feature_snapshot_json,
             regime, trend_strength, playbook_id, discovery_overlap)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            prediction_data.get("trend_strength"),
            playbook_id,
            1 if structural_filter else 0  # discovery_overlap = 1 if structural
        ))

        self.conn.commit()
        return prediction_id

    def _get_playbook_for_strategy(self, strategy_id: str) -> str:
        """Map strategy_id to playbook_id"""
        sid = strategy_id.lower()

        # High-quality playbooks
        if "realness_repricer" in sid or "balance_sheet" in sid:
            return "distressed_repricer"
        if "ownership_vacuum" in sid:
            return "early_accumulation_breakout"
        if "silent_compounder" in sid:
            return "silent_compounder_trend_adoption"
        if "narrative_lag" in sid:
            return "narrative_lag_catchup"

        # Strategy family mapping
        if "ml" in sid or "factor" in sid or "ai_" in sid:
            return "ml_factor_playbook"
        if "technical" in sid:
            return "technical_playbook"
        if "sentiment" in sid:
            return "sentiment_playbook"
        if "baseline" in sid or "momentum" in sid:
            return "momentum_playbook"

        return "unclassified"

    def _check_structural_candidate(self, symbol: str, tenant_id: str) -> bool:
        """Check if symbol is a structural candidate"""
        row = self.conn.execute("""
            SELECT 1 FROM structural_candidates
            WHERE symbol = ? AND tenant_id = ?
            LIMIT 1
        """, (symbol, tenant_id)).fetchone()
        return row is not None

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

    def save_trade(self, trade_data: Dict[str, Any], tenant_id: str = "default") -> str:
        """Save a trade to the database (links to predictions when prediction_id set)."""
        trade_id = trade_data.get("id") or str(uuid4())
        src = (trade_data.get("source") or "").strip().lower()
        if src and src != "manual" and not (trade_data.get("prediction_id") or "").strip():
            logger.warning(
                "Trade missing prediction_id (source=%s, trade_id=%s, ticker=%s)",
                src,
                trade_id,
                trade_data.get("ticker"),
            )
        self.conn.execute("""
            INSERT OR REPLACE INTO trades 
            (id, tenant_id, ticker, direction, quantity, entry_price, exit_price, pnl, status, mode, strategy_id, timestamp, analysis, llm_prediction, engine_decision, llm_status, llm_agrees, prediction_id, broker_order_id, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_id,
            tenant_id,
            trade_data["ticker"],
            trade_data["direction"],
            trade_data["quantity"],
            trade_data["entry_price"],
            trade_data.get("exit_price"),
            trade_data.get("pnl"),
            trade_data["status"],
            trade_data["mode"],
            trade_data.get("strategy_id"),
            trade_data.get("timestamp") or self.now_iso(),
            trade_data.get("analysis"),
            trade_data.get("llm_prediction"),
            trade_data.get("engine_decision"),
            trade_data.get("llm_status"),
            trade_data.get("llm_agrees"),
            trade_data.get("prediction_id"),
            trade_data.get("broker_order_id"),
            trade_data.get("source"),
        ))
        self.conn.commit()
        return trade_id
    
    def upsert_position(self, position_data: Dict[str, Any], tenant_id: str = "default") -> None:
        """Insert or update a position row (ticker + tenant + mode)."""
        self.conn.execute("""
            INSERT OR REPLACE INTO positions
            (ticker, tenant_id, direction, quantity, average_entry_price, mode)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            position_data["ticker"],
            tenant_id,
            position_data["direction"],
            float(position_data["quantity"]),
            float(position_data["average_entry_price"]),
            position_data["mode"],
        ))
        self.conn.commit()

    def get_positions(self, mode: Optional[str] = None, tenant_id: str = "default") -> List[Dict[str, Any]]:
        """Get positions, optionally filtered by mode."""
        query = "SELECT * FROM positions WHERE tenant_id = ?"
        params: List[Any] = [tenant_id]
        if mode:
            query += " AND mode = ?"
            params.append(mode)
        rows = self.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_trades(self, ticker: Optional[str] = None, tenant_id: str = "default") -> List[Dict[str, Any]]:
        """Get all trades, optionally filtered by ticker."""
        query = "SELECT * FROM trades WHERE tenant_id = ?"
        params = [tenant_id]
        
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker)
            
        rows = self.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def save_price_bars(self, ticker: str, timeframe: str, bars: List[Any], tenant_id: str = "default") -> None:
        """
        Save OHLCV bars to the price_bars table.
        Args:
            ticker: Ticker symbol
            timeframe: Bar timeframe (e.g., '1d', '1h')
            bars: List of Bar objects (must have timestamp, open, high, low, close, volume attributes)
            tenant_id: Tenant identifier
        """
        if not bars:
            return

        rows = []
        for bar in bars:
            rows.append((
                tenant_id,
                ticker,
                timeframe,
                bar.timestamp,
                float(bar.open),
                float(bar.high),
                float(bar.low),
                float(bar.close),
                float(bar.volume)
            ))

        self.conn.executemany("""
            INSERT OR REPLACE INTO price_bars 
            (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        self.conn.commit()

    def upsert_fundamentals_snapshot(self, row: Dict[str, Any], tenant_id: str = "default") -> None:
        """
        Upsert a fundamentals snapshot for (tenant, ticker, as_of_date).

        Expected keys:
          ticker, as_of_date, revenue_ttm, revenue_growth, shares_outstanding, shares_growth, sector, industry
        """
        self.conn.execute(
            """
            INSERT OR REPLACE INTO fundamentals_snapshot
              (tenant_id, ticker, as_of_date, revenue_ttm, revenue_growth, shares_outstanding, shares_growth, sector, industry)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                str(row["ticker"]).upper(),
                str(row["as_of_date"]),
                row.get("revenue_ttm"),
                row.get("revenue_growth"),
                row.get("shares_outstanding"),
                row.get("shares_growth"),
                row.get("sector"),
                row.get("industry"),
            ),
        )
        self.conn.commit()

    def upsert_discovery_candidates(
        self, as_of_date: str, candidates: List[Dict[str, Any]], tenant_id: str = "default"
    ) -> None:
        """
        Upsert discovery candidates for a given as_of_date.

        Each row must include: symbol, strategy_type, score, reason, metadata_json
        """
        if not candidates:
            return
        rows = []
        for c in candidates:
            rows.append(
                (
                    tenant_id,
                    str(as_of_date),
                    str(c["symbol"]).upper(),
                    str(c["strategy_type"]),
                    float(c["score"]),
                    str(c.get("reason") or ""),
                    str(c.get("metadata_json") or "{}"),
                )
            )
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO discovery_candidates
              (tenant_id, as_of_date, symbol, strategy_type, score, reason, metadata_json)
            VALUES
              (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def upsert_discovery_watchlist(
        self, as_of_date: str, rows_in: List[Dict[str, Any]], tenant_id: str = "default"
    ) -> None:
        if not rows_in:
            return
        rows = []
        for r in rows_in:
            rows.append(
                (
                    tenant_id,
                    str(as_of_date),
                    str(r["symbol"]).upper(),
                    int(r["overlap_count"]),
                    int(r["days_seen"]),
                    float(r["avg_score"]),
                    str(r.get("playbook_id") or ""),
                    str(r.get("prediction_plan_json") or "{}"),
                    str(r.get("strategies_json") or "[]"),
                )
            )
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO discovery_watchlist
              (tenant_id, as_of_date, symbol, overlap_count, days_seen, avg_score, playbook_id, prediction_plan_json, strategies_json)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def upsert_prediction_queue(
        self, as_of_date: str, rows_in: List[Dict[str, Any]], tenant_id: str = "default"
    ) -> None:
        if not rows_in:
            return
        rows = []
        for r in rows_in:
            rows.append(
                (
                    tenant_id,
                    str(as_of_date),
                    str(r["symbol"]).upper(),
                    str(r.get("source") or "discovery"),
                    int(r.get("priority") or 0),
                    str(r.get("status") or "pending"),
                    str(r.get("metadata_json") or "{}"),
                )
            )
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO prediction_queue
              (tenant_id, as_of_date, symbol, source, priority, status, metadata_json)
            VALUES
              (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def list_prediction_queue(
        self,
        *,
        as_of_date: str | None = None,
        status: str = "pending",
        limit: int = 200,
        tenant_id: str = "default",
    ) -> list[dict[str, Any]]:
        where = ["tenant_id = ?", "status = ?"]
        params: list[Any] = [str(tenant_id), str(status)]
        if as_of_date is not None:
            where.append("as_of_date = ?")
            params.append(str(as_of_date))
        where_sql = " AND ".join(where)
        rows = self.conn.execute(
            f"""
            SELECT tenant_id, as_of_date, symbol, source, priority, status, metadata_json, created_at
            FROM prediction_queue
            WHERE {where_sql}
            ORDER BY priority DESC, created_at DESC, symbol ASC
            LIMIT ?
            """,
            tuple(params + [int(limit)]),
        ).fetchall()
        return [dict(r) for r in (rows or [])]

    def set_prediction_queue_status(
        self,
        *,
        as_of_date: str,
        symbol: str,
        source: str = "discovery",
        status: str,
        metadata_json: str | None = None,
        tenant_id: str = "default",
    ) -> None:
        if metadata_json is None:
            self.conn.execute(
                """
                UPDATE prediction_queue
                SET status = ?
                WHERE tenant_id = ? AND as_of_date = ? AND symbol = ? AND source = ?
                """,
                (str(status), str(tenant_id), str(as_of_date), str(symbol).upper(), str(source)),
            )
        else:
            self.conn.execute(
                """
                UPDATE prediction_queue
                SET status = ?, metadata_json = ?
                WHERE tenant_id = ? AND as_of_date = ? AND symbol = ? AND source = ?
                """,
                (str(status), str(metadata_json), str(tenant_id), str(as_of_date), str(symbol).upper(), str(source)),
            )
        self.conn.commit()

    def set_prediction_queue_status_many(
        self,
        *,
        rows: list[dict[str, Any]],
        tenant_id: str = "default",
    ) -> None:
        """
        Bulk status updates for prediction_queue.

        Each row must include: as_of_date, symbol, source, status.
        Optional: metadata_json.
        """
        if not rows:
            return
        has_meta = any(("metadata_json" in r and r.get("metadata_json") is not None) for r in rows)
        if has_meta:
            payload = []
            for r in rows:
                payload.append(
                    (
                        str(r.get("status")),
                        str(r.get("metadata_json") or "{}"),
                        str(tenant_id),
                        str(r.get("as_of_date")),
                        str(r.get("symbol") or "").upper(),
                        str(r.get("source") or "discovery"),
                    )
                )
            self.conn.executemany(
                """
                UPDATE prediction_queue
                SET status = ?, metadata_json = ?
                WHERE tenant_id = ? AND as_of_date = ? AND symbol = ? AND source = ?
                """,
                payload,
            )
        else:
            payload = []
            for r in rows:
                payload.append(
                    (
                        str(r.get("status")),
                        str(tenant_id),
                        str(r.get("as_of_date")),
                        str(r.get("symbol") or "").upper(),
                        str(r.get("source") or "discovery"),
                    )
                )
            self.conn.executemany(
                """
                UPDATE prediction_queue
                SET status = ?
                WHERE tenant_id = ? AND as_of_date = ? AND symbol = ? AND source = ?
                """,
                payload,
            )
        self.conn.commit()

    def update_prediction_queue_metadata_many(
        self,
        *,
        rows: list[dict[str, Any]],
        tenant_id: str = "default",
    ) -> None:
        """
        Bulk metadata-only updates for prediction_queue.

        Each row must include: as_of_date, symbol, source, metadata_json.
        """
        if not rows:
            return
        payload = []
        for r in rows:
            payload.append(
                (
                    str(r.get("metadata_json") or "{}"),
                    str(tenant_id),
                    str(r.get("as_of_date")),
                    str(r.get("symbol") or "").upper(),
                    str(r.get("source") or "discovery"),
                )
            )
        self.conn.executemany(
            """
            UPDATE prediction_queue
            SET metadata_json = ?
            WHERE tenant_id = ? AND as_of_date = ? AND symbol = ? AND source = ?
            """,
            payload,
        )
        self.conn.commit()

    def upsert_experiment_class(
        self,
        *,
        class_key: str,
        display_name: str,
        description: str = "",
        active: bool = True,
        tenant_id: str = "default",
    ) -> str:
        row = self.conn.execute(
            """
            SELECT id
            FROM experiment_classes
            WHERE tenant_id = ? AND class_key = ?
            """,
            (str(tenant_id), str(class_key)),
        ).fetchone()
        rid = str(row["id"]) if row is not None else str(uuid4())
        self.conn.execute(
            """
            INSERT OR REPLACE INTO experiment_classes
              (id, tenant_id, class_key, display_name, description, active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM experiment_classes WHERE id = ?), CURRENT_TIMESTAMP))
            """,
            (
                rid,
                str(tenant_id),
                str(class_key),
                str(display_name),
                str(description),
                1 if bool(active) else 0,
                rid,
            ),
        )
        self.conn.commit()
        return rid

    def upsert_experiment(
        self,
        *,
        class_key: str,
        experiment_key: str,
        display_name: str,
        status: str = "sandbox",
        version: str = "v1",
        config_json: str = "{}",
        metadata_json: str = "{}",
        active: bool = True,
        tenant_id: str = "default",
    ) -> str:
        row = self.conn.execute(
            """
            SELECT id, created_at
            FROM experiments
            WHERE tenant_id = ? AND class_key = ? AND experiment_key = ?
            """,
            (str(tenant_id), str(class_key), str(experiment_key)),
        ).fetchone()
        rid = str(row["id"]) if row is not None else str(uuid4())
        created_at = str(row["created_at"]) if row is not None else None
        self.conn.execute(
            """
            INSERT OR REPLACE INTO experiments
              (id, tenant_id, class_key, experiment_key, display_name, status, version, config_json, metadata_json, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                rid,
                str(tenant_id),
                str(class_key),
                str(experiment_key),
                str(display_name),
                str(status),
                str(version),
                str(config_json or "{}"),
                str(metadata_json or "{}"),
                1 if bool(active) else 0,
                created_at or datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.conn.commit()
        return rid

    def start_experiment_run(
        self,
        *,
        class_key: str,
        experiment_key: str,
        as_of_date: str | None = None,
        metadata_json: str = "{}",
        tenant_id: str = "default",
    ) -> str:
        run_id = str(uuid4())
        self.conn.execute(
            """
            INSERT INTO experiment_runs (id, tenant_id, class_key, experiment_key, as_of_date, status, started_at, metadata_json)
            VALUES (?, ?, ?, ?, ?, 'running', CURRENT_TIMESTAMP, ?)
            """,
            (
                run_id,
                str(tenant_id),
                str(class_key),
                str(experiment_key),
                (str(as_of_date) if as_of_date else None),
                str(metadata_json or "{}"),
            ),
        )
        self.conn.commit()
        return run_id

    def finish_experiment_run(
        self,
        *,
        run_id: str,
        status: str,
        metadata_json: str | None = None,
        tenant_id: str = "default",
    ) -> None:
        if metadata_json is None:
            self.conn.execute(
                """
                UPDATE experiment_runs
                SET status = ?, completed_at = CURRENT_TIMESTAMP
                WHERE id = ? AND tenant_id = ?
                """,
                (str(status), str(run_id), str(tenant_id)),
            )
        else:
            self.conn.execute(
                """
                UPDATE experiment_runs
                SET status = ?, completed_at = CURRENT_TIMESTAMP, metadata_json = ?
                WHERE id = ? AND tenant_id = ?
                """,
                (str(status), str(metadata_json), str(run_id), str(tenant_id)),
            )
        self.conn.commit()

    def insert_experiment_result(
        self,
        *,
        run_id: str,
        class_key: str,
        experiment_key: str,
        metric_5d_return: float | None = None,
        metric_20d_return: float | None = None,
        win_rate: float | None = None,
        drawdown: float | None = None,
        turnover: float | None = None,
        regime_json: str = "{}",
        calibration_json: str = "{}",
        overlap_json: str = "{}",
        metadata_json: str = "{}",
        tenant_id: str = "default",
    ) -> str:
        rid = str(uuid4())
        self.conn.execute(
            """
            INSERT INTO experiment_results
              (id, tenant_id, run_id, class_key, experiment_key, metric_5d_return, metric_20d_return, win_rate, drawdown, turnover, regime_json, calibration_json, overlap_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rid,
                str(tenant_id),
                str(run_id),
                str(class_key),
                str(experiment_key),
                metric_5d_return,
                metric_20d_return,
                win_rate,
                drawdown,
                turnover,
                str(regime_json or "{}"),
                str(calibration_json or "{}"),
                str(overlap_json or "{}"),
                str(metadata_json or "{}"),
            ),
        )
        self.conn.commit()
        return rid

    def set_experiment_status(
        self,
        *,
        class_key: str,
        experiment_key: str,
        status: str,
        tenant_id: str = "default",
    ) -> None:
        self.conn.execute(
            """
            UPDATE experiments
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE tenant_id = ? AND class_key = ? AND experiment_key = ?
            """,
            (str(status), str(tenant_id), str(class_key), str(experiment_key)),
        )
        self.conn.commit()

    def prediction_queue_status_counts(
        self,
        *,
        as_of_date: str | None = None,
        tenant_id: str = "default",
    ) -> dict[str, int]:
        where = ["tenant_id = ?"]
        params: list[Any] = [str(tenant_id)]
        if as_of_date is not None:
            where.append("as_of_date = ?")
            params.append(str(as_of_date))
        where_sql = " AND ".join(where)
        rows = self.conn.execute(
            f"""
            SELECT status, COUNT(*) as n
            FROM prediction_queue
            WHERE {where_sql}
            GROUP BY status
            """,
            tuple(params),
        ).fetchall()
        out: dict[str, int] = {}
        for r in rows or []:
            out[str(r["status"])] = int(r["n"] or 0)
        return out

    def start_prediction_job(
        self,
        *,
        job_type: str,
        as_of_date: str | None = None,
        run_id: str | None = None,
        tenant_id: str = "default",
    ) -> str:
        job_id = str(uuid4())
        started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.conn.execute(
            """
            INSERT INTO prediction_jobs (tenant_id, id, job_type, as_of_date, run_id, status, started_at, completed_at, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (str(tenant_id), job_id, str(job_type), (str(as_of_date) if as_of_date else None), (str(run_id) if run_id else None), "running", started_at),
        )
        self.conn.commit()
        return job_id

    def finish_prediction_job(
        self,
        *,
        job_id: str,
        status: str,
        message: str | None = None,
        tenant_id: str = "default",
    ) -> None:
        completed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.conn.execute(
            """
            UPDATE prediction_jobs
            SET status = ?, completed_at = ?, message = ?
            WHERE tenant_id = ? AND id = ?
            """,
            (str(status), completed_at, (str(message) if message else None), str(tenant_id), str(job_id)),
        )
        self.conn.commit()

    def latest_prediction_job(self, *, tenant_id: str = "default") -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT tenant_id, id, job_type, as_of_date, run_id, status, started_at, completed_at, message
            FROM prediction_jobs
            WHERE tenant_id = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (str(tenant_id),),
        ).fetchone()
        return dict(row) if row else None

    def upsert_discovery_outcomes(
        self, watchlist_date: str, rows_in: List[Dict[str, Any]], tenant_id: str = "default"
    ) -> None:
        if not rows_in:
            return
        rows = []
        for r in rows_in:
            rows.append(
                (
                    tenant_id,
                    str(watchlist_date),
                    str(r["symbol"]).upper(),
                    int(r["horizon_days"]),
                    str(r["entry_date"]),
                    r.get("exit_date"),
                    float(r["entry_close"]),
                    r.get("exit_close"),
                    r.get("return_pct"),
                    r.get("overlap_count"),
                    r.get("days_seen"),
                    str(r.get("strategies_json") or "[]"),
                )
            )
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO discovery_outcomes
              (tenant_id, watchlist_date, symbol, horizon_days, entry_date, exit_date, entry_close, exit_close, return_pct, overlap_count, days_seen, strategies_json)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def upsert_discovery_candidate_outcomes(
        self, as_of_date: str, rows_in: List[Dict[str, Any]], tenant_id: str = "default"
    ) -> None:
        if not rows_in:
            return
        rows = []
        for r in rows_in:
            rows.append(
                (
                    tenant_id,
                    str(as_of_date),
                    str(r["symbol"]).upper(),
                    str(r["strategy_type"]),
                    int(r["horizon_days"]),
                    str(r["entry_date"]),
                    r.get("exit_date"),
                    float(r["entry_close"]),
                    r.get("exit_close"),
                    r.get("return_pct"),
                )
            )
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO discovery_candidate_outcomes
              (tenant_id, as_of_date, symbol, strategy_type, horizon_days, entry_date, exit_date, entry_close, exit_close, return_pct)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def insert_discovery_stats(self, rows_in: List[Dict[str, Any]], tenant_id: str = "default") -> None:
        if not rows_in:
            return
        # Delete stale rows for the same (tenant, end_date, window_days, horizon_days) combos
        # before inserting so re-runs are idempotent.
        keys = set(
            (str(r["end_date"]), int(r["window_days"]), int(r["horizon_days"]))
            for r in rows_in
        )
        for end_date, window_days, horizon_days in keys:
            self.conn.execute(
                """
                DELETE FROM discovery_stats
                WHERE tenant_id = ? AND end_date = ? AND window_days = ? AND horizon_days = ?
                """,
                (tenant_id, end_date, window_days, horizon_days),
            )
        rows = []
        for r in rows_in:
            rows.append(
                (
                    tenant_id,
                    str(r["end_date"]),
                    int(r["window_days"]),
                    int(r["horizon_days"]),
                    str(r["group_type"]),
                    str(r["group_value"]),
                    int(r["n"]),
                    float(r["avg_return"]),
                    float(r["win_rate"]),
                    float(r.get("lift") or 0.0),
                    str(r.get("status") or ""),
                )
            )
        self.conn.executemany(
            """
            INSERT INTO discovery_stats
              (tenant_id, end_date, window_days, horizon_days, group_type, group_value, n, avg_return, win_rate, lift, status)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def start_discovery_job(self, *, job_type: str, tenant_id: str = "default") -> str:
        job_id = str(uuid4())
        started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.conn.execute(
            """
            INSERT INTO discovery_jobs (tenant_id, id, job_type, status, started_at, completed_at, message)
            VALUES (?, ?, ?, ?, ?, NULL, NULL)
            """,
            (str(tenant_id), job_id, str(job_type), "running", started_at),
        )
        self.conn.commit()
        return job_id

    def finish_discovery_job(
        self,
        *,
        job_id: str,
        status: str,
        message: str | None = None,
        tenant_id: str = "default",
    ) -> None:
        completed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        self.conn.execute(
            """
            UPDATE discovery_jobs
            SET status = ?, completed_at = ?, message = ?
            WHERE tenant_id = ? AND id = ?
            """,
            (str(status), completed_at, (str(message) if message else None), str(tenant_id), str(job_id)),
        )
        self.conn.commit()

    def close(self) -> None:
        """Close database connection"""
        self.conn.close()
