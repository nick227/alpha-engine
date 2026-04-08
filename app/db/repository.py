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
        """
        self.conn.executescript(schema)

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

    def now_iso(self) -> str:
        """Get current UTC timestamp as ISO string"""
        return datetime.now(timezone.utc).isoformat()

    def close(self) -> None:
        """Close database connection"""
        self.conn.close()
