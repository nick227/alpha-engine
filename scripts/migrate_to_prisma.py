#!/usr/bin/env python3
"""
Migration Script: Manual SQL → Prisma Unified Schema

This script migrates data from the old manual SQL schema to the unified Prisma schema.
Run this once to migrate existing data, then delete the old SQL repository.
"""
from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime
from pathlib import Path

# TODO: Uncomment after Prisma generation
# from prisma import PrismaClient


class DataMigrator:
    """Migrate data from old SQL schema to unified Prisma schema"""

    def __init__(self, old_db_path: str, new_db_path: str):
        self.old_conn = sqlite3.connect(old_db_path)
        self.new_conn = sqlite3.connect(new_db_path)
        # self.prisma = PrismaClient(datasource_url=f"file:{new_db_path}")

    def migrate_all(self):
        """Run complete migration"""
        print("Starting migration from old SQL to unified Prisma schema...")
        
        # Create new schema tables first
        self._create_unified_schema()
        
        # Migrate data in order of dependencies
        self._migrate_raw_events()
        self._migrate_scored_events()
        self._migrate_mra_outcomes()
        self._migrate_strategies()
        self._migrate_predictions()
        self._migrate_prediction_outcomes()
        self._migrate_price_bars()
        self._migrate_strategy_performance()
        self._migrate_regime_performance()
        self._migrate_strategy_stability()
        
        print("Migration completed successfully!")

    def _create_unified_schema(self):
        """Create unified schema tables"""
        print("Creating unified schema tables...")
        
        # This will be replaced by: npx prisma migrate dev
        schema_sql = """
        -- Raw Events
        CREATE TABLE IF NOT EXISTS RawEvent (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            timestamp TEXT NOT NULL,
            source TEXT NOT NULL,
            text TEXT NOT NULL,
            tickersJson TEXT NOT NULL,
            metadataJson TEXT NOT NULL
        );

        -- Scored Events  
        CREATE TABLE IF NOT EXISTS ScoredEvent (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            rawEventId TEXT NOT NULL,
            primaryTicker TEXT NOT NULL,
            category TEXT NOT NULL,
            materiality REAL NOT NULL,
            direction TEXT NOT NULL,
            confidence REAL NOT NULL,
            companyRelevance REAL NOT NULL,
            conceptTagsJson TEXT NOT NULL,
            explanationTermsJson TEXT NOT NULL,
            scorerVersion TEXT NOT NULL,
            taxonomyVersion TEXT NOT NULL
        );

        -- MRA Outcomes
        CREATE TABLE IF NOT EXISTS MraOutcome (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            scoredEventId TEXT NOT NULL,
            return1m REAL NOT NULL,
            return5m REAL NOT NULL,
            return15m REAL NOT NULL,
            return1h REAL NOT NULL,
            volumeRatio REAL NOT NULL,
            vwapDistance REAL NOT NULL,
            rangeExpansion REAL NOT NULL,
            continuationSlope REAL NOT NULL,
            pullbackDepth REAL NOT NULL,
            mraScore REAL NOT NULL,
            marketContextJson TEXT NOT NULL
        );

        -- Strategies
        CREATE TABLE IF NOT EXISTS Strategy (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            track TEXT NOT NULL,
            parentId TEXT,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            strategyType TEXT NOT NULL,
            mode TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            configJson TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'CANDIDATE',
            regimeFocus TEXT,
            gateLogs TEXT,
            isChampion INTEGER NOT NULL DEFAULT 0,
            backtestScore REAL NOT NULL DEFAULT 0,
            forwardScore REAL NOT NULL DEFAULT 0,
            liveScore REAL NOT NULL DEFAULT 0,
            stabilityScore REAL NOT NULL DEFAULT 0,
            sampleSize INTEGER NOT NULL DEFAULT 0,
            createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            activatedAt TEXT,
            deactivatedAt TEXT
        );

        -- Predictions
        CREATE TABLE IF NOT EXISTS Prediction (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            strategyId TEXT NOT NULL,
            scoredEventId TEXT NOT NULL,
            ticker TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            prediction TEXT NOT NULL,
            confidence REAL NOT NULL,
            horizon TEXT NOT NULL,
            entryPrice REAL NOT NULL,
            mode TEXT NOT NULL,
            featureSnapshotJson TEXT NOT NULL,
            regime TEXT,
            trendStrength TEXT,
            scoredOutcomeId TEXT,
            scoredAt TEXT
        );

        -- Prediction Outcomes
        CREATE TABLE IF NOT EXISTS PredictionOutcome (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            predictionId TEXT NOT NULL,
            exitPrice REAL NOT NULL,
            returnPct REAL NOT NULL,
            directionCorrect INTEGER NOT NULL,
            maxRunup REAL NOT NULL,
            maxDrawdown REAL NOT NULL,
            evaluatedAt TEXT NOT NULL,
            exitReason TEXT NOT NULL,
            residualAlpha REAL NOT NULL DEFAULT 0.0
        );

        -- Price Bars
        CREATE TABLE IF NOT EXISTS PriceBar (
            tenantId TEXT NOT NULL,
            ticker TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (tenantId, ticker, timestamp)
        );

        -- Strategy Performance
        CREATE TABLE IF NOT EXISTS StrategyPerformance (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            strategyId TEXT NOT NULL,
            horizon TEXT NOT NULL,
            predictionCount INTEGER NOT NULL,
            accuracy REAL NOT NULL,
            avgReturn REAL NOT NULL,
            avgResidualAlpha REAL NOT NULL,
            updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenantId, strategyId, horizon)
        );

        -- Regime Performance
        CREATE TABLE IF NOT EXISTS RegimePerformance (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            regime TEXT NOT NULL,
            predictionCount INTEGER NOT NULL,
            accuracy REAL NOT NULL,
            avgReturn REAL NOT NULL,
            updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenantId, regime)
        );

        -- Strategy Stability
        CREATE TABLE IF NOT EXISTS StrategyStability (
            id TEXT PRIMARY KEY,
            tenantId TEXT NOT NULL DEFAULT 'default',
            strategyId TEXT NOT NULL,
            backtestAccuracy REAL NOT NULL,
            liveAccuracy REAL NOT NULL,
            stabilityScore REAL NOT NULL,
            updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(tenantId, strategyId)
        );
        """
        
        self.new_conn.executescript(schema_sql)
        self.new_conn.commit()

    def _migrate_raw_events(self):
        """Migrate raw_events table"""
        print("Migrating raw events...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, timestamp, source, text, tickers_json, metadata_json
            FROM raw_events
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO RawEvent 
                (id, tenantId, timestamp, source, text, tickersJson, metadataJson)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row[0], row[1], row[2], row[3], row[4], row[5], row[6]
            ))
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} raw events")

    def _migrate_scored_events(self):
        """Migrate scored_events table"""
        print("Migrating scored events...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, raw_event_id, primary_ticker, category, materiality,
                   direction, confidence, company_relevance, concept_tags_json,
                   explanation_terms_json, scorer_version, taxonomy_version
            FROM scored_events
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO ScoredEvent 
                (id, tenantId, rawEventId, primaryTicker, category, materiality,
                 direction, confidence, companyRelevance, conceptTagsJson,
                 explanationTermsJson, scorerVersion, taxonomyVersion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} scored events")

    def _migrate_mra_outcomes(self):
        """Migrate mra_outcomes table"""
        print("Migrating MRA outcomes...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, scored_event_id, return_1m, return_5m, return_15m,
                   return_1h, volume_ratio, vwap_distance, range_expansion,
                   continuation_slope, pullback_depth, mra_score, market_context_json
            FROM mra_outcomes
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO MraOutcome 
                (id, tenantId, scoredEventId, return1m, return5m, return15m,
                 return1h, volumeRatio, vwapDistance, rangeExpansion,
                 continuationSlope, pullbackDepth, mraScore, marketContextJson)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} MRA outcomes")

    def _migrate_strategies(self):
        """Migrate strategies table"""
        print("Migrating strategies...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, name, version, strategy_type, mode, active, config_json
            FROM strategies
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO Strategy 
                (id, tenantId, name, version, strategyType, mode, active, configJson)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} strategies")

    def _migrate_predictions(self):
        """Migrate predictions table"""
        print("Migrating predictions...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, strategy_id, scored_event_id, ticker, timestamp,
                   prediction, confidence, horizon, entry_price, mode, 
                   feature_snapshot_json, regime, trend_strength
            FROM predictions
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO Prediction 
                (id, tenantId, strategyId, scoredEventId, ticker, timestamp,
                 prediction, confidence, horizon, entryPrice, mode, 
                 featureSnapshotJson, regime, trendStrength)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} predictions")

    def _migrate_prediction_outcomes(self):
        """Migrate prediction_outcomes table"""
        print("Migrating prediction outcomes...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, prediction_id, exit_price, return_pct, 
                   direction_correct, max_runup, max_drawdown, evaluated_at, exit_reason
            FROM prediction_outcomes
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO PredictionOutcome 
                (id, tenantId, predictionId, exitPrice, returnPct, 
                 directionCorrect, maxRunup, maxDrawdown, evaluatedAt, exitReason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} prediction outcomes")

    def _migrate_price_bars(self):
        """Migrate price_bars table"""
        print("Migrating price bars...")
        
        old_rows = self.old_conn.execute("""
            SELECT tenant_id, ticker, timestamp, open, high, low, close, volume
            FROM price_bars
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO PriceBar 
                (tenantId, ticker, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} price bars")

    def _migrate_strategy_performance(self):
        """Migrate strategy_performance table"""
        print("Migrating strategy performance...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, strategy_id, horizon, prediction_count,
                   accuracy, avg_return, avg_residual_alpha, updated_at
            FROM strategy_performance
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO StrategyPerformance 
                (id, tenantId, strategyId, horizon, predictionCount,
                 accuracy, avgReturn, avgResidualAlpha, updatedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} strategy performance records")

    def _migrate_regime_performance(self):
        """Migrate regime_performance table"""
        print("Migrating regime performance...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, regime, prediction_count, accuracy, avg_return, updated_at
            FROM regime_performance
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO RegimePerformance 
                (id, tenantId, regime, predictionCount, accuracy, avgReturn, updatedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} regime performance records")

    def _migrate_strategy_stability(self):
        """Migrate strategy_stability table"""
        print("Migrating strategy stability...")
        
        old_rows = self.old_conn.execute("""
            SELECT id, tenant_id, strategy_id, backtest_accuracy, 
                   live_accuracy, stability_score, updated_at
            FROM strategy_stability
        """).fetchall()
        
        for row in old_rows:
            self.new_conn.execute("""
                INSERT OR REPLACE INTO StrategyStability 
                (id, tenantId, strategyId, backtestAccuracy, 
                 liveAccuracy, stabilityScore, updatedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row)
        
        self.new_conn.commit()
        print(f"Migrated {len(old_rows)} strategy stability records")

    def close(self):
        """Close database connections"""
        self.old_conn.close()
        self.new_conn.close()


def main():
    """Run migration"""
    project_root = Path(__file__).parent.parent
    old_db = project_root / "data" / "alpha.db"
    new_db = project_root / "data" / "alpha_unified.db"
    
    if not old_db.exists():
        print(f"Old database not found at {old_db}")
        return
    
    migrator = DataMigrator(str(old_db), str(new_db))
    try:
        migrator.migrate_all()
        print(f"\nMigration successful!")
        print(f"Old database: {old_db}")
        print(f"New unified database: {new_db}")
        print("\nNext steps:")
        print("1. Test the new unified database")
        print("2. Update .env to point to alpha_unified.db") 
        print("3. Delete old SQL repository files")
        print("4. Run: npx prisma generate")
    except Exception as e:
        print(f"Migration failed: {e}")
    finally:
        migrator.close()


if __name__ == "__main__":
    main()
