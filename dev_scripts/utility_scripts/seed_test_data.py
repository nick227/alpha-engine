#!/usr/bin/env python3
"""
Simple Test Data Seeder for Portfolio Simulator

Creates minimal test data for Phase 1 portfolio simulation.
Uses only the core tables: Prediction and PredictionOutcome
"""

import sqlite3
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

# Configuration
TENANT_ID = "default"
DB_PATH = "cache/alpha.db"

# Test data constants
TICKERS = ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "NFLX"]
STRATEGIES = [
    {"id": "sentiment_v1", "name": "Sentiment Analysis v1", "track": "sentiment"},
    {"id": "technical_v2", "name": "Technical Analysis v2", "track": "quant"},
    {"id": "quant_v3", "name": "Quant Strategy v3", "track": "quant"},
    {"id": "baseline_v1", "name": "Baseline Strategy v1", "track": "quant"},
    {"id": "text_ml_v2", "name": "Text ML v2", "track": "sentiment"},
]
REGIMES = ["bull_market", "bear_market", "sideways", "high_volatility", "low_volatility"]
HORIZONS = ["15m", "1h", "1d", "7d"]
DIRECTIONS = ["UP", "DOWN", "FLAT"]

class TestDataSeeder:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = None
        self.now = datetime.now(timezone.utc)
        
    def connect(self):
        """Initialize database connection"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            
    def seed_all(self):
        """Seed all test data"""
        print("Starting test data seeding for portfolio simulator...")
        
        self.connect()
        
        try:
            # Create tables if they don't exist
            self.create_tables()
            
            # Seed core data
            self.seed_strategies()
            self.seed_predictions()
            self.seed_prediction_outcomes()
            
            self.conn.commit()
            print("Test data seeding completed successfully!")
            
        except Exception as e:
            print(f"Error seeding data: {e}")
            self.conn.rollback()
            raise
        finally:
            self.close()
            
    def create_tables(self):
        """Create minimal tables needed for simulation"""
        print("Creating tables...")
        
        # Create Strategy table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS Strategy (
                id TEXT PRIMARY KEY,
                tenantId TEXT DEFAULT 'default',
                track TEXT,
                parentId TEXT,
                name TEXT,
                version TEXT,
                strategyType TEXT,
                mode TEXT,
                active BOOLEAN DEFAULT 1,
                configJson TEXT,
                status TEXT DEFAULT 'CANDIDATE',
                regimeFocus TEXT,
                gateLogs TEXT,
                isChampion BOOLEAN DEFAULT 0,
                backtestScore REAL DEFAULT 0,
                forwardScore REAL DEFAULT 0,
                liveScore REAL DEFAULT 0,
                stabilityScore REAL DEFAULT 0,
                sampleSize INTEGER DEFAULT 0,
                createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
                activatedAt DATETIME,
                deactivatedAt DATETIME
            )
        """)
        
        # Create Prediction table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS Prediction (
                id TEXT PRIMARY KEY,
                tenantId TEXT DEFAULT 'default',
                strategyId TEXT,
                scoredEventId TEXT,
                ticker TEXT,
                timestamp DATETIME,
                prediction TEXT,
                confidence REAL,
                horizon TEXT,
                entryPrice REAL,
                mode TEXT,
                featureSnapshotJson TEXT,
                regime TEXT,
                trendStrength TEXT,
                scoredOutcomeId TEXT,
                scoredAt DATETIME,
                FOREIGN KEY (strategyId) REFERENCES Strategy(id)
            )
        """)
        
        # Create PredictionOutcome table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS PredictionOutcome (
                id TEXT PRIMARY KEY,
                tenantId TEXT DEFAULT 'default',
                predictionId TEXT,
                exitPrice REAL,
                returnPct REAL,
                directionCorrect BOOLEAN,
                maxRunup REAL,
                maxDrawdown REAL,
                evaluatedAt DATETIME,
                exitReason TEXT,
                residualAlpha REAL DEFAULT 0.0,
                FOREIGN KEY (predictionId) REFERENCES Prediction(id)
            )
        """)
        
        # Create indexes for performance
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_prediction_tenant_ticker_timestamp 
            ON Prediction(tenantId, ticker, timestamp)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_prediction_tenant_strategy 
            ON Prediction(tenantId, strategyId)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_outcome_tenant_prediction 
            ON PredictionOutcome(tenantId, predictionId)
        """)
            
    def seed_strategies(self):
        """Seed strategy data"""
        print("Seeding strategies...")
        
        for strategy in STRATEGIES:
            strategy_id = f"{strategy['id']}"
            
            # Insert strategy
            self.conn.execute("""
                INSERT OR REPLACE INTO Strategy 
                (id, tenantId, track, name, version, strategyType, mode, active, 
                 configJson, status, isChampion, backtestScore, forwardScore, 
                 liveScore, stabilityScore, sampleSize, createdAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                strategy_id, TENANT_ID, strategy['track'], strategy['name'], "1.0",
                strategy['track'], "paper", True, "{}",
                "ACTIVE", random.choice([0, 1]),
                round(random.uniform(-0.05, 0.15), 3),
                round(random.uniform(-0.05, 0.15), 3),
                round(random.uniform(-0.05, 0.15), 3),
                round(random.uniform(0.4, 0.9), 3),
                random.randint(50, 500),
                (self.now - timedelta(days=random.randint(30, 90))).isoformat()
            ))
            
    def seed_predictions(self):
        """Seed prediction data"""
        print("Seeding predictions...")
        
        # Generate predictions for the past 90 days
        for days_ago in range(90, 0, -1):
            date = self.now - timedelta(days=days_ago)
            
            # Generate 5-20 predictions per day
            num_predictions = random.randint(5, 20)
            
            for _ in range(num_predictions):
                strategy = random.choice(STRATEGIES)
                ticker = random.choice(TICKERS)
                horizon = random.choice(HORIZONS)
                
                # Create prediction timestamp during market hours
                hour = random.randint(9, 16)
                minute = random.randint(0, 59)
                prediction_time = date.replace(hour=hour, minute=minute)
                
                # Generate realistic prediction data
                confidence = random.uniform(0.3, 0.95)
                direction = random.choice(DIRECTIONS)
                entry_price = random.uniform(100, 500)
                
                self.conn.execute("""
                    INSERT INTO Prediction
                    (id, tenantId, strategyId, ticker, timestamp, prediction, 
                     confidence, horizon, entryPrice, mode, featureSnapshotJson, 
                     regime, trendStrength)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, strategy['id'], ticker,
                    prediction_time.isoformat(), direction, confidence, horizon,
                    entry_price, "paper", "{}", random.choice(REGIMES),
                    random.choice(["strong", "moderate", "weak"])
                ))
                
    def seed_prediction_outcomes(self):
        """Seed prediction outcomes"""
        print("Seeding prediction outcomes...")
        
        # Get all predictions without outcomes
        predictions = self.conn.execute("""
            SELECT id, ticker, entryPrice, horizon, prediction, timestamp
            FROM Prediction 
            WHERE tenantId = ? AND scoredOutcomeId IS NULL
        """, (TENANT_ID,)).fetchall()
        
        for pred in predictions:
            # Calculate outcome based on prediction
            horizon_minutes = self._horizon_to_minutes(pred[3])
            
            # Exit time is prediction time + horizon
            prediction_time = datetime.fromisoformat(pred[5])
            exit_time = prediction_time + timedelta(minutes=horizon_minutes)
            
            # Generate realistic price movement
            if pred[4] == "UP":
                return_pct = random.uniform(-0.05, 0.15)  # UP predictions: -5% to +15%
                direction_correct = return_pct > 0.01  # Correct if > 1% gain
            elif pred[4] == "DOWN":
                return_pct = random.uniform(-0.15, 0.05)  # DOWN predictions: -15% to +5%
                direction_correct = return_pct < -0.01  # Correct if > 1% loss
            else:  # FLAT
                return_pct = random.uniform(-0.03, 0.03)  # FLAT predictions: -3% to +3%
                direction_correct = abs(return_pct) < 0.02  # Correct if < 2% movement
            
            # Calculate exit price
            exit_price = pred[2] * (1 + return_pct)
            
            # Generate MFE and MAE
            max_runup = max(0, return_pct * random.uniform(1.2, 2.0))
            max_drawdown = min(0, return_pct * random.uniform(1.2, 2.0))
            
            # Insert outcome
            outcome_id = str(uuid.uuid4())
            self.conn.execute("""
                INSERT INTO PredictionOutcome
                (id, tenantId, predictionId, exitPrice, returnPct, directionCorrect,
                 maxRunup, maxDrawdown, evaluatedAt, exitReason, residualAlpha)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                outcome_id, TENANT_ID, pred[0],
                round(exit_price, 2), round(return_pct, 6), direction_correct,
                round(max_runup, 6), round(max_drawdown, 6),
                exit_time.isoformat(), "horizon_reached", round(return_pct * 0.8, 6)
            ))
            
            # Link prediction to outcome
            self.conn.execute("""
                UPDATE Prediction SET scoredOutcomeId = ? WHERE id = ?
            """, (outcome_id, pred[0]))
            
    def _horizon_to_minutes(self, horizon: str) -> int:
        """Convert horizon string to minutes"""
        if horizon == "15m":
            return 15
        elif horizon == "1h":
            return 60
        elif horizon == "1d":
            return 1440
        elif horizon == "7d":
            return 10080
        else:
            return 60  # Default to 1 hour

def main():
    """Main entry point"""
    seeder = TestDataSeeder()
    seeder.seed_all()

if __name__ == "__main__":
    main()
