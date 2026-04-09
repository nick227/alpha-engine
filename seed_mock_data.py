#!/usr/bin/env python3
"""
Mock Data Seeder for Alpha Engine UI Testing

Generates realistic mock data to populate the database for UI testing.
Covers all major UI components: Dashboard, Intelligence Hub, and Audit pages.
"""

import sqlite3
import uuid
import json
import random
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import hashlib

# Configuration
TENANT_ID = "default"
DB_PATH = "data/alpha.db"

# Mock data constants
TICKERS = ["AAPL", "NVDA", "TSLA", "MSFT", "GOOGL", "AMZN", "META", "NFLX"]
STRATEGIES = [
    {"id": "sentiment_v1", "name": "Sentiment Analysis v1", "track": "sentiment", "type": "sentiment"},
    {"id": "technical_v2", "name": "Technical Analysis v2", "track": "quant", "type": "technical"},
    {"id": "quant_v3", "name": "Quant Strategy v3", "track": "quant", "type": "quant"},
    {"id": "baseline_v1", "name": "Baseline Strategy v1", "track": "quant", "type": "baseline"},
    {"id": "text_ml_v2", "name": "Text ML v2", "track": "sentiment", "type": "text_sentiment"},
]

REGIMES = ["bull_market", "bear_market", "sideways", "high_volatility", "low_volatility"]
HORIZONS = ["1d", "7d", "30d"]
DIRECTIONS = ["BUY", "SELL", "HOLD"]
CATEGORIES = ["earnings", "news", "technical", "macro", "sector"]
SOURCES = ["bloomberg", "reuters", "yahoo_finance", "sec_filings", "twitter"]

class MockDataSeeder:
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
        """Seed all mock data"""
        print("Starting mock data seeding...")
        
        self.connect()
        
        try:
            # Core entities
            self.seed_strategies()
            self.seed_prediction_runs()
            
            # Time series data
            self.seed_price_data()
            self.seed_raw_events()
            self.seed_scored_events()
            
            # Predictions and outcomes
            self.seed_predictions()
            self.seed_prediction_outcomes()
            
            # Performance data
            self.seed_strategy_performance()
            self.seed_consensus_signals()
            self.seed_champion_data()
            
            # System data
            self.seed_system_heartbeats()
            
            self.conn.commit()
            print("Mock data seeding completed successfully!")
            
        except Exception as e:
            print(f"Error seeding data: {e}")
            self.conn.rollback()
            raise
        finally:
            self.close()
            
    def seed_strategies(self):
        """Seed strategy data"""
        print("Seeding strategies...")
        
        for strategy in STRATEGIES:
            strategy_id = f"{strategy['id']}_{TENANT_ID}"
            
            # Insert strategy
            self.conn.execute("""
                INSERT OR REPLACE INTO strategies 
                (id, tenant_id, track, name, version, strategy_type, mode, active, 
                 config_json, status, is_champion, alpha_strategy, regime_focus,
                 gate_logs, sample_size, activated_at, deactivated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                strategy_id, TENANT_ID, strategy['track'], strategy['name'], "1.0",
                strategy['type'], strategy['type'], True, json.dumps({"param": "value"}),
                "ACTIVE", random.choice([0, 1]),  # is_champion as integer
                round(random.uniform(-0.05, 0.15), 3),  # alpha_strategy
                random.choice(REGIMES),
                json.dumps({"promotion_history": []}),
                random.randint(50, 500),
                (self.now - timedelta(days=random.randint(30, 90))).isoformat(),
                None  # deactivated_at
            ))
            
            # Insert performance data
            for horizon in HORIZONS + ["ALL"]:  # Add ALL horizon for champions
                self.conn.execute("""
                    INSERT INTO strategy_performance
                    (id, tenant_id, strategy_id, horizon, prediction_count, accuracy,
                     avg_return, avg_residual_alpha, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, strategy_id, horizon,
                    random.randint(20, 200) if horizon != "ALL" else random.randint(100, 500),
                    round(random.uniform(0.45, 0.85), 3),
                    round(random.uniform(-0.05, 0.15), 4),
                    round(random.uniform(-0.02, 0.08), 4),
                    self.now.isoformat()
                ))
                
            # Insert stability data
            self.conn.execute("""
                INSERT INTO strategy_stability
                (id, tenant_id, strategy_id, backtest_accuracy, live_accuracy,
                 stability_score, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()), TENANT_ID, strategy_id,
                round(random.uniform(0.6, 0.9), 3),
                round(random.uniform(0.5, 0.85), 3),
                round(random.uniform(0.4, 0.8), 3),
                self.now.isoformat()
            ))
            
            # Insert strategy weights for champions
            self.conn.execute("""
                INSERT INTO strategy_weights
                (id, tenant_id, strategy_id, win_rate, alpha, stability,
                 confidence_weight, regime_strength_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()), TENANT_ID, strategy_id,
                round(random.uniform(0.5, 0.85), 3),
                round(random.uniform(-0.05, 0.15), 3),
                round(random.uniform(0.4, 0.9), 3),
                round(random.uniform(0.3, 0.8), 3),
                json.dumps({regime: round(random.uniform(0.3, 0.9), 3) for regime in REGIMES}),
                self.now.isoformat()
            ))
            
    def seed_prediction_runs(self):
        """Seed prediction run data"""
        print("Seeding prediction runs...")
        
        # Create multiple runs for different time periods
        for i in range(3):
            run_id = str(uuid.uuid4())
            start_date = self.now - timedelta(days=30 * (i + 1))
            end_date = self.now - timedelta(days=30 * i)
            
            self.conn.execute("""
                INSERT OR REPLACE INTO prediction_runs
                (id, tenant_id, ingress_start, ingress_end, prediction_start,
                 prediction_end, timeframe, regime, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id, TENANT_ID,
                start_date.isoformat(),
                (start_date + timedelta(days=1)).isoformat(),
                start_date.isoformat(),
                end_date.isoformat(),
                "1d", random.choice(REGIMES),
                start_date.isoformat()
            ))
            
    def seed_price_data(self):
        """Seed historical price data"""
        print("Seeding price data...")
        
        for ticker in TICKERS:
            base_price = random.uniform(100, 500)
            
            for days_ago in range(90, 0, -1):
                date = self.now - timedelta(days=days_ago)
                
                # Generate realistic OHLC data
                open_price = base_price + random.uniform(-5, 5)
                close_price = open_price + random.uniform(-10, 10)
                high_price = max(open_price, close_price) + random.uniform(0, 5)
                low_price = min(open_price, close_price) - random.uniform(0, 5)
                volume = random.randint(1000000, 10000000)
                
                self.conn.execute("""
                    INSERT OR REPLACE INTO price_bars
                    (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    TENANT_ID, ticker, "1d", date.isoformat(),
                    round(open_price, 2), round(high_price, 2), 
                    round(low_price, 2), round(close_price, 2), volume
                ))
                
                base_price = close_price  # Use close as next day's base
                
    def seed_raw_events(self):
        """Seed raw news/events data"""
        print("Seeding raw events...")
        
        event_templates = [
            "{ticker} reports stronger than expected Q{quarter} earnings",
            "Analysts upgrade {ticker} rating from {old_rating} to {new_rating}",
            "{ticker} announces new product launch in {category} segment",
            "Market volatility affects {ticker} stock price",
            "{ticker} CEO discusses growth strategy in investor conference",
            "Regulatory approval benefits {ticker} operations",
            "{ticker} beats revenue estimates by {percent}%",
            "Supply chain concerns impact {ticker} production forecasts"
        ]
        
        for days_ago in range(30, 0, -1):
            # Generate 5-15 events per day
            for _ in range(random.randint(5, 15)):
                event_time = self.now - timedelta(days=days_ago, hours=random.randint(0, 23))
                ticker = random.choice(TICKERS)
                
                template = random.choice(event_templates)
                text = template.format(
                    ticker=ticker,
                    quarter=random.randint(1, 4),
                    old_rating=random.choice(["Hold", "Sell", "Underperform"]),
                    new_rating=random.choice(["Buy", "Outperform", "Strong Buy"]),
                    category=random.choice(["AI", "Cloud", "Automotive", "Healthcare"]),
                    percent=round(random.uniform(1, 20), 1)
                )
                
                # Extract tickers from text (simplified)
                mentioned_tickers = [ticker] + random.sample([t for t in TICKERS if t != ticker], 
                                                           random.randint(0, 2))
                
                self.conn.execute("""
                    INSERT OR REPLACE INTO raw_events
                    (id, tenant_id, timestamp, source, text, tickers_json, metadata_json, ingested_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, event_time.isoformat(),
                    random.choice(SOURCES), text,
                    json.dumps(mentioned_tickers),
                    json.dumps({"category": random.choice(CATEGORIES), "sentiment": random.uniform(-1, 1)}),
                    event_time.isoformat()  # Use event_time as ingested_at
                ))
                
    def seed_scored_events(self):
        """Seed scored events based on raw events"""
        print("Seeding scored events...")
        
        # Get raw events
        raw_events = self.conn.execute("""
            SELECT id, tickers_json FROM raw_events 
            WHERE tenant_id = ? ORDER BY timestamp DESC LIMIT 100
        """, (TENANT_ID,)).fetchall()
        
        for raw_event in raw_events:
            try:
                tickers = json.loads(raw_event[1])  # tickers_json is at index 1
            except Exception as e:
                print(f"Failed to parse JSON for raw_event {raw_event[0]}: {raw_event[1]}")
                continue
            
            for ticker in tickers[:2]:  # Score for up to 2 tickers per event
                self.conn.execute("""
                    INSERT OR REPLACE INTO scored_events
                    (id, tenant_id, raw_event_id, primary_ticker, category, materiality,
                     direction, confidence, company_relevance, concept_tags_json,
                     explanation_terms_json, scorer_version, taxonomy_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, raw_event[0], ticker,
                    random.choice(CATEGORIES),
                    round(random.uniform(0.1, 1.0), 3),
                    random.choice(DIRECTIONS),
                    round(random.uniform(0.3, 0.95), 3),
                    round(random.uniform(0.5, 1.0), 3),
                    json.dumps([f"tag_{i}" for i in range(random.randint(1, 4))]),
                    json.dumps([f"term_{i}" for i in range(random.randint(1, 3))]),
                    "v1.0", "v2.0"
                ))
                
    def seed_predictions(self):
        """Seed prediction data"""
        print("Seeding predictions...")
        
        # Get scored events to link predictions to
        scored_events = self.conn.execute("""
            SELECT id, primary_ticker FROM scored_events
            WHERE tenant_id = ? LIMIT 50
        """, (TENANT_ID,)).fetchall()
        
        for scored_event in scored_events:
            for strategy in STRATEGIES:
                strategy_id = f"{strategy['id']}_{TENANT_ID}"
                
                # Create prediction
                prediction_time = self.now - timedelta(days=random.randint(1, 30))
                horizon = random.choice([1, 7, 30])
                
                self.conn.execute("""
                    INSERT INTO predictions
                    (id, tenant_id, strategy_id, scored_event_id, ticker, timestamp,
                     prediction, confidence, horizon, entry_price, mode,
                     feature_snapshot_json, regime, trend_strength, idempotency_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, strategy_id, scored_event[0],
                    scored_event[1], prediction_time.isoformat(),
                    "BUY", 0.8,
                    f"{horizon}d",
                    round(random.uniform(100, 500), 2),
                    strategy['type'],
                    json.dumps({"feature1": random.uniform(0, 1), "feature2": random.uniform(0, 1)}),
                    random.choice(REGIMES),
                    random.choice(["strong", "moderate", "weak"]),
                    f"{strategy_id}_{scored_event[0]}_{prediction_time.isoformat()}"
                ))
                
    def seed_prediction_outcomes(self):
        """Seed prediction outcomes"""
        print("Seeding prediction outcomes...")
        
        # Get predictions
        predictions = self.conn.execute("""
            SELECT id, ticker, entry_price, horizon, prediction FROM predictions
            WHERE tenant_id = ? AND scored_outcome_id IS NULL
            LIMIT 100
        """, (TENANT_ID,)).fetchall()
        
        for pred in predictions:
            # Simulate outcome
            horizon_days = int(pred[3].replace("d", ""))
            exit_price = pred[2] * (1 + random.uniform(-0.1, 0.15))
            return_pct = (exit_price - pred[2]) / pred[2]
            
            # Determine if direction was correct
            direction_correct = (
                (pred[3] == "BUY" and return_pct > 0) or
                (pred[3] == "SELL" and return_pct < 0) or
                (pred[3] == "HOLD" and abs(return_pct) < 0.02)
            )
            
            self.conn.execute("""
                INSERT OR REPLACE INTO prediction_outcomes
                (id, tenant_id, prediction_id, exit_price, return_pct, direction_correct,
                 max_runup, max_drawdown, evaluated_at, exit_reason, residual_alpha)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()), TENANT_ID, pred[0],
                round(exit_price, 2), round(return_pct, 4), direction_correct,
                round(random.uniform(0.01, 0.2), 4), round(random.uniform(-0.15, -0.01), 4),
                (self.now + timedelta(days=horizon_days)).isoformat(),
                "horizon_reached", round(random.uniform(-0.05, 0.05), 4)
            ))
            
            # Link prediction to outcome
            self.conn.execute("""
                UPDATE predictions SET scored_outcome_id = ? WHERE id = ?
            """, (str(uuid.uuid4()), pred[0]))
            
    def seed_strategy_performance(self):
        """Seed additional strategy performance metrics"""
        print("Seeding strategy performance metrics...")
        
        # This is already handled in seed_strategies(), but we can add more detailed data here
        
        # Seed ranking snapshots for dashboard
        print("Seeding ranking snapshots...")
        for ticker in TICKERS:
            for days_ago in range(30, 0, -3):  # Every 3 days for past month
                timestamp = self.now - timedelta(days=days_ago)
                
                self.conn.execute("""
                    INSERT OR REPLACE INTO ranking_snapshots
                    (id, tenant_id, ticker, score, conviction, attribution_json, regime, timestamp, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, ticker,
                    round(random.uniform(0.3, 0.9), 3),
                    round(random.uniform(0.5, 1.0), 3),
                    json.dumps({"strategies": [s["id"] for s in STRATEGIES[:2]]}),
                    random.choice(REGIMES),
                    timestamp.isoformat(),
                    timestamp.isoformat()
                ))
        
        # Seed regime performance
        for regime in REGIMES:
            self.conn.execute("""
                INSERT OR REPLACE INTO regime_performance
                (id, tenant_id, regime, prediction_count, accuracy, avg_return, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()), TENANT_ID, regime,
                random.randint(50, 300),
                round(random.uniform(0.4, 0.8), 3),
                round(random.uniform(-0.05, 0.12), 4),
                self.now.isoformat()
            ))
            
    def seed_consensus_signals(self):
        """Seed consensus signals"""
        print("Seeding consensus signals...")
        
        for ticker in TICKERS:
            for days_ago in range(30, 0, -7):  # Weekly consensus
                timestamp = self.now - timedelta(days=days_ago)
                
                # Find strategies for this ticker
                sentiment_strategy = random.choice([s for s in STRATEGIES if s['track'] == 'sentiment'])
                quant_strategy = random.choice([s for s in STRATEGIES if s['track'] == 'quant'])
                
                self.conn.execute("""
                    INSERT OR REPLACE INTO consensus_signals
                    (id, tenant_id, ticker, regime, direction, confidence, total_weight,
                     participating_strategies, sentiment_strategy_id, quant_strategy_id,
                     sentiment_score, quant_score, ws, wq, agreement_bonus, p_final,
                     stability_score, created_at, horizon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, ticker, random.choice(REGIMES),
                    random.choice(DIRECTIONS), round(random.uniform(0.3, 0.9), 3),
                    round(random.uniform(0.5, 1.0), 3), random.randint(2, 5),
                    f"{sentiment_strategy['id']}_{TENANT_ID}",
                    f"{quant_strategy['id']}_{TENANT_ID}",
                    round(random.uniform(-1, 1), 3), round(random.uniform(-1, 1), 3),
                    round(random.uniform(0.3, 0.7), 3), round(random.uniform(0.3, 0.7), 3),
                    round(random.uniform(0, 0.2), 3), round(random.uniform(-1, 1), 3),
                    round(random.uniform(0.5, 0.9), 3), timestamp.isoformat(),
                    random.choice(HORIZONS)
                ))
                
    def seed_champion_data(self):
        """Seed champion strategy data"""
        print("Seeding champion data...")
        
        # This creates data that will be used by the champion selection logic
        # The actual champion determination happens at query time
        
        # Create some promotion events to show champion history
        for strategy in STRATEGIES:
            strategy_id = f"{strategy['id']}_{TENANT_ID}"
            
            if random.random() > 0.5:  # 50% chance of having promotion events
                self.conn.execute("""
                    INSERT OR REPLACE INTO promotion_events
                    (id, tenant_id, strategy_id, prev_status, new_status, event_type, metadata_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, strategy_id,
                    "CANDIDATE", "CHAMPION",
                    random.choice(["nominated", "promoted", "rolled_back"]),
                    json.dumps({"reason": random.choice(["Strong performance", "Improved accuracy", "Market adaptation"])}),
                    (self.now - timedelta(days=random.randint(1, 60))).isoformat()
                ))
                
    def seed_system_heartbeats(self):
        """Seed system loop heartbeat data"""
        print("Seeding system heartbeats...")
        
        loop_types = ["live", "replay", "optimizer"]
        
        for loop_type in loop_types:
            for hours_ago in range(24, 0, -4):  # Every 4 hours for past 24 hours
                timestamp = self.now - timedelta(hours=hours_ago)
                
                self.conn.execute("""
                    INSERT OR REPLACE INTO loop_heartbeats
                    (id, tenant_id, loop_type, status, notes, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()), TENANT_ID, loop_type,
                    random.choice(["running", "completed", "failed", "idle"]),
                    random.choice(["Normal operation", "Processing batch", "Maintenance", "Error recovery"]),
                    timestamp.isoformat()
                ))

def main():
    """Main entry point"""
    seeder = MockDataSeeder()
    seeder.seed_all()

if __name__ == "__main__":
    main()
