import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
import json

def seed():
    conn = sqlite3.connect("data/alpha.db")
    conn.row_factory = sqlite3.Row
    
    # Ensure tables exist
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS prediction_runs (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            ingress_start TEXT NOT NULL,
            ingress_end TEXT NOT NULL,
            prediction_start TEXT NOT NULL,
            prediction_end TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
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
        CREATE TABLE IF NOT EXISTS prediction_scores (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            strategy_version TEXT,
            ticker TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            forecast_days INTEGER NOT NULL,
            direction_hit_rate REAL NOT NULL,
            sync_rate REAL NOT NULL,
            total_return_actual REAL NOT NULL,
            total_return_pred REAL NOT NULL,
            total_return_error REAL NOT NULL,
            magnitude_error REAL NOT NULL,
            horizon_weight REAL NOT NULL,
            efficiency_rating REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    tenant_id = "default"

    run_id = str(uuid.uuid4())
    
    # 1. Create a Prediction Run
    print(f"Seeding run: {run_id}")
    now = datetime.now(timezone.utc)
    p_start = (now - timedelta(days=7)).isoformat()
    p_end = now.isoformat()
    
    conn.execute("""
        INSERT OR REPLACE INTO prediction_runs 
        (id, tenant_id, ingress_start, ingress_end, prediction_start, prediction_end, timeframe)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (run_id, tenant_id, p_start, p_start, p_start, p_end, "1d"))

    # 2. Seed some tickers and strategies
    tickers = ["AAPL", "NVDA", "TSLA"]
    strategies = ["Sentiment_v1", "Technical_v2"]
    
    for ticker in tickers:
        # Seed actual series
        actual_points = []
        for d in range(8):
            ts = (now - timedelta(days=7-d)).replace(hour=16, minute=0, second=0).isoformat()
            val = 150.0 + (d * 2.5) + (hash(ticker) % 10)
            actual_points.append((str(uuid.uuid4()), tenant_id, run_id, ticker, "1d", ts, val))
            
        conn.executemany("""
            INSERT OR REPLACE INTO actual_series_points 
            (id, tenant_id, run_id, ticker, timeframe, timestamp, value)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, actual_points)

        for strategy in strategies:
            # Seed predicted series
            pred_points = []
            for d in range(8):
                ts = (now - timedelta(days=7-d)).replace(hour=16, minute=0, second=0).isoformat()
                # Add some "error" to the prediction
                val = 150.0 + (d * 2.3) + (hash(ticker) % 10) + (hash(strategy) % 5)
                pred_points.append((str(uuid.uuid4()), tenant_id, run_id, strategy, "1.0", ticker, "1d", ts, val))
                
            conn.executemany("""
                INSERT OR REPLACE INTO predicted_series_points 
                (id, tenant_id, run_id, strategy_id, strategy_version, ticker, timeframe, timestamp, value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, pred_points)
            
            # Seed scores
            conn.execute("""
                INSERT OR REPLACE INTO prediction_scores
                (id, tenant_id, run_id, strategy_id, strategy_version, ticker, timeframe, forecast_days,
                 direction_hit_rate, sync_rate, total_return_actual, total_return_pred, total_return_error,
                 magnitude_error, horizon_weight, efficiency_rating)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()), tenant_id, run_id, strategy, "1.0", ticker, "1d", 7,
                0.8, 0.75, 5.2, 4.8, 0.4, 0.02, 1.0, 0.85 + (hash(strategy) % 10 / 100.0)
            ))

    conn.commit()
    conn.close()
    print("Seeding complete.")

if __name__ == "__main__":
    seed()
