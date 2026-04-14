import sqlite3
from datetime import datetime, timezone, timedelta

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check our discovery predictions in detail
predictions = conn.execute("""
    SELECT p.id, p.strategy_id, p.ticker, p.timestamp, p.prediction, p.confidence,
           p.horizon, p.entry_price, p.mode, p.feature_snapshot_json, p.regime
    FROM predictions p
    WHERE p.mode = 'discovery' AND p.strategy_id LIKE '%_v1_default'
    ORDER BY p.timestamp
""").fetchall()

print(f"Discovery predictions: {len(predictions)}")

for p in predictions:
    created_at = datetime.fromisoformat(p['timestamp'].replace('Z', '+00:00'))
    horizon_minutes = 7200  # 5d = 5 * 24 * 60
    expiry = created_at + timedelta(minutes=horizon_minutes)
    
    print(f"\n{p['strategy_id']} {p['ticker']}:")
    print(f"  ID: {p['id']}")
    print(f"  Created: {created_at.isoformat()}")
    print(f"  Horizon: {p['horizon']} ({horizon_minutes} minutes)")
    print(f"  Expiry: {expiry.isoformat()}")
    print(f"  Entry Price: {p['entry_price']}")
    print(f"  Direction: {p['prediction']}")
    print(f"  Confidence: {p['confidence']}")
    print(f"  Mode: {p['mode']}")
    print(f"  Regime: {p['regime']}")
    
    # Check if it has an outcome
    outcome = conn.execute("""
        SELECT id FROM prediction_outcomes WHERE prediction_id = ?
    """, (p['id'],)).fetchone()
    
    print(f"  Has Outcome: {'Yes' if outcome else 'No'}")

# Test the exact query that replay uses
now = datetime(2026, 4, 16, tzinfo=timezone.utc)
print(f"\n=== Testing Replay Query at {now.isoformat()} ===")

replay_query = """
SELECT
  p.id,
  p.strategy_id,
  p.ticker,
  p.mode,
  p.horizon,
  p.timestamp,
  p.entry_price,
  p.prediction,
  p.feature_snapshot_json,
  p.regime,
  s.strategy_type
FROM predictions p
JOIN strategies s
  ON s.id = p.strategy_id
 AND s.tenant_id = p.tenant_id
LEFT JOIN prediction_outcomes o
  ON o.prediction_id = p.id
 AND o.tenant_id = p.tenant_id
WHERE p.tenant_id = ?
  AND o.id IS NULL
ORDER BY p.timestamp ASC
"""

rows = conn.execute(replay_query, ("default",)).fetchall()

print(f"Total unscored predictions: {len(rows)}")

discovery_rows = [r for r in rows if r['mode'] == 'discovery']
print(f"Discovery predictions in query: {len(discovery_rows)}")

for r in discovery_rows:
    created_at = datetime.fromisoformat(r['timestamp'].replace('Z', '+00:00'))
    expiry = created_at + timedelta(minutes=7200)
    print(f"  {r['strategy_id']} {r['ticker']}: expires {expiry.isoformat()}, now: {now.isoformat()}, expired: {expiry <= now}")

conn.close()
