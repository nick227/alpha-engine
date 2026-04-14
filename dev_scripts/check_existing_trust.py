import sqlite3
from datetime import datetime, timezone

# Check existing trust data
conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check strategy_trust table
trust_data = conn.execute("""
    SELECT strategy_id, horizon, trust_score, sample_size, computed_at
    FROM strategy_trust
    ORDER BY computed_at DESC, strategy_id, horizon
    LIMIT 20
""").fetchall()

print("=== Existing Trust Data ===")
for row in trust_data:
    print(f"{row['strategy_id']} {row['horizon']}: trust={row['trust_score']:.3f}, sample={row['sample_size']}, computed_at={row['computed_at']}")

# Check prediction outcomes
outcomes = conn.execute("""
    SELECT COUNT(*) as count
    FROM prediction_outcomes
    WHERE evaluated_at >= '2026-01-01'
""").fetchone()

print(f"\n=== Recent Outcomes ===")
print(f"Total outcomes since 2026-01-01: {outcomes['count']}")

# Check predictions
predictions = conn.execute("""
    SELECT COUNT(*) as count
    FROM predictions
    WHERE timestamp >= '2026-01-01'
""").fetchone()

print(f"Total predictions since 2026-01-01: {predictions['count']}")

# Check discovery candidates
candidates = conn.execute("""
    SELECT strategy_type, COUNT(*) as count
    FROM discovery_candidates
    WHERE as_of_date = '2026-04-10'
    GROUP BY strategy_type
""").fetchall()

print(f"\n=== Discovery Candidates Saved ===")
for row in candidates:
    print(f"{row['strategy_type']}: {row['count']} candidates")
