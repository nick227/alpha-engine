import sqlite3
from datetime import datetime, timezone

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check trust for our new discovery strategies
discovery_strategies = [
    "realness_repricer_v1_default",
    "silent_compounder_v1_default", 
    "narrative_lag_v1_default",
    "balance_sheet_survivor_v1_default",
    "ownership_vacuum_v1_default"
]

print("=== Discovery Strategy Trust ===")
for strategy_id in discovery_strategies:
    trust_data = conn.execute("""
        SELECT horizon, trust_score, sample_size, computed_at
        FROM strategy_trust
        WHERE strategy_id = ? AND tenant_id = 'default'
        ORDER BY computed_at DESC
        LIMIT 2
    """, (strategy_id,)).fetchall()
    
    if trust_data:
        for row in trust_data:
            print(f"{strategy_id} {row['horizon']}: trust={row['trust_score']:.3f} (sample={row['sample_size']})")
    else:
        print(f"{strategy_id}: No trust data yet")

# Check predictions count
predictions = conn.execute("""
    SELECT strategy_id, COUNT(*) as count
    FROM predictions
    WHERE strategy_id LIKE '%_v1_default' AND timestamp >= '2026-04-10'
    GROUP BY strategy_id
""").fetchall()

print(f"\n=== Discovery Predictions Count ===")
for row in predictions:
    print(f"{row['strategy_id']}: {row['count']} predictions")

# Check outcomes
outcomes = conn.execute("""
    SELECT p.strategy_id, COUNT(*) as count
    FROM predictions p
    JOIN prediction_outcomes o ON p.id = o.prediction_id
    WHERE p.strategy_id LIKE '%_v1_default' AND p.timestamp >= '2026-04-10'
    GROUP BY p.strategy_id
""").fetchall()

print(f"\n=== Discovery Outcomes Count ===")
for row in outcomes:
    print(f"{row['strategy_id']}: {row['count']} outcomes")

# Compare with baseline
baseline = conn.execute("""
    SELECT AVG(trust_score) as avg_trust
    FROM strategy_trust
    WHERE tenant_id = 'default' 
    AND computed_at >= '2026-04-10'
    AND strategy_id IN ('sentiment_v1_default', 'technical_v2_default')
""").fetchone()

if baseline and baseline['avg_trust']:
    print(f"\n=== Baseline Comparison ===")
    print(f"Baseline trust (sentiment/technical): {baseline['avg_trust']:.3f}")
    
    # Get discovery average
    discovery_avg = conn.execute("""
        SELECT AVG(trust_score) as avg_trust
        FROM strategy_trust
        WHERE tenant_id = 'default' 
        AND computed_at >= '2026-04-10'
        AND strategy_id LIKE '%_v1_default'
    """).fetchone()
    
    if discovery_avg and discovery_avg['avg_trust']:
        improvement = discovery_avg['avg_trust'] - baseline['avg_trust']
        print(f"Discovery average trust: {discovery_avg['avg_trust']:.3f}")
        print(f"Improvement: {improvement:+.3f}")
        
        if improvement > 0:
            print("✅ Discovery BEATS baseline!")
        else:
            print("❌ Discovery does NOT beat baseline")
