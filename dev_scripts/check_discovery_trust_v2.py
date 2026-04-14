import sqlite3

conn = sqlite3.connect("data/alpha.db")
conn.row_factory = sqlite3.Row

# Check discovery predictions with outcomes
discovery_with_outcomes = conn.execute("""
    SELECT p.strategy_id, p.horizon, COUNT(*) as prediction_count,
           AVG(CASE WHEN o.direction_correct = 1 THEN 1 ELSE 0 END) as accuracy,
           AVG(o.return_pct) as avg_return
    FROM predictions p
    JOIN prediction_outcomes o ON o.prediction_id = p.id
    WHERE p.strategy_id LIKE '%_v1_default' AND p.mode = 'discovery'
    GROUP BY p.strategy_id, p.horizon
""").fetchall()

print("=== Discovery Strategy Performance ===")
for row in discovery_with_outcomes:
    print(f"{row['strategy_id']} {row['horizon']}:")
    print(f"  Predictions: {row['prediction_count']}")
    print(f"  Accuracy: {row['accuracy']:.3f}")
    print(f"  Avg Return: {row['avg_return']:.3f}")
    print()

# Check current trust scores
trust_scores = conn.execute("""
    SELECT strategy_id, horizon, trust_score, calibration_score, stability_score
    FROM strategy_trust
    WHERE strategy_id LIKE '%_v1_default'
    ORDER BY trust_score DESC
""").fetchall()

print("=== Current Trust Scores ===")
for row in trust_scores:
    if row['trust_score'] > 0:
        print(f"{row['strategy_id']} {row['horizon']}: trust={row['trust_score']:.3f} cal={row['calibration_score']:.3f} stab={row['stability_score']:.3f}")

# Compare with baseline
baseline = conn.execute("""
    SELECT strategy_id, trust_score
    FROM strategy_trust
    WHERE strategy_id IN ('sentiment_v1_default', 'technical_v2_default')
    ORDER BY trust_score DESC
    LIMIT 2
""").fetchall()

print("\n=== Baseline Comparison ===")
for row in baseline:
    print(f"{row['strategy_id']}: trust={row['trust_score']:.3f}")
