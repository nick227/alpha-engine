"""
Build structural_candidates from feature_snapshot (price-based only)
"""
import sqlite3
from datetime import datetime, timezone

conn = sqlite3.connect("data/alpha.db")

# Clear existing
conn.execute("DELETE FROM structural_candidates")
print("Cleared structural_candidates")

now = datetime.now(timezone.utc).isoformat()

# Structural Type 1: Distressed Repricer - VERY TIGHT
# Only most beaten down stocks with strong negative momentum
conn.execute("""
    INSERT INTO structural_candidates
    (tenant_id, symbol, structural_type, confidence, price_percentile, revenue_trend, features_json, identified_at)
    SELECT
        'default',
        symbol,
        'structural_repricer',
        0.90,
        AVG(price_percentile_252d),
        AVG(return_63d),
        '{"return_63d": ' || CAST(AVG(return_63d) AS TEXT) || '}',
        ?
    FROM feature_snapshot
    WHERE price_percentile_252d < 0.05
      AND return_63d < -0.40
      AND dollar_volume > 5000000
    GROUP BY symbol
    HAVING COUNT(*) >= 5
""", (now,))

# Structural Type 2: Silent Compounder - VERY TIGHT
# Steady performers with low volatility
conn.execute("""
    INSERT INTO structural_candidates
    (tenant_id, symbol, structural_type, confidence, price_percentile, revenue_trend, features_json, identified_at)
    SELECT
        'default',
        symbol,
        'structural_compounder',
        0.80,
        AVG(price_percentile_252d),
        AVG(return_63d),
        '{"return_63d": ' || CAST(AVG(return_63d) AS TEXT) || '}',
        ?
    FROM feature_snapshot
    WHERE price_percentile_252d BETWEEN 0.45 AND 0.60
      AND return_63d BETWEEN 0.15 AND 0.30
      AND volatility_20d < 0.02
      AND dollar_volume > 10000000
    GROUP BY symbol
    HAVING COUNT(*) >= 10
""", (now,))

# Structural Type 3: Ownership Vacuum - VERY TIGHT
# Strong accumulation signals
conn.execute("""
    INSERT INTO structural_candidates
    (tenant_id, symbol, structural_type, confidence, price_percentile, revenue_trend, features_json, identified_at)
    SELECT
        'default',
        symbol,
        'structural_vacuum',
        0.75,
        AVG(price_percentile_252d),
        AVG(return_63d),
        '{"vol_zscore": ' || CAST(AVG(volume_zscore_20d) AS TEXT) || '}',
        ?
    FROM feature_snapshot
    WHERE volume_zscore_20d > 3.0
      AND return_63d > 0.05
      AND dollar_volume > 5000000
    GROUP BY symbol
    HAVING COUNT(*) >= 3
""", (now,))

# Structural Type 4: Narrative Lag
# Sector moving but this stock not yet (need sector data - skip for now)

conn.commit()

# Summary
summary = conn.execute("""
    SELECT structural_type, COUNT(*) as cnt
    FROM structural_candidates
    WHERE tenant_id = 'default'
    GROUP BY structural_type
""").fetchall()

print("\n=== Structural Candidates ===")
total = sum(x[1] for x in summary)
print(f"Total: {total}")
for s in summary:
    print(f"  {s[0]}: {s[1]}")

# Top examples
top = conn.execute("""
    SELECT symbol, structural_type, confidence
    FROM structural_candidates
    WHERE tenant_id = 'default'
    ORDER BY confidence DESC
    LIMIT 10
""").fetchall()

print("\nTop candidates:")
for t in top:
    print(f"  {t[0]}: {t[1]} (conf={t[2]})")

conn.close()
