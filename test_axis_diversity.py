"""
Test Axis Diversity: Verify we get multiple different axis_keys.
"""

from __future__ import annotations

from app.ml.dimensional_tagger import get_dimensional_tagger, tag_and_store_prediction


def create_diverse_test_features():
    """Create diverse test features to generate different axis_keys."""
    
    diverse_features = {
        "TECH_HIGH_VOL": {
            "volatility_20d": 0.045,  # HIGH_VOL
            "return_5d": 0.025,       # TREND
            "return_63d": 0.035,
            "dollar_volume": 3000000,
            "volume_zscore_20d": 2.5,
            "price_percentile_252d": 0.8,
            "sector": "technology",
        },
        "FIN_LOW_VOL": {
            "volatility_20d": 0.012,  # LOW_VOL
            "return_5d": 0.003,       # STABLE
            "return_63d": 0.008,
            "dollar_volume": 2000000,
            "volume_zscore_20d": 0.5,
            "price_percentile_252d": 0.4,
            "sector": "financials",
        },
        "HEA_CHOPPY": {
            "volatility_20d": 0.018,  # MED_VOL
            "return_5d": -0.002,      # CHOP
            "return_63d": 0.005,
            "dollar_volume": 1500000,
            "volume_zscore_20d": -0.5,
            "price_percentile_252d": 0.6,
            "sector": "healthcare",
        },
        "ENE_TREND": {
            "volatility_20d": 0.038,  # HIGH_VOL
            "return_5d": 0.018,       # TREND
            "return_63d": 0.028,
            "dollar_volume": 2500000,
            "volume_zscore_20d": 1.5,
            "price_percentile_252d": 0.7,
            "sector": "energy",
        },
        "CON_STABLE": {
            "volatility_20d": 0.015,  # LOW_VOL
            "return_5d": 0.008,       # STABLE
            "return_63d": 0.012,
            "dollar_volume": 1800000,
            "volume_zscore_20d": 0.8,
            "price_percentile_252d": 0.5,
            "sector": "consumer",
        }
    }
    
    return diverse_features


def test_axis_diversity():
    """Test that we generate diverse axis_keys."""
    
    print("=== TESTING AXIS DIVERSITY ===")
    
    tagger = get_dimensional_tagger()
    diverse_features = create_diverse_test_features()
    
    axis_keys = []
    
    for symbol_name, features in diverse_features.items():
        # Create different predictions to get different model tags
        if "HIGH_VOL" in symbol_name:
            prediction, confidence = 0.025, 0.85  # AGGRESSIVE
        elif "LOW_VOL" in symbol_name:
            prediction, confidence = 0.008, 0.65  # BALANCED
        elif "CHOPPY" in symbol_name:
            prediction, confidence = -0.005, 0.55  # DEFENSIVE
        else:
            prediction, confidence = 0.015, 0.75  # BALANCED
        
        axis_key = tagger.store_dimensional_prediction(
            symbol_name, features, prediction, confidence, "7d"
        )
        
        axis_keys.append(axis_key)
        
        # Show the breakdown
        env_tag = tagger.extract_environment_tag(features)
        sector_tag = tagger.extract_sector_tag(features)
        model_tag = tagger.extract_model_tag(prediction, confidence)
        
        print(f"  {symbol_name}:")
        print(f"    Environment: {env_tag}")
        print(f"    Sector: {sector_tag}")
        print(f"    Model: {model_tag}")
        print(f"    Axis Key: {axis_key}")
        print()
    
    # Check diversity
    unique_axis_keys = set(axis_keys)
    
    print(f"=== DIVERSITY RESULTS ===")
    print(f"Total predictions: {len(axis_keys)}")
    print(f"Unique axis keys: {len(unique_axis_keys)}")
    print(f"Diversity ratio: {len(unique_axis_keys)/len(axis_keys):.1%}")
    
    if len(unique_axis_keys) >= 3:
        print(f"  GOOD: Multiple axis keys generated")
        return True
    else:
        print(f"  ISSUE: Not enough diversity in axis keys")
        return False


def verify_database_storage():
    """Verify rows are actually stored in database."""
    
    print(f"\n=== VERIFYING DATABASE STORAGE ===")
    
    import sqlite3
    
    conn = sqlite3.connect("data/alpha.db")
    
    # Count total rows
    cursor = conn.execute("SELECT COUNT(*) FROM dimensional_predictions")
    total_count = cursor.fetchone()[0]
    print(f"Total rows in database: {total_count}")
    
    # Show axis key diversity
    cursor = conn.execute("""
        SELECT axis_key, COUNT(*) 
        FROM dimensional_predictions 
        GROUP BY axis_key 
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)
    
    axis_counts = cursor.fetchall()
    print(f"Axis key diversity:")
    for axis_key, count in axis_counts:
        print(f"  {axis_key}: {count} rows")
    
    # Show sector distribution
    cursor = conn.execute("""
        SELECT sector_tag, COUNT(*) 
        FROM dimensional_predictions 
        GROUP BY sector_tag
    """)
    
    sector_counts = cursor.fetchall()
    print(f"Sector distribution:")
    for sector, count in sector_counts:
        print(f"  {sector}: {count} rows")
    
    conn.close()
    
    return total_count > 0, len(axis_counts) > 1


def main():
    """Run axis diversity verification."""
    
    print("AXIS DIVERSITY VERIFICATION")
    print("=" * 50)
    
    # Test 1: Generate diverse axis keys
    diversity_ok = test_axis_diversity()
    
    # Test 2: Verify database storage
    storage_ok, axis_diversity_ok = verify_database_storage()
    
    print(f"\n" + "=" * 50)
    print("VERIFICATION RESULTS:")
    print(f"  Axis Generation: {'PASS' if diversity_ok else 'FAIL'}")
    print(f"  Database Storage: {'PASS' if storage_ok else 'FAIL'}")
    print(f"  Axis Diversity: {'PASS' if axis_diversity_ok else 'FAIL'}")
    
    if diversity_ok and storage_ok and axis_diversity_ok:
        print(f"\n  ALL CHECKS PASS!")
        print("  System ready for diverse data collection.")
    else:
        print(f"\n  SOME CHECKS FAILED!")
        print("  Fix issues before proceeding.")
    
    print(f"\nNEXT STEPS:")
    print("1. Ensure diverse axis keys are generated")
    print("2. Monitor database storage counts")
    print("3. Build statistical significance per axis_key")


if __name__ == "__main__":
    main()
