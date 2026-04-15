"""
Test Dimensional ML Fixes: Verify storage and cold start mode work.
"""

from __future__ import annotations

from app.ml.dimensional_tagger import get_dimensional_tagger, tag_and_store_prediction
from app.ml.lightweight_dimensional_ml import get_lightweight_dimensional_ml


def test_storage_fix():
    """Test that the storage fix works (no column mismatch)."""
    
    print("=== TESTING STORAGE FIX ===")
    
    tagger = get_dimensional_tagger()
    
    # Create test features
    features = {
        "volatility_20d": 0.025,
        "return_5d": 0.015,
        "return_63d": 0.022,
        "dollar_volume": 2000000,
        "volume_zscore_20d": 1.5,
        "price_percentile_252d": 0.6,
        "sector": "technology",
    }
    
    # Test storing a prediction
    try:
        axis_key = tagger.store_dimensional_prediction(
            "TEST_SYMBOL", features, 0.018, 0.75, "7d"
        )
        print(f"  Storage: SUCCESS")
        print(f"  Axis Key: {axis_key}")
        return True
    except Exception as e:
        print(f"  Storage: FAILED - {e}")
        return False


def test_cold_start_mode():
    """Test that cold start mode allows trading without history."""
    
    print("\n=== TESTING COLD START MODE ===")
    
    tagger = get_dimensional_tagger()
    
    # Test activation for axis with no history
    axis_key = "HIGH_VOL_TECH_AGGRESSIVE_7d"
    
    # Cold start mode should allow activation
    should_activate = tagger.should_activate_prediction(axis_key, cold_start_mode=True)
    print(f"  Cold start activation: {should_activate}")
    
    # Normal mode should block activation
    should_activate_normal = tagger.should_activate_prediction(axis_key, cold_start_mode=False)
    print(f"  Normal mode activation: {should_activate_normal}")
    
    return should_activate and not should_activate_normal


def test_lightweight_ml_integration():
    """Test the lightweight ML integration with cold start mode."""
    
    print("\n=== TESTING LIGHTWEIGHT ML INTEGRATION ===")
    
    ml_system = get_lightweight_dimensional_ml()
    
    # Create test features
    features = {
        "volatility_20d": 0.025,
        "return_5d": 0.015,
        "return_63d": 0.022,
        "dollar_volume": 2000000,
        "volume_zscore_20d": 1.5,
        "price_percentile_252d": 0.6,
        "sector": "technology",
    }
    
    # Test prediction with dimensional tagging
    try:
        result = ml_system.predict_with_dimensional_tagging(
            features, 0.018, 0.75, "data/alpha.db", "2024-01-01"
        )
        print(f"  Integration: SUCCESS")
        print(f"  Symbol: {result['symbol']}")
        print(f"  Should Activate: {result['should_activate']}")
        print(f"  Axis Key: {result['axis_key']}")
        print(f"  Environment Tag: {result['environment_tag']}")
        print(f"  Sector Tag: {result['sector_tag']}")
        return True
    except Exception as e:
        print(f"  Integration: FAILED - {e}")
        return False


def main():
    """Run all tests to verify fixes work."""
    
    print("TESTING DIMENSIONAL ML FIXES")
    print("=" * 50)
    
    # Test 1: Storage fix
    storage_ok = test_storage_fix()
    
    # Test 2: Cold start mode
    cold_start_ok = test_cold_start_mode()
    
    # Test 3: Integration
    integration_ok = test_lightweight_ml_integration()
    
    print("\n" + "=" * 50)
    print("TEST RESULTS:")
    print(f"  Storage Fix: {'PASS' if storage_ok else 'FAIL'}")
    print(f"  Cold Start Mode: {'PASS' if cold_start_ok else 'FAIL'}")
    print(f"  Integration: {'PASS' if integration_ok else 'FAIL'}")
    
    if storage_ok and cold_start_ok and integration_ok:
        print("\n  ALL TESTS PASSED!")
        print("  System is ready for data collection phase.")
    else:
        print("\n  SOME TESTS FAILED!")
        print("  Fix remaining issues before proceeding.")
    
    print("\nNEXT STEPS:")
    print("1. Run system to collect axis_key -> outcomes data")
    print("2. Wait for 50-200 samples per axis_key")
    print("3. Then enable selective activation gating")


if __name__ == "__main__":
    main()
