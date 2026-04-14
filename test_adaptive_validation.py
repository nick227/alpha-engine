"""
CRITICAL: Validate that sync_adaptive is actually working, not just simulated.

The only proof that matters: Do different configs produce different outcomes?
"""

from __future__ import annotations

from app.discovery.strategies import silent_compounder, DEFAULT_STRATEGY_CONFIGS
from app.discovery.types import FeatureRow
from app.discovery.adaptive_stats import lookup_best_config
from app.core.environment import bucket_env, build_env_snapshot
from datetime import date


def create_test_feature_row(volatility: float, return_63d: float) -> FeatureRow:
    """Create test feature row with specific volatility and return."""
    return FeatureRow(
        symbol='TEST',
        as_of_date='2024-01-01',
        close=100.0,
        volume=1000000,
        dollar_volume=100000000,
        avg_dollar_volume_20d=100000000,
        return_1d=0.01,
        return_5d=0.02,
        return_20d=0.03,
        return_63d=return_63d,
        return_252d=0.15,
        volatility_20d=volatility,
        max_drawdown_252d=0.1,
        price_percentile_252d=0.5,
        volume_zscore_20d=1.0,
        dollar_volume_zscore_20d=1.0,
        revenue_ttm=None,
        revenue_growth=None,
        shares_outstanding=None,
        shares_growth=None,
        sector=None,
        industry=None,
        sector_return_63d=None,
        peer_relative_return_63d=None,
        price_bucket=None,
    )


def test_config_divergence():
    """TEST 1: Do extreme configs produce different results?"""
    print("=== TEST 1: Config Divergence ===")
    
    # Create two test stocks - one optimal for each config
    low_vol_stock = create_test_feature_row(volatility=0.012, return_63d=0.05)  # Good for tight vol band
    high_vol_stock = create_test_feature_row(volatility=0.035, return_63d=0.05)  # Good for wide vol band
    
    # Extreme configs
    config_a = {"vol_band": 0.01, "threshold": 0.9, "min_vol": 0.005, "max_vol": 0.02}  # Very strict
    config_b = {"vol_band": 0.04, "threshold": 0.1, "min_vol": 0.02, "max_vol": 0.06}  # Very loose
    
    # Test low volatility stock
    result_a_low = silent_compounder(low_vol_stock, config=config_a)
    result_b_low = silent_compounder(low_vol_stock, config=config_b)
    
    # Test high volatility stock  
    result_a_high = silent_compounder(high_vol_stock, config=config_a)
    result_b_high = silent_compounder(high_vol_stock, config=config_b)
    
    print(f"Low vol stock - Strict config: {result_a_low[0]}")
    print(f"Low vol stock - Loose config: {result_b_low[0]}")
    print(f"High vol stock - Strict config: {result_a_high[0]}")
    print(f"High vol stock - Loose config: {result_b_high[0]}")
    
    # Check for divergence - look at both scores and reasons
    low_vol_diverges = (result_a_low[0] != result_b_low[0]) or (result_a_low[1] != result_b_low[1])
    high_vol_diverges = (result_a_high[0] != result_b_high[0]) or (result_a_high[1] != result_b_high[1])
    
    print(f"\nLow vol divergence: {low_vol_diverges}")
    print(f"High vol divergence: {high_vol_diverges}")
    
    # More detailed analysis
    print(f"\nDetailed analysis:")
    print(f"  Low vol - Strict: score={result_a_low[0]}, reason='{result_a_low[1]}'")
    print(f"  Low vol - Loose: score={result_b_low[0]}, reason='{result_b_low[1]}'")
    print(f"  High vol - Strict: score={result_a_high[0]}, reason='{result_a_high[1]}'")
    print(f"  High vol - Loose: score={result_b_high[0]}, reason='{result_b_high[1]}'")
    
    if low_vol_diverges or high_vol_diverges:
        print(">>> CONFIGS ARE WORKING - Different configs produce different results!")
        return True
    else:
        print(">>> FAKE ADAPTATION - Configs have no effect!")
        return False


def test_environment_divergence():
    """TEST 2: Do different environments select different configs?"""
    print("\n=== TEST 2: Environment Divergence ===")
    
    # Simulate different environments
    env_a_bucket = ("LOW", "CHOP", "LOW")      # Quiet market
    env_b_bucket = ("HIGH", "TRENDING", "HIGH") # Volatile market
    
    # Check what configs would be selected (mock for now)
    # In real system, this would query actual adaptive_stats
    config_a = lookup_best_config(
        db_path='data/alpha.db',
        strategy='silent_compounder',
        env_bucket=env_a_bucket,
        min_samples=1  # Low threshold for test
    )
    
    config_b = lookup_best_config(
        db_path='data/alpha.db', 
        strategy='silent_compounder',
        env_bucket=env_b_bucket,
        min_samples=1  # Low threshold for test
    )
    
    print(f"Environment A config: {config_a}")
    print(f"Environment B config: {config_b}")
    
    # For now, both will be None (no real data)
    # This test becomes meaningful after real data collection
    if config_a and config_b and config_a != config_b:
        print(">>> ENVIRONMENTS SELECT DIFFERENT CONFIGS!")
        return True
    else:
        print(">>> INSUFFICIENT DATA - Need real outcomes to test environment divergence")
        return None


def test_sample_density():
    """TEST 3: Do we have enough samples per (env, config)?"""
    print("\n=== TEST 3: Sample Density ===")
    
    # This would query real adaptive_stats
    # For now, we'll simulate the check
    
    from app.discovery.adaptive_stats import get_adaptive_stats_summary
    
    summary = get_adaptive_stats_summary(db_path='data/alpha.db', strategy='silent_compounder')
    
    print(f"Adaptive stats entries: {len(summary)}")
    
    for entry in summary[:3]:  # Show first 3
        env_bucket = entry['env_bucket']
        total_samples = entry['total_samples']
        config_count = entry['config_count']
        
        print(f"  Env {env_bucket}: {total_samples} samples across {config_count} configs")
        
        if total_samples >= 30:
            print(f"    >>> SUFFICIENT samples for adaptation")
        else:
            print(f"    >>> INSUFFICIENT samples - will fallback to default")
    
    return len(summary) > 0


def main():
    """Run all validation tests."""
    print("sync_adaptive VALIDATION - Is this real or fake?\n")
    
    # Test 1: Config divergence (most critical)
    configs_work = test_config_divergence()
    
    # Test 2: Environment divergence (needs real data)
    env_works = test_environment_divergence()
    
    # Test 3: Sample density (needs real data)
    samples_ok = test_sample_density()
    
    print("\n=== VALIDATION SUMMARY ===")
    print(f"Config divergence: {'PASS' if configs_work else 'FAIL'}")
    print(f"Environment divergence: {'PASS' if env_works else 'NEED_DATA' if env_works is None else 'FAIL'}")
    print(f"Sample density: {'PASS' if samples_ok else 'NEED_DATA'}")
    
    if configs_work:
        print("\n>>> CORE ADAPTATION IS WORKING!")
        print(">>> Different configs produce different outcomes.")
    else:
        print("\n>>> CRITICAL: Configs have no effect - adaptation is fake!")
    
    if env_works is None:
        print(">>> NEXT: Run real discovery to collect (env, config) outcomes")
    
    return configs_work


if __name__ == "__main__":
    main()
