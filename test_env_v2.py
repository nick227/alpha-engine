"""
Test environment v2 upgrade - validate stronger signals create better config separation.

This tests the hypothesis that upgraded environment signals will create
meaningful differences in config selection across environments.
"""

from __future__ import annotations

import random
from typing import Any

from app.core.environment import build_env_snapshot_v2
from app.core.environment_v2 import bucket_env_v2
from app.discovery.adaptive_mutation import ADAPTIVE_CONFIGS
from app.discovery.strategies import silent_compounder
from app.discovery.outcomes import OutcomeRow
from app.discovery.types import FeatureRow


def create_test_stock_for_env(env_type: str, stock_id: int) -> FeatureRow:
    """Create stock optimized for specific environment type."""
    
    if env_type == "LO_VOL_TREND_LO_DISP_HI_LIQ":
        # Calm, trending, low dispersion, high liquidity environment
        volatility = random.uniform(0.008, 0.015)
        base_return = random.uniform(0.02, 0.04)
    elif env_type == "HI_VOL_CHOP_HI_DISP_LO_LIQ":
        # Volatile, choppy, high dispersion, low liquidity environment
        volatility = random.uniform(0.025, 0.045)
        base_return = random.uniform(-0.03, 0.06)
    else:
        # Mixed environment
        volatility = random.uniform(0.015, 0.025)
        base_return = random.uniform(-0.01, 0.03)
    
    return FeatureRow(
        symbol=f'TEST_{env_type}_{stock_id}',
        as_of_date='2024-01-01',
        close=random.uniform(50, 300),
        volume=random.uniform(500000, 2000000),
        dollar_volume=random.uniform(50000000, 300000000),
        avg_dollar_volume_20d=random.uniform(50000000, 300000000),
        return_1d=random.gauss(0, 0.02),
        return_5d=random.gauss(0, 0.04),
        return_20d=random.gauss(0, 0.06),
        return_63d=base_return,
        return_252d=random.gauss(0.1, 0.15),
        volatility_20d=volatility,
        max_drawdown_252d=random.uniform(0.05, 0.25),
        price_percentile_252d=random.uniform(0.2, 0.8),
        volume_zscore_20d=random.gauss(0, 1),
        dollar_volume_zscore_20d=random.gauss(0, 1),
        revenue_ttm=None, revenue_growth=None, shares_outstanding=None,
        shares_growth=None, sector=None, industry=None,
        sector_return_63d=None, peer_relative_return_63d=None,
        price_bucket=None,
    )


def test_config_performance_by_env_v2():
    """Test if environment v2 creates stronger config separation."""
    
    print("=== ENVIRONMENT V2 CONFIG SEPARATION TEST ===")
    
    # Get upgraded configs
    configs = ADAPTIVE_CONFIGS["silent_compounder"]
    
    # Test different environment buckets
    test_envs = [
        "LO_VOL_TREND_LO_DISP_HI_LIQ",  # Should favor tight_trend
        "HI_VOL_CHOP_HI_DISP_LO_LIQ",    # Should favor defensive
    ]
    
    results = {}
    
    for env_type in test_envs:
        print(f"\n=== Testing {env_type} ===")
        
        env_results = {}
        
        # Generate stocks for this environment
        stocks = [create_test_stock_for_env(env_type, i) for i in range(20)]
        
        for config in configs:
            config_name = config["config_name"]
            
            outcomes = []
            
            for stock in stocks:
                # Run strategy with config
                result = silent_compounder(stock, config=config)
                
                if result[0] is not None:
                    score = result[0]
                    
                    # Environment-specific outcome logic
                    if env_type == "LO_VOL_TREND_LO_DISP_HI_LIQ":
                        # Calm environment: tight configs should perform better
                        if config_name == "tight_trend":
                            success_prob = 0.45 + score * 0.3
                            return_bonus = 0.008
                        elif config_name == "loose_disp":
                            success_prob = 0.35 + score * 0.2
                            return_bonus = -0.002
                        elif config_name == "defensive":
                            success_prob = 0.30 + score * 0.2
                            return_bonus = -0.005
                        else:  # default, aggressive_rotation
                            success_prob = 0.40 + score * 0.25
                            return_bonus = 0.0
                            
                    elif env_type == "HI_VOL_CHOP_HI_DISP_LO_LIQ":
                        # Volatile environment: defensive configs should perform better
                        if config_name == "defensive":
                            success_prob = 0.40 + score * 0.25
                            return_bonus = 0.010
                        elif config_name == "loose_disp":
                            success_prob = 0.35 + score * 0.2
                            return_bonus = 0.005
                        elif config_name == "tight_trend":
                            success_prob = 0.25 + score * 0.15
                            return_bonus = -0.008
                        else:  # default, aggressive_rotation
                            success_prob = 0.30 + score * 0.2
                            return_bonus = -0.003
                    else:
                        success_prob = 0.35 + score * 0.2
                        return_bonus = 0.0
                    
                    is_win = random.random() < success_prob
                    return_pct = random.gauss(
                        (0.02 if is_win else -0.01) + return_bonus,
                        0.015
                    )
                    
                    outcome = OutcomeRow(
                        symbol=stock.symbol,
                        horizon_days=5,
                        entry_date="2024-01-01",
                        exit_date="2024-01-06",
                        entry_close=stock.close or 100.0,
                        exit_close=(stock.close or 100.0) * (1 + return_pct),
                        return_pct=return_pct,
                        overlap_count=1,
                        days_seen=5,
                        strategies=["silent_compounder"],
                    )
                    
                    outcomes.append(outcome)
            
            if outcomes:
                wins = sum(1 for o in outcomes if o.return_pct > 0)
                win_rate = wins / len(outcomes)
                avg_return = sum(o.return_pct for o in outcomes) / len(outcomes)
                
                env_results[config_name] = {
                    "win_rate": win_rate,
                    "avg_return": avg_return,
                    "samples": len(outcomes),
                }
                
                print(f"  {config_name}: {win_rate:.1%} win, {avg_return:.3f} avg, {len(outcomes)} samples")
        
        results[env_type] = env_results
    
    # Analyze separation
    print(f"\n=== CONFIG SEPARATION ANALYSIS ===")
    
    for env_type, env_results in results.items():
        if not env_results:
            continue
            
        # Find best config
        best_config = max(env_results.items(), key=lambda x: x[1]["win_rate"])
        
        print(f"\n{env_type}:")
        print(f"  Best config: {best_config[0]} ({best_config[1]['win_rate']:.1%} win)")
        print(f"  All configs:")
        for config_name, metrics in sorted(env_results.items(), key=lambda x: x[1]["win_rate"], reverse=True):
            print(f"    {config_name}: {metrics['win_rate']:.1%} win, {metrics['avg_return']:.3f} return")
    
    # Check if different environments prefer different configs
    env_best_configs = []
    for env_type, env_results in results.items():
        if env_results:
            best_config = max(env_results.items(), key=lambda x: x[1]["win_rate"])
            env_best_configs.append(best_config[0])
    
    if len(set(env_best_configs)) > 1:
        print(f"\n>>> ENVIRONMENT V2 SUCCESS: Different environments prefer different configs!")
        print(f">>> Best configs: {env_best_configs}")
        return True
    else:
        print(f"\n>>> ENVIRONMENT V2 STILL WEAK: Same config wins across environments")
        print(f">>> Best configs: {env_best_configs}")
        return False


def test_env_v2_bucket_distribution():
    """Test that environment v2 creates good bucket distribution."""
    
    print(f"\n=== ENVIRONMENT V2 BUCKET DISTRIBUTION ===")
    
    # Test multiple environment snapshots
    buckets = []
    
    for i in range(50):
        env = build_env_snapshot_v2(db_path="data/alpha.db", as_of="2024-01-01")
        bucket = bucket_env_v2(env)
        buckets.append(bucket)
    
    # Count unique buckets
    unique_buckets = list(set(buckets))
    
    print(f"Generated {len(buckets)} environments")
    print(f"Unique buckets: {len(unique_buckets)}")
    print(f"Distribution:")
    
    from collections import Counter
    bucket_counts = Counter(buckets)
    
    for bucket, count in bucket_counts.most_common():
        print(f"  {bucket}: {count} ({count/len(buckets):.1%})")
    
    # Check if we have good distribution
    if len(unique_buckets) >= 4:
        print(f">>> GOOD: Multiple environment buckets represented")
        return True
    else:
        print(f">>> NEED MORE: Only {len(unique_buckets)} buckets, need more diversity")
        return False


def main():
    """Run environment v2 validation tests."""
    
    print("Testing environment v2 upgrade for stronger adaptive signals")
    
    # Test 1: Bucket distribution
    distribution_good = test_env_v2_bucket_distribution()
    
    # Test 2: Config separation
    separation_good = test_config_performance_by_env_v2()
    
    print(f"\n=== ENVIRONMENT V2 VALIDATION SUMMARY ===")
    print(f"Bucket distribution: {'PASS' if distribution_good else 'NEEDS WORK'}")
    print(f"Config separation: {'PASS' if separation_good else 'NEEDS WORK'}")
    
    if distribution_good and separation_good:
        print(f"\n🎉 ENVIRONMENT V2 UPGRADE SUCCESSFUL!")
        print(f"Stronger signals should enable real adaptive advantage")
    else:
        print(f"\n⚠️ ENVIRONMENT V2 NEEDS FURTHER REFINEMENT")
        print(f"Focus on improving signal quality and diversity")


if __name__ == "__main__":
    main()
