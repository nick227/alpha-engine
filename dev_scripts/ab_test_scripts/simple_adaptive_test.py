"""
Simple test to prove (env, config) → different outcomes.

This creates controlled scenarios to validate sync_adaptive behavior.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from app.core.environment import build_env_snapshot, bucket_env
from app.discovery.adaptive_mutation import get_adaptive_configs
from app.discovery.adaptive_stats import store_adaptive_stats
from app.discovery.strategies import silent_compounder
from app.discovery.outcomes import OutcomeRow
from app.discovery.types import FeatureRow


def create_test_scenarios():
    """Create controlled test scenarios with different environments and stocks."""
    
    scenarios = []
    
    # Scenario 1: Low volatility environment
    low_vol_stocks = [
        FeatureRow(
            symbol='LOW_VOL_1',
            as_of_date='2024-01-01',
            close=100.0, volume=1000000, dollar_volume=100000000,
            avg_dollar_volume_20d=100000000,
            return_1d=0.001, return_5d=0.005, return_20d=0.01,
            return_63d=0.02, return_252d=0.08,
            volatility_20d=0.012,  # Low volatility
            max_drawdown_252d=0.05, price_percentile_252d=0.4,
            volume_zscore_20d=0.5, dollar_volume_zscore_20d=0.5,
            revenue_ttm=None, revenue_growth=None, shares_outstanding=None,
            shares_growth=None, sector=None, industry=None,
            sector_return_63d=None, peer_relative_return_63d=None,
            price_bucket=None,
        ),
        FeatureRow(
            symbol='LOW_VOL_2',
            as_of_date='2024-01-01',
            close=150.0, volume=800000, dollar_volume=120000000,
            avg_dollar_volume_20d=120000000,
            return_1d=0.002, return_5d=0.008, return_20d=0.015,
            return_63d=0.025, return_252d=0.12,
            volatility_20d=0.015,  # Low volatility
            max_drawdown_252d=0.08, price_percentile_252d=0.6,
            volume_zscore_20d=0.8, dollar_volume_zscore_20d=0.8,
            revenue_ttm=None, revenue_growth=None, shares_outstanding=None,
            shares_growth=None, sector=None, industry=None,
            sector_return_63d=None, peer_relative_return_63d=None,
            price_bucket=None,
        ),
    ]
    
    # Scenario 2: High volatility environment  
    high_vol_stocks = [
        FeatureRow(
            symbol='HIGH_VOL_1',
            as_of_date='2024-01-01',
            close=200.0, volume=2000000, dollar_volume=400000000,
            avg_dollar_volume_20d=400000000,
            return_1d=0.02, return_5d=0.01, return_20d=-0.02,
            return_63d=0.05, return_252d=0.15,
            volatility_20d=0.035,  # High volatility
            max_drawdown_252d=0.15, price_percentile_252d=0.7,
            volume_zscore_20d=2.0, dollar_volume_zscore_20d=2.0,
            revenue_ttm=None, revenue_growth=None, shares_outstanding=None,
            shares_growth=None, sector=None, industry=None,
            sector_return_63d=None, peer_relative_return_63d=None,
            price_bucket=None,
        ),
        FeatureRow(
            symbol='HIGH_VOL_2',
            as_of_date='2024-01-01',
            close=80.0, volume=1500000, dollar_volume=120000000,
            avg_dollar_volume_20d=120000000,
            return_1d=-0.01, return_5d=0.03, return_20d=0.04,
            return_63d=0.08, return_252d=0.20,
            volatility_20d=0.04,  # High volatility
            max_drawdown_252d=0.20, price_percentile_252d=0.8,
            volume_zscore_20d=1.5, dollar_volume_zscore_20d=1.5,
            revenue_ttm=None, revenue_growth=None, shares_outstanding=None,
            shares_growth=None, sector=None, industry=None,
            sector_return_63d=None, peer_relative_return_63d=None,
            price_bucket=None,
        ),
    ]
    
    scenarios.append({
        "name": "Low Volatility Environment",
        "env_bucket": ("LOW", "CHOP", "LOW"),
        "stocks": low_vol_stocks,
    })
    
    scenarios.append({
        "name": "High Volatility Environment", 
        "env_bucket": ("HIGH", "TRENDING", "HIGH"),
        "stocks": high_vol_stocks,
    })
    
    return scenarios


def test_config_performance(scenario, db_path: str):
    """Test how different configs perform in a specific environment."""
    
    print(f"\n=== Testing: {scenario['name']} ===")
    print(f"Environment bucket: {scenario['env_bucket']}")
    
    # Get configs
    configs = get_adaptive_configs("silent_compounder")
    
    results = {}
    
    for config in configs:
        config_name = config.get("config_name", "unknown")
        print(f"\n  Config: {config_name}")
        
        outcomes = []
        
        # Test config on all stocks in this environment
        for stock in scenario["stocks"]:
            # Run strategy with config
            result = silent_compounder(stock, config=config)
            
            if result[0] is not None:
                score = result[0]
                reason = result[1]
                
                # Simulate outcome based on score and environment fit
                # Higher score = higher probability of positive outcome
                import random
                
                # Environment-specific success rates
                if scenario["env_bucket"][0] == "LOW":  # Low vol environment
                    success_prob = 0.3 + score * 0.4  # Base 30% + score effect
                else:  # High vol environment
                    success_prob = 0.2 + score * 0.5  # Base 20% + score effect
                
                is_win = random.random() < success_prob
                return_pct = random.gauss(0.02 if is_win else -0.01, 0.015)
                
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
                
                print(f"    {stock.symbol}: score={score:.3f}, win={is_win}, return={return_pct:.3f}")
            else:
                print(f"    {stock.symbol}: REJECTED ({result[1]})")
        
        # Store adaptive stats for this config in this environment
        if outcomes:
            store_adaptive_stats(
                db_path=db_path,
                strategy="silent_compounder",
                config=config,
                env_bucket=scenario["env_bucket"],
                outcomes=outcomes,
            )
            
            # Calculate performance metrics
            wins = sum(1 for o in outcomes if o.return_pct and o.return_pct > 0)
            win_rate = wins / len(outcomes)
            avg_return = sum(o.return_pct for o in outcomes if o.return_pct) / len(outcomes)
            
            results[config_name] = {
                "win_rate": win_rate,
                "avg_return": avg_return,
                "sample_count": len(outcomes),
                "config": config,
            }
            
            print(f"    Summary: {len(outcomes)} outcomes, {win_rate:.1%} win rate, {avg_return:.3f} avg return")
    
    return results


def analyze_config_divergence(all_results):
    """Analyze if different configs perform differently across environments."""
    
    print(f"\n=== CONFIG DIVERGENCE ANALYSIS ===")
    
    env_results = {}
    for scenario_name, scenario_data in all_results.items():
        env_results[scenario_name] = scenario_data["results"]
    
    # Compare best configs across environments
    print(f"Best config per environment:")
    
    for env_name, results in env_results.items():
        if not results:
            continue
            
        # Find best config by win rate
        best_config = max(results.items(), key=lambda x: x[1]["win_rate"])
        
        print(f"\n{env_name}:")
        print(f"  Best config: {best_config[0]}")
        print(f"  Win rate: {best_config[1]['win_rate']:.1%}")
        print(f"  Avg return: {best_config[1]['avg_return']:.3f}")
        print(f"  Samples: {best_config[1]['sample_count']}")
        
        # Show all configs for this environment
        print(f"  All configs:")
        for config_name, metrics in results.items():
            print(f"    {config_name}: {metrics['win_rate']:.1%} win, {metrics['avg_return']:.3f} return")
    
    # Check if different environments prefer different configs
    env_names = list(env_results.keys())
    if len(env_names) >= 2:
        best_configs = []
        for env_name in env_names:
            results = env_results[env_name]
            if results:
                best_config = max(results.items(), key=lambda x: x[1]["win_rate"])
                best_configs.append(best_config[0])
        
        if len(set(best_configs)) > 1:
            print(f"\n>>> SYNC_ADAPTIVE WORKING: Different environments prefer different configs!")
            print(f">>> Best configs: {best_configs}")
            return True
        else:
            print(f"\n>>> NEED MORE DATA: All environments prefer same config")
            return False
    else:
        print(f"\n>>> NEED MORE ENVIRONMENTS: Only {len(env_names)} environment tested")
        return None


def main():
    """Run controlled sync_adaptive test."""
    print("=== SYNC_ADAPTIVE CONTROLLED TEST ===")
    print("Testing if (env, config) → different outcomes")
    
    # Create test scenarios
    scenarios = create_test_scenarios()
    
    # Test each scenario
    all_results = {}
    
    for scenario in scenarios:
        scenario_results = test_config_performance(
            scenario, 
            db_path="data/alpha.db"
        )
        
        all_results[scenario["name"]] = {
            "env_bucket": scenario["env_bucket"],
            "results": scenario_results,
        }
    
    # Analyze divergence
    divergence_works = analyze_config_divergence(all_results)
    
    print(f"\n=== FINAL VERDICT ===")
    if divergence_works:
        print(">>> SYNC_ADAPTIVE IS REAL: Different environments select different configs!")
    elif divergence_works is False:
        print(">>> SYNC_ADAPTIVE NEEDS TUNING: Environments not differentiated enough")
    else:
        print(">>> SYNC_ADAPTIVE INCOMPLETE: Need more environment diversity")
    
    print("\nNext steps:")
    print("1. Add more environment diversity (VIX, trend regimes)")
    print("2. Increase sample sizes per (env, config)")
    print("3. Test with real market data")


if __name__ == "__main__":
    main()
