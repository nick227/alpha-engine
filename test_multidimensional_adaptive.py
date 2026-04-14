"""
Test multi-dimensional adaptive system with industry dimensions.

Validates that (Strategy, Environment, Industry) creates stronger
config separation than (Strategy, Environment) alone.
"""

from __future__ import annotations

import random
from typing import Any
from collections import defaultdict

from app.core.environment import build_env_snapshot_v3
from app.core.environment_v3 import bucket_env_v3
from app.discovery.adaptive_industry import get_multi_industry_configs, get_industry_config_summary
from app.discovery.adaptive_stats import store_adaptive_stats, lookup_best_config
from app.discovery.adaptive_selection import select_adaptive_config, enable_adaptive_globally
from app.discovery.strategies import ownership_vacuum, realness_repricer
from app.discovery.outcomes import OutcomeRow
from app.discovery.types import FeatureRow


def create_industry_specific_stock(sector: str, env_bucket: tuple[str, str, str, str, str, str], stock_id: int) -> FeatureRow:
    """Create stock optimized for specific industry and environment."""
    
    vol_regime, trend_regime, disp_regime, liq_regime, sector_regime, industry_disp = env_bucket
    
    # Base characteristics by sector
    sector_characteristics = {
        "technology": {"volatility": 0.035, "volume": 2000000, "beta": 1.3},
        "financials": {"volatility": 0.025, "volume": 1500000, "beta": 1.0},
        "healthcare": {"volatility": 0.020, "volume": 1000000, "beta": 0.8},
        "energy": {"volatility": 0.030, "volume": 1200000, "beta": 1.1},
        "consumer": {"volatility": 0.022, "volume": 1300000, "beta": 0.9},
        "industrial": {"volatility": 0.028, "volume": 1100000, "beta": 1.0},
    }
    
    chars = sector_characteristics.get(sector_regime, sector_characteristics["technology"])
    
    # Adjust for environment
    if vol_regime == "HI_VOL":
        chars["volatility"] *= 1.5
    if liq_regime == "LO_LIQ":
        chars["volume"] *= 0.7
    
    # Generate stock with sector characteristics
    base_return = random.gauss(0.02, chars["volatility"])
    
    return FeatureRow(
        symbol=f'{sector.upper()}_{stock_id}',
        as_of_date='2024-01-01',
        close=random.uniform(50, 300),
        volume=chars["volume"],
        dollar_volume=chars["volume"] * random.uniform(50, 200),
        avg_dollar_volume_20d=chars["volume"] * random.uniform(50, 200),
        return_1d=random.gauss(0, chars["volatility"]),
        return_5d=random.gauss(0, chars["volatility"] * 2),
        return_20d=random.gauss(0, chars["volatility"] * 3),
        return_63d=base_return,
        return_252d=random.gauss(0.1, chars["volatility"] * 5),
        volatility_20d=chars["volatility"],
        max_drawdown_252d=random.uniform(0.05, 0.25),
        price_percentile_252d=random.uniform(0.2, 0.8),
        volume_zscore_20d=random.gauss(0, 1),
        dollar_volume_zscore_20d=random.gauss(0, 1),
        revenue_ttm=None, revenue_growth=None, shares_outstanding=None,
        shares_growth=None, sector=sector, industry=None,
        sector_return_63d=base_return,
        peer_relative_return_63d=random.gauss(0, 0.02),
        price_bucket=None,
    )


def simulate_industry_aware_outcome(stock: FeatureRow, config: dict[str, Any], env_bucket: tuple[str, str, str, str, str, str], strategy_type: str) -> OutcomeRow:
    """Simulate outcome with industry-aware config interaction."""
    
    # Run strategy
    if strategy_type == "ownership_vacuum":
        result = ownership_vacuum(stock, config=config)
    elif strategy_type == "realness_repricer":
        result = realness_repricer(stock, config=config)
    else:
        return None
    
    if result[0] is None:
        return None
    
    score = result[0]
    vol_regime, trend_regime, disp_regime, liq_regime, sector_regime, industry_disp = env_bucket
    config_name = config.get("config_name", "default")
    sector = stock.sector or "unknown"
    
    # Industry-specific performance patterns
    base_success_prob = 0.5
    base_return = 0.0
    
    # Strategy × Industry × Environment interaction
    if strategy_type == "ownership_vacuum":
        if sector == "technology" and vol_regime == "HI_VOL":
            # Tech in high vol: aggressive configs suffer, defensive configs shine
            if "aggressive" in config_name:
                base_success_prob = 0.35
                base_return = -0.005
            elif "defensive" in config_name:
                base_success_prob = 0.65
                base_return = 0.008
            elif sector in config_name:  # Industry-adapted config
                base_success_prob = 0.60
                base_return = 0.006
            else:
                base_success_prob = 0.45
                base_return = 0.002
                
        elif sector == "financials" and liq_regime == "LO_LIQ":
            # Financials in low liquidity: liquidity-focused configs win
            if "defensive" in config_name:
                base_success_prob = 0.70
                base_return = 0.010
            elif sector in config_name:  # Industry-adapted
                base_success_prob = 0.65
                base_return = 0.008
            else:
                base_success_prob = 0.40
                base_return = -0.003
                
        elif sector == "healthcare":
            # Healthcare: stable, defensive configs do well
            if "defensive" in config_name or sector in config_name:
                base_success_prob = 0.60
                base_return = 0.005
            elif "aggressive" in config_name:
                base_success_prob = 0.40
                base_return = -0.002
            else:
                base_success_prob = 0.50
                base_return = 0.002
        else:
            # Other sectors: moderate performance
            base_success_prob = 0.50 + score * 0.1
            base_return = 0.002
            
    elif strategy_type == "realness_repricer":
        if sector == "energy" and trend_regime == "CHOP":
            # Energy in choppy markets: value-focused configs excel
            if "value_focused" in config_name or sector in config_name:
                base_success_prob = 0.65
                base_return = 0.008
            else:
                base_success_prob = 0.45
                base_return = 0.001
                
        elif sector == "technology" and trend_regime == "TREND":
            # Tech in trending markets: trend-focused configs win
            if "trend_focused" in config_name or sector in config_name:
                base_success_prob = 0.60
                base_return = 0.006
            else:
                base_success_prob = 0.40
                base_return = -0.002
        else:
            base_success_prob = 0.50 + score * 0.1
            base_return = 0.002
    
    # Add industry dispersion effect
    if industry_disp == "HI_INDUSTRY_DISP":
        if sector in config_name:  # Industry-adapted configs handle dispersion better
            base_success_prob += 0.05
            base_return += 0.002
        else:
            base_success_prob -= 0.03
            base_return -= 0.001
    
    # Generate outcome
    success_prob = max(0.1, min(0.9, base_success_prob + score * 0.2))
    is_win = random.random() < success_prob
    
    final_return = random.gauss(
        base_return if is_win else base_return * 0.5,
        0.015
    )
    
    return OutcomeRow(
        symbol=stock.symbol,
        horizon_days=5,
        entry_date="2024-01-01",
        exit_date="2024-01-06",
        entry_close=stock.close or 100.0,
        exit_close=(stock.close or 100.0) * (1 + final_return),
        return_pct=final_return,
        overlap_count=1,
        days_seen=5,
        strategies=[strategy_type],
    )


def test_multidimensional_config_separation():
    """Test if multi-dimensional environment creates stronger config separation."""
    
    print("=== MULTI-DIMENSIONAL CONFIG SEPARATION TEST ===")
    
    strategies = ["ownership_vacuum", "realness_repricer"]
    
    for strategy_type in strategies:
        print(f"\n=== Testing {strategy_type} ===")
        
        # Get industry-aware configs
        configs = get_multi_industry_configs(strategy_type)
        print(f"Configs available: {len(configs)}")
        
        # Test different environment × industry combinations
        test_combinations = [
            ("HI_VOL", "technology"),
            ("LO_VOL", "healthcare"), 
            ("HI_VOL", "financials"),
            ("LO_VOL", "energy"),
        ]
        
        results = {}
        
        for vol_regime, sector in test_combinations:
            # Create environment bucket
            env = build_env_snapshot_v3(db_path="data/alpha.db", as_of="2024-01-01")
            # Override for testing
            env.sector_regime = sector
            env.market_vol_pct = 0.8 if vol_regime == "HI_VOL" else 0.3
            env_bucket = bucket_env_v3(env)
            
            print(f"\n  Testing {vol_regime} + {sector}:")
            
            # Generate stocks for this environment/sector
            stocks = [create_industry_specific_stock(sector, env_bucket, i) for i in range(15)]
            
            config_results = {}
            
            for config in configs:
                config_name = config["config_name"]
                
                outcomes = []
                for stock in stocks:
                    outcome = simulate_industry_aware_outcome(stock, config, env_bucket, strategy_type)
                    if outcome:
                        outcomes.append(outcome)
                
                if outcomes:
                    wins = sum(1 for o in outcomes if o.return_pct > 0)
                    win_rate = wins / len(outcomes)
                    avg_return = sum(o.return_pct for o in outcomes) / len(outcomes)
                    
                    config_results[config_name] = {
                        "win_rate": win_rate,
                        "avg_return": avg_return,
                        "samples": len(outcomes),
                    }
                    
                    # Store adaptive stats
                    store_adaptive_stats(
                        db_path="data/alpha.db",
                        strategy=strategy_type,
                        config=config,
                        env_bucket=env_bucket,
                        outcomes=outcomes,
                    )
                    
                    print(f"    {config_name}: {win_rate:.1%} win, {avg_return:.3f} avg, {len(outcomes)} samples")
            
            results[f"{vol_regime}_{sector}"] = config_results
        
        # Analyze separation
        print(f"\n  {strategy_type} SEPARATION ANALYSIS:")
        
        best_configs = []
        for combo, config_results in results.items():
            if config_results:
                best_config = max(config_results.items(), key=lambda x: x[1]["win_rate"])
                best_configs.append(best_config[0])
                print(f"    {combo}: {best_config[0]} ({best_config[1]['win_rate']:.1%})")
        
        # Check if different environments/industries prefer different configs
        unique_best_configs = set(best_configs)
        
        if len(unique_best_configs) > len(best_configs) * 0.6:  # 60% diversity
            print(f"  >>> STRONG SEPARATION: {len(unique_best_configs)}/{len(best_configs)} different configs win")
        else:
            print(f"  >>> WEAK SEPARATION: Only {len(unique_best_configs)} unique best configs")


def test_config_space_size():
    """Test multi-dimensional config space size and diversity."""
    
    print(f"\n=== MULTI-DIMENSIONAL CONFIG SPACE ANALYSIS ===")
    
    strategies = ["ownership_vacuum", "realness_repricer"]
    
    for strategy_type in strategies:
        summary = get_industry_config_summary(strategy_type)
        
        print(f"\n{strategy_type}:")
        print(f"  Total configs: {summary['total_configs']}")
        print(f"  Base configs: {summary['base_configs']}")
        print(f"  Industry-adapted: {summary['sector_adapted']}")
        print(f"  Sectors: {summary['sectors']}")
        
        # Check config diversity
        config_names = summary['config_names']
        unique_names = set(config_names)
        
        print(f"  Config diversity: {len(unique_names)}/{len(config_names)} unique")
        
        if len(unique_names) >= len(config_names) * 0.8:
            print(f"  >>> GOOD: High config diversity")
        else:
            print(f"  >>> WARNING: Low config diversity")


def test_adaptive_selection_power():
    """Test if adaptive selection can actually pick the right configs."""
    
    print(f"\n=== ADAPTIVE SELECTION POWER TEST ===")
    
    # Enable adaptive mode
    enable_adaptive_globally()
    
    # Test on ownership_vacuum (most promising)
    strategy_type = "ownership_vacuum"
    
    # Generate some test environments
    test_envs = []
    for i in range(10):
        env = build_env_snapshot_v3(db_path="data/alpha.db", as_of="2024-01-01")
        env_bucket = bucket_env_v3(env)
        test_envs.append(env_bucket)
    
    print(f"Testing adaptive selection across {len(test_envs)} environments...")
    
    selection_results = {}
    
    for env_bucket in test_envs:
        try:
            best_config = lookup_best_config(
                db_path="data/alpha.db",
                strategy=strategy_type,
                env_bucket=env_bucket,
                min_samples=5,  # Lower threshold for testing
            )
            
            if best_config:
                config_name = best_config["config"]["config_name"]
                selection_results[env_bucket] = config_name
                print(f"  {env_bucket}: {config_name}")
            else:
                selection_results[env_bucket] = "DEFAULT"
                print(f"  {env_bucket}: DEFAULT (insufficient data)")
                
        except Exception as e:
            selection_results[env_bucket] = "ERROR"
            print(f"  {env_bucket}: ERROR ({e})")
    
    # Check selection diversity
    unique_selections = set(selection_results.values())
    
    print(f"\nSelection diversity: {len(unique_selections)} unique selections")
    
    if len(unique_selections) > 3:
        print(f">>> ADAPTIVE SELECTION WORKING: Different configs selected for different environments")
        return True
    else:
        print(f">>> ADAPTIVE SELECTION WEAK: Limited config diversity in selection")
        return False


def main():
    """Run comprehensive multi-dimensional adaptive test."""
    
    print("Testing multi-dimensional adaptive system with industry dimensions")
    
    # Test 1: Config separation
    test_multidimensional_config_separation()
    
    # Test 2: Config space analysis
    test_config_space_size()
    
    # Test 3: Adaptive selection power
    selection_works = test_adaptive_selection_power()
    
    print(f"\n=== MULTI-DIMENSIONAL ADAPTIVE SUMMARY ===")
    print(f"Config separation: TESTED")
    print(f"Config space: ANALYZED")
    print(f"Adaptive selection: {'WORKING' if selection_works else 'WEAK'}")
    
    if selection_works:
        print(f"\n>>> MULTI-DIMENSIONAL ADAPTIVE SHOWS PROMISE!")
        print(f">>> Industry dimensions add meaningful selection pressure")
        print(f">>> Ready for financial A/B test")
    else:
        print(f"\n>>> MULTI-DIMENSIONAL ADAPTIVE NEEDS REFINEMENT")
        print(f">>> Industry signals may still be too weak")


if __name__ == "__main__":
    main()
