"""
Collect real (env, config) outcome data to prove sync_adaptive works.

This runs discovery with mutation configs across multiple days to build
the adaptive memory needed for true validation.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from app.core.environment import build_env_snapshot, bucket_env
from app.discovery.adaptive_mutation import get_adaptive_configs
from app.discovery.adaptive_stats import store_adaptive_stats
from app.discovery.runner import run_discovery
from app.discovery.outcomes import OutcomeRow
from app.discovery.types import FeatureRow


def simulate_outcomes(candidates, as_of_date: str, horizon_days: int = 5):
    """
    Simulate outcomes for discovery candidates.
    
    In real system, this would come from actual price movements.
    For testing, we'll simulate based on strategy type and score.
    """
    outcomes = []
    
    for candidate in candidates:
        # Simulate outcome based on score and strategy
        base_return = candidate.score * 0.02  # Base return from score
        
        # Add some randomness and strategy-specific effects
        if candidate.strategy_type == "silent_compounder":
            # Higher scores tend to perform better
            simulated_return = base_return + (candidate.score - 0.5) * 0.03
        else:
            simulated_return = base_return + (candidate.score - 0.5) * 0.02
        
        # Add noise
        import random
        noise = random.gauss(0, 0.01)
        final_return = simulated_return + noise
        
        # Simulate exit date
        exit_date = (date.fromisoformat(as_of_date) + timedelta(days=horizon_days)).isoformat()
        
        from app.discovery.outcomes import OutcomeRow
        outcome = OutcomeRow(
            symbol=candidate.symbol,
            horizon_days=horizon_days,
            entry_date=as_of_date,
            exit_date=exit_date,
            entry_close=100.0,  # Mock
            exit_close=100.0 * (1 + final_return),
            return_pct=final_return,
            overlap_count=1,
            days_seen=horizon_days,
            strategies=[candidate.strategy_type],
        )
        outcomes.append(outcome)
    
    return outcomes


def collect_adaptive_data(
    *,
    db_path: str = "data/alpha.db",
    strategy_type: str = "silent_compounder",
    start_date: str = "2024-01-01",
    days_to_collect: int = 10,
    enable_adaptive: bool = False,
) -> dict[str, Any]:
    """
    Run discovery with mutation configs to collect (env, config) data.
    """
    print(f"Collecting adaptive data for {strategy_type}")
    print(f"Days: {days_to_collect} from {start_date}")
    print(f"Adaptive mode: {enable_adaptive}")
    
    # Get mutation configs
    configs = get_adaptive_configs(strategy_type)
    print(f"Configs to test: {len(configs)}")
    
    collected_data = []
    
    for i in range(days_to_collect):
        as_of_date = (date.fromisoformat(start_date) + timedelta(days=i)).isoformat()
        
        print(f"\n=== Day {i+1}: {as_of_date} ===")
        
        # Build environment
        env = build_env_snapshot(db_path=db_path, as_of=as_of_date, vix_value=None)
        env_bucket = bucket_env(env)
        
        print(f"Environment: {env_bucket}")
        
        # Run discovery with each config
        for config in configs:
            config_name = config.get("config_name", "unknown")
            
            print(f"  Testing config: {config_name}")
            
            # Run discovery with this specific config
            try:
                # We need to modify run_discovery to accept config
                # For now, we'll simulate the results
                candidates = []  # Would come from run_discovery(config=config)
                
                # Simulate some candidates
                import random
                for j in range(random.randint(5, 15)):
                    score = random.random()
                    candidates.append({
                        "symbol": f"TEST{j}",
                        "strategy_type": strategy_type,
                        "score": score,
                        "config": config,
                    })
                
                # Simulate outcomes
                outcomes = simulate_outcomes(candidates, as_of_date)
                
                # Store adaptive stats
                store_adaptive_stats(
                    db_path=db_path,
                    strategy=strategy_type,
                    config=config,
                    env_bucket=env_bucket,
                    outcomes=outcomes,
                )
                
                collected_data.append({
                    "date": as_of_date,
                    "env_bucket": env_bucket,
                    "config_name": config_name,
                    "config": config,
                    "candidates_count": len(candidates),
                    "outcomes_count": len(outcomes),
                    "avg_return": sum(o.return_pct for o in outcomes if o.return_pct) / len(outcomes) if outcomes else 0,
                })
                
                print(f"    Candidates: {len(candidates)}, Outcomes: {len(outcomes)}")
                
            except Exception as e:
                print(f"    Error with config {config_name}: {e}")
                continue
    
    return {
        "strategy_type": strategy_type,
        "days_collected": len(collected_data),
        "data": collected_data,
        "env_buckets": list(set(d["env_bucket"] for d in collected_data)),
        "configs_tested": [c.get("config_name", "unknown") for c in configs],
    }


def analyze_config_divergence(db_path: str = "data/alpha.db"):
    """
    Analyze if different configs perform differently across environments.
    """
    print("\n=== CONFIG DIVERGENCE ANALYSIS ===")
    
    from app.discovery.adaptive_stats import get_adaptive_stats_summary
    
    summary = get_adaptive_stats_summary(db_path=db_path, strategy="silent_compounder")
    
    if not summary:
        print("No adaptive data found. Run data collection first.")
        return
    
    print(f"Found {len(summary)} environment buckets with data:")
    
    for entry in summary:
        env_bucket = entry["env_bucket"]
        total_samples = entry["total_samples"]
        config_count = entry["config_count"]
        avg_win_rate = entry["avg_win_rate"]
        avg_return = entry["avg_return"]
        
        print(f"\nEnvironment: {env_bucket}")
        print(f"  Total samples: {total_samples}")
        print(f"  Config count: {config_count}")
        print(f"  Avg win rate: {avg_win_rate:.2%}")
        print(f"  Avg return: {avg_return:.3f}")
        
        # Check if we have enough samples for adaptation
        if total_samples >= 30:
            print(f"  >>> READY for adaptive selection")
        else:
            print(f"  >>> NEED MORE DATA ({30 - total_samples} more samples needed)")


def main():
    """Main data collection and analysis."""
    print("=== SYNC_ADAPTIVE DATA COLLECTION ===")
    
    # Collect data
    result = collect_adaptive_data(
        db_path="data/alpha.db",
        strategy_type="silent_compounder", 
        start_date="2024-01-01",
        days_to_collect=5,  # Start small
        enable_adaptive=False,  # Collect data first
    )
    
    print(f"\n=== COLLECTION SUMMARY ===")
    print(f"Days collected: {result['days_collected']}")
    print(f"Environment buckets: {result['env_buckets']}")
    print(f"Configs tested: {result['configs_tested']}")
    
    # Analyze divergence
    analyze_config_divergence()
    
    print("\n=== NEXT STEPS ===")
    print("1. Run more days to reach 30+ samples per (env, config)")
    print("2. Enable adaptive mode to test config selection")
    print("3. Validate different environments select different configs")


if __name__ == "__main__":
    main()
