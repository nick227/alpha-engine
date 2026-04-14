"""
Industry-aware adaptive configurations.

Creates strategy configs adapted for specific industry characteristics
to maximize adaptive selection pressure.
"""

from __future__ import annotations

from typing import Any, List
from copy import deepcopy

from app.core.environment_v3 import get_industry_aware_config
from app.discovery.strategies import DEFAULT_STRATEGY_CONFIGS


# Industry-specific adaptive configs
INDUSTRY_AWARE_CONFIGS = {
    "ownership_vacuum": [
        # Base configs
        {
            "config_name": "default",
            "spike_threshold": 2.0,
            "liquidity_threshold": 0.3,
            "volume_multiplier": 1.2,
            "low_liquidity_weight": 0.3,
            "power": 1.8,
        },
        {
            "config_name": "aggressive",
            "spike_threshold": 1.8,
            "liquidity_threshold": 0.25,
            "volume_multiplier": 1.0,
            "low_liquidity_weight": 0.4,
            "power": 1.6,
        },
        {
            "config_name": "defensive",
            "spike_threshold": 2.5,
            "liquidity_threshold": 0.4,
            "volume_multiplier": 1.5,
            "low_liquidity_weight": 0.2,
            "power": 2.0,
        },
    ],
    "realness_repricer": [
        # Base configs
        {
            "config_name": "default",
            "depressed_weight": 0.6,
            "drawdown_weight": 0.4,
            "power": 2.3,
            "min_score": 0.85,
        },
        {
            "config_name": "value_focused",
            "depressed_weight": 0.8,
            "drawdown_weight": 0.2,
            "power": 2.5,
            "min_score": 0.80,
        },
        {
            "config_name": "trend_focused",
            "depressed_weight": 0.4,
            "drawdown_weight": 0.6,
            "power": 2.0,
            "min_score": 0.90,
        },
    ],
}


def get_industry_adaptive_configs(strategy_type: str, sector: str | None = None) -> List[dict[str, Any]]:
    """
    Get adaptive configs for strategy, optionally adapted for specific industry.
    
    Args:
        strategy_type: Strategy name
        sector: Industry sector for adaptation (optional)
    
    Returns:
        List of configuration dictionaries
    """
    base_configs = INDUSTRY_AWARE_CONFIGS.get(strategy_type, [])
    
    if not base_configs:
        # Fallback to default strategy configs
        default_config = DEFAULT_STRATEGY_CONFIGS.get(strategy_type, {})
        return [{
            "config_name": "default",
            **default_config
        }]
    
    if sector:
        # Adapt configs for industry
        adapted_configs = []
        for base_config in base_configs:
            adapted_config = get_industry_aware_config(strategy_type, sector, base_config)
            adapted_configs.append(adapted_config)
        return adapted_configs
    else:
        # Return base configs
        return base_configs


def get_multi_industry_configs(strategy_type: str) -> List[dict[str, Any]]:
    """
    Get configs adapted for multiple key industries.
    
    This creates the full multi-dimensional config space.
    """
    key_sectors = ["technology", "financials", "healthcare", "energy"]
    all_configs = []
    
    # Add base configs (non-sector specific)
    base_configs = get_industry_adaptive_configs(strategy_type)
    all_configs.extend(base_configs)
    
    # Add industry-adapted configs
    for sector in key_sectors:
        sector_configs = get_industry_adaptive_configs(strategy_type, sector)
        all_configs.extend(sector_configs)
    
    return all_configs


def validate_industry_config_space(strategy_type: str, max_configs: int = 20) -> bool:
    """
    Validate that industry-aware config space is controlled.
    
    Args:
        strategy_type: Strategy to validate
        max_configs: Maximum allowed configs
    
    Returns:
        True if config space is reasonable
    """
    configs = get_multi_industry_configs(strategy_type)
    
    if len(configs) > max_configs:
        return False
    
    # Check for config diversity
    config_names = [c.get("config_name", "unknown") for c in configs]
    unique_names = set(config_names)
    
    if len(unique_names) < len(configs) * 0.7:  # At least 70% unique
        return False
    
    return True


def get_industry_config_summary(strategy_type: str) -> dict[str, Any]:
    """
    Get summary of industry-aware config space for monitoring.
    """
    all_configs = get_multi_industry_configs(strategy_type)
    
    # Count by sector
    sector_counts = {}
    base_count = 0
    
    for config in all_configs:
        if config.get("sector_adapted"):
            sector = config.get("target_sector", "unknown")
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
        else:
            base_count += 1
    
    return {
        "strategy_type": strategy_type,
        "total_configs": len(all_configs),
        "base_configs": base_count,
        "sector_adapted": len(all_configs) - base_count,
        "sectors": sector_counts,
        "config_names": [c.get("config_name", "unknown") for c in all_configs],
    }


# Global industry-aware configs for backward compatibility
INDUSTRY_ADAPTIVE_CONFIGS_GLOBAL = {}

for strategy in ["ownership_vacuum", "realness_repricer"]:
    INDUSTRY_ADAPTIVE_CONFIGS_GLOBAL[strategy] = get_multi_industry_configs(strategy)
