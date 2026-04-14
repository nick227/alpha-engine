from __future__ import annotations

from copy import deepcopy
from typing import Any, List
from uuid import uuid4

from app.discovery.strategies import DEFAULT_STRATEGY_CONFIGS


class AdaptiveMutationEngine:
    """
    Controlled mutation engine for adaptive strategies.
    
    Phase 4: Small, interpretable mutation sets (5-10 configs max).
    """
    
    def __init__(self, max_configs_per_strategy: int = 8):
        self.max_configs_per_strategy = max_configs_per_strategy
        self.ADAPTIVE_CONFIGS = {
            "silent_compounder": [
                # Core config (baseline)
                {
                    "config_name": "default",
                    "vol_band": 0.020,
                    "threshold": 0.50,
                    "min_vol": 0.010,
                    "max_vol": 0.040,
                },
                # Tight trend following (for calm, trending markets)
                {
                    "config_name": "tight_trend",
                    "vol_band": 0.015,
                    "threshold": 0.60,
                    "min_vol": 0.006,
                    "max_vol": 0.030,
                },
                # Loose dispersion (for high dispersion environments)
                {
                    "config_name": "loose_disp",
                    "vol_band": 0.030,
                    "threshold": 0.45,
                    "min_vol": 0.012,
                    "max_vol": 0.050,
                },
                # Defensive (for illiquid/stress environments)
                {
                    "config_name": "defensive",
                    "vol_band": 0.018,
                    "threshold": 0.65,
                    "min_vol": 0.015,
                    "max_vol": 0.035,
                },
                # Aggressive rotation (for high volatility/trend)
                {
                    "config_name": "aggressive_rotation",
                    "vol_band": 0.035,
                    "threshold": 0.40,
                    "min_vol": 0.008,
                    "max_vol": 0.060,
                },
            ],
        }
    
    def generate_configs(self, strategy_type: str) -> List[dict[str, Any]]:
        """
        Generate small controlled mutation set for strategy.
        
        Returns list of config dictionaries with metadata.
        """
        if strategy_type not in DEFAULT_STRATEGY_CONFIGS:
            return []
        
        base_config = DEFAULT_STRATEGY_CONFIGS[strategy_type]
        mutation_steps = self.mutation_steps.get(strategy_type, {})
        
        configs = []
        
        # Always include default config
        default_config = deepcopy(base_config)
        default_config["config_id"] = "default"
        default_config["config_name"] = f"{strategy_type}_default"
        configs.append(default_config)
        
        # Generate mutations by perturbing one parameter at a time
        param_count = 0
        for param, values in mutation_steps.items():
            if param not in base_config or param_count >= self.max_configs_per_strategy - 1:
                continue
                
            base_value = base_config[param]
            
            # Generate 2-3 variations per parameter
            for value in values[:2]:  # Limit to 2 per param to control explosion
                if value == base_value:
                    continue
                    
                mutated_config = deepcopy(base_config)
                mutated_config[param] = value
                mutated_config["config_id"] = str(uuid4())[:8]
                mutated_config["config_name"] = f"{strategy_type}_{param}_{value}"
                mutated_config["mutation_type"] = f"{param}_perturbation"
                mutated_config["parent_config"] = "default"
                
                configs.append(mutated_config)
                param_count += 1
                
                if len(configs) >= self.max_configs_per_strategy:
                    return configs
        
        return configs[:self.max_configs_per_strategy]
    
    def get_config_summary(self, strategy_type: str) -> dict[str, Any]:
        """Get summary of available configs for monitoring."""
        configs = self.generate_configs(strategy_type)
        
        return {
            "strategy_type": strategy_type,
            "total_configs": len(configs),
            "default_config": configs[0] if configs else None,
            "mutation_configs": configs[1:],
            "mutation_params": list(self.mutation_steps.get(strategy_type, {}).keys()),
        }


# Global adaptive configs for direct access (upgraded for better separation)
ADAPTIVE_CONFIGS = {
    "silent_compounder": [
        # Core config (baseline)
        {
            "config_name": "default",
            "vol_band": 0.020,
            "threshold": 0.50,
            "min_vol": 0.010,
            "max_vol": 0.040,
        },
        # Tight trend following (for calm, trending markets)
        {
            "config_name": "tight_trend",
            "vol_band": 0.015,
            "threshold": 0.60,
            "min_vol": 0.006,
            "max_vol": 0.030,
        },
        # Loose dispersion (for high dispersion environments)
        {
            "config_name": "loose_disp",
            "vol_band": 0.030,
            "threshold": 0.45,
            "min_vol": 0.012,
            "max_vol": 0.050,
        },
        # Defensive (for illiquid/stress environments)
        {
            "config_name": "defensive",
            "vol_band": 0.018,
            "threshold": 0.65,
            "min_vol": 0.015,
            "max_vol": 0.035,
        },
        # Aggressive rotation (for high volatility/trend)
        {
            "config_name": "aggressive_rotation",
            "vol_band": 0.035,
            "threshold": 0.40,
            "min_vol": 0.008,
            "max_vol": 0.060,
        },
    ],
}

# Global mutation engine instance
_mutation_engine = AdaptiveMutationEngine()


def get_adaptive_configs(strategy_type: str) -> List[dict[str, Any]]:
    """Get adaptive configs for strategy type."""
    return _mutation_engine.generate_configs(strategy_type)


def get_mutation_summary(strategy_type: str) -> dict[str, Any]:
    """Get mutation summary for strategy type."""
    return _mutation_engine.get_config_summary(strategy_type)


def validate_config_space(strategy_type: str) -> bool:
    """
    Validate that config space is reasonable (not too large).
    
    Returns True if config space is controlled.
    """
    configs = get_adaptive_configs(strategy_type)
    
    # Guardrails
    if len(configs) > 10:
        return False  # Too many configs
    
    # Check that configs are interpretable
    for config in configs:
        if len(config) > 8:  # Too many parameters
            return False
    
    return True
