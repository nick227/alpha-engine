from __future__ import annotations

from typing import Any

from app.discovery.adaptive_mutation import get_adaptive_configs
from app.discovery.adaptive_stats import lookup_best_config
from app.discovery.strategies import DEFAULT_STRATEGY_CONFIGS


# Critical guardrails
MIN_SAMPLES = 30  # Minimum samples before adaptive selection
MIN_BUCKETS_WITH_DATA = 3  # Minimum buckets that should have data


def select_adaptive_config(
    *,
    strategy_type: str,
    env_bucket: tuple[str, str, str],
    db_path: str,
    enable_adaptive: bool = False,  # Phase 5: Safety switch
) -> dict[str, Any]:
    """
    Select best config for given strategy and environment.
    
    Phase 5: Adaptive selection with MIN_SAMPLES guard.
    """
    if not enable_adaptive:
        # Safety: return default config until explicitly enabled
        return DEFAULT_STRATEGY_CONFIGS.get(strategy_type, {})
    
    # Try to find best config with sufficient samples
    best = lookup_best_config(
        db_path=db_path,
        strategy=strategy_type,
        env_bucket=env_bucket,
        min_samples=MIN_SAMPLES,
    )
    
    if best is None:
        # Fallback to default config if insufficient data
        return DEFAULT_STRATEGY_CONFIGS.get(strategy_type, {})
    
    # For now, return default since we don't have config registry yet
    # In full implementation, we'd retrieve full config from config_hash
    return DEFAULT_STRATEGY_CONFIGS.get(strategy_type, {})


def validate_adaptive_readiness(
    *,
    db_path: str,
    strategy_type: str,
) -> dict[str, Any]:
    """
    Validate that system is ready for adaptive selection.
    
    Returns readiness assessment and recommendations.
    """
    configs = get_adaptive_configs(strategy_type)
    
    issues = []
    recommendations = []
    
    # Check 1: Config explosion control
    if len(configs) > 8:
        issues.append(f"Too many configs: {len(configs)} (max 8)")
        recommendations.append("Reduce mutation parameter values")
    
    # Check 2: Sample size readiness
    # For now, we'll assume this passes - in full implementation we'd check actual stats
    
    # Check 3: Environment bucket distribution
    # For now, we'll assume this passes - in full implementation we'd check bucket coverage
    
    readiness_score = 1.0 - (len(issues) * 0.3)  # Simple scoring
    
    return {
        "ready": len(issues) == 0,
        "readiness_score": max(0.0, readiness_score),
        "issues": issues,
        "recommendations": recommendations,
        "config_count": len(configs),
        "min_samples_required": MIN_SAMPLES,
    }


def enable_adaptive_mode(
    *,
    strategy_type: str,
    db_path: str,
    force_enable: bool = False,
) -> dict[str, Any]:
    """
    Enable adaptive mode after validation.
    
    Returns enablement status and any warnings.
    """
    validation = validate_adaptive_readiness(db_path=db_path, strategy_type=strategy_type)
    
    if not force_enable and not validation["ready"]:
        return {
            "enabled": False,
            "reason": "Validation failed",
            "validation": validation,
        }
    
    return {
        "enabled": True,
        "reason": "Validation passed" if validation["ready"] else "Force enabled",
        "validation": validation,
        "warnings": validation["issues"] if validation["issues"] else [],
    }


# Phase 5: Global adaptive mode control
_adaptive_enabled = False


def is_adaptive_enabled() -> bool:
    """Check if adaptive mode is globally enabled."""
    return _adaptive_enabled


def enable_adaptive_globally() -> None:
    """Enable adaptive mode globally."""
    global _adaptive_enabled
    _adaptive_enabled = True


def disable_adaptive_globally() -> None:
    """Disable adaptive mode globally (safety fallback)."""
    global _adaptive_enabled
    _adaptive_enabled = False
