"""
Environment v3: Multi-dimensional adaptive with industry/company type dimensions.

Adds sector leadership, industry dispersion, and size regime to create
stronger selection pressure for adaptive config selection.
"""

from dataclasses import dataclass


@dataclass
class EnvironmentSnapshotV3:
    market_vol_pct: float          # Realized vol percentile, 0-1
    trend_strength: float          # Signed trend score, -1 to 1  
    cross_sectional_disp: float   # Stock return dispersion, 0-1
    liquidity_regime: float        # Normalized liquidity score, 0-1
    
    # NEW: Industry dimensions
    sector_regime: str             # Which sector is leading/lagging
    industry_dispersion: float     # Cross-industry dispersion, 0-1
    size_regime: str              # Large vs small cap leadership


def bucket_env_v3(env: EnvironmentSnapshotV3) -> tuple[str, str, str, str, str, str]:
    """
    Bucket environment into 64 manageable states with industry dimensions.
    
    Creates meaningful selection pressure across market + industry conditions.
    """
    return (
        "HI_VOL" if env.market_vol_pct >= 0.7 else "LO_VOL",
        "TREND" if abs(env.trend_strength) >= 0.2 else "CHOP", 
        "HI_DISP" if env.cross_sectional_disp >= 0.5 else "LO_DISP",
        "HI_LIQ" if env.liquidity_regime >= 0.5 else "LO_LIQ",
        # Industry dimensions
        "TECH_LEAD" if env.sector_regime == "technology" else "FIN_LEAD" if env.sector_regime == "financials" else "BALANCED",
        "HI_INDUSTRY_DISP" if env.industry_dispersion >= 0.5 else "LO_INDUSTRY_DISP",
    )


def get_sector_leadership(sector_returns: dict[str, float]) -> str:
    """
    Determine which sector is leading/lagging based on relative performance.
    
    Args:
        sector_returns: Dict of sector -> return for current period
    
    Returns:
        Sector name with strongest relative performance
    """
    if not sector_returns:
        return "balanced"
    
    # Find best performing sector
    best_sector = max(sector_returns.items(), key=lambda x: x[1])
    worst_sector = min(sector_returns.items(), key=lambda x: x[1])
    
    # Only declare leadership if spread is meaningful
    spread = best_sector[1] - worst_sector[1]
    if spread > 0.02:  # 2% spread threshold
        return best_sector[0]
    else:
        return "balanced"


def compute_industry_dispersion(sector_returns: dict[str, float]) -> float:
    """
    Compute cross-industry dispersion.
    
    Higher dispersion = more sector rotation opportunities
    Lower dispersion = unified market movement
    """
    if not sector_returns or len(sector_returns) < 2:
        return 0.5  # Default medium dispersion
    
    returns = list(sector_returns.values())
    mean_return = sum(returns) / len(returns)
    
    # Calculate dispersion as standard deviation
    variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
    dispersion = (variance ** 0.5) / 0.05  # Normalize by 5% typical dispersion
    
    return min(1.0, max(0.0, dispersion))


def get_size_regime(large_cap_returns: list[float], small_cap_returns: list[float]) -> str:
    """
    Determine size regime based on relative performance.
    
    Args:
        large_cap_returns: Returns of large cap stocks
        small_cap_returns: Returns of small cap stocks
    
    Returns:
        "large_cap_lead", "small_cap_lead", or "balanced"
    """
    if not large_cap_returns or not small_cap_returns:
        return "balanced"
    
    large_avg = sum(large_cap_returns) / len(large_cap_returns)
    small_avg = sum(small_cap_returns) / len(small_cap_returns)
    
    spread = large_avg - small_avg
    
    if spread > 0.01:  # 1% threshold
        return "large_cap_lead"
    elif spread < -0.01:
        return "small_cap_lead"
    else:
        return "balanced"


# Industry-specific config templates
INDUSTRY_CONFIG_TEMPLATES = {
    "technology": {
        "ownership_vacuum": {
            "spike_threshold": 2.2,      # Higher threshold for volatile tech
            "liquidity_threshold": 0.35,  # More selective on liquidity
            "volume_multiplier": 1.3,     # Higher volume requirements
        },
        "realness_repricer": {
            "depressed_weight": 0.5,     # Less weight to price depression
            "drawdown_weight": 0.5,      # More weight to trend
            "power": 2.0,                # Less aggressive convexity
        },
    },
    "financials": {
        "ownership_vacuum": {
            "spike_threshold": 1.8,      # Lower threshold for stable financials
            "liquidity_threshold": 0.25,  # Standard liquidity filter
            "volume_multiplier": 1.1,     # Standard volume requirements
        },
        "realness_repricer": {
            "depressed_weight": 0.7,     # More weight to price depression
            "drawdown_weight": 0.3,      # Less weight to trend
            "power": 2.5,                # More aggressive convexity
        },
    },
    "healthcare": {
        "ownership_vacuum": {
            "spike_threshold": 1.5,      # Lower threshold for stable healthcare
            "liquidity_threshold": 0.20,  # More forgiving on liquidity
            "volume_multiplier": 1.0,     # Standard volume
        },
        "realness_repricer": {
            "depressed_weight": 0.6,      # Balanced weights
            "drawdown_weight": 0.4,      # Balanced trend
            "power": 2.2,                # Moderate convexity
        },
    },
    "energy": {
        "ownership_vacuum": {
            "spike_threshold": 2.0,      # Standard threshold for energy
            "liquidity_threshold": 0.30,  # Standard liquidity
            "volume_multiplier": 1.2,     # Higher volume for commodity volatility
        },
        "realness_repricer": {
            "depressed_weight": 0.8,     # High weight to price depression
            "drawdown_weight": 0.2,      # Low weight to trend
            "power": 2.8,                # High convexity for mean reversion
        },
    },
}


def get_industry_aware_config(strategy_name: str, sector: str, base_config: dict[str, any]) -> dict[str, any]:
    """
    Adapt base config for specific industry characteristics.
    
    Args:
        strategy_name: Strategy type
        sector: Industry sector
        base_config: Base configuration to adapt
    
    Returns:
        Industry-adapted configuration
    """
    # Get industry template if available
    templates = INDUSTRY_CONFIG_TEMPLATES.get(sector, {})
    strategy_template = templates.get(strategy_name, {})
    
    # Merge base config with industry adaptations
    adapted_config = base_config.copy()
    
    for key, value in strategy_template.items():
        adapted_config[key] = value
    
    # Add industry metadata
    adapted_config["config_name"] = f"{base_config.get('config_name', 'default')}_{sector}"
    adapted_config["sector_adapted"] = True
    adapted_config["target_sector"] = sector
    
    return adapted_config
