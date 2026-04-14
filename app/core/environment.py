from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from app.core.regime import build_regime_snapshot, RegimeSnapshot


@dataclass(frozen=True)
class EnvironmentSnapshot:
    vol_regime: str
    trend_regime: str
    vix_bucket: str
    vix_value: float | None = None
    raw_features: dict[str, Any] | None = None


def bucket_vix(vix: float | None) -> str:
    """Bucket VIX into LOW/MEDIUM/HIGH based on typical market ranges."""
    if vix is None:
        return "UNKNOWN"
    
    if vix < 16:
        return "LOW"
    elif vix < 25:
        return "MEDIUM"
    else:
        return "HIGH"


def build_env_snapshot(
    *,
    db_path: str,
    as_of: str | date,
    vix_value: float | None = None,
) -> EnvironmentSnapshot:
    """
    Build minimal environment snapshot from existing regime data.
    
    Phase 1: Simple 3-variable environment (vol_regime, trend_regime, vix_bucket)
    """
    # For now, we'll create a simple regime snapshot
    # In Phase 2+ this can pull actual market data from DB
    regime = RegimeSnapshot(
        volatility_regime="NORMAL",  # placeholder
        trend_regime="UNKNOWN",     # placeholder
        volatility_value=0.02,
        trend_value=None,
        sentiment_weight=0.5,
        quant_weight=0.5,
    )
    
    env_bucket = (
        regime.volatility_regime,
        regime.trend_regime,
        bucket_vix(vix_value)
    )
    
    raw_features = {
        "regime_volatility": regime.volatility_regime,
        "regime_trend": regime.trend_regime,
        "vix_value": vix_value,
        "vix_bucket": bucket_vix(vix_value),
        "env_bucket": env_bucket,
    }
    
    return EnvironmentSnapshot(
        vol_regime=regime.volatility_regime,
        trend_regime=regime.trend_regime,
        vix_bucket=bucket_vix(vix_value),
        vix_value=vix_value,
        raw_features=raw_features,
    )


def bucket_env(env: EnvironmentSnapshot) -> tuple[str, str, str]:
    """Convert environment to discrete bucket key."""
    return (env.vol_regime, env.trend_regime, env.vix_bucket)


# =========================
# ENVIRONMENT V2 INTEGRATION
# =========================

from app.core.environment_v2 import EnvironmentSnapshotV2


def build_env_snapshot_v2(db_path: str, as_of: str, vix_value: float | None = None) -> EnvironmentSnapshotV2:
    """
    Build enhanced environment snapshot with stronger signals.
    
    Wrapper around existing build_env_snapshot for safe rollout.
    """
    # Get base environment
    base_env = build_env_snapshot(db_path=db_path, as_of=as_of, vix_value=vix_value)
    
    return EnvironmentSnapshotV2(
        market_vol_pct=base_env.vix_value / 40.0 if base_env.vix_value else 0.5,  # Normalize VIX
        trend_strength=map_trend_to_strength(base_env.trend_regime),
        cross_sectional_disp=compute_dispersion(db_path, as_of),
        liquidity_regime=compute_liquidity(db_path, as_of),
    )


def map_trend_to_strength(trend_regime: str) -> float:
    """Convert trend regime to continuous strength score."""
    if trend_regime == "UP":
        return 0.5
    elif trend_regime == "DOWN":
        return -0.5
    return 0.0


def compute_dispersion(db_path: str, as_of: str) -> float:
    """
    Compute cross-sectional dispersion for given date.
    
    Measures how spread out stock returns are - key for adaptive pressure.
    """
    # Simplified implementation - in production use actual data
    import random
    
    # Simulate dispersion based on volatility regime
    # Higher volatility = higher dispersion
    base_dispersion = 0.3 + random.uniform(0, 0.4)
    
    return min(1.0, base_dispersion)


def compute_liquidity(db_path: str, as_of: str) -> float:
    """
    Compute market-level liquidity regime.
    
    Normalized liquidity score affecting execution quality.
    """
    # Simplified implementation - in production use actual market liquidity data
    import random
    
    # Simulate liquidity - tends to be stable with occasional stress
    base_liquidity = 0.6 + random.uniform(-0.2, 0.3)
    
    return max(0.0, min(1.0, base_liquidity))


# =========================
# ENVIRONMENT V3 INTEGRATION
# =========================

from app.core.environment_v3 import EnvironmentSnapshotV3, get_sector_leadership, compute_industry_dispersion, get_size_regime


def build_env_snapshot_v3(db_path: str, as_of: str, vix_value: float | None = None) -> EnvironmentSnapshotV3:
    """
    Build multi-dimensional environment snapshot with industry signals.
    
    Extends v2 with sector leadership, industry dispersion, and size regime.
    """
    # Get base v2 environment
    from app.core.environment import build_env_snapshot_v2
    base_env = build_env_snapshot_v2(db_path=db_path, as_of=as_of, vix_value=vix_value)
    
    # Generate industry signals (simplified for now)
    sector_returns = generate_mock_sector_returns()
    large_cap_returns = generate_mock_returns(20, 0.01, 0.02)
    small_cap_returns = generate_mock_returns(20, 0.015, 0.025)
    
    return EnvironmentSnapshotV3(
        market_vol_pct=base_env.market_vol_pct,
        trend_strength=base_env.trend_strength,
        cross_sectional_disp=base_env.cross_sectional_disp,
        liquidity_regime=base_env.liquidity_regime,
        
        # NEW: Industry dimensions
        sector_regime=get_sector_leadership(sector_returns),
        industry_dispersion=compute_industry_dispersion(sector_returns),
        size_regime=get_size_regime(large_cap_returns, small_cap_returns),
    )


def generate_mock_sector_returns() -> dict[str, float]:
    """Generate mock sector returns for testing."""
    import random
    
    sectors = ["technology", "financials", "healthcare", "energy", "consumer", "industrial"]
    returns = {}
    
    # Create some sector leadership dynamics
    leading_sector = random.choice(sectors)
    
    for sector in sectors:
        if sector == leading_sector:
            returns[sector] = random.uniform(0.02, 0.04)  # Outperforming
        else:
            returns[sector] = random.uniform(-0.01, 0.02)  # Underperforming
    
    return returns


def generate_mock_returns(count: int, mean: float, std: float) -> list[float]:
    """Generate mock returns for size regime calculation."""
    import random
    
    returns = []
    for _ in range(count):
        returns.append(random.gauss(mean, std))
    
    return returns
