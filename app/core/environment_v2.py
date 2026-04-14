"""
Environment v2: Stronger, continuous signals for better adaptive selection.

Replaces coarse labels with continuous measures that create meaningful
selection pressure between configs.
"""

from dataclasses import dataclass


@dataclass
class EnvironmentSnapshotV2:
    market_vol_pct: float      # Realized vol percentile, 0-1
    trend_strength: float      # Signed trend score, -1 to 1  
    cross_sectional_disp: float # Stock return dispersion, 0-1
    liquidity_regime: float    # Normalized liquidity score, 0-1


def bucket_env_v2(env: EnvironmentSnapshotV2) -> tuple[str, str, str, str]:
    """
    Bucket environment into 16 manageable states.
    
    Uses meaningful thresholds that should create config separation.
    """
    return (
        "HI_VOL" if env.market_vol_pct >= 0.7 else "LO_VOL",
        "TREND" if abs(env.trend_strength) >= 0.2 else "CHOP", 
        "HI_DISP" if env.cross_sectional_disp >= 0.5 else "LO_DISP",
        "HI_LIQ" if env.liquidity_regime >= 0.5 else "LO_LIQ",
    )
