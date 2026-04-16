"""
Industry filtering for adaptive discovery pipeline.
"""

from typing import Dict, List
from app.discovery.types import FeatureRow


def filter_by_industry(features: Dict[str, FeatureRow], target_sectors: List[str]) -> Dict[str, FeatureRow]:
    """Filter universe to specific industries."""
    # Fallback: if no sector data, return all features
    has_sectors = any(fr.sector is not None for fr in features.values())
    if not has_sectors:
        return features
    
    targets = {str(s).strip().lower() for s in target_sectors if str(s).strip()}
    out: Dict[str, FeatureRow] = {}
    for sym, fr in features.items():
        sector = str(fr.sector).strip().lower() if fr.sector is not None else ""
        if sector in targets:
            out[sym] = fr
    return out


def get_sectors_for_env(env_bucket: tuple[str, str, str, str, str, str, str]) -> List[str]:
    """Map environment to relevant sectors."""
    vol_regime, trend_regime, disp_regime, liq_regime, sector_regime, industry_disp = env_bucket
    
    # Environment → sector mapping (more inclusive)
    all_sectors = ["technology", "financials", "healthcare", "energy", "consumer", "industrial"]
    
    if vol_regime == "HI_VOL":
        if sector_regime == "technology":
            return ["technology", "healthcare", "consumer"]
        elif sector_regime == "financials":
            return ["financials", "industrial", "energy"]
        elif sector_regime == "consumer":
            return ["consumer", "technology", "healthcare"]
        else:
            return all_sectors
    else:  # LO_VOL
        if sector_regime == "energy":
            return ["energy", "industrial", "financials"]
        elif sector_regime == "consumer":
            return ["consumer", "healthcare", "technology"]
        else:
            return all_sectors


def get_industry_universe(features: Dict[str, FeatureRow], env_bucket: tuple[str, str, str, str, str, str]) -> Dict[str, FeatureRow]:
    """Build industry-specific universe for environment."""
    target_sectors = get_sectors_for_env(env_bucket)
    return filter_by_industry(features, target_sectors)
