from __future__ import annotations

from app.core.types import StrategyConfig
from app.strategies.baseline_momentum import BaselineMomentumStrategy
from app.strategies.technical.bollinger_reversion import BollingerReversionStrategy
from app.strategies.technical.rsi_reversion import RSIMeanReversionStrategy
from app.strategies.technical.vwap_reclaim import VWAPReclaimStrategy
from app.strategies.technical.vol_crush_reversion import VolCrushMeanReversionStrategy
from app.strategies.technical.vol_expansion_continuation import VolExpansionContinuationStrategy
from app.strategies.technical.range_breakout_continuation import RangeBreakoutContinuationStrategy
from app.strategies.cross_asset.relative_strength import RelativeStrengthVsBenchmarkStrategy
from app.ml.predict import MLPredictor
from app.strategies.text_mra import TextMRAStrategy


def build_strategy_instance(config: StrategyConfig):
    """
    Factory for StrategyBase implementations used by the runtime.

    Unknown strategy types return None so callers can safely skip.
    """
    mapping = {
        "text_mra": TextMRAStrategy,
        "baseline_momentum": BaselineMomentumStrategy,
        "technical_vwap_reclaim": VWAPReclaimStrategy,
        "technical_rsi_reversion": RSIMeanReversionStrategy,
        "technical_bollinger_reversion": BollingerReversionStrategy,
        "technical_vol_expansion_continuation": VolExpansionContinuationStrategy,
        "technical_vol_crush_reversion": VolCrushMeanReversionStrategy,
        "technical_range_breakout_continuation": RangeBreakoutContinuationStrategy,
        "cross_asset_relative_strength": RelativeStrengthVsBenchmarkStrategy,
        "ml_factor": MLPredictor,
    }
    cls = mapping.get(config.strategy_type)
    if cls is None:
        return None
    return cls(config)
