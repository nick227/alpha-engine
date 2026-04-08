from __future__ import annotations

from app.core.types import StrategyConfig
from app.strategies.baseline_momentum import BaselineMomentumStrategy
from app.strategies.technical.bollinger_reversion import BollingerReversionStrategy
from app.strategies.technical.rsi_reversion import RSIMeanReversionStrategy
from app.strategies.technical.vwap_reclaim import VWAPReclaimStrategy
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
    }
    cls = mapping.get(config.strategy_type)
    if cls is None:
        return None
    return cls(config)

