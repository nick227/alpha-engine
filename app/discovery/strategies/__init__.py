from __future__ import annotations

"""
Discovery strategy implementations + registry.

This package replaces the previous `app.discovery.strategies` module to avoid
module/package name conflicts and circular registration imports.
"""

from .registry import DEFAULT_STRATEGY_CONFIGS, STRATEGIES, THRESHOLDS, score_candidates, to_repo_rows
from .volatility_breakout import create_volatility_breakout_candidates, volatility_breakout
from .temporal_correlation_strategy import create_temporal_correlation_strategy, TemporalCorrelationStrategy

__all__ = [
    "DEFAULT_STRATEGY_CONFIGS",
    "STRATEGIES",
    "THRESHOLDS",
    "score_candidates",
    "to_repo_rows",
    "volatility_breakout",
    "create_volatility_breakout_candidates",
    "create_temporal_correlation_strategy",
    "TemporalCorrelationStrategy",
]

