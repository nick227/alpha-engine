"""
Volatility Breakout Strategy

Core alpha source: Identifies volatility expansion patterns for trend-following trades.
Only activates in expansion regimes with established trends.
"""

from __future__ import annotations
from typing import Dict, Any, Optional, Tuple
import numpy as np

from app.discovery.scoring import clamp, clamp01
from app.discovery.types import DiscoveryCandidate, FeatureRow
from app.core.regime_v3 import RegimeClassification, TrendRegime, VolatilityRegime


def volatility_breakout(
    fr: FeatureRow,
    config: Dict[str, Any] | None = None,
    context: Dict[str, Any] | None = None,
) -> Tuple[float | None, str, Dict[str, Any]]:
    """
    Volatility Breakout: Detects volatility expansion for trend-following entries.
    
    Core logic:
    1. Volatility must be in expansion (ATR > p80)
    2. Price must be above moving averages (uptrend) or below (downtrend)
    3. Volume confirmation (optional but recommended)
    4. Price momentum confirmation
    
    Expected behavior:
    - 120-160 signals/year (vs 223 for unfiltered momentum)
    - Higher win rate due to regime filtering
    - Improved Sharpe from quality filtering
    """
    
    if config is None:
        config = {
            "min_atr_percentile": 0.80,  # Must be in top 20% of volatility
            "min_price_vs_ma50": 0.02,   # 2% above/below MA50
            "min_ma50_vs_ma200": 0.05,  # 5% separation between MAs
            "volume_confirmation": True,
            "min_volume_zscore": 1.5,
            "momentum_confirmation": True,
            "min_return_5d": 0.03,       # 3% 5-day momentum
            "score_threshold": 0.60,
        }
    
    # Check regime context if provided
    if context and 'regime' in context:
        regime = context['regime']
        if not isinstance(regime, RegimeClassification):
            return None, "invalid regime context", {}
        
        # HARD GATE: Only allow in expansion regimes
        if regime.volatility_regime != VolatilityRegime.EXPANSION:
            return None, f"volatility not in expansion (current: {regime.volatility_regime.value})", {}
        
        # HARD GATE: Must have trend direction
        if regime.trend_regime == TrendRegime.CHOP:
            return None, f"no established trend (current: {regime.trend_regime.value})", {}
    
    # Required features
    required_features = ['close', 'ma50', 'ma200', 'atr', 'volume_zscore_20d', 'return_5d']
    for feature in required_features:
        if getattr(fr, feature, None) is None:
            return None, f"missing {feature}", {}
    
    # GATE 1: Volatility expansion
    # In production, this would compare to historical ATR distribution
    # For now, use absolute threshold
    atr_ratio = fr.atr / fr.close
    min_atr_ratio = config.get("min_atr_percentile", 0.80) * 0.03  # Rough approximation
    
    if atr_ratio < min_atr_ratio:
        return None, f"volatility not expanded (ATR ratio: {atr_ratio:.4f})", {}
    
    # GATE 2: Trend establishment
    price_vs_ma50 = (fr.close - fr.ma50) / fr.ma50
    ma50_vs_ma200 = (fr.ma50 - fr.ma200) / fr.ma200
    
    # Determine trend direction
    if price_vs_ma50 > 0 and ma50_vs_ma200 > 0:
        trend_direction = "bull"
        trend_strength = min(price_vs_ma50, ma50_vs_ma200)
    elif price_vs_ma50 < 0 and ma50_vs_ma200 < 0:
        trend_direction = "bear"
        trend_strength = min(abs(price_vs_ma50), abs(ma50_vs_ma200))
    else:
        return None, "no clear trend direction", {}
    
    min_trend_strength = config.get("min_price_vs_ma50", 0.02)
    if trend_strength < min_trend_strength:
        return None, f"trend too weak (strength: {trend_strength:.3f})", {}
    
    # GATE 3: Volume confirmation (optional)
    if config.get("volume_confirmation", True):
        min_volume_zscore = config.get("min_volume_zscore", 1.5)
        if fr.volume_zscore_20d < min_volume_zscore:
            return None, f"insufficient volume (zscore: {fr.volume_zscore_20d:.2f})", {}
    
    # GATE 4: Momentum confirmation (optional)
    if config.get("momentum_confirmation", True):
        min_return_5d = config.get("min_return_5d", 0.03)
        if trend_direction == "bull" and fr.return_5d < min_return_5d:
            return None, f"insufficient upside momentum (5d return: {fr.return_5d:.2%})", {}
        elif trend_direction == "bear" and fr.return_5d > -min_return_5d:
            return None, f"insufficient downside momentum (5d return: {fr.return_5d:.2%})", {}
    
    # SCORING: Multi-component quality assessment
    # 1. Volatility expansion score (0-1)
    vol_score = clamp01((atr_ratio - min_atr_ratio) / (min_atr_ratio * 2))
    
    # 2. Trend strength score (0-1)
    trend_score = clamp01(trend_strength / min_trend_strength)
    
    # 3. Volume score (0-1)
    volume_score = clamp01(fr.volume_zscore_20d / 3.0)  # Normalize to zscore=3
    
    # 4. Momentum score (0-1)
    momentum_score = clamp01(abs(fr.return_5d) / min_return_5d)
    
    # 5. Regime alignment bonus (if regime provided)
    regime_bonus = 0.0
    if context and 'regime' in context:
        regime = context['regime']
        if isinstance(regime, RegimeClassification):
            if regime.volatility_regime == VolatilityRegime.EXPANSION:
                regime_bonus += 0.1
            if regime.trend_regime in [TrendRegime.BULL, TrendRegime.BEAR]:
                regime_bonus += 0.1
    
    # Final score (weighted average)
    raw_score = (
        0.30 * vol_score +
        0.25 * trend_score +
        0.20 * volume_score +
        0.15 * momentum_score +
        0.10 * regime_bonus
    )
    
    # Apply threshold
    score_threshold = config.get("score_threshold", 0.60)
    if raw_score < score_threshold:
        return None, f"score below threshold ({raw_score:.3f} < {score_threshold})", {}
    
    # Build metadata
    metadata = {
        "strategy_type": "volatility_breakout",
        "trend_direction": trend_direction,
        "trend_strength": trend_strength,
        "atr_ratio": atr_ratio,
        "volume_zscore": fr.volume_zscore_20d,
        "momentum_5d": fr.return_5d,
        "score_components": {
            "volatility": vol_score,
            "trend": trend_score,
            "volume": volume_score,
            "momentum": momentum_score,
            "regime_bonus": regime_bonus
        },
        "regime_aligned": regime_bonus > 0.1 if context and 'regime' in context else False
    }
    
    reason = f"volatility breakout ({trend_direction} trend, ATR expansion: {atr_ratio:.3f})"
    
    return raw_score, reason, metadata


def create_volatility_breakout_candidates(
    features: Dict[str, FeatureRow],
    regime_context: Optional[Dict[str, RegimeClassification]] = None,
    config: Optional[Dict[str, Any]] = None
) -> list[DiscoveryCandidate]:
    """
    Create volatility breakout candidates with regime gating.
    
    Args:
        features: Feature rows by symbol
        regime_context: Regime classifications by symbol
        config: Strategy configuration
    
    Returns:
        List of discovery candidates
    """
    
    candidates = []
    
    for symbol, fr in features.items():
        # Skip low-quality symbols
        if fr.close is None or fr.close < 10.0:
            continue
        if fr.dollar_volume is None or fr.dollar_volume < 5_000_000:
            continue
        if symbol.startswith('^'):  # Skip indices
            continue
        
        # Get regime context for this symbol
        context = {}
        if regime_context and symbol in regime_context:
            context['regime'] = regime_context[symbol]
        
        # Generate signal
        score, reason, metadata = volatility_breakout(fr, config=config, context=context)
        
        if score is not None:
            candidate = DiscoveryCandidate(
                symbol=symbol,
                strategy_type="volatility_breakout",
                score=score,
                reason=reason,
                metadata={
                    **metadata,
                    "close": fr.close,
                    "dollar_volume": fr.dollar_volume,
                }
            )
            candidates.append(candidate)
    
    # Sort by score
    candidates.sort(key=lambda c: c.score, reverse=True)
    
    return candidates


# Strategy registration
def register_strategy():
    """Register volatility breakout strategy with the strategy registry"""
    from app.discovery.strategies import STRATEGIES, THRESHOLDS
    
    STRATEGIES["volatility_breakout"] = volatility_breakout
    THRESHOLDS["volatility_breakout"] = 0.60


# Auto-register on import
register_strategy()
