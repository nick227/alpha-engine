# Alpha Engine ML Services Documentation

## Overview

This document provides comprehensive coverage of all machine learning services within the Alpha Engine system. It covers the complete ML pipeline from feature engineering through prediction generation, dimensional learning, and adaptive model management.

## Table of Contents

1. [ML Architecture Overview](#ml-architecture-overview)
2. [Feature Engineering Services](#feature-engineering-services)
3. [Discovery Strategy Services](#discovery-strategy-services)
4. [Consensus and Scoring Services](#consensus-and-scoring-services)
5. [Dimensional ML Services](#dimensional-ml-services)
6. [Regime Detection Services](#regime-detection-services)
7. [Adaptive Learning Services](#adaptive-learning-services)
8. [Model Management Services](#model-management-services)
9. [Performance Analytics Services](#performance-analytics-services)
10. [ML Configuration and Deployment](#ml-configuration-and-deployment)

---

## ML Architecture Overview

### ML Service Stack

```
                    Feature Engineering Layer
                                   |
                                   v
                    Discovery Strategy Layer
                                   |
                                   v
                    Consensus & Scoring Layer
                                   |
                                   v
                    Dimensional ML Layer
                                   |
                                   v
                    Adaptive Learning Layer
                                   |
                                   v
                    Performance Analytics Layer
```

### Service Dependencies

```python
# Core ML Service Dependencies
ML_SERVICES = {
    "feature_engine": "app.core.feature_engine",
    "discovery_strategies": "app.discovery.strategies",
    "consensus_models": "app.core.consensus_models",
    "dimensional_ml": "app.ml.lightweight_dimensional_ml",
    "dimensional_tagger": "app.ml.dimensional_tagger",
    "regime_detection": "app.core.regime_v3",
    "adaptive_learning": "app.ml.regime_aware_ml",
    "outcome_models": "app.core.outcome_models"
}
```

---

## Feature Engineering Services

### 1. Feature Engine (`app.core.feature_engine`)

#### Service Overview
The Feature Engine is responsible for transforming raw market data into predictive features used by all ML models.

#### Core Features Generated

##### Price-Based Features
```python
@dataclass
class PriceFeatures:
    # Returns
    return_1d: float      # 1-day return
    return_5d: float      # 5-day return
    return_20d: float     # 20-day return
    return_63d: float     # 63-day return (quarter)
    return_252d: float    # 252-day return (year)
    
    # Momentum
    momentum_5d: float    # 5-day momentum
    momentum_20d: float   # 20-day momentum
    momentum_63d: float   # 63-day momentum
    
    # Position
    price_percentile_252d: float  # Price percentile in 252-day range
    price_zscore_20d: float       # Price z-score vs 20-day average
```

##### Volatility Features
```python
@dataclass
class VolatilityFeatures:
    volatility_20d: float          # 20-day realized volatility
    volatility_63d: float          # 63-day realized volatility
    volatility_252d: float         # 252-day realized volatility
    
    # ATR-based
    atr_14d: float                 # 14-day Average True Range
    atr_ratio: float               # ATR as percentage of price
    
    # Volatility regime
    vol_regime: str               # LOW/MEDIUM/HIGH volatility
    vol_expansion: bool            # Volatility expansion signal
```

##### Volume Features
```python
@dataclass
class VolumeFeatures:
    volume_zscore_20d: float       # Volume z-score vs 20-day average
    dollar_volume: float           # Dollar volume
    avg_dollar_volume_20d: float   # Average daily dollar volume
    
    # Volume patterns
    volume_spike: bool             # Volume spike detection
    volume_trend: str              # INCREASING/DECREASING/STABLE
```

##### Technical Indicators
```python
@dataclass
class TechnicalFeatures:
    # Moving averages
    ma50: float                    # 50-day moving average
    ma200: float                   # 200-day moving average
    ma_cross: str                  # Golden/Death cross signals
    
    # Oscillators
    rsi_14d: float                 # 14-day RSI
    macd_signal: str               # MACD buy/sell signals
    bollinger_position: str        # Position relative to Bollinger Bands
    
    # Patterns
    support_resistance: float      # Support/resistance levels
    trend_strength: float          # Trend strength indicator
```

#### Feature Generation Pipeline

```python
class FeatureEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.feature_cache = {}
        
    def generate_features(self, symbol: str, market_data: List[MarketData]) -> FeatureRow:
        """
        Generate complete feature set for a symbol.
        
        Args:
            symbol: Stock symbol
            market_data: Historical market data
            
        Returns:
            FeatureRow with all calculated features
        """
        # 1. Calculate returns
        returns = self._calculate_returns(market_data)
        
        # 2. Calculate volatility metrics
        volatility = self._calculate_volatility(market_data, returns)
        
        # 3. Calculate volume metrics
        volume_metrics = self._calculate_volume_metrics(market_data)
        
        # 4. Calculate technical indicators
        technical = self._calculate_technical_indicators(market_data)
        
        # 5. Combine into FeatureRow
        return self._build_feature_row(symbol, returns, volatility, volume_metrics, technical)
    
    def _calculate_returns(self, data: List[MarketData]) -> Dict[str, float]:
        """Calculate return-based features."""
        prices = [d.close for d in data]
        
        returns = {}
        for period in [1, 5, 20, 63, 252]:
            if len(prices) > period:
                returns[f"return_{period}d"] = (prices[-1] - prices[-period-1]) / prices[-period-1]
        
        return returns
    
    def _calculate_volatility(self, data: List[MarketData], returns: Dict[str, float]) -> Dict[str, float]:
        """Calculate volatility-based features."""
        prices = [d.close for d in data]
        
        # Realized volatility
        log_returns = [np.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
        
        volatility = {}
        for period in [20, 63, 252]:
            if len(log_returns) >= period:
                vol = np.std(log_returns[-period:]) * np.sqrt(252)  # Annualized
                volatility[f"volatility_{period}d"] = vol
        
        # ATR calculation
        if len(data) >= 14:
            atr = self._calculate_atr(data[-14:])
            volatility["atr_14d"] = atr
            volatility["atr_ratio"] = atr / prices[-1]
        
        return volatility
    
    def _calculate_volume_metrics(self, data: List[MarketData]) -> Dict[str, float]:
        """Calculate volume-based features."""
        volumes = [d.volume for d in data]
        prices = [d.close for d in data]
        
        volume_metrics = {}
        
        # Volume z-score
        if len(volumes) >= 20:
            recent_vol = volumes[-1]
            avg_vol = np.mean(volumes[-20:-1])
            vol_std = np.std(volumes[-20:-1])
            volume_metrics["volume_zscore_20d"] = (recent_vol - avg_vol) / vol_std
        
        # Dollar volume
        if len(volumes) > 0 and len(prices) > 0:
            volume_metrics["dollar_volume"] = volumes[-1] * prices[-1]
            volume_metrics["avg_dollar_volume_20d"] = np.mean([
                v * p for v, p in zip(volumes[-20:-1], prices[-20:-1])
            ])
        
        return volume_metrics
    
    def _calculate_technical_indicators(self, data: List[MarketData]) -> Dict[str, Any]:
        """Calculate technical indicator features."""
        prices = [d.close for d in data]
        
        technical = {}
        
        # Moving averages
        if len(prices) >= 200:
            technical["ma50"] = np.mean(prices[-50:])
            technical["ma200"] = np.mean(prices[-200:])
            
            # Cross signals
            ma50 = technical["ma50"]
            ma200 = technical["ma200"]
            if ma50 > ma200:
                technical["ma_cross"] = "GOLDEN"
            elif ma50 < ma200:
                technical["ma_cross"] = "DEATH"
            else:
                technical["ma_cross"] = "NEUTRAL"
        
        # RSI
        if len(prices) >= 14:
            technical["rsi_14d"] = self._calculate_rsi(prices[-14:])
        
        return technical
```

### 2. Feature Builder (`app.ml.feature_builder`)

#### Service Overview
The Feature Builder orchestrates feature generation across multiple symbols and manages feature caching.

#### Batch Feature Generation

```python
class FeatureBuilder:
    def __init__(self, feature_engine: FeatureEngine):
        self.feature_engine = feature_engine
        self.feature_cache = {}
        
    def build_features_for_universe(self, symbols: List[str], as_of: str) -> Dict[str, FeatureRow]:
        """
        Build features for entire symbol universe.
        
        Args:
            symbols: List of symbols to process
            as_of: Date string for feature calculation
            
        Returns:
            Dictionary mapping symbols to FeatureRow objects
        """
        features = {}
        
        for symbol in symbols:
            try:
                # Check cache first
                cache_key = f"{symbol}_{as_of}"
                if cache_key in self.feature_cache:
                    features[symbol] = self.feature_cache[cache_key]
                    continue
                
                # Generate features
                market_data = self._get_market_data(symbol, as_of)
                feature_row = self.feature_engine.generate_features(symbol, market_data)
                
                # Cache result
                features[symbol] = feature_row
                self.feature_cache[cache_key] = feature_row
                
            except Exception as e:
                logger.error(f"Failed to build features for {symbol}: {e}")
                continue
        
        return features
    
    def validate_feature_quality(self, features: Dict[str, FeatureRow]) -> Dict[str, List[str]]:
        """
        Validate feature quality across universe.
        
        Returns:
            Dictionary mapping symbols to list of quality issues
        """
        quality_issues = {}
        
        for symbol, features in features.items():
            issues = []
            
            # Check for missing critical features
            if features.close is None:
                issues.append("missing_close")
            if features.volatility_20d is None:
                issues.append("missing_volatility")
            if features.volume_zscore_20d is None:
                issues.append("missing_volume_zscore")
            
            # Check for outlier values
            if features.volatility_20d and features.volatility_20d > 0.1:
                issues.append("extreme_volatility")
            if features.volume_zscore_20d and abs(features.volume_zscore_20d) > 5:
                issues.append("extreme_volume")
            
            if issues:
                quality_issues[symbol] = issues
        
        return quality_issues
```

---

## Discovery Strategy Services

### 1. Strategy Registry (`app.discovery.strategies.registry`)

#### Service Overview
The Strategy Registry manages all trading strategies and provides a unified interface for signal generation.

#### Available Strategies

##### Volatility Breakout Strategy
```python
def volatility_breakout(
    fr: FeatureRow,
    config: Dict[str, Any] | None = None,
    context: Dict[str, Any] | None = None
) -> Tuple[float | None, str, Dict[str, Any]]:
    """
    Volatility Breakout: Detects volatility expansion for trend-following entries.
    
    Core logic:
    1. Volatility must be in expansion (ATR > p80)
    2. Price must be above/below moving averages (trend confirmation)
    3. Volume confirmation (optional)
    4. Momentum confirmation (optional)
    
    Expected behavior:
    - 120-160 signals/year (vs 223 for unfiltered momentum)
    - Higher win rate due to regime filtering
    - Improved Sharpe from quality filtering
    """
    
    # Regime filtering
    if context and 'regime' in context:
        regime = context['regime']
        if regime.volatility_regime != VolatilityRegime.EXPANSION:
            return None, "volatility not in expansion", {}
    
    # Volatility expansion check
    atr_ratio = fr.atr / fr.close
    min_atr_ratio = config.get("min_atr_percentile", 0.80) * 0.03
    
    if atr_ratio < min_atr_ratio:
        return None, f"volatility not expanded (ATR ratio: {atr_ratio:.4f})", {}
    
    # Trend establishment
    price_vs_ma50 = (fr.close - fr.ma50) / fr.ma50
    ma50_vs_ma200 = (fr.ma50 - fr.ma200) / fr.ma200
    
    if price_vs_ma50 > 0 and ma50_vs_ma200 > 0:
        trend_direction = "bull"
        trend_strength = min(price_vs_ma50, ma50_vs_ma200)
    elif price_vs_ma50 < 0 and ma50_vs_ma200 < 0:
        trend_direction = "bear"
        trend_strength = min(abs(price_vs_ma50), abs(ma50_vs_ma200))
    else:
        return None, "no clear trend direction", {}
    
    # Scoring
    vol_score = clamp01((atr_ratio - min_atr_ratio) / (min_atr_ratio * 2))
    trend_score = clamp01(trend_strength / config.get("min_price_vs_ma50", 0.02))
    
    raw_score = 0.30 * vol_score + 0.25 * trend_score
    
    return raw_score, f"volatility breakout ({trend_direction} trend)", {
        "trend_direction": trend_direction,
        "atr_ratio": atr_ratio,
        "score_components": {"volatility": vol_score, "trend": trend_score}
    }
```

##### Sniper Coil Strategy
```python
def sniper_coil(
    fr: FeatureRow,
    config: Dict[str, Any] | None = None,
    context: Dict[str, Any] | None = None
) -> Tuple[float | None, str, Dict[str, Any]]:
    """
    Sniper Coil: AND-gated, regime-locked, multiplicative scoring.
    
    All five gates must pass before scoring begins:
    1. Fear regime required (VIX > VIX3M)
    2. Price in bottom quintile of 252d range
    3. Volatility compressed (accumulation phase)
    4. Anomalous volume (accumulation signal)
    5. In a downtrend (directional compression)
    
    Expected output: 0-3 candidates per fear-regime day
    """
    
    # HARD GATE 1: Fear regime required
    if not context.get("fear_regime", False):
        return None, "not fear regime", {}
    
    # HARD GATE 2: Price compression
    if fr.price_percentile_252d > config.get("price_gate", 0.20):
        return None, "not compressed", {}
    
    # HARD GATE 3: Volatility compression
    if fr.volatility_20d > config.get("vol_gate", 0.018):
        return None, "not tight", {}
    
    # HARD GATE 4: Volume anomaly
    if fr.volume_zscore_20d < config.get("volume_zscore_gate", 2.0):
        return None, "no volume signal", {}
    
    # HARD GATE 5: Downtrend
    if fr.return_63d > config.get("trend_gate", -0.05):
        return None, "not in downtrend", {}
    
    # Multiplicative scoring
    price_extreme = clamp01((config["price_gate"] - fr.price_percentile_252d) / config["price_gate"])
    vol_extreme = clamp01((config["vol_gate"] - fr.volatility_20d) / config["vol_gate"])
    vol_spike = clamp01((fr.volume_zscore_20d - config["volume_zscore_gate"]) / 3.0)
    trend_extreme = clamp01((abs(fr.return_63d) - abs(config["trend_gate"])) / 0.15)
    
    score = (price_extreme * vol_extreme * vol_spike * trend_extreme) ** 0.25
    
    return score, "sniper_coil: compressed price + vol + volume spike + fear regime", {
        "fear_regime": True,
        "extremes": {
            "price": price_extreme,
            "vol": vol_extreme,
            "spike": vol_spike,
            "trend": trend_extreme
        }
    }
```

##### Silent Compounder Strategy
```python
def silent_compounder(
    fr: FeatureRow,
    config: Dict[str, Any] | None = None,
    context: Dict[str, Any] | None = None
) -> Tuple[float | None, str, Dict[str, Any]]:
    """
    Silent Compounder: Optimal volatility band + steady price appreciation.
    
    IC: ~0.02-0.03 (weak but positive)
    
    Core logic:
    1. Volatility in optimal band (not too high, not too low)
    2. Positive price drift
    3. Quality universe filtering
    """
    
    # Volatility band filter
    ideal_vol = config.get("vol_band", 0.02)
    min_vol = config.get("min_vol", 0.01)
    max_vol = config.get("max_vol", 0.04)
    
    if fr.volatility_20d < min_vol or fr.volatility_20d > max_vol:
        return None, "volatility out of range", {}
    
    # Volatility score
    vol_score = max(0.0, 1.0 - abs(fr.volatility_20d - ideal_vol) / ideal_vol)
    
    # Positive drift
    steady = 1.0 if fr.return_63d > 0 else 0.0
    
    # Combined score
    raw = 0.6 * vol_score + 0.4 * steady
    
    if raw < config.get("threshold", 0.5):
        return None, "low score", {}
    
    return raw, f"optimal vol ({fr.volatility_20d:.3f}) + positive drift", {
        "volatility": fr.volatility_20d,
        "return_63d": fr.return_63d
    }
```

#### Strategy Execution Pipeline

```python
class StrategyExecutor:
    def __init__(self, registry: StrategyRegistry):
        self.registry = registry
        self.execution_cache = {}
        
    def execute_all_strategies(
        self,
        features: Dict[str, FeatureRow],
        regime_context: Dict[str, Any] | None = None
    ) -> Dict[str, List[DiscoveryCandidate]]:
        """
        Execute all enabled strategies on feature universe.
        
        Returns:
            Dictionary mapping strategy names to candidate lists
        """
        results = {}
        
        for strategy_name, strategy_fn in self.registry.STRATEGIES.items():
            if not self.registry.is_strategy_enabled(strategy_name):
                continue
            
            try:
                candidates = self.registry.score_candidates(
                    features,
                    strategy_type=strategy_name,
                    regime_context=regime_context
                )
                results[strategy_name] = candidates
                
            except Exception as e:
                logger.error(f"Strategy {strategy_name} failed: {e}")
                results[strategy_name] = []
        
        return results
    
    def merge_strategy_results(
        self,
        strategy_results: Dict[str, List[DiscoveryCandidate]],
        merge_method: str = "weighted_average"
    ) -> List[DiscoveryCandidate]:
        """
        Merge results from multiple strategies.
        
        Args:
            strategy_results: Results from individual strategies
            merge_method: Method for merging (weighted_average, rank_fusion, consensus)
            
        Returns:
            Merged candidate list
        """
        if merge_method == "weighted_average":
            return self._weighted_average_merge(strategy_results)
        elif merge_method == "rank_fusion":
            return self._rank_fusion_merge(strategy_results)
        elif merge_method == "consensus":
            return self._consensus_merge(strategy_results)
        else:
            raise ValueError(f"Unknown merge method: {merge_method}")
```

---

## Consensus and Scoring Services

### 1. Canonical Scoring (`app.core.canonical_scoring`)

#### Service Overview
Canonical Scoring provides standardized scoring mechanisms for combining signals from multiple strategies.

#### Scoring Methods

##### Cross-Sectional Ranking
```python
def cross_sectional_rank(
    candidates: List[DiscoveryCandidate],
    rank_method: str = "percentile"
) -> List[DiscoveryCandidate]:
    """
    Apply cross-sectional ranking to candidates.
    
    Args:
        candidates: List of discovery candidates
        rank_method: Ranking method (percentile, zscore, rank)
        
    Returns:
        Candidates with normalized scores
    """
    if not candidates:
        return []
    
    # Extract raw scores
    scores = [c.score for c in candidates]
    
    if rank_method == "percentile":
        # Percentile ranking (0-1)
        ranks = pct_rank(scores)
    elif rank_method == "zscore":
        # Z-score normalization
        mean_score = np.mean(scores)
        std_score = np.std(scores)
        ranks = [(s - mean_score) / std_score for s in scores]
    elif rank_method == "rank":
        # Simple rank normalization
        order = sorted(range(len(scores)), key=lambda i: scores[i])
        ranks = [0.0] * len(scores)
        for r, idx in enumerate(order):
            ranks[idx] = r / (len(scores) - 1)
    else:
        raise ValueError(f"Unknown rank method: {rank_method}")
    
    # Update candidates with normalized scores
    for candidate, rank_score in zip(candidates, ranks):
        candidate.metadata["canonical_score"] = rank_score
        candidate.score = rank_score
    
    return candidates
```

##### Consensus Weighting
```python
def consensus_weighting(
    strategy_results: Dict[str, List[DiscoveryCandidate]],
    strategy_weights: Dict[str, float] | None = None
) -> Dict[str, float]:
    """
    Calculate consensus weights for strategies based on recent performance.
    
    Args:
        strategy_results: Recent results from each strategy
        strategy_weights: Base weights for strategies
        
    Returns:
        Updated strategy weights
    """
    if strategy_weights is None:
        strategy_weights = {name: 1.0 for name in strategy_results.keys()}
    
    # Calculate performance metrics for each strategy
    performance_metrics = {}
    for strategy_name, candidates in strategy_results.items():
        if not candidates:
            performance_metrics[strategy_name] = 0.0
            continue
        
        # Calculate recent performance
        recent_returns = []
        for candidate in candidates[-20:]:  # Last 20 candidates
            if "actual_return" in candidate.metadata:
                recent_returns.append(candidate.metadata["actual_return"])
        
        if recent_returns:
            avg_return = np.mean(recent_returns)
            win_rate = sum(1 for r in recent_returns if r > 0) / len(recent_returns)
            performance_metrics[strategy_name] = avg_return * win_rate
        else:
            performance_metrics[strategy_name] = 0.0
    
    # Update weights based on performance
    updated_weights = {}
    for strategy_name, base_weight in strategy_weights.items():
        performance = performance_metrics.get(strategy_name, 0.0)
        
        # Performance-based adjustment
        if performance > 0.02:  # Good performance
            adjustment = 1.2
        elif performance > 0.0:  # Positive performance
            adjustment = 1.1
        elif performance > -0.02:  # Slightly negative
            adjustment = 0.9
        else:  # Poor performance
            adjustment = 0.7
        
        updated_weights[strategy_name] = base_weight * adjustment
    
    # Normalize weights
    total_weight = sum(updated_weights.values())
    if total_weight > 0:
        updated_weights = {k: v / total_weight for k, v in updated_weights.items()}
    
    return updated_weights
```

### 2. Consensus Models (`app.core.consensus_models`)

#### Service Overview
Consensus Models provide mechanisms for combining predictions from multiple strategies into unified signals.

#### Model Types

##### Weighted Ensemble
```python
class WeightedEnsemble:
    def __init__(self, strategy_weights: Dict[str, float]):
        self.strategy_weights = strategy_weights
        
    def predict(
        self,
        strategy_results: Dict[str, List[DiscoveryCandidate]],
        features: Dict[str, FeatureRow]
    ) -> List[DiscoveryCandidate]:
        """
        Generate ensemble predictions from strategy results.
        
        Args:
            strategy_results: Results from individual strategies
            features: Feature data for context
            
        Returns:
            Ensemble predictions
        """
        # Collect all unique symbols
        all_symbols = set()
        for candidates in strategy_results.values():
            for candidate in candidates:
                all_symbols.add(candidate.symbol)
        
        ensemble_predictions = []
        
        for symbol in all_symbols:
            # Collect predictions for this symbol
            symbol_predictions = []
            strategy_scores = []
            
            for strategy_name, candidates in strategy_results.items():
                # Find candidate for this symbol
                symbol_candidate = next((c for c in candidates if c.symbol == symbol), None)
                if symbol_candidate:
                    symbol_predictions.append(symbol_candidate)
                    strategy_scores.append(self.strategy_weights.get(strategy_name, 0.0))
            
            if not symbol_predictions:
                continue
            
            # Calculate weighted ensemble score
            ensemble_score = sum(
                pred.score * weight 
                for pred, weight in zip(symbol_predictions, strategy_scores)
            ) / sum(strategy_scores)
            
            # Create ensemble candidate
            ensemble_candidate = DiscoveryCandidate(
                symbol=symbol,
                strategy_type="weighted_ensemble",
                score=ensemble_score,
                reason=f"ensemble of {len(symbol_predictions)} strategies",
                metadata={
                    "component_strategies": [pred.strategy_type for pred in symbol_predictions],
                    "component_scores": [pred.score for pred in symbol_predictions],
                    "strategy_weights": dict(zip(
                        [pred.strategy_type for pred in symbol_predictions],
                        strategy_scores
                    ))
                }
            )
            
            ensemble_predictions.append(ensemble_candidate)
        
        # Sort by ensemble score
        ensemble_predictions.sort(key=lambda c: c.score, reverse=True)
        
        return ensemble_predictions
```

##### Rank Fusion Ensemble
```python
class RankFusionEnsemble:
    def __init__(self, fusion_method: str = "borda_count"):
        self.fusion_method = fusion_method
        
    def predict(
        self,
        strategy_results: Dict[str, List[DiscoveryCandidate]],
        features: Dict[str, FeatureRow]
    ) -> List[DiscoveryCandidate]:
        """
        Generate rank fusion predictions.
        
        Args:
            strategy_results: Results from individual strategies
            features: Feature data for context
            
        Returns:
            Rank fusion predictions
        """
        # Collect all unique symbols
        all_symbols = set()
        for candidates in strategy_results.values():
            for candidate in candidates:
                all_symbols.add(candidate.symbol)
        
        # Calculate Borda count for each symbol
        borda_scores = {}
        
        for symbol in all_symbols:
            total_score = 0
            
            for strategy_name, candidates in strategy_results.items():
                # Find rank of this symbol in this strategy
                sorted_candidates = sorted(candidates, key=lambda c: c.score, reverse=True)
                
                try:
                    rank = next(
                        i for i, c in enumerate(sorted_candidates) 
                        if c.symbol == symbol
                    )
                    # Borda count: (N - rank) where N is total candidates
                    borda_points = len(sorted_candidates) - rank
                    total_score += borda_points
                except StopIteration:
                    # Symbol not in this strategy's candidates
                    continue
            
            borda_scores[symbol] = total_score
        
        # Create fusion candidates
        fusion_predictions = []
        max_score = max(borda_scores.values()) if borda_scores else 1.0
        
        for symbol, score in borda_scores.items():
            normalized_score = score / max_score
            
            fusion_candidate = DiscoveryCandidate(
                symbol=symbol,
                strategy_type="rank_fusion_ensemble",
                score=normalized_score,
                reason=f"rank fusion (borda score: {score})",
                metadata={
                    "borda_score": score,
                    "normalized_score": normalized_score,
                    "fusion_method": self.fusion_method
                }
            )
            
            fusion_predictions.append(fusion_candidate)
        
        # Sort by fusion score
        fusion_predictions.sort(key=lambda c: c.score, reverse=True)
        
        return fusion_predictions
```

---

## Dimensional ML Services

### 1. Dimensional Tagger (`app.ml.dimensional_tagger`)

#### Service Overview
The Dimensional Tagger implements lightweight axis-based prediction tagging and performance tracking.

#### Tagging System

##### Prediction Axes
```python
class PredictionAxis(Enum):
    """Lightweight prediction axes for dimensional tagging."""
    ENVIRONMENT = "environment"      # HI_VOL, LO_VOL, TREND, CHOP
    SECTOR = "sector"              # TECH, FINANCIALS, HEALTHCARE, ENERGY
    MODEL = "model"                # AGGRESSIVE, DEFENSIVE, BALANCED
    HORIZON = "horizon"             # 1d, 5d, 7d, 20d
    VOLATILITY = "volatility"       # HIGH_VOL, MED_VOL, LOW_VOL
    LIQUIDITY = "liquidity"         # HIGH_LIQ, MED_LIQ, LOW_LIQ
```

##### Tag Extraction Functions
```python
def extract_environment_tag(self, features: Dict[str, Any]) -> str:
    """Extract environment tag from features."""
    
    vol = features.get("volatility_20d", 0.02)
    trend = features.get("return_5d", 0.0)
    
    # Environment classification
    if vol > 0.03:
        env_tag = "HIGH_VOL"
    elif vol < 0.015:
        env_tag = "LOW_VOL"
    else:
        env_tag = "MED_VOL"
    
    if abs(trend) > 0.02:
        env_tag += "_TREND"
    elif abs(trend) < 0.005:
        env_tag += "_CHOP"
    else:
        env_tag += "_STABLE"
    
    return env_tag

def extract_sector_tag(self, features: Dict[str, Any]) -> str:
    """Extract sector tag from features with normalization."""
    
    SECTOR_MAP = {
        "technology": "TECH",
        "tech": "TECH",
        "financial": "FINA",
        "financials": "FINA",
        "healthcare": "HEAL",
        "energy": "ENER",
        "consumer": "CONS",
        "industrial": "INDU",
        "materials": "MATL",
        "utilities": "UTIL",
        "real_estate": "REIT",
        "telecom": "TELE",
    }
    
    raw_sector = features.get("sector", "").lower()
    return SECTOR_MAP.get(raw_sector, "UNK")

def extract_model_tag(self, prediction: float, confidence: float) -> str:
    """Extract model behavior tag from prediction characteristics."""
    
    if confidence > 0.8 and abs(prediction) > 0.02:
        return "AGGRESSIVE"
    elif confidence > 0.7 and prediction < 0:
        return "DEFENSIVE"
    else:
        return "BALANCED"
```

##### Dimensional Tag Creation
```python
def create_dimensional_tags(
    self,
    features: Dict[str, Any],
    prediction: float,
    confidence: float,
    horizon: str = "7d"
) -> DimensionalTags:
    """Create lightweight dimensional tags for prediction."""
    
    # Extract components
    environment = self.extract_environment_tag(features)
    sector = self.extract_sector_tag(features)
    model = self.extract_model_tag(prediction, confidence)
    volatility = self.extract_volatility_tag(features)
    liquidity = self.extract_liquidity_tag(features)
    
    # Data integrity validation
    VALID_SECTORS = {"TECH", "FINA", "HEAL", "ENER", "CONS", "UNK"}
    VALID_ENVIRONMENTS = {
        "HIGH_VOL_TREND", "HIGH_VOL_STABLE", "HIGH_VOL_CHOP",
        "MED_VOL_TREND", "MED_VOL_STABLE", "MED_VOL_CHOP",
        "LOW_VOL_TREND", "LOW_VOL_STABLE", "LOW_VOL_CHOP"
    }
    
    # Fail-safe validation
    if sector not in VALID_SECTORS:
        sector = "UNK"
    
    if environment not in VALID_ENVIRONMENTS:
        environment = "MED_VOL_STABLE"
    
    # Data awareness for unknown sectors
    if sector == "UNK":
        metrics = self._get_persistent_data_quality_metrics()
        bad_ratio = metrics["bad_sector_ratio"]
        confidence_multiplier = max(0.3, 1 - bad_ratio)
        confidence *= confidence_multiplier
    
    # Self-correcting adjustment
    axis_key = f"{environment}_{sector}_{model}_{horizon}"
    adjusted_confidence = self.apply_self_correcting_adjustment(axis_key, confidence)
    
    return DimensionalTags(
        environment=environment,
        sector=sector,
        model=model,
        horizon=horizon,
        volatility=volatility,
        liquidity=liquidity,
        confidence=adjusted_confidence,
        prediction=prediction
    )
```

#### Performance Tracking

##### Axis Performance Calculation
```python
def get_axis_performance_metrics(self) -> Dict[str, Any]:
    """Get performance metrics for all axes."""
    
    conn = sqlite3.connect(self.db_path)
    try:
        cursor = conn.execute("""
            SELECT 
                axis_key,
                COUNT(*) as sample_count,
                AVG(prediction) as avg_prediction,
                AVG(confidence) as avg_confidence,
                AVG(CASE WHEN actual_return IS NOT NULL THEN prediction_error ELSE NULL END) as avg_error,
                COUNT(CASE WHEN actual_return IS NOT NULL THEN 1 ELSE NULL END) as outcome_count,
                AVG(CASE WHEN actual_return IS NOT NULL THEN actual_return ELSE NULL END) as avg_actual_return,
                COUNT(CASE WHEN actual_return IS NOT NULL AND actual_return > 0 THEN 1 ELSE NULL END) as win_count
            FROM dimensional_predictions 
            GROUP BY axis_key
            HAVING sample_count >= 5
            ORDER BY sample_count DESC
        """)
        
        axes_data = cursor.fetchall()
        axis_metrics = {}
        
        for row in axes_data:
            axis_key, sample_count, avg_prediction, avg_confidence, avg_error, outcome_count, avg_actual_return, win_count = row
            
            # Real outcome-based metrics
            if outcome_count < 10:
                performance_score = 0.5  # Neutral until sufficient data
                win_rate = 0.0
            else:
                win_rate = win_count / outcome_count if outcome_count > 0 else 0.0
                performance_score = win_rate + (avg_actual_return if avg_actual_return else 0.0)
                performance_score = max(0.0, min(1.0, performance_score))
            
            reliability = outcome_count / sample_count if sample_count > 0 else 0.0
            
            axis_metrics[axis_key] = {
                "sample_count": sample_count,
                "avg_prediction": avg_prediction,
                "avg_confidence": avg_confidence,
                "avg_error": avg_error,
                "outcome_count": outcome_count,
                "avg_actual_return": avg_actual_return,
                "win_count": win_count,
                "win_rate": win_rate,
                "reliability": reliability,
                "performance_score": performance_score,
                "has_real_outcomes": outcome_count > 0
            }
        
        return axis_metrics
        
    finally:
        conn.close()
```

##### Self-Correcting Weights
```python
def calculate_axis_weights(self) -> Dict[str, float]:
    """Calculate self-correcting axis weights based on real performance."""
    
    if not self._self_correcting_enabled:
        return {"message": "Self-correcting disabled"}
    
    # Check if we have sufficient real outcomes
    outcome_status = self.get_real_outcome_status()
    if not outcome_status.get("can_self_correct", False):
        return {"message": "Insufficient real outcomes for self-correction"}
    
    axis_metrics = self.get_axis_performance_metrics()
    
    if "error" in axis_metrics:
        return axis_metrics
    
    weights = {}
    
    for axis_key, metrics in axis_metrics.items():
        # Only use axes with real outcomes
        if not metrics.get("has_real_outcomes", False) or metrics.get("outcome_count", 0) < 10:
            weights[axis_key] = 0.5  # Neutral weight
            continue
        
        # Base weight from real win rate + actual returns
        base_weight = metrics["performance_score"]
        
        # Boost for high reliability
        reliability_boost = metrics["reliability"] * 0.2
        
        # Boost for sufficient sample size
        sample_boost = min(0.1, metrics["outcome_count"] / 100) if metrics["outcome_count"] >= 30 else 0.0
        
        # Penalty for unknown sector
        unk_penalty = 0.3 if "UNK" in axis_key else 0.0
        
        # Calculate final weight
        final_weight = base_weight + reliability_boost + sample_boost - unk_penalty
        final_weight = max(0.1, min(1.0, final_weight))
        
        weights[axis_key] = final_weight
    
    return weights
```

### 2. Lightweight Dimensional ML (`app.ml.lightweight_dimensional_ml`)

#### Service Overview
Lightweight Dimensional ML provides production-ready system using axis tagging for selective activation.

#### Prediction Pipeline

##### Batch Prediction with Tagging
```python
def batch_predict_and_tag(
    self,
    features_dict: Dict[str, Any],
    predictions: Dict[str, float],
    confidences: Dict[str, float],
    db_path: str,
    as_of: str
) -> List[Dict[str, Any]]:
    """
    Batch process multiple predictions with dimensional tagging.
    
    Args:
        features_dict: Feature data by symbol
        predictions: Predictions by symbol
        confidences: Confidence scores by symbol
        db_path: Database path for storage
        as_of: Analysis date
        
    Returns:
        List of prediction results with dimensional tags
    """
    results = []
    
    print(f"Batch dimensional prediction: {len(predictions)} predictions")
    
    for symbol, prediction in predictions.items():
        if symbol in features_dict:
            features = features_dict[symbol]
            confidence = confidences.get(symbol, 0.5)
            
            result = self.predict_with_dimensional_tagging(
                features, prediction, confidence, db_path, as_of
            )
            
            results.append(result)
    
    # Summary statistics
    activated_count = sum(1 for r in results if r["should_activate"])
    blocked_count = len(results) - activated_count
    
    print(f"Batch summary:")
    print(f"  Activated: {activated_count} predictions")
    print(f"  Blocked: {blocked_count} predictions")
    print(f"  Activation rate: {activated_count/len(results):.1%}")
    
    return results

def predict_with_dimensional_tagging(
    self,
    features: Dict[str, Any],
    base_prediction: float,
    confidence: float,
    db_path: str,
    as_of: str
) -> Dict[str, Any]:
    """
    Make prediction with lightweight dimensional tagging.
    
    Args:
        features: Feature data for symbol
        base_prediction: Raw prediction value
        confidence: Base confidence score
        db_path: Database for storage
        as_of: Analysis date
        
    Returns:
        Prediction result with dimensional context
    """
    # Get current environment for context
    env = build_env_snapshot_v3(db_path=db_path, as_of=as_of)
    
    # Create dimensional tags
    symbol = list(features.keys())[0]  # Get first symbol
    axis_key = tag_and_store_prediction(
        symbol, features, base_prediction, confidence
    )
    
    # Check if we should activate this prediction
    should_activate = self.tagger.should_activate_prediction(axis_key, cold_start_mode=True)
    
    # Extract environment and sector from features
    env_tag = self.tagger.extract_environment_tag(features)
    sector_tag = self.tagger.extract_sector_tag(features)
    
    return {
        "symbol": symbol,
        "prediction": base_prediction,
        "confidence": confidence,
        "axis_key": axis_key,
        "should_activate": should_activate,
        "environment_tag": env_tag,
        "sector_tag": sector_tag,
        "activation_reason": self._get_activation_reason(should_activate, axis_key),
        "dimensional_context": {
            "market_vol_pct": env.market_vol_pct,
            "trend_strength": env.trend_strength,
            "sector_regime": env.sector_regime,
            "industry_dispersion": env.industry_dispersion,
        }
    }
```

##### Selective Activation Pipeline
```python
def selective_activation_pipeline(
    self,
    features_dict: Dict[str, Any],
    db_path: str,
    as_of: str,
    activation_mode: str = "conservative"
) -> List[Dict[str, Any]]:
    """
    Production pipeline with selective activation based on proven edges.
    
    Args:
        features_dict: Feature data by symbol
        db_path: Database for context
        as_of: Analysis date
        activation_mode: conservative, moderate, aggressive
        
    Returns:
        List of activated predictions
    """
    # Get current environment
    env = build_env_snapshot_v3(db_path=db_path, as_of=as_of)
    
    # Get activation rules
    activation_matrix = get_activation_rules()
    
    print(f"Selective activation pipeline ({activation_mode})")
    print(f"Environment: {env.sector_regime} regime, vol: {env.market_vol_pct:.2f}")
    
    activated_predictions = []
    blocked_predictions = []
    
    for symbol, features in features_dict.items():
        # Extract dimensional tags
        env_tag = self.tagger.extract_environment_tag(features)
        sector_tag = self.tagger.extract_sector_tag(features)
        
        # Check activation rules
        should_activate = False
        activation_reason = "No matching rule"
        
        if (env_tag in activation_matrix and 
            sector_tag in activation_matrix[env_tag]):
            
            for model, should_activate_model in activation_matrix[env_tag][sector_tag].items():
                if should_activate_model:
                    should_activate = True
                    activation_reason = f"ACTIVATED: {model} edge for {env_tag}_{sector_tag}"
                    break
        
        # Apply activation mode filters
        if should_activate:
            if activation_mode == "conservative":
                should_activate = (self._get_confidence_for_symbol(features) > 0.8 and 
                                  self._get_historical_performance(symbol) > 0.6)
            elif activation_mode == "moderate":
                should_activate = self._get_historical_performance(symbol) > 0.45
        
        result = {
            "symbol": symbol,
            "should_activate": should_activate,
            "activation_reason": activation_reason,
            "environment_tag": env_tag,
            "sector_tag": sector_tag,
            "confidence": self._get_confidence_for_symbol(features),
            "historical_performance": self._get_historical_performance(symbol),
            "activation_mode": activation_mode
        }
        
        if should_activate:
            activated_predictions.append(result)
        else:
            blocked_predictions.append(result)
    
    # Summary
    print(f"Activation summary:")
    print(f"  Activated: {len(activated_predictions)} predictions")
    print(f"  Blocked: {len(blocked_predictions)} predictions")
    print(f"  Selectivity: {len(activated_predictions)/(len(activated_predictions) + len(blocked_predictions)):.1%}")
    
    return activated_predictions
```

---

## Regime Detection Services

### 1. Regime Detection V3 (`app.core.regime_v3`)

#### Service Overview
Regime Detection V3 provides sophisticated market regime classification using multiple indicators and machine learning.

#### Regime Classification

##### Regime Types
```python
class TrendRegime(Enum):
    BULL = "bull"           # Strong uptrend
    BEAR = "bear"           # Strong downtrend
    SIDEWAYS = "sideways"   # Range-bound
    CHOP = "chop"          # No clear direction

class VolatilityRegime(Enum):
    EXPANSION = "expansion"    # Volatility expanding
    CONTRACTION = "contraction"  # Volatility contracting
    NORMAL = "normal"          # Normal volatility
    EXTREME = "extreme"        # Extreme volatility

@dataclass
class RegimeClassification:
    trend_regime: TrendRegime
    volatility_regime: VolatilityRegime
    confidence: float
    market_vol_pct: float      # Market volatility percentile
    trend_strength: float      # Trend strength indicator
    sector_regime: str         # Sector-level regime
    industry_dispersion: float # Industry dispersion metric
    classification_timestamp: datetime
```

##### Regime Detection Algorithm
```python
def classify_market_regime(
    market_data: List[MarketData],
    sector_data: Dict[str, List[MarketData]],
    lookback_days: int = 252
) -> RegimeClassification:
    """
    Classify current market regime using multiple indicators.
    
    Args:
        market_data: Broad market data (e.g., SPY)
        sector_data: Sector ETF data
        lookback_days: Lookback period for analysis
        
    Returns:
        RegimeClassification with current regime information
    """
    
    # 1. Trend Analysis
    trend_regime, trend_strength = _classify_trend_regime(market_data, lookback_days)
    
    # 2. Volatility Analysis
    volatility_regime, market_vol_pct = _classify_volatility_regime(market_data, lookback_days)
    
    # 3. Sector Analysis
    sector_regime, industry_dispersion = _analyze_sector_regime(sector_data, lookback_days)
    
    # 4. Confidence Calculation
    confidence = _calculate_regime_confidence(
        trend_strength, market_vol_pct, industry_dispersion
    )
    
    return RegimeClassification(
        trend_regime=trend_regime,
        volatility_regime=volatility_regime,
        confidence=confidence,
        market_vol_pct=market_vol_pct,
        trend_strength=trend_strength,
        sector_regime=sector_regime,
        industry_dispersion=industry_dispersion,
        classification_timestamp=datetime.now()
    )

def _classify_trend_regime(
    market_data: List[MarketData],
    lookback_days: int
) -> Tuple[TrendRegime, float]:
    """Classify trend regime and calculate strength."""
    
    if len(market_data) < lookback_days:
        return TrendRegime.CHOP, 0.0
    
    prices = [d.close for d in market_data[-lookback_days:]]
    
    # Calculate moving averages
    ma50 = np.mean(prices[-50:])
    ma200 = np.mean(prices[-200:])
    
    # Calculate trend strength
    price_change = (prices[-1] - prices[0]) / prices[0]
    volatility = np.std(np.diff(np.log(prices))) * np.sqrt(252)
    
    trend_strength = abs(price_change) / volatility
    
    # Classify trend
    if ma50 > ma200 and price_change > 0.05:
        return TrendRegime.BULL, trend_strength
    elif ma50 < ma200 and price_change < -0.05:
        return TrendRegime.BEAR, trend_strength
    elif abs(price_change) < 0.02:
        return TrendRegime.SIDEWAYS, trend_strength
    else:
        return TrendRegime.CHOP, trend_strength

def _classify_volatility_regime(
    market_data: List[MarketData],
    lookback_days: int
) -> Tuple[VolatilityRegime, float]:
    """Classify volatility regime and calculate percentile."""
    
    if len(market_data) < lookback_days:
        return VolatilityRegime.NORMAL, 0.5
    
    prices = [d.close for d in market_data[-lookback_days:]]
    
    # Calculate realized volatility
    log_returns = [np.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
    current_vol = np.std(log_returns[-20:]) * np.sqrt(252)  # 20-day vol
    
    # Calculate historical volatility distribution
    historical_vols = []
    for i in range(20, len(log_returns)):
        vol = np.std(log_returns[i-20:i]) * np.sqrt(252)
        historical_vols.append(vol)
    
    # Calculate percentile
    vol_percentile = sum(1 for v in historical_vols if v < current_vol) / len(historical_vols)
    
    # Classify volatility regime
    if vol_percentile > 0.8:
        return VolatilityRegime.EXPANSION, vol_percentile
    elif vol_percentile < 0.2:
        return VolatilityRegime.CONTRACTION, vol_percentile
    elif vol_percentile > 0.95:
        return VolatilityRegime.EXTREME, vol_percentile
    else:
        return VolatilityRegime.NORMAL, vol_percentile
```

### 2. Regime Manager (`app.core.regime_manager`)

#### Service Overview
The Regime Manager coordinates regime detection across multiple timeframes and manages regime transitions.

#### Multi-Timeframe Analysis

```python
class RegimeManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.regime_history = {}
        self.regime_cache = {}
        
    def analyze_regime_across_timeframes(
        self,
        symbol: str,
        market_data: List[MarketData],
        current_time: datetime
    ) -> Dict[str, RegimeClassification]:
        """
        Analyze regime across multiple timeframes.
        
        Args:
            symbol: Symbol to analyze
            market_data: Historical market data
            current_time: Current timestamp
            
        Returns:
            Dictionary mapping timeframes to regime classifications
        """
        timeframes = {
            "intraday": 1,
            "daily": 5,
            "weekly": 22,
            "monthly": 252
        }
        
        regime_classifications = {}
        
        for timeframe_name, days in timeframes.items():
            if len(market_data) >= days:
                regime = classify_market_regime(market_data, {}, days)
                regime_classifications[timeframe_name] = regime
        
        return regime_classifications
    
    def detect_regime_transition(
        self,
        symbol: str,
        current_regime: RegimeClassification,
        previous_regime: RegimeClassification
    ) -> Optional[RegimeTransition]:
        """
        Detect regime transitions and generate transition events.
        
        Args:
            symbol: Symbol to analyze
            current_regime: Current regime classification
            previous_regime: Previous regime classification
            
        Returns:
            RegimeTransition event if transition detected
        """
        
        # Check for trend regime change
        if current_regime.trend_regime != previous_regime.trend_regime:
            return RegimeTransition(
                symbol=symbol,
                transition_type="trend",
                from_regime=previous_regime.trend_regime.value,
                to_regime=current_regime.trend_regime.value,
                transition_time=current_regime.classification_timestamp,
                confidence=current_regime.confidence
            )
        
        # Check for volatility regime change
        if current_regime.volatility_regime != previous_regime.volatility_regime:
            return RegimeTransition(
                symbol=symbol,
                transition_type="volatility",
                from_regime=previous_regime.volatility_regime.value,
                to_regime=current_regime.volatility_regime.value,
                transition_time=current_regime.classification_timestamp,
                confidence=current_regime.confidence
            )
        
        return None
```

---

## Adaptive Learning Services

### 1. Regime-Aware ML (`app.ml.regime_aware_ml`)

#### Service Overview
Regime-Aware ML provides adaptive machine learning that adjusts predictions based on current market regimes.

#### Adaptive Model Selection

```python
class RegimeAwareML:
    def __init__(self, model_registry: Dict[str, Any]):
        self.model_registry = model_registry
        self.regime_performance_history = {}
        
    def select_model_for_regime(
        self,
        current_regime: RegimeClassification,
        prediction_task: str
    ) -> str:
        """
        Select best performing model for current regime.
        
        Args:
            current_regime: Current market regime
            prediction_task: Type of prediction task
            
        Returns:
            Model identifier best suited for current regime
        """
        
        # Get performance history for this regime
        regime_key = f"{current_regime.trend_regime.value}_{current_regime.volatility_regime.value}"
        
        if regime_key not in self.regime_performance_history:
            # No history, use default model
            return self.model_registry.get_default_model(prediction_task)
        
        # Select best performing model for this regime
        performance_data = self.regime_performance_history[regime_key]
        best_model = max(
            performance_data.items(),
            key=lambda x: x[1]["performance_score"]
        )[0]
        
        return best_model
    
    def adapt_prediction_for_regime(
        self,
        base_prediction: float,
        confidence: float,
        current_regime: RegimeClassification,
        symbol_features: Dict[str, Any]
    ) -> Tuple[float, float]:
        """
        Adapt prediction and confidence based on current regime.
        
        Args:
            base_prediction: Original prediction
            confidence: Original confidence
            current_regime: Current market regime
            symbol_features: Symbol-specific features
            
        Returns:
            Adjusted prediction and confidence
        """
        
        # Regime-specific adjustments
        if current_regime.volatility_regime == VolatilityRegime.EXPANSION:
            # Reduce confidence in high volatility
            confidence *= 0.8
            # Reduce prediction magnitude
            base_prediction *= 0.7
        
        elif current_regime.volatility_regime == VolatilityRegime.CONTRACTION:
            # Increase confidence in low volatility
            confidence *= 1.2
            # Increase prediction magnitude
            base_prediction *= 1.1
        
        # Trend-specific adjustments
        if current_regime.trend_regime == TrendRegime.BULL:
            if base_prediction < 0:  # Short signal in bull market
                confidence *= 0.6
                base_prediction *= 0.5
        
        elif current_regime.trend_regime == TrendRegime.BEAR:
            if base_prediction > 0:  # Long signal in bear market
                confidence *= 0.6
                base_prediction *= 0.5
        
        # Ensure confidence stays in valid range
        confidence = max(0.1, min(0.95, confidence))
        
        return base_prediction, confidence
```

### 2. Online Learning Service

#### Service Overview
Online Learning Service provides continuous model adaptation using streaming data.

#### Incremental Learning

```python
class OnlineLearningService:
    def __init__(self, learning_rate: float = 0.01):
        self.learning_rate = learning_rate
        self.model_weights = {}
        self.feature_importance = {}
        
    def update_model_from_outcome(
        self,
        prediction: float,
        actual_outcome: float,
        features: Dict[str, float],
        model_id: str
    ):
        """
        Update model weights based on prediction outcome.
        
        Args:
            prediction: Model prediction
            actual_outcome: Actual outcome
            features: Feature values used for prediction
            model_id: Model identifier
        """
        
        # Calculate prediction error
        error = actual_outcome - prediction
        
        # Update model weights using gradient descent
        if model_id not in self.model_weights:
            self.model_weights[model_id] = {}
        
        for feature_name, feature_value in features.items():
            if feature_name not in self.model_weights[model_id]:
                self.model_weights[model_id][feature_name] = 0.0
            
            # Gradient descent update
            gradient = error * feature_value
            self.model_weights[model_id][feature_name] += self.learning_rate * gradient
        
        # Update feature importance
        for feature_name, feature_value in features.items():
            if feature_name not in self.feature_importance:
                self.feature_importance[feature_name] = 0.0
            
            # Track feature contribution to error
            contribution = abs(error * feature_value)
            self.feature_importance[feature_name] = (
                0.9 * self.feature_importance[feature_name] + 0.1 * contribution
            )
    
    def predict_with_online_model(
        self,
        features: Dict[str, float],
        model_id: str
    ) -> float:
        """
        Make prediction using online-trained model.
        
        Args:
            features: Feature values
            model_id: Model identifier
            
        Returns:
            Prediction value
        """
        
        if model_id not in self.model_weights:
            return 0.0  # Neutral prediction if model not trained
        
        prediction = 0.0
        weights = self.model_weights[model_id]
        
        for feature_name, feature_value in features.items():
            if feature_name in weights:
                prediction += weights[feature_name] * feature_value
        
        # Apply sigmoid for probability-like output
        prediction = 1 / (1 + np.exp(-prediction))
        
        return prediction
```

---

## Model Management Services

### 1. Model Registry (`app.ml.model_registry`)

#### Service Overview
The Model Registry manages all ML models, their versions, and deployment configurations.

#### Model Registration

```python
class ModelRegistry:
    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        self.registered_models = {}
        self.model_versions = {}
        self.deployment_configs = {}
        
    def register_model(
        self,
        model_id: str,
        model_type: str,
        model_object: Any,
        metadata: Dict[str, Any]
    ):
        """
        Register a new model in the registry.
        
        Args:
            model_id: Unique model identifier
            model_type: Type of model (classifier, regressor, etc.)
            model_object: Trained model object
            metadata: Model metadata
        """
        
        # Generate model version
        version = len(self.model_versions.get(model_id, [])) + 1
        
        # Store model
        model_info = {
            "model_id": model_id,
            "model_type": model_type,
            "model_object": model_object,
            "version": version,
            "metadata": metadata,
            "registered_at": datetime.now(),
            "status": "registered"
        }
        
        self.registered_models[model_id] = model_info
        
        # Track versions
        if model_id not in self.model_versions:
            self.model_versions[model_id] = []
        self.model_versions[model_id].append(model_info)
        
        print(f"Model registered: {model_id} v{version}")
    
    def deploy_model(
        self,
        model_id: str,
        version: int | None = None,
        deployment_config: Dict[str, Any] | None = None
    ):
        """
        Deploy a model to production.
        
        Args:
            model_id: Model identifier
            version: Specific version to deploy (latest if None)
            deployment_config: Deployment configuration
        """
        
        if model_id not in self.registered_models:
            raise ValueError(f"Model {model_id} not found")
        
        # Select version
        if version is None:
            model_info = self.registered_models[model_id]
        else:
            model_info = self._get_model_version(model_id, version)
        
        # Update deployment status
        model_info["status"] = "deployed"
        model_info["deployed_at"] = datetime.now()
        
        # Store deployment config
        if deployment_config:
            self.deployment_configs[model_id] = deployment_config
        
        print(f"Model deployed: {model_id} v{model_info['version']}")
    
    def get_model_for_prediction(
        self,
        model_id: str,
        fallback_to_latest: bool = True
    ) -> Any:
        """
        Get model object for prediction.
        
        Args:
            model_id: Model identifier
            fallback_to_latest: Use latest version if deployed version not found
            
        Returns:
            Model object for prediction
        """
        
        if model_id not in self.registered_models:
            raise ValueError(f"Model {model_id} not found")
        
        model_info = self.registered_models[model_id]
        
        if model_info["status"] != "deployed" and fallback_to_latest:
            # Fallback to latest version
            return model_info["model_object"]
        
        return model_info["model_object"]
```

### 2. Model Performance Monitor

#### Service Overview
The Model Performance Monitor tracks model performance over time and detects degradation.

#### Performance Tracking

```python
class ModelPerformanceMonitor:
    def __init__(self, metrics_storage_path: str):
        self.metrics_storage_path = metrics_storage_path
        self.performance_history = {}
        self.alert_thresholds = {
            "accuracy_drop": 0.05,      # 5% accuracy drop
            "error_increase": 0.1,      # 10% error increase
            "prediction_latency": 1000,  # 1000ms latency
            "memory_usage": 0.8         # 80% memory usage
        }
        
    def track_prediction_performance(
        self,
        model_id: str,
        predictions: List[float],
        actuals: List[float],
        timestamp: datetime
    ):
        """
        Track model prediction performance.
        
        Args:
            model_id: Model identifier
            predictions: Model predictions
            actuals: Actual outcomes
            timestamp: Prediction timestamp
        """
        
        # Calculate performance metrics
        mse = mean_squared_error(actuals, predictions)
        mae = mean_absolute_error(actuals, predictions)
        
        # Calculate accuracy for classification
        if all(p in [0, 1] for p in predictions):
            accuracy = accuracy_score(actuals, [round(p) for p in predictions])
        else:
            # For regression, use correlation as accuracy proxy
            correlation = np.corrcoef(predictions, actuals)[0, 1]
            accuracy = abs(correlation) if not np.isnan(correlation) else 0.0
        
        # Store performance metrics
        if model_id not in self.performance_history:
            self.performance_history[model_id] = []
        
        performance_record = {
            "timestamp": timestamp,
            "mse": mse,
            "mae": mae,
            "accuracy": accuracy,
            "sample_count": len(predictions)
        }
        
        self.performance_history[model_id].append(performance_record)
        
        # Check for performance degradation
        self._check_performance_degradation(model_id, performance_record)
    
    def _check_performance_degradation(
        self,
        model_id: str,
        current_performance: Dict[str, float]
    ):
        """Check if model performance has degraded beyond thresholds."""
        
        if model_id not in self.performance_history:
            return
        
        history = self.performance_history[model_id]
        
        # Need at least 10 historical records for comparison
        if len(history) < 10:
            return
        
        # Calculate baseline performance (average of last 10 records)
        baseline_records = history[-11:-1]  # Exclude current record
        baseline_accuracy = np.mean([r["accuracy"] for r in baseline_records])
        baseline_mse = np.mean([r["mse"] for r in baseline_records])
        
        # Check for degradation
        accuracy_drop = baseline_accuracy - current_performance["accuracy"]
        error_increase = (current_performance["mse"] - baseline_mse) / baseline_mse
        
        if accuracy_drop > self.alert_thresholds["accuracy_drop"]:
            self._send_alert(
                model_id,
                "accuracy_degradation",
                f"Accuracy dropped by {accuracy_drop:.2%}",
                current_performance
            )
        
        if error_increase > self.alert_thresholds["error_increase"]:
            self._send_alert(
                model_id,
                "error_increase",
                f"Error increased by {error_increase:.2%}",
                current_performance
            )
    
    def _send_alert(
        self,
        model_id: str,
        alert_type: str,
        message: str,
        performance_data: Dict[str, float]
    ):
        """Send performance alert."""
        
        alert = {
            "model_id": model_id,
            "alert_type": alert_type,
            "message": message,
            "timestamp": datetime.now(),
            "performance_data": performance_data
        }
        
        # Log alert
        logger.warning(f"Model Performance Alert: {alert}")
        
        # In production, this would send to monitoring system
        # monitoring_system.send_alert(alert)
```

---

## Performance Analytics Services

### 1. Performance Analytics (`app.analytics.performance_analytics`)

#### Service Overview
Performance Analytics provides comprehensive analysis of trading and ML performance across multiple dimensions.

#### Analytics Components

##### Trade Performance Analysis
```python
class TradePerformanceAnalyzer:
    def __init__(self, trade_data_source: Any):
        self.trade_data_source = trade_data_source
        
    def analyze_trade_performance(
        self,
        start_date: datetime,
        end_date: datetime,
        group_by: str = "strategy"
    ) -> Dict[str, Any]:
        """
        Analyze trade performance over specified period.
        
        Args:
            start_date: Analysis start date
            end_date: Analysis end date
            group_by: Grouping dimension (strategy, sector, regime)
            
        Returns:
            Performance analysis results
        """
        
        # Get trades for period
        trades = self.trade_data_source.get_trades(start_date, end_date)
        
        if not trades:
            return {"error": "No trades found for period"}
        
        # Group trades
        grouped_trades = self._group_trades(trades, group_by)
        
        # Calculate performance metrics for each group
        performance_results = {}
        
        for group_key, group_trades in grouped_trades.items():
            performance_metrics = self._calculate_performance_metrics(group_trades)
            performance_results[group_key] = performance_metrics
        
        # Calculate overall performance
        overall_metrics = self._calculate_performance_metrics(trades)
        
        return {
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "total_trades": len(trades)
            },
            "overall_performance": overall_metrics,
            "grouped_performance": performance_results,
            "analysis_timestamp": datetime.now().isoformat()
        }
    
    def _calculate_performance_metrics(self, trades: List[Trade]) -> Dict[str, Any]:
        """Calculate comprehensive performance metrics for trade group."""
        
        if not trades:
            return {}
        
        # Basic metrics
        total_pnl = sum(trade.realized_pnl for trade in trades)
        winning_trades = [trade for trade in trades if trade.realized_pnl > 0]
        losing_trades = [trade for trade in trades if trade.realized_pnl < 0]
        
        win_rate = len(winning_trades) / len(trades) if trades else 0.0
        
        # Return metrics
        returns = [trade.realized_pnl for trade in trades]
        avg_return = np.mean(returns)
        std_return = np.std(returns)
        sharpe_ratio = avg_return / std_return if std_return > 0 else 0.0
        
        # Drawdown analysis
        cumulative_returns = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdowns = cumulative_returns - running_max
        max_drawdown = np.min(drawdowns) if len(drawdowns) > 0 else 0.0
        
        # Trade duration analysis
        durations = []
        for trade in trades:
            if trade.entry_timestamp and trade.exit_timestamp:
                duration = (trade.exit_timestamp - trade.entry_timestamp).total_seconds() / 3600  # hours
                durations.append(duration)
        
        avg_duration = np.mean(durations) if durations else 0.0
        
        return {
            "total_trades": len(trades),
            "total_pnl": total_pnl,
            "win_rate": win_rate,
            "avg_return": avg_return,
            "std_return": std_return,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "avg_duration_hours": avg_duration,
            "profit_factor": sum(trade.realized_pnl for trade in winning_trades) / 
                           abs(sum(trade.realized_pnl for trade in losing_trades)) if losing_trades else float('inf')
        }
```

##### ML Performance Analysis
```python
class MLPerformanceAnalyzer:
    def __init__(self, prediction_data_source: Any):
        self.prediction_data_source = prediction_data_source
        
    def analyze_prediction_performance(
        self,
        start_date: datetime,
        end_date: datetime,
        model_id: str | None = None
    ) -> Dict[str, Any]:
        """
        Analyze ML prediction performance.
        
        Args:
            start_date: Analysis start date
            end_date: Analysis end date
            model_id: Specific model to analyze (all if None)
            
        Returns:
            ML performance analysis results
        """
        
        # Get predictions for period
        predictions = self.prediction_data_source.get_predictions(
            start_date, end_date, model_id
        )
        
        if not predictions:
            return {"error": "No predictions found for period"}
        
        # Calculate prediction accuracy metrics
        accuracy_metrics = self._calculate_prediction_accuracy(predictions)
        
        # Analyze by prediction confidence buckets
        confidence_analysis = self._analyze_by_confidence(predictions)
        
        # Analyze by market regime
        regime_analysis = self._analyze_by_regime(predictions)
        
        # Feature importance analysis
        feature_analysis = self._analyze_feature_importance(predictions)
        
        return {
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "total_predictions": len(predictions)
            },
            "accuracy_metrics": accuracy_metrics,
            "confidence_analysis": confidence_analysis,
            "regime_analysis": regime_analysis,
            "feature_analysis": feature_analysis,
            "analysis_timestamp": datetime.now().isoformat()
        }
    
    def _calculate_prediction_accuracy(self, predictions: List[Dict[str, Any]]) -> Dict[str, float]:
        """Calculate prediction accuracy metrics."""
        
        actual_returns = [p["actual_return"] for p in predictions if "actual_return" in p]
        predicted_returns = [p["prediction"] for p in predictions if "actual_return" in p]
        
        if not actual_returns:
            return {}
        
        # Correlation analysis
        correlation = np.corrcoef(predicted_returns, actual_returns)[0, 1]
        
        # Directional accuracy
        correct_direction = sum(
            1 for pred, actual in zip(predicted_returns, actual_returns)
            if (pred > 0 and actual > 0) or (pred < 0 and actual < 0)
        )
        directional_accuracy = correct_direction / len(actual_returns)
        
        # Mean squared error
        mse = mean_squared_error(actual_returns, predicted_returns)
        
        # Mean absolute error
        mae = mean_absolute_error(actual_returns, predicted_returns)
        
        return {
            "correlation": correlation if not np.isnan(correlation) else 0.0,
            "directional_accuracy": directional_accuracy,
            "mse": mse,
            "mae": mae,
            "sample_count": len(actual_returns)
        }
```

---

## ML Configuration and Deployment

### 1. ML Configuration Management

#### Configuration Structure
```yaml
# config/ml_config.yaml
ml_services:
  feature_engine:
    enabled: true
    cache_size: 10000
    update_frequency: "daily"
    
  discovery_strategies:
    enabled_strategies:
      - volatility_breakout
      - sniper_coil
      - silent_compounder
      - realness_repricer
    strategy_weights:
      volatility_breakout: 0.25
      sniper_coil: 0.15
      silent_compounder: 0.20
      realness_repricer: 0.20
      narrative_lag: 0.10
      ownership_vacuum: 0.10
    
  dimensional_ml:
    enabled: true
    self_correcting: true
    min_samples_for_activation: 50
    confidence_threshold: 0.6
    
  regime_detection:
    enabled: true
    lookback_days: 252
    update_frequency: "daily"
    
  adaptive_learning:
    enabled: true
    learning_rate: 0.01
    update_frequency: "continuous"
    
model_management:
  model_registry:
    storage_path: "models/"
    max_versions: 10
    
  performance_monitoring:
    alert_thresholds:
      accuracy_drop: 0.05
      error_increase: 0.1
      prediction_latency: 1000
    
  deployment:
    rollout_strategy: "canary"
    canary_percentage: 0.1
    health_check_interval: 300  # seconds
```

### 2. Deployment Architecture

#### Service Deployment
```python
class MLServiceDeployment:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.deployed_services = {}
        
    def deploy_ml_services(self):
        """Deploy all ML services according to configuration."""
        
        # Deploy Feature Engine
        if self.config["ml_services"]["feature_engine"]["enabled"]:
            self._deploy_feature_engine()
        
        # Deploy Discovery Strategies
        if self.config["ml_services"]["discovery_strategies"]["enabled"]:
            self._deploy_discovery_strategies()
        
        # Deploy Dimensional ML
        if self.config["ml_services"]["dimensional_ml"]["enabled"]:
            self._deploy_dimensional_ml()
        
        # Deploy Regime Detection
        if self.config["ml_services"]["regime_detection"]["enabled"]:
            self._deploy_regime_detection()
        
        # Deploy Adaptive Learning
        if self.config["ml_services"]["adaptive_learning"]["enabled"]:
            self._deploy_adaptive_learning()
        
        print("All ML services deployed successfully")
    
    def _deploy_feature_engine(self):
        """Deploy Feature Engine service."""
        
        feature_engine = FeatureEngine(self.config["ml_services"]["feature_engine"])
        
        # Deploy as microservice
        service_config = {
            "name": "feature_engine",
            "port": 8001,
            "replicas": 2,
            "resources": {
                "cpu": "500m",
                "memory": "1Gi"
            }
        }
        
        self.deployed_services["feature_engine"] = {
            "instance": feature_engine,
            "config": service_config,
            "status": "deployed"
        }
    
    def health_check_all_services(self) -> Dict[str, bool]:
        """Perform health check on all deployed services."""
        
        health_status = {}
        
        for service_name, service_info in self.deployed_services.items():
            try:
                # Perform health check
                service_instance = service_info["instance"]
                
                if hasattr(service_instance, "health_check"):
                    is_healthy = service_instance.health_check()
                else:
                    # Default health check
                    is_healthy = True
                
                health_status[service_name] = is_healthy
                
            except Exception as e:
                logger.error(f"Health check failed for {service_name}: {e}")
                health_status[service_name] = False
        
        return health_status
```

---

## Conclusion

The Alpha Engine ML Services provide a comprehensive, production-ready machine learning pipeline that:

1. **Transforms Raw Data** into predictive features through sophisticated feature engineering
2. **Generates Trading Signals** using multiple discovery strategies with proven edge
3. **Applies Consensus Mechanisms** to combine signals into unified predictions
4. **Uses Dimensional Tagging** to track performance across market conditions
5. **Detects Market Regimes** to adapt predictions to current market environment
6. **Learns Continuously** from outcomes to improve future predictions
7. **Manages Model Lifecycle** with comprehensive registry and monitoring
8. **Analyzes Performance** across multiple dimensions for continuous improvement

The system is designed for:
- **Scalability**: Microservices architecture with horizontal scaling
- **Reliability**: Comprehensive monitoring and health checks
- **Adaptability**: Self-correcting mechanisms and regime-aware learning
- **Maintainability**: Clear service boundaries and comprehensive documentation
- **Performance**: Optimized algorithms and efficient data structures

This architecture enables Alpha Engine to maintain competitive edge in algorithmic trading through continuous learning and adaptation to changing market conditions.

---

*Document Version: 1.0*
*Last Updated: 2026-04-16*
*Next Review: 2026-05-16*
