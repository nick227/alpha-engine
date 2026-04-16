# Regime-Aware Alpha Engine Implementation Guide

## Overview

This guide maps the regime-aware framework directly into your Alpha Engine structure for a 1-pass implementation. The system transforms from "take all valid signals" to "only act when market conditions support the signal type."

## Architecture Changes

### 1. New Regime Model (`app/core/regime_v3.py`)

**2-Axis Classification:**
- **Trend Axis**: BULL/BEAR/CHOP based on moving averages
- **Volatility Axis**: COMPRESSION/NORMAL/EXPANSION based on ATR percentiles

**Key Classes:**
- `RegimeClassifierV3`: Classifies market regime
- `SignalGating`: Implements signal gating logic
- `QualityScoreV3`: Enhanced quality scoring
- `PositionSizerV3`: Quality-based position sizing

### 2. Signal Gating Rules

| Strategy Type | Allowed Regimes | Blocked Regimes |
|---------------|----------------|-----------------|
| **Volatility Breakout** | (BULL, EXPANSION), (BEAR, EXPANSION) | All COMPRESSION, CHOP + NORMAL |
| **Momentum** | (BULL, NORMAL/EXPANSION), (BEAR, NORMAL/EXPANSION) | CHOP |
| **Mean Reversion** | (CHOP, COMPRESSION) | EXPANSION |

### 3. Enhanced Quality Score

```
Q = 0.30 * signal_strength
  + 0.25 * regime_alignment
  + 0.20 * volatility_quality
  + 0.15 * agreement_score
  + 0.10 * liquidity_confidence
```

### 4. Position Sizing Formula

```
position_size = base_size * (Q²)
```

This pushes capital into top decile signals only.

## Implementation Steps

### Step 1: Integration Points

**File: `app/trading/alpha_integration.py`**
- Added regime classifier initialization
- Added signal gating in `process_consensus_signals()`
- Added quality score calculation
- Added enhanced position sizer

**Key Changes:**
```python
# New imports
from app.core.regime_v3 import RegimeClassifierV3, SignalGating, QualityScoreV3, PositionSizerV3
from app.trading.position_sizing_v3 import EnhancedPositionSizer, RegimeAwarePortfolioManager

# In constructor
self.regime_classifier = RegimeClassifierV3()
self.position_sizer = EnhancedPositionSizer()
self.portfolio_manager = RegimeAwarePortfolioManager(self.position_sizer)

# In signal processing
gated_signal = self._apply_regime_gating(trade_signal, consensus)
quality_score = self._calculate_quality_score(gated_signal, consensus)
```

### Step 2: Volatility Breakout Strategy

**File: `app/discovery/strategies/volatility_breakout.py`**

**Core Logic:**
1. Volatility must be in expansion (ATR > p80)
2. Price must be above/below moving averages (trend)
3. Volume confirmation (optional)
4. Momentum confirmation (optional)

**Expected Output:**
- 120-160 signals/year (vs 223 for unfiltered)
- Higher win rate from regime filtering
- Improved Sharpe from quality filtering

### Step 3: Enhanced Position Sizing

**File: `app/trading/position_sizing_v3.py`**

**Key Features:**
- Quality-score based allocations
- Regime concentration limits
- Portfolio-level risk management
- Capital efficiency optimization

### Step 4: Strategy Registry Update

**File: `app/discovery/strategies.py`**
- Added volatility breakout to STRATEGIES dict
- Added threshold configuration
- Auto-registration on import

## Expected Performance Improvements

### Before (Current System)
- Trade count: 223
- Win rate: 48%
- Sharpe: 0.8
- Single-strategy dependent
- Regime-blind

### After (Regime-Aware System)
- Trade count: ~145 (35% reduction)
- Win rate: ~60% (+12%)
- Sharpe: ~1.1 (+40%)
- Regime-aware
- Quality-weighted positions

### Key Improvements
1. **Trade Count Reduction**: From 223 to ~145 trades
2. **Win Rate Improvement**: 48% to 60%
3. **Sharpe Boost**: 0.8 to 1.1
4. **Capital Efficiency**: Top decile gets 3-5x more capital

## Validation Script

**File: `validate_regime_aware_system.py`**

**Tests:**
1. Regime classification accuracy
2. Signal gating effectiveness
3. Quality score discrimination
4. Position sizing impact
5. Expected performance improvements

**Run:**
```bash
python validate_regime_aware_system.py
```

## Migration Path

### Phase 1: Baseline Lock
1. Freeze current system as baseline
2. Add regime tagging without changing execution
3. Validate regime classification accuracy

### Phase 2: Quality Score Update
1. Replace existing quality score with enhanced version
2. Maintain current execution logic
3. Validate score discrimination

### Phase 3: Signal Gating
1. Add regime-based signal gating
2. Monitor trade reduction and win rate improvement
3. Adjust gating thresholds if needed

### Phase 4: Position Sizing
1. Implement quality-based position sizing
2. Monitor capital concentration and Sharpe improvement
3. Fine-tune position size limits

### Phase 5: Full Integration
1. Combine all components
2. Reintroduce other signals (momentum, mean reversion)
3. Optimize portfolio-level allocations

## Configuration

### Regime Classifier Settings
```python
RegimeClassifierV3(
    lookback_period=252,  # 1 year of data
)
```

### Position Sizer Settings
```python
EnhancedPositionSizer(
    base_position_size=0.02,      # 2% base position
    max_total_allocation=0.95,    # 95% max total
    min_position_size=0.005,      # 0.5% min position
    max_position_size=0.10,       # 10% max position
    use_squared_quality=True      # Q² weighting
)
```

### Volatility Breakout Settings
```python
config = {
    "min_atr_percentile": 0.80,     # Top 20% volatility
    "min_price_vs_ma50": 0.02,      # 2% trend threshold
    "min_ma50_vs_ma200": 0.05,      # 5% MA separation
    "volume_confirmation": True,     # Require volume
    "min_volume_zscore": 1.5,       # Volume spike
    "score_threshold": 0.60,        # Quality threshold
}
```

## Monitoring and Metrics

### Key Metrics to Track
1. **Regime Distribution**: How often each regime occurs
2. **Gating Effectiveness**: % of signals blocked by regime
3. **Quality Score Distribution**: Separation between good/bad signals
4. **Position Sizing Impact**: Capital concentration in top decile
5. **Performance Metrics**: Win rate, Sharpe, drawdown

### Alert Thresholds
- Win rate < 55%: Review regime alignment
- Sharpe < 1.0: Review position sizing
- Trade count > 200: Review gating thresholds
- Quality separation < 0.15: Review scoring components

## Troubleshooting

### Common Issues

**1. Too Few Signals**
- Check regime classification accuracy
- Verify ATR percentile calculations
- Adjust gating thresholds

**2. Poor Quality Score Separation**
- Review regime alignment scoring
- Check volatility quality calculation
- Adjust component weights

**3. Excessive Capital Concentration**
- Lower max_position_size
- Adjust use_squared_quality setting
- Add regime concentration limits

**4. Low Win Rate**
- Verify signal gating logic
- Check regime alignment scores
- Review strategy-specific rules

## Rollback Plan

If issues arise:
1. Disable regime gating: Set `allowed = True` in `SignalGating.gate_signal()`
2. Disable quality weighting: Set `use_squared_quality = False`
3. Revert to baseline: Comment out new components in `alpha_integration.py`

## Next Steps

1. **Run validation script** to verify implementation
2. **Paper trade test** with regime-aware system
3. **Monitor performance** for 30 days
4. **Fine-tune parameters** based on results
5. **Scale to production** when metrics meet targets

## Success Criteria

- Win rate: 55-60%
- Sharpe: >1.0
- Trade count: 120-160/year
- Quality score separation: >0.15
- Top/bottom allocation ratio: >3x

This implementation transforms Alpha Engine from a signal generation system into a true adaptive trading system with decision-making capabilities.
