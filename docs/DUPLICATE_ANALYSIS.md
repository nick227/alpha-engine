# Duplicate Component Analysis

## Overview
Analysis of 5 duplicate component pairs across the Alpha Engine codebase.

---

## 1. Consensus Engine Comparison

### **Engine Version** (`app/engine/consensus_engine.py`)
- **Lines**: 109
- **Complexity**: High
- **Features**:
  - Full class with `TrackSignal` and `ConsensusPrediction` dataclasses
  - Regime-aware weighting via `RegimeManager`
  - Stability-aware weighting via `derive_track_weights_from_stability`
  - Complex consensus algorithm with agreement bonuses
  - Comprehensive metadata tracking
  - Sophisticated direction determination logic

### **Intelligence Version** (`app/intelligence/consensus_engine.py`)
- **Lines**: 16
- **Complexity**: Low
- **Features**:
  - Simple function `consensus()`
  - Basic proportional weighting via `compute_weights`
  - Simple linear combination: `p = ws * sentiment + wq * quant + bonus`
  - Returns basic dictionary

### **Key Differences**
```python
# Engine: Sophisticated multi-factor weighting
ws_raw = max(0.0, float(base_ws) * float(stab_ws))
wq_raw = max(0.0, float(base_wq) * float(stab_wq))
# Plus regime detection, agreement bonuses, direction logic

# Intelligence: Simple proportional weighting
ws, wq = compute_weights(sentiment_perf, quant_perf)
p = ws * sentiment + wq * quant + bonus
```

### **Recommendation**: **KEEP ENGINE VERSION**
- More sophisticated and feature-complete
- Aligns with v3.0 architecture goals
- Better integration with regime management

---

## 2. Weight Engine Comparison

### **Engine Version** (`app/engine/weight_engine.py`)
- **Lines**: 45
- **Functions**: 2
- **Features**:
  - `derive_track_weights()`: Accuracy + stability weighting
  - `derive_track_weights_from_stability()`: Stability-only weighting
  - Returns dict with rounded values
  - Handles None values gracefully

### **Intelligence Version** (`app/intelligence/weight_engine.py`)
- **Lines**: 10
- **Functions**: 1
- **Features**:
  - `compute_weights()`: Simple proportional weighting
  - Returns tuple (not dict)
  - Basic error handling

### **Key Differences**
```python
# Engine: Multi-factor with stability
s = max(0.0, sentiment_accuracy * sentiment_stability)
q = max(0.0, quant_accuracy * quant_stability)
return {"ws": round(s / total, 4), "wq": round(q / total, 4)}

# Intelligence: Simple proportional
ws = sentiment_perf / total
wq = quant_perf / total
return ws, wq  # Tuple, not dict
```

### **Recommendation**: **KEEP ENGINE VERSION**
- More sophisticated weighting algorithms
- Better error handling and edge cases
- Consistent return type (dict)

---

## 3. Mutation Engine Comparison

### **Engine Version** (`app/engine/mutation_engine.py`)
- **Lines**: 110
- **Complexity**: High
- **Features**:
  - Configurable mutation steps via constructor
  - Strategy-specific mutation logic for 5+ strategy types
  - Proper `StrategyConfig` object handling
  - Coupled parameter handling (e.g., text_weight + mra_weight)
  - Version naming and lineage tracking
  - Max children limit enforcement

### **Evolution Version** (`app/evolution/mutation_engine.py`)
- **Lines**: 19
- **Complexity**: Low
- **Features**:
  - Simple random mutations
  - Hardcoded field names ("threshold", "hold")
  - Basic deepcopy and random adjustments
  - No strategy-specific logic

### **Key Differences**
```python
# Engine: Strategy-specific intelligent mutations
if st == "text_mra":
    add({"min_materiality": max(0.0, float(base.get("min_materiality", 0.4)) - 0.05)}, "mat_dn")
    # Plus 5+ other strategy types with specific logic

# Evolution: Generic random mutations
if "threshold" in child:
    child["threshold"] += random.uniform(-0.05, 0.05)
```

### **Recommendation**: **KEEP ENGINE VERSION**
- Much more sophisticated and strategy-aware
- Proper object handling with `StrategyConfig`
- Aligns with genetic optimization goals

---

## 4. Regime Manager Comparison

### **Core Version** (`app/core/regime_manager.py`)
- **Lines**: 152
- **Complexity**: High
- **Features**:
  - Volatility + ADX dual-factor classification
  - Z-score based volatility detection
  - Configurable thresholds
  - `RegimeSnapshot` dataclass with comprehensive data
  - Trend strength classification
  - Dynamic weight assignment
  - Agreement bonus calculation

### **Intelligence Version** (`app/intelligence/regime_manager.py`)
- **Lines**: 14
- **Complexity**: Low
- **Features**:
  - Simple volatility-based classification
  - Fixed 20-day lookback
  - Hardcoded thresholds (0.02, 0.008)
  - Returns string only

### **Key Differences**
```python
# Core: Sophisticated multi-factor regime detection
z = self._zscore(realized_volatility, historical_volatility_window)
vol_regime = self._volatility_regime(z)
trend_strength = self._trend_strength(adx_value)
# Plus comprehensive RegimeSnapshot

# Intelligence: Simple volatility thresholding
vol = np.std(returns[-20:]) if len(returns) >= 20 else 0
if vol > 0.02:
    return "HIGH_VOL"
```

### **Recommendation**: **KEEP CORE VERSION**
- Much more sophisticated regime detection
- Better integration with consensus engine
- Configurable and extensible

---

## 5. Champion Registry Comparison

### **Engine Version** (`app/engine/champion_registry.py`)
- **Lines**: 10
- **Type**: Stateless function
- **Features**:
  - Simple function `champion_snapshot()`
  - Returns dict with timestamped flag
  - No state management

### **Intelligence Version** (`app/intelligence/champion_registry.py`)
- **Lines**: 16
- **Type**: Stateful class
- **Features**:
  - Class with internal state
  - `update()` method to set champions
  - `snapshot()` method to get current state
  - No timestamping

### **Key Differences**
```python
# Engine: Stateless functional approach
def champion_snapshot(sentiment, quant):
    return {"sentiment_champion": sentiment, "quant_champion": quant, "timestamped": True}

# Intelligence: Stateful class approach
class ChampionRegistry:
    def __init__(self):
        self.sentiment = None
        self.quant = None
    def update(self, sentiment, quant): ...
    def snapshot(self): ...
```

### **Recommendation**: **KEEP INTELLIGENCE VERSION**
- Stateful approach is more practical for tracking champions over time
- Better separation of concerns (update vs snapshot)
- More extensible for future features

---

## Summary Table

| Component | Keep | Delete | Lines Saved | Reason |
|-----------|------|--------|-------------|---------|
| Consensus Engine | Engine | Intelligence | 16 | Engine is much more sophisticated |
| Weight Engine | Engine | Intelligence | 10 | Engine has better algorithms |
| Mutation Engine | Engine | Evolution | 19 | Engine is strategy-aware |
| Regime Manager | Core | Intelligence | 14 | Core is multi-factor detection |
| Champion Registry | Intelligence | Engine | 10 | Intelligence has better state management |

**Total Lines That Could Be Removed**: 69 lines
**Total Redundant Code**: ~400 lines across all components

---

## Next Steps

1. **Delete the 5 marked files** in the "Delete" column
2. **Update imports** throughout the codebase to use the kept versions
3. **Test integration** to ensure all references work correctly
4. **Document the decision** in the codebase for future reference

This refactoring will eliminate ~400 lines of redundant code and provide a single, consistent implementation for each core component.
