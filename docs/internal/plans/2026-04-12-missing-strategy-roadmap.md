# Missing Strategy Roadmap

**Date**: 2026-04-12  
**Status**: Draft  
**Priority**: High

## Goal

Close the highest-value gaps in the current strategy set without expanding the platform in too many directions at once.

The top three additions are:

1. Explicit regime gating and routing
2. Volatility-native strategies
3. Cross-asset / breakout strategies with medium-horizon coverage

## Why These Three First

- They address the main weakness in the current system: strategies can fire without a strong decision layer that says when they should be trusted.
- They make better use of the existing strategy families rather than only adding more isolated predictors.
- They fit the current architecture: there is already partial regime infrastructure, adaptive weighting, macro inputs, and a strategy factory pattern.

## Current State

- Runtime strategies today are mainly event-driven sentiment, short-term momentum, mean reversion, and ML factor prediction.
- Regime logic exists in `app/core/regime.py` and `app/core/regime_manager.py`, but it is mostly used for weighting concepts rather than as a first-class prediction gate.
- `app/engine/weight_engine.py` can already apply regime-aware weights, but strategy families are not yet cleanly separated by regime intent.
- The current implementations are mostly short-horizon and do not explicitly model volatility trades, cross-asset relative strength, or structural breakouts.

## Priority 1: Explicit Regime Layer

### Objective

Turn regime detection into a real decision-maker that influences which strategies are allowed, boosted, or suppressed.

### Scope

- Add a canonical regime snapshot builder for runtime use.
- Classify at minimum:
  - volatility: `LOW | NORMAL | HIGH`
  - trend: `STRONG | NORMAL | WEAK` or `TRENDING | CHOP`
  - risk tone: `RISK_ON | RISK_OFF` if macro inputs are available
- Persist the regime snapshot on predictions, signals, and consensus rows where possible.
- Add strategy-family routing rules, for example:
  - momentum and breakout favored in trending conditions
  - mean reversion favored in chop or post-volatility-spike conditions
  - sentiment favored in high-volatility event-driven windows

### Implementation Notes

- Reuse `app/core/regime_manager.py` as the base classifier instead of creating a parallel system.
- Introduce a small runtime adapter that converts available bars and macro context into one normalized snapshot.
- Add optional gating metadata to `StrategyConfig`, for example:
  - `allowed_volatility_regimes`
  - `preferred_trend_regimes`
  - `min_regime_score`
- Apply regime gating before final prediction persistence, not only at consensus weighting time.

### Deliverables

- `app/engine/regime_service.py` or equivalent runtime wrapper
- strategy config support for regime preferences
- regime metadata attached to `feature_snapshot`
- tests proving strategies are suppressed or boosted correctly by regime

### Acceptance Criteria

- At least one existing momentum strategy is reduced or blocked in chop.
- At least one existing mean-reversion strategy is reduced or blocked in strong-trend conditions.
- Consensus outputs can show which regime was active when the signal was produced.

## Priority 2: Volatility-Native Strategies

### Objective

Promote volatility from a passive feature into an explicit source of signals.

### Strategy Candidates

- `vol_expansion_continuation`
  - Thesis: when realized volatility expands alongside directional price movement and range expansion, continuation is more likely than fade.
- `vol_crush_mean_reversion`
  - Thesis: after a volatility spike begins collapsing, overshot moves are more likely to mean-revert.

### Inputs

- realized volatility
- ATR expansion
- range expansion
- volume ratio
- gap size
- VIX level and short-term VIX change if available

### Implementation Notes

- These strategies should be independent strategy types, not only conditions inside ML.
- Use the existing `price_context` / `MRAOutcome` path first, then enrich with macro volatility features if the data is available.
- Keep the first version rule-based and auditable:
  - threshold-based triggers
  - explicit confidence formula
  - explicit horizon mapping

### Suggested Horizons

- fast tactical: `1d`
- medium tactical: `7d`

### Deliverables

- `app/strategies/technical/vol_expansion_continuation.py`
- `app/strategies/technical/vol_crush_mean_reversion.py`
- new experiment configs under `experiments/strategies/`
- factory registration in `app/engine/strategy_factory.py`

### Acceptance Criteria

- Both strategies can generate predictions in replay/backfill.
- Predictions include volatility-specific features in `feature_snapshot`.
- Strategy performance can be analyzed separately from generic momentum and mean reversion.

## Priority 3: Cross-Asset + Breakout Layer

### Objective

Add strategy logic that recognizes relative leadership and structural breakouts instead of relying only on short-term trend and reversion.

### Strategy Candidates

- `relative_strength_sector`
  - Thesis: a stock outperforming its sector ETF or benchmark under supportive market conditions is more likely to continue.
- `credit_equity_confirmation`
  - Thesis: equity upside is more trustworthy when credit risk is supportive; divergence is a warning signal.
- `range_breakout_continuation`
  - Thesis: multi-day range escape with volatility and volume expansion is a distinct setup from ordinary short-term momentum.

### Inputs

- stock return vs sector ETF return
- stock return vs SPY or QQQ
- HYG/LQD or related credit proxy trend
- breakout above rolling highs or below rolling lows
- volume expansion
- ATR or realized volatility expansion

### Medium-Horizon Goal

This priority should also introduce cleaner horizon separation:

- `1d`: tactical move
- `7d`: short swing
- `30d`: medium trend / structural follow-through

### Implementation Notes

- Start with one relative-strength strategy and one breakout strategy.
- Avoid mixing too many ideas into one strategy; keep cross-asset and breakout logic separable for attribution.
- Use benchmark and sector series already entering the data pipeline wherever possible.

### Deliverables

- one relative-strength strategy
- one breakout strategy
- config additions for benchmark and sector references where needed
- replay/backfill support for medium-horizon predictions

### Acceptance Criteria

- At least one strategy produces `30d` predictions.
- Relative-strength signals can explain their benchmark comparison in `feature_snapshot`.
- Breakout signals are distinguishable from baseline momentum in both logic and analytics.

## Recommended Sequence

### Phase 1

- Wire explicit regime gating into the runtime.
- Update existing strategies with regime preferences.
- Add tests for suppression and routing behavior.

### Phase 2

- Add the two volatility-native strategies.
- Replay and score them on existing data.
- Compare them against current momentum and mean-reversion families.

### Phase 3

- Add one relative-strength strategy and one breakout strategy.
- Extend horizon coverage to include a clear medium-term path such as `30d`.
- Evaluate whether consensus weighting improves once these families are separated.

## What Not To Do Yet

- Do not add many small overlapping indicators before the regime layer is active.
- Do not hide these gaps inside the ML strategy alone; the system needs explicit auditable signal generators.
- Do not merge cross-asset, breakout, and volatility behavior into one large hybrid rule set on the first pass.

## Success Criteria

After this roadmap is implemented:

- the system knows when to trust momentum, mean reversion, or sentiment
- volatility becomes a direct source of predictions
- relative leadership and structural breakout behavior are modeled explicitly
- horizon coverage is more intentionally separated
- consensus weighting has cleaner, more distinct signal families to work with
