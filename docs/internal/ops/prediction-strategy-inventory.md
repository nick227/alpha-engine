# Prediction Strategy Inventory

This document lists the prediction strategies currently present in the codebase.

## Active Runtime Strategies

- `text_mra_v1` (`text_mra`): Event-driven sentiment strategy that only predicts on selected positive catalyst categories, then combines text confidence with MRA score to issue short-horizon directional calls.
- `text_mra_v2` (`text_mra`): Event-driven sentiment strategy tuned for negative-risk categories, using slightly looser filters and heavier text weighting to catch downside moves.
- `baseline_momentum_v1` (`baseline_momentum`): Simple trend-following strategy that predicts up or down when short-term price trend exceeds a minimum threshold.
- `technical_vwap_v1` (`technical_vwap_reclaim`): Microstructure strategy that looks for VWAP reclaim or VWAP rejection with sufficient volume before issuing a directional signal.
- `technical_rsi_v1` (`technical_rsi_reversion`): Mean-reversion strategy that buys oversold conditions and sells overbought conditions using RSI-14 thresholds.
- `technical_bollinger_v1` (`technical_bollinger_reversion`): Mean-reversion strategy that reacts to large z-score moves outside a Bollinger-style threshold and predicts a snapback.
- `ml_factor_v1` (`ml_factor`): Factor-model strategy that loads the latest passing ML model, builds point-in-time features, and predicts direction only when feature coverage and model freshness pass guardrails.

## Hybrid / Champion / Optimizer Scaffolds

- `hybrid_dual_track_v1`: Early hybrid configuration that combines sentiment and quantitative tracks with equal base weights and an agreement bonus.
- `hybrid_dual_track_v2`: Regime-aware hybrid concept that shifts weighting based on volatility and trend-strength filters.
- `hybrid_dual_track_v2_7`: More explicit hybrid weighting map that favors sentiment in high volatility and quant in low volatility, with ADX-based adjustments.
- `hybrid_dual_track_v2_8`: Replay-aware hybrid scaffold intended to source track weights from the weight engine and incorporate replay feedback.
- `sentiment_champion_v3_0`: Champion-track scaffold for the sentiment side of the recursive engine, focused on high-volatility regimes.
- `quant_champion_v3_0`: Champion-track scaffold for the quant side of the recursive engine, focused on low-volatility regimes.
- `genetic_optimizer_v2_9`: Evolution scaffold that defines mutation fields, tournament metric, forward gate, probation, rollback, and reaper behavior for strategy optimization.

## Notes

- The runtime strategy factory currently instantiates: `text_mra`, `baseline_momentum`, `technical_vwap_reclaim`, `technical_rsi_reversion`, `technical_bollinger_reversion`, and `ml_factor`.
- The hybrid, champion, and optimizer JSON files are present as design/config artifacts, but they are not currently instantiated directly by `app/engine/strategy_factory.py`.

## Coverage Gaps

- Volatility regime strategies: Volatility is currently used as an input to ML and weighting logic, but there is no explicit strategy that trades volatility expansion, volatility compression, or vol-crush mean reversion directly.
- Cross-asset and relative-strength strategies: The system ingests broader market and macro context, but there is no dedicated signal generator for stock-vs-sector strength, sector rotation, or credit-vs-equity confirmation/divergence.
- Breakout and structural momentum strategies: The current set covers short-term momentum and mean reversion, but not true breakout logic such as multi-day range breaks, price-plus-volatility expansion, or new-regime continuation behavior.
- Explicit regime classifier: There is no standalone regime decision layer that classifies conditions like trend, chop, panic, or calm and then decides which strategy families should be trusted more or suppressed.
- Medium-term horizon strategies: Most implemented strategies are effectively short-horizon. There is limited explicit separation between fast tactical moves and slower multi-week trend behavior.

## Overlap To Watch

- `technical_rsi_v1` and `technical_bollinger_v1` are both mean-reversion strategies built around the same core idea: stretched price conditions snapping back toward normal. They are not identical, but they cover closely related behavior.

## Priority View

- The highest-leverage gap is explicit regime and volatility-aware behavior.
- That would improve strategy selection, reduce blind firing across market states, and create cleaner separation between when momentum, mean reversion, and news-driven signals should be trusted.
- Implementation roadmap: `docs/plans/2026-04-12-missing-strategy-roadmap.md`
