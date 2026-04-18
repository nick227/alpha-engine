# Strategies overview (what you can use)

This project has two main strategy surfaces: **engine strategies** (event/price pipeline, configured as JSON under `experiments/strategies/`) and **discovery strategies** (feature-based screens registered in `app/discovery/strategies/registry.py`).

---

## Engine strategies (runtime pipeline)

These are built in `app/engine/strategy_factory.py`. Each is selected by the `strategy_type` field in a strategy config JSON. Typical location: `experiments/strategies/<name>.json`.

| `strategy_type` | Role |
|-----------------|------|
| `text_mra` | News/text signals combined with MRA outcome thresholds (materiality, relevance, categories). |
| `baseline_momentum` | Baseline momentum-style logic on price context. |
| `technical_vwap_reclaim` | VWAP reclaim setups. |
| `technical_rsi_reversion` | RSI mean reversion. |
| `technical_bollinger_reversion` | Bollinger-band mean reversion. |
| `technical_vol_expansion_continuation` | Continuation when volatility expands. |
| `technical_vol_crush_reversion` | Mean reversion after vol crush. |
| `technical_range_breakout_continuation` | Range breakout continuation. |
| `cross_asset_relative_strength` | Relative strength vs a benchmark. |
| `ml_factor` | ML-based factor model (`MLPredictor`). |
| `earnings_drift` | Earnings-related drift pattern. |

**How to leverage:** copy or edit a JSON under `experiments/strategies/`, set `strategy_type` to one of the values above, and run the pipeline with that config (same pattern as other experiment configs in the repo).

---

## Discovery strategies (screening / candidates)

These score symbols from **feature rows** (volatility, returns, percentiles, volume, etc.). They are registered in `STRATEGIES` in `app/discovery/strategies/registry.py` and used by discovery scoring (`score_candidates`, thresholds in `THRESHOLDS`).

| Name | Idea |
|------|------|
| `realness_repricer` | Deeply depressed 252d price percentile plus negative 63d drift — “repricer” recovery setup. |
| `narrative_lag` | Lagging 63d performance plus cheap vs 252d range — possible catch-up. |
| `silent_compounder` | Mid-band 20d vol plus positive 63d drift — steady compounder profile. |
| `ownership_vacuum` | Volume spike with relatively lower dollar liquidity — attention/flow anomaly. |
| `balance_sheet_survivor` | Drawdown (distress) plus lower vol — stabilization / bounce thesis. |
| `sniper_coil` | Strict AND gates: fear regime, compressed price/vol, volume spike, downtrend — rare, regime-locked setups. |
| `volatility_breakout` | Vol expansion with trend and confirmations — regime-filtered trend following. |

Default knobs for several of these live in `DEFAULT_STRATEGY_CONFIGS` in the same registry module.

**How to leverage:** run discovery with the desired strategy name, or wire configs through the discovery runner / DB strategy rows (see `dev_scripts/create_discovery_strategies.py` for example seed names). Some strategies are **promoted** into the prediction path with explicit queue rules in `app/engine/discovery_integration.py` (e.g. `silent_compounder`, `balance_sheet_survivor`).

---

## Scheduling / temporal layer

`TemporalCorrelationStrategy` (`app/discovery/strategies/temporal_correlation_strategy.py`) layers **time-based** context (sentiment, events, regimes, seasonality, etc.) on top of discovery-style analysis. Use it when you need timing and sizing informed by temporal correlation tooling (`scripts/analysis/temporal_correlation_analyzer.py`, `insights_engine.py`), not as a generic `strategy_type` in the small engine factory table above.

---

## Quick reference

- **Engine type strings:** `app/engine/strategy_factory.py` → `mapping` keys.
- **Discovery registry:** `app/discovery/strategies/registry.py` → `STRATEGIES`, `THRESHOLDS`, `DEFAULT_STRATEGY_CONFIGS`.
- **Example JSON configs:** `experiments/strategies/*.json`.
