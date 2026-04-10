# Alpha Engine v2.6 Overlay

This overlay introduces the first recursive integration layer:

- live predictions continue as before
- replay scoring loop evaluates expired predictions
- regime detection starts with **volatility**
- trend strength is introduced as a **second-stage enhancer**
- dynamic track weighting uses recent regime-aware accuracy
- recursive optimizer scaffolding creates and ranks child strategy variants

## Primary additions

1. `app/core/regime.py`
   - volatility regime detection
   - optional trend-strength overlay
   - track weight calculation

2. `app/engine/replay_service.py`
   - finds expired predictions
   - computes realized outcomes
   - marks predictions as evaluated

3. `app/engine/weight_engine.py`
   - builds dynamic sentiment/quant weights from recent performance

4. `app/engine/recursive_optimizer.py`
   - mutates strategy configs
   - prepares tournament variants for replay/backtest

5. `app/engine/runner.py`
   - integrates regime + weighting into aggregate predictions

## Implementation note

This is an overlay-safe scaffold. It is designed to be merged into the existing v2.x project and then wired to the real database / price services.

## Rollout order

1. volatility regime on
2. replay service on
3. dynamic weights on
4. trend strength on
5. optimizer tournament loop on
