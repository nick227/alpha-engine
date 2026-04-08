# Alpha Engine v2.7 Overlay

This overlay-safe update introduces **Regime-Aware Scoring (Context Layer)**.

## Added
- `RegimeManager` with **volatility first, ADX second**
- LOW / NORMAL / HIGH volatility regime detection
- dynamic Sentiment vs Quant weighting
- weighted consensus formula:
  `P = Ws*Ss + Wq*Sq + bonus`
- regime stored on prediction payloads
- first adaptive dual-track signals

## Modified / Additive Files
- `app/core/regime_manager.py`
- `app/engine/consensus_engine.py`
- `app/engine/runner.py`
- `app/strategies/hybrid_dual_track_v2_7.py`
- `experiments/strategies/hybrid_dual_track_v2_7.json`

## Integration Notes
- This is overlay-safe and only includes additive / updated files.
- Existing persistence can store `prediction["regime"]` and `prediction["regime_snapshot"]`.
- Volatility drives the primary weighting; ADX lightly shifts the blend.
