# Alpha Engine v2.8 Overlay

This overlay adds the **Replay Scoring Loop (Memory Layer)**.

## Included
- Background replay worker
- Prediction outcome model
- Residual alpha + correctness scoring
- Strategy performance summarization
- Regime performance summarization
- Stability calculation helper
- Weight engine input refresh point
- Prisma schema additions for outcomes + performance

## Core flow
```text
predictions
→ replay worker waits until horizon expires
→ compute return / residual alpha / correctness
→ write PredictionOutcome
→ update StrategyPerformance
→ update RegimePerformance
→ refresh Weight Engine inputs
```

## Files
- `app/engine/replay_worker.py`
- `app/engine/performance_engine.py`
- `app/engine/weight_engine.py`
- `app/core/outcome_models.py`
- `prisma/schema.prisma`
- `experiments/strategies/hybrid_dual_track_v2_8.json`

## Notes
This is an overlay-safe scaffold. It introduces the memory loop and the feedback path required for v2.9 optimization and v3.0 recursion.
