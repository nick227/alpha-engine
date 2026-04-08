# Alpha Engine v2.3 Overlay-Safe Update

This zip contains **only modified/additive files** for the v2.3 update.

Focus of v2.3:
- improved **dual-track** analysis (`sentiment` + `quantitative`)
- continuous **backtest time slicing**
- explicit **live + backtest** parallel engine model
- rolling-window comparison helpers for prediction consistency
- strategy/track aggregate metrics that can be reused by the UI later

## Key idea

The engine now treats both tracks the same way:

```text
time window + track + strategies
→ predictions
→ outcomes
→ slice metrics
→ forward comparison
→ stability / drift analysis
```

## Added / Updated files

- `app/core/time_analysis.py`
- `app/core/track_aggregation.py`
- `app/engine/runner.py`
- `app/engine/backtest_service.py`
- `app/engine/live_service.py`
- `app/engine/continuous_learning.py`
- `app/ui/dashboard.py`
- `experiments/strategies/hybrid_dual_track_v1.json`
- `CHANGELOG_V2_3.md`

## Overlay-safe usage

Extract this zip into the project root and overwrite matching files.

## Intent

This is still a scaffold / foundation update.
It is designed to clarify the architecture and provide reusable analysis helpers,
not a fully production-wired trading system.
