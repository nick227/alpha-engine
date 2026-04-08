# Alpha Engine v3.0 Overlay

This overlay adds the **Recursive Alpha Engine**.

## Included
- Dual tracks evolve independently (Sentiment + Quant)
- Continuous live + replay + optimizer loop scaffolds
- Regime-aware dynamic weighting
- Candidate promotion state machine hooks
- Stability-based rollback safety net hooks
- Genetic lineage + audit trail tables
- Consensus signal model from track champions
- Minimal mission-control dashboard elements
- Autonomous improvement architecture scaffold

## Core flow
```text
live loop
→ replay loop
→ optimizer loop
→ choose track champions
→ build weighted consensus signal
→ promote / rollback through lifecycle guardrails
```

## Files
- `app/engine/recursive_alpha_engine.py`
- `app/engine/live_loop_service.py`
- `app/engine/replay_loop_service.py`
- `app/engine/optimizer_loop_service.py`
- `app/engine/champion_registry.py`
- `app/core/consensus_models.py`
- `app/ui/dashboard.py`
- `prisma/schema.prisma`
- `experiments/strategies/sentiment_champion_v3_0.json`
- `experiments/strategies/quant_champion_v3_0.json`

This is an overlay-safe scaffold for the v3.0 milestone.
