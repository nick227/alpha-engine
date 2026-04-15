# Alpha Engine v2.9 Overlay

This overlay adds the **Genetic Optimizer (Evolution Layer)**.

## Included
- Mutation engine creates strategy variants
- Tournament ranking over a rolling window
- Forward slice gate vs parent
- Candidate → probation → active lifecycle scaffold
- Lineage tracking via `parentId` and `version`
- Reaper logic for degraded strategies
- Auto-promotion + rollback guardrail scaffolds
- Prisma schema updates for strategy lineage + promotion events

## Core flow
```text
active strategy
→ mutation engine creates candidates
→ tournament ranks variants
→ forward slice gate compares winner vs parent
→ pass: candidate enters probation
→ probation success: active
→ degradation: rollback to parent / reap candidate
```

## Files
- `app/engine/mutation_engine.py`
- `app/engine/tournament_engine.py`
- `app/engine/promotion_gate.py`
- `app/engine/lifecycle_engine.py`
- `app/engine/reaper_engine.py`
- `app/engine/optimizer_service.py`
- `prisma/schema.prisma`
- `experiments/strategies/genetic_optimizer_v2_9.json`

This is an overlay-safe scaffold for the v2.9 evolution layer.
