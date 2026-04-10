# Glossary (Internal)

## Purpose
Define internal terms and implementation-specific concepts used in audit/architecture docs.

## Audience
- Developers
- Auditors

## When to use this
- A term is used in internal docs but isn’t fully defined publicly.

## Prereqs
- None

---

## Terms (extend as needed)
- **Adapter**: A concrete ingestion integration selected by `config/sources.yaml`.
- **Deduplication**: Prevention of identical/near-identical events entering the pipeline.
- **Continuous learner**: Component that updates performance/weights based on outcomes.
- **Promotion engine**: Component that promotes/demotes strategy configurations (champions/challengers).
- **Canonical chart shape**: The unified data structure expected by UI charts (see `docs/archive/ARCHITECTURE_SETTLED.md`).
- **Idempotency key**: A deterministic hash used to upsert/ignore rows on rerun (common in replay/outcome writers).
- **Run id (`run_id`)**: A stable identifier for a replay/scoring window used to group artifacts and prevent duplicates.
- **Tenant id (`tenant_id`)**: A logical partition key used across tables; backfill/replay often uses a dedicated tenant (see `BACKFILL_TENANT_ID` in `app/ingest/backfill_runner.py`).
