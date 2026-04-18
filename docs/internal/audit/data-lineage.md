# Data Lineage (Internal)

## Purpose
Map inputs → transforms → persistence → outputs so reviewers can audit provenance.

## Audience
- Auditors
- Developers

## When to use this
- You need to answer: “Where did this number come from?” or “What data was used?”

## Prereqs
- Repo access

---

## Lineage overview (current implementation)
```mermaid
flowchart LR
  A[Sources config: config/sources.yaml] --> B[Adapters: app/ingest/adapters/*]
  B --> C[Extractor + timestamp normalize]
  C --> D[Validator: timestamp + empty text rules]
  D --> E[Dedupe: SHA256 source|timestamp|text]
  E --> F[(SQLite: data/alpha.db events table)]
  F --> G[Routing: sentiment/quant/regime/crowd/alpha tracks]
  G --> H[Pipeline: scoring + MRA + predictions + consensus]
  H --> I[Exports: outputs/*.csv (demo)]
  I --> J[UI: app/ui/*]
```

## Persistence locations
- **Ingestion event store**: SQLite at `data/alpha.db` created/managed by `app/ingest/event_store.py`
  - Table: `events` (id, source, timestamp, ticker, text, tags, weight, numeric_json, created_at)
  - Additional tables for backfill and run ledgers exist (e.g. `ingest_runs`, `backfill_slice_markers`)
- **DB schema (runtime)**:
  - Ingestion tables are created/extended by `app/ingest/event_store.py`
  - Core engine tables are created by `app/db/repository.py`
  - Table **`trades`**: execution history; optional **`prediction_id`**, **`broker_order_id`**, **`source`** (`alpaca` \| `paper` \| `manual`) link fills to `predictions` for real vs simulated learning
  - Replay tables (raw/scored/mra/predictions/outcomes/signals) are created/extended by `app/ingest/replay_engine.py`
- **Prisma schema (optional tooling)**: `prisma/schema.prisma` exists in the repo but is not currently referenced by Python runtime imports
- **Exports** (demo runner): `outputs/` created by `scripts/demo_run.py`
- **Config inputs**: `config/sources.yaml`, `config/target_stocks.yaml`, `config/keys.yaml`

## Source ingestion chain (exact code path)
Live ingestion uses `app/ingest/async_runner.py`:
- `validate_sources_yaml()` reads and validates `config/sources.yaml` (Pydantic schema in `app/ingest/source_spec.py`)
- `safe_adapter_fetch()` calls an adapter resolved by `app/ingest/registry.py`
- `Extractor.normalize_many()` applies `extract.*` mappings and `normalize_timestamp()`
- `validate_events()` drops empty/bad timestamps and empty news text
- `Deduper.process()` sets deterministic SHA256 event IDs and drops duplicates within-run
- `EventStore.save_batch()` persists events to `data/alpha.db` (SQLite) using INSERT-OR-IGNORE semantics
- `EventRouter.route()` emits track buckets (sentiment/quant/regime/crowd/alpha)

## Demo prediction chain (exact code path)
The deterministic demo run is `scripts/demo_run.py`:
- Creates `RawEvent` items and synthetic `price_contexts`
- Calls `app/runtime/pipeline.py:run_pipeline()` which:
  - Scores events via `app/core/scoring.py:score_event()`
  - Computes MRA via `app/core/mra.py:compute_mra()`
  - Generates demo `Prediction` rows and demo consensus via `app/runtime/consensus.py`
  - Returns dicts that are exported to CSV by `scripts/demo_run.py`

## Verification steps
- Run: `python scripts/demo_run.py` and confirm expected CSVs exist in `outputs/`.
- Run: `python -m app.ingest.async_runner` and confirm `data/alpha.db` is created/updated.
- Verify dedupe is deterministic by checking that re-running ingestion does not inflate inserts for identical `(source_id, timestamp, text)` payloads.
