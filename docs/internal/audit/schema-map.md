# Schema Map (Internal)

## Purpose
Provide a practical “what tables exist and who writes them” map for auditors and maintainers.

## Audience
- Auditors
- Developers
- Operators

## When to use this
- You need to understand what is persisted in `data/alpha.db` and which code paths produce it.

## Prereqs
- A populated `data/alpha.db` (run backfill/replay or ingestion first)

---

## SQLite location
- Default DB path: `data/alpha.db`
- Streamlit UI reads DB from: `ALPHA_DB_PATH` (defaults to `data/alpha.db`, see `app/ui/app.py`)

## Table families (by subsystem)

### Ingestion (live and backfill fetch ledgers)
Primary writer: `app/ingest/event_store.py`
- `events` (raw-ish ingested events for live/backfill fetch)
- `ingest_runs`, `ingest_run_stats` (idempotency and ingest health ledgers)
- `backfill_slice_markers`, `backfill_horizons` (coverage markers)

### Replay evidence trail (historical processing)
Primary writer: `app/ingest/replay_engine.py`
- `raw_events`, `scored_events`, `mra_outcomes`
- `predictions`, `prediction_outcomes`
- `signals`, `consensus_signals` (consensus may be placeholder depending on run mode)
- `loop_heartbeats` (run status/markers)

### Engine + evaluation utilities
Writers vary by path:
- Outcome schema helpers: `app/engine/outcome_resolver.py`
- Prediction scoring CLI uses: `app/engine/score_predictions_cli.py` and repository methods in `app/db/repository.py`
- Windowing tables: `prediction_runs`, `predicted_series_points` (created by `app/db/repository.py`)

### UI expectations (diagnostic)
The audit UI encodes expected tables/columns in:
- `app/ui/audit.py` (`SCHEMA_EXPECTATIONS`)

## How to inspect schema (quick commands)
From PowerShell:
- `python -c "import sqlite3; c=sqlite3.connect('data/alpha.db'); print([r[0] for r in c.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall()])"`
- `python -c \"import sqlite3; c=sqlite3.connect('data/alpha.db'); print(c.execute('PRAGMA table_info(prediction_outcomes)').fetchall())\"`

## Notes / gotchas (current repo)
- Multiple schema “authors” exist (ingest store, replay engine, unified repository). Table sets may differ depending on what you’ve run.
- `prisma/schema.prisma` exists for optional tooling, but the runtime schema is created by Python code (SQLite DDL in the modules above).

## “Which command creates which tables?” (practical)
- `python scripts/demo_run.py`:
  - Writes CSVs under `outputs/`
  - Does not guarantee any DB tables (demo pipeline persistence is not the primary artifact path)
- `python -m app.ingest.async_runner`:
  - Ensures ingestion tables exist (notably `events`) via `app/ingest/event_store.py`
- `python -m app.ingest.backfill_cli backfill-range ...`:
  - Ensures replay evidence-trail tables exist via `app/ingest/replay_engine.py`
- `python -m app.engine.score_predictions_cli ...`:
  - Ensures prediction run windowing/series scoring tables exist via `app/db/repository.py`
