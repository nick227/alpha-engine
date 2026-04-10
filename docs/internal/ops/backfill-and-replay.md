# Backfill and Replay Operations (Internal)

## Purpose
Describe how to backfill historical windows and (optionally) replay them through the engine for learning/evaluation.

## Audience
- Operators
- Developers

## When to use this
- You need to populate `data/alpha.db` with history or validate coverage over a time range.

## Prereqs
- Repo + environment access
- Keys configured for any network sources you enable

---

## Recommended interface
Use the Backfill CLI:
- `python -m app.ingest.backfill_cli --help`

## Backfill last N days
- `python -m app.ingest.backfill_cli run --days 90`

## Backfill a specific date range
- `python -m app.ingest.backfill_cli backfill-range --start 2024-02-20 --end 2024-03-20`

Useful flags (see `app/ingest/backfill_cli.py`):
- `--no-replay` to fetch without replay
- `--check-only` to do a no-network coverage check and exit
- `--force-replay` to ignore replay markers and rerun the full window
- `--force-refetch-source <source_id>` to ignore idempotency markers for one source

## Coverage and health reporting (DB-based)
The CLI includes DB-backed reports:
- Window idempotency summary:
  - `python -m app.ingest.backfill_cli ingest-runs --db data/alpha.db`
- Health KPIs over a date range:
  - `python -m app.ingest.backfill_cli ingest-health --start 2024-03-01 --end 2024-03-31`

## Resumability model (why reruns are safe)
Backfill/insertion is designed to be resumable:
- Deterministic event IDs (SHA256) + INSERT-OR-IGNORE means re-fetching the same windows should not multiply duplicates.
- Slice markers and run ledgers are stored in SQLite (see `app/ingest/event_store.py` tables like `ingest_runs` and `backfill_slice_markers`).

