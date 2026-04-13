# Dev Workflow (Internal)

## Purpose
Explain how to develop, test, and evolve the Alpha Engine codebase.

## Audience
- Developers

## When to use this
- You are making changes and need consistent build/test expectations.

## Prereqs
- Python environment and dependencies installed

---

## Local setup (baseline)
- Follow `README.md` quick start.
- Use `python start.py` for interactive runs.
- Use `python -m streamlit run app/ui/app.py` for full UI.

## Common workflows (current repo)

### Deterministic demo (no network)
Generates reproducible CSV artifacts under `outputs/`:
- `python scripts/demo_run.py`

### One-shot ingestion (live)
Fetches enabled sources from `config/sources.yaml`, validates/dedupes, writes to SQLite:
- `python -m app.ingest.async_runner`

### Adapter diagnose (fast sanity check)
By default, diagnose blocks network calls as a guardrail:
- `python -m app.ingest.diagnose`

To enable network fetch calls:
- Set `ALPHA_DIAGNOSE_ALLOW_NETWORK=1` then run `python -m app.ingest.diagnose`

### Backfill + replay (historical windows)
Use the backfill CLI:
- `python -m app.ingest.backfill_cli --help`
- Example: `python -m app.ingest.backfill_cli run --days 90`

### Prisma tooling (optional)
This repo includes a Prisma schema in `prisma/schema.prisma` (SQLite datasource `file:./alpha.db`), but the Python runtime currently creates/updates tables directly (SQLite via `app/db/repository.py`, `app/ingest/event_store.py`, `app/ingest/replay_engine.py`).

If you use Prisma tooling locally:
- `npm install`
- `npx prisma format`
- `npx prisma generate`

## Testing (baseline)
- Run pytest: `pytest`
- Prefer targeted tests for changed areas when iterating.

## Linting (baseline)
- Pylint config: `.pylintrc`
- Run on the codebase: `pylint app`

## Environment variables (high-signal)
- DB location for the Streamlit shell: `ALPHA_DB_PATH` (defaults to `data/alpha.db`, see `app/ui/app.py`)
- Sources config override for live ingestion: `ALPHA_SOURCES_YAML` (defaults to `config/sources.yaml`, see `app/ingest/async_runner.py`)
- Diagnose network gate: `ALPHA_DIAGNOSE_ALLOW_NETWORK` (see `app/ingest/diagnose.py`)

## Documentation updates
- Public docs must link to `docs/public/legal/disclaimer.md`.
- Internal audit pages should include verification steps and avoid marketing claims.
