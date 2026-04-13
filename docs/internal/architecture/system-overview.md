# System Overview (Internal)

## Purpose
Provide a concrete map of Alpha Engine subsystems, runtime modes, and the evidence trail they produce.

## Audience
- Developers
- Operators
- Auditors who want an implementation-level map

## When to use this
- You need to understand “what runs where,” what gets persisted, and which entrypoints to use for a goal.

## Prereqs
- Repo familiarity

---

## Big picture
```mermaid
flowchart LR
  cfg[config/*.yaml] --> ingest[Ingestion]
  ingest --> db[(SQLite: data/alpha.db)]
  db --> replay[Backfill + Replay]
  replay --> engine[Engine + Outcomes]
  engine --> analytics[Analytics (weights/consensus/promotions)]
  db --> ui[Streamlit UI]
  engine --> exports[outputs/ (demo CSVs)]
```

## Runtime modes (what you should run)

### 1) Deterministic demo (no network)
Goal: validate pipeline wiring and generate fast artifacts for documentation, demos, and regression checks.
- Run: `python scripts/demo_run.py`
- Writes: `outputs/scored_events.csv`, `outputs/mra_outcomes.csv`, `outputs/predictions.csv`, `outputs/strategy_performance.csv`
- Orchestrator: `app/runtime/pipeline.py`

### 2) Live ingestion (one-shot)
Goal: fetch enabled sources, validate/dedupe, and persist raw events.
- Run: `python -m app.ingest.async_runner`
- Reads: `config/sources.yaml`, provider keys via `config/keys.yaml` + env vars
- Writes: `data/alpha.db` table `events` (and ingest run ledgers)

### 3) Historical backfill + replay (evidence trail in DB)
Goal: build an auditable corpus of events → predictions → outcomes in SQLite.
- Run: `python -m app.ingest.backfill_cli backfill-range --start YYYY-MM-DD --end YYYY-MM-DD`
- Fetch path: `app/ingest/backfill_runner.py`
- Replay writer: `app/ingest/replay_engine.py` (writes `raw_events`, `scored_events`, `mra_outcomes`, `predictions`, `prediction_outcomes`, `signals`, `consensus_signals`, `loop_heartbeats`)

### 4) Window scoring + ranking (series-level evaluation)
Goal: score predicted series vs actual bars for a defined prediction window and rank strategies.
- CLI: `python -m app.engine.score_predictions_cli eval-window --range YYYY-MM-DD:YYYY-MM-DD --timeframe 1d --rank-limit 10`
- This creates/uses `prediction_runs` plus series scoring tables in SQLite.

### 5) Streamlit UI (dashboard + intelligence + audit)
Goal: evaluate results visually and inspect DB health.
- Run: `python -m streamlit run app/ui/app.py`
- Audit view: `python -m streamlit run app/ui/audit.py`
- DB location: `ALPHA_DB_PATH` (default `data/alpha.db`)

## Persistence model (what goes where)
- `outputs/` is primarily for demo/export workflows (CSV).
- `data/alpha.db` is the audit/evidence trail for ingestion, replay, predictions, outcomes, and UI views.

## Known sources of confusion (current repo)
- There are multiple schema “authors” for SQLite:
  - `app/ingest/event_store.py` (ingestion tables like `events`, ingest ledgers)
  - `app/db/repository.py` (engine tables like `predictions`, `prediction_outcomes`, `strategies`, etc.)
  - `app/ingest/replay_engine.py` (replay tables and idempotency keys)
- `prisma/schema.prisma` exists for optional tooling, but the runtime schema is created by Python code.
