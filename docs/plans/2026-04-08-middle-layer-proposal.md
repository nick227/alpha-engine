# Middle Layer Proposal (Dashboard ↔ Engine Data)

Date: 2026-04-08

## Why this exists

`app/ui/dashboard.py` is currently hard-coded UI placeholders. The engine runtime persists real artifacts into `data/alpha.db`, but there is no stable “read model” API for the UI to consume, and the persistence/schema story is currently inconsistent across:

- `app/core/repository_sql_old.py` (SQLite schema + `Repository`)
- `app/db/repository.py` (`AlphaRepository`, “Prisma-like” schema)
- `prisma/schema.prisma` (desired target schema)

We want a middle layer that:

1. Gives the dashboard real data + real option lists now.
2. Allows us to change/repair the underlying schema without rewriting Streamlit UI code repeatedly.
3. Lets us challenge the current schema and converge toward one canonical representation.

## Goals

- **UI reads through one API**: dashboard asks for “champion pick”, “recent signals”, “available tickers”, etc.
- **No engine imports in UI** (as much as possible): UI depends on a “read model” module, not on engine loops.
- **DB/schema volatility isolated**: schema quirks handled in the middle layer, not in `dashboard.py`.
- **Fast, cacheable**: safe to call repeatedly in Streamlit (supports `st.cache_resource`/`st.cache_data`).

Non-goals (for this phase):

- Full “write model” for the UI (start/stop loops, mutate champions, run backtests) — can come later.
- Perfect normalization of the data model — we’ll build toward it.

## Proposed architecture

```
Streamlit UI (app/ui/dashboard.py)
  └── DashboardService (app/ui/middle/dashboard_service.py)  [read model]
        └── EngineReadStore (app/ui/middle/engine_read_store.py)  [SQL queries]
              └── data/alpha.db
```

### Middle-layer responsibilities

**DashboardService** (domain-ish, UI oriented)

- Convert raw DB rows into view models the UI can render directly.
- Provide “option lists” for widgets (tickers, tenants, strategies, lookbacks).
- Provide “page aggregates” (champion + challenger + regime + consensus + recent signals).

**EngineReadStore** (data access / query layer)

- Encapsulate SQL and schema differences (missing columns, mixed schemas, optional tables).
- Provide small, composable query methods that return plain `dict`/dataclasses.

## UI API contract (v0)

The dashboard should only need these calls:

- `list_tenants() -> list[str]` (fallback to `["default"]`)
- `list_tickers(tenant_id) -> list[str]`
- `get_champions(tenant_id) -> {sentiment, quant}` (best picks)
- `get_challengers(tenant_id) -> {sentiment, quant}` (runner-up picks; optional)
- `get_latest_consensus(tenant_id, ticker) -> ConsensusSummary | None`
- `get_recent_signals(tenant_id, *, ticker=None, limit=…) -> list[SignalRow]`
- `get_loop_health(tenant_id) -> LoopHealthSummary`

This is intentionally read-only.

## Challenging the current schema (what’s wrong today)

### 1) “Three schemas” problem

We currently have 3 sources of truth that disagree:

- `prisma/schema.prisma` includes `Strategy.track`, `Strategy.status`, lineage, and richer metadata.
- `app/db/AlphaRepository` claims to “match Prisma”, but the live DB (`data/alpha.db`) does not.
- `app/core/repository_sql_old.py` is the runtime facade used by most of the engine; it creates tables that are a partial superset, and it uses JSON blobs for many structured fields.

This makes it hard to reason about:

- what columns exist at runtime
- which tables are expected to be populated
- how to join data reliably (e.g. “track” is inferred from `strategy_type` in code, but also exists as a column in Prisma)

### 2) Strategy identity and lineage are underspecified

Today, “track” is inferred via `_strategy_track(strategy_type)` rather than being first-class.

Recommended:

- Make `strategies.track` canonical (`sentiment|quant|consensus`)
- Add/standardize `strategies.status` (`CANDIDATE|PROBATION|ACTIVE|…`)
- Add `strategies.parent_id` to enable lineage queries and “champion vs challenger” UX

### 3) JSON as TEXT everywhere

Fields like `config_json`, `feature_snapshot_json`, `market_context_json` are stored as TEXT JSON.

This is acceptable for iteration, but we should:

- standardize JSON schema versions (e.g. `feature_snapshot_version`)
- avoid pushing UI logic into JSON parsing (middle layer should do it)
- promote frequently queried values to indexed columns (e.g. `regime`, `trend_strength`, `track`)

### 4) Weak constraints and timestamp inconsistency

- Foreign keys are disabled in `Repository` (`PRAGMA foreign_keys=OFF`).
- Timestamps are mixed (`...Z` vs `...+00:00` vs SQLite defaults).

Recommendation:

- Use UTC ISO-8601 with `Z` consistently, or store timestamps as epoch integers.
- Enable foreign keys once schema is stabilized, at least in non-hot-path read models.

## Proposed schema convergence plan

### Phase A (now): Add a middle layer, don’t break the engine

- Keep engine writing to `data/alpha.db` as-is.
- Build UI read models that tolerate missing columns and mixed tables.
- Centralize “track inference” and JSON parsing in the middle layer.

### Phase B (next): Make strategies table Prisma-compatible

Add columns to `strategies` (safe, additive):

- `track TEXT NOT NULL DEFAULT 'unknown'`
- `status TEXT NOT NULL DEFAULT 'CANDIDATE'`
- `parent_id TEXT NULL`
- `created_at TEXT`, `activated_at TEXT`, `deactivated_at TEXT`

Backfill:

- `track = inferred_from(strategy_type)` for existing rows
- `status = 'ACTIVE'` for `active=1` else `'ARCHIVED'` (or similar)

### Phase C: Consolidate repository implementations

Choose **one**:

- (Option 1) Runtime uses Prisma client everywhere (Python Prisma client)
- (Option 2) Runtime uses one SQLite repository facade that *exactly* matches `schema.prisma`

Deprecate the other repository class to avoid drift.

## Wiring the dashboard with real data (implementation note)

The dashboard should:

- show champions per track from DB-derived metrics
- show regime/consensus from latest consensus prediction for the selected ticker
- show “recent signals” from the most recent consensus predictions (fallback to any predictions)
- drive widget options from DB (distinct tickers; tenant list)

## Deliverables in this repo

- `app/ui/middle/dashboard_service.py`: single entrypoint for dashboard reads
- `app/ui/dashboard.py`: now renders real options + real data via the service
