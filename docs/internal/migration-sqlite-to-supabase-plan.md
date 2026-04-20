# Migration plan: SQLite → Supabase (PostgreSQL)

## Goal

Replace local `data/alpha.db` (SQLite) with [Supabase](https://supabase.com/) as the hosted database. Supabase exposes **PostgreSQL**, so this migration is effectively **SQLite → Postgres**, with Supabase providing auth, storage, dashboards, and connection pooling (optional) on top.

## Current state (this repository)

| Area | Details |
|------|---------|
| **Default path** | `data/alpha.db`; override via `ALPHA_DB_PATH` (e.g. Streamlit in `app/ui/app.py`). |
| **Runtime** | Python `sqlite3`; schema is created in code (not only Prisma). PrimaryDDL lives in `app/db/repository.py` (`AlphaRepository._create_schema`), plus additional tables from `app/ingest/event_store.py`, `app/ingest/replay_engine.py`, read-model writers, `app/engine/replay_sqlite.py`, `app/engine/trust_engine.py`, etc. |
| **Optional tooling** | `prisma/schema.prisma` targets SQLite for optional Prisma Client usage; runtime truth is Python. |
| **Documentation** | `docs/internal/audit/schema-map.md` maps subsystems → tables. |

**Implication:** Migration is not a single-file swap; it touches the unified repository, ingest/replay paths, analysis scripts, dev scripts, and tests that open SQLite directly.

## Target architecture

- **Database:** PostgreSQL (Supabase project).
- **Connection:** Prefer `psycopg` (v3) or `asyncpg` if you later async the hot paths; start with sync `psycopg` for smallest change surface.
- **Config:** `SUPABASE_DB_URL` or split `PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGSSLMODE=require` in `.env` (never commit secrets).
- **Schema management:** One of:
  - **A)** SQL migration files (e.g. `supabase/migrations` CLI, or plain versioned `.sql`), applied to Supabase; Python stops issuing ad-hoc `CREATE TABLE IF NOT EXISTS` at startup for production, or
  - **B)** Generate baseline from Prisma after switching `datasource` to `postgresql` and reconciling models with actual Python DDL (more work up front, better codegen).

Choose **A** if you want minimal Prisma churn; choose **B** if you want Prisma to own the schema long term.

## Phased work plan

### Phase 0 — Inventory and baseline

1. Export a **full list of tables** from a representative `alpha.db` (ingest + replay + scoring exercised): use `sqlite_master` / `.schema` as in `schema-map.md`.
2. List **SQLite-specific SQL**: `INSERT OR IGNORE`, `PRAGMA`, `AUTOINCREMENT`, `sqlite3` row patterns; grep for `sqlite3` and `INSERT OR` across `app/`, `scripts/`, `tests/`.
3. Decide **environments**: local dev (Supabase branch or docker Postgres vs cloud project), staging, production.

**Exit criteria:** Written table list + grep inventory tagged by priority (runtime vs offline scripts).

### Phase 1 — Postgres schema

1. For each table, define PostgreSQL types:
   - `TEXT` → `TEXT` or `UUID` where IDs are true UUIDs.
   - `REAL` → `DOUBLE PRECISION`.
   - SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` → `BIGSERIAL` or `UUID` + default, matching existing ID generation in code (`cuid`, `uuid4`, etc.).
2. Replace SQLite-only constructs:
   - `INSERT OR IGNORE` → `INSERT ... ON CONFLICT DO NOTHING` with explicit `UNIQUE`/`PRIMARY KEY` constraints.
   - Add missing `UNIQUE` indexes that SQLite was relying on implicitly.
3. Add **indexes** to match hot paths (tenant + time, foreign keys).
4. Apply migrations to a dev Supabase instance; validate with a small fixture dataset.

**Exit criteria:** All tables created; constraints documented; no dependency on SQLite `PRAGMA` for correctness.

### Phase 2 — Connection abstraction

1. Introduce a thin **DB module** (e.g. `app/db/connection.py`) that returns either:
   - a Postgres connection from env, or
   - during transition, optionally still SQLite behind a feature flag (see Phase 5).
2. Refactor **`AlphaRepository`** to use a Postgres connection:
   - Parameterize queries that differ between SQLite and Postgres, or use a minimal compatibility layer for `?` → `%s` placeholders (`psycopg` uses `%s`).
3. Update **`event_store`**, **`replay_engine`**, **`replay_sqlite`** (rename conceptually to “replay store”), trust engine, read models, and ML modules that take `sqlite3.Connection` to accept a **protocol** or abstract cursor interface, or pass the same Postgres connection type throughout.

**Exit criteria:** Core engine and ingest/replay paths run read/write against Postgres in dev.

### Phase 3 — Data migration (one-time)

1. **Order:** Migrate parent tables before dependents (events → scored → predictions → outcomes, etc.), or use `COPY`/pgloader with deferred constraints if available.
2. **Tools:** pgloader (SQLite → Postgres), or custom Python ETL for tricky columns (JSON columns, timestamps as TEXT).
3. **Validate:** Row counts per table, checksum samples, compare a subset of joins (e.g. predictions ↔ outcomes) between old SQLite export and Supabase.

**Exit criteria:** Acceptance queries pass; documented cutoff timestamp for incremental sync if needed.

### Phase 4 — Application and script sweep

1. **Runtime paths:** `app/` — replace `sqlite3.connect` with pooled or single Postgres connections; ensure long-running jobs use **connection pooling** (Supabase pooler URI for serverless-style workloads).
2. **Scripts:** `scripts/` and `dev_scripts/` — either migrate to shared DB helper or mark as “SQLite offline only” and leave on local export snapshots.
3. **Tests:** `tests/` using `:memory:` or temp SQLite — options:
   - run tests against a **throwaway Postgres** (CI service container), or
   - keep SQLite for unit tests only where logic is SQL-agnostic, and add integration tests against Postgres.

**Exit criteria:** CI green; documented which scripts require Postgres.

### Phase 5 — Cutover and rollback

1. **Dual-write / dual-read (optional):** short window mirroring writes to both DBs for validation (adds complexity; skip for small teams if downtime is acceptable).
2. **Cutover:** point `ALPHA_DB_PATH` replacement env (e.g. `DATABASE_URL`) to Supabase; restart workers/UI.
3. **Rollback:** keep last good SQLite snapshot and previous release; revert env to file path if needed.

**Exit criteria:** Production reads/writes on Supabase; rollback path tested once in staging.

### Phase 6 — Cleanup

1. Remove dead SQLite-only helpers; rename `replay_sqlite` if misleading.
2. Update `schema-map.md` and ops docs to reference Postgres/Supabase.
3. If using Prisma: switch `provider = "postgresql"` and regenerate client when schema is aligned.

## SQL and semantic differences to watch

| SQLite | PostgreSQL |
|--------|----------------|
| `?` placeholders | `%s` (`psycopg`) |
| `INSERT OR IGNORE` | `ON CONFLICT DO NOTHING` |
| `lastrowid` | `RETURNING id` |
| Dynamic `PRAGMA table_info` for migrations | `information_schema.columns` / `pg_catalog` |
| File DB concurrency | Connection limits, pooler, transactions |

## Security and operations

- Use **SSL** to Supabase; restrict IP if possible; rotate keys in Supabase dashboard.
- Enable **backups** and **Point-in-Time Recovery** per Supabase plan.
- Document **connection limits** (pool mode vs direct) for async workers and Streamlit.

## Risks specific to this codebase

- **Multiple DDL authors** — replay and ingest may create overlapping tables; Postgres needs a single ordered migration set to avoid drift.
- **Large single-file scripts** — `app/db/repository.py` is large; plan incremental refactors or split by domain to keep merges safe.
- **Offline analysis** — many `scripts/*` use raw `sqlite3`; batch-update or scope them explicitly to avoid half-migrated tooling.

## Suggested milestones (commit after each)

1. Schema migration SQL checked in + applied to dev Supabase.
2. `AlphaRepository` + one ingest path verified on Postgres.
3. Full replay pipeline verified on Postgres.
4. Data migration dry run + validation report.
5. Production cutover + doc updates.

---

## Appendix: quick discovery commands (SQLite, pre-migration)

Use while building the Postgres DDL list:

```powershell
python -c "import sqlite3; c=sqlite3.connect('data/alpha.db'); print([r[0] for r in c.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall()])"
```

See also `docs/internal/audit/schema-map.md` for subsystem → table mapping.
