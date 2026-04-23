# App Folder Action Sheet (Conservative Pass)

## Goal

Create cleanup clarity without behavior risk.  
This pass is intentionally modest: classify ownership, identify low-risk cleanup, and defer deletion until evidence is strong.

---

## Immediate Priority Targets (lowest risk, highest clarity)

| Target | Action | Why | Gate before deletion |
|---|---|---|---|
| `app/__pycache__` | Remove + ensure ignored | Build artifact only | None |
| `app/core/repository_sql_old.py` | Freeze now, verify no imports, then remove | Strong legacy signal; appears unreferenced | `rg` import scan + full test run |
| `app/engine/ranking.py` | Review against live ranking path | Likely superseded by newer ranking flow | Confirm no runtime/test imports |
| `app/models` | Audit imports, mark transitional or remove | May be replaced by `app/core/types.py` | Zero runtime/test references |
| `app/docs` | Move outside `/app` (or keep as docs-only) | Not runtime code; reduces `/app` noise | Links/paths updated |

---

## Keep as Core Runtime (active)

These folders are foundational and should remain first-class:

- `app/internal_read_v1` (internal API service)
- `app/discovery` (candidate/signal generation)
- `app/core` (domain logic and shared primitives)
- `app/engine` (orchestration, queues, ranking runs)
- `app/db` (persistence layer)
- `app/strategies` (strategy implementations)
- `app/ingest` (data intake)
- `app/ui` (operator/user interfaces)

---

## Folder-by-folder status matrix

| Folder | Status | Confidence | Notes |
|---|---|---|---|
| `app/internal_read_v1` | Keep (active) | High | Runtime entrypoint and tested API surface. |
| `app/discovery` | Keep (active) | High | Feeds downstream queue/ranking/recommendation artifacts. |
| `app/core` | Keep (active) | High | Shared domain logic used across engine/ingest/discovery. |
| `app/engine` | Keep (active) | High | Main orchestration and batch execution flows. |
| `app/db` | Keep (active) | High | Central repository/storage utilities. |
| `app/strategies` | Keep (active) | High | Strategy implementations consumed by engine. |
| `app/ingest` | Keep (active) | High | Data ingestion/backfill/adapters. |
| `app/ui` | Keep (active) | High | Streamlit/dashboard/ops entrypoints. |
| `app/cli` | Keep (active) | High | Operational command launcher. |
| `app/jobs` | Keep (transitional) | Medium | Daily orchestrators; retain and monitor use. |
| `app/ml` | Keep (transitional) | Medium | Active scripts/integration; boundary still evolving. |
| `app/runtime` | Keep (transitional) | Medium | Some overlap with engine orchestration; avoid abrupt removal. |
| `app/analytics` | Keep (transitional) | Medium | Reporting/feedback paths used in some flows. |
| `app/paper` | Keep (transitional) | Medium | Used for paper-trading workflows. |
| `app/read_models` | Keep (transitional) | Medium | Supports write/read model persistence paths. |
| `app/regulatory` | Keep (transitional) | Medium | Integrated but not core path in all runs. |
| `app/simulation` | Keep (transitional) | Medium | Useful for validation/backtest utilities. |
| `app/testing` | Keep (transitional) | Medium | Internal adapters/helpers; not user-facing runtime. |
| `app/ai` | Review (possible legacy) | Medium | Mostly context material; unclear runtime ownership. |
| `app/evolution` | Review (possible legacy) | Medium | Appears specialized; low clear entrypoint usage. |
| `app/models` | Review (possible legacy) | Medium | Potentially superseded by richer core types. |
| `app/portfolio` | Review (possible legacy) | Low | Narrow footprint; verify before changes. |
| `app/docs` | Review (possible legacy) | Low | Non-runtime docs under runtime tree. |

---

## Explicit answers from this pass

### 1) Is `app/internal_read_v1` used? How?

Yes. It is an active runtime service:

- launched via `python -m app.internal_read_v1`
- used by startup scripts (`scripts/start_internal_read_api.ps1`, `scripts/run_read_api.cjs`)
- exposed as process entry in `Procfile`
- covered by internal API test suites.

### 2) Does `app/discovery` provide API value?

Yes. Discovery is upstream and materially affects API outputs:

- generates candidate/queue/admission artifacts
- engine jobs consume discovery outputs and write prediction/ranking/consensus tables
- internal API endpoints read those downstream tables.

### 3) Are `core` and `engine` overlapping?

Partially, yes. Safe boundary:

- **Keep in `app/core`**: types, scoring formulas, regime classifiers, repository interfaces, reusable utilities.
- **Keep in `app/engine`**: scheduled jobs, prediction runs, ranking batch jobs, queue processors, orchestration flows.

Rule to enforce: **engine imports core; core must not depend on engine**.

---

## Conservative cleanup plan

## Phase 1 (no behavior change)

1. Generate import/reference map for all `app/*`.
2. Mark unreferenced modules as review candidates.
3. Tag legacy candidates as `*_deprecated` (no deletion yet).
4. Add short README ownership notes per top-level folder.

## Phase 2 (structural clarity)

1. Consolidate repository access layer (`core.repository` vs `db.repository` boundary decision).
2. Move non-runtime docs out of `/app`.
3. Merge obvious duplicate responsibilities only where tests prove parity.

## Phase 3 (deletion, delayed)

Delete only after:

- at least 30 days of clean runtime,
- clean CI/test runs,
- zero import/reference evidence,
- no ops scripts depending on target modules.

---

## Final note

The biggest risk is usually mixed ownership, not raw file count.  
Prioritize clear boundaries and confidence gates over mass deletion.
