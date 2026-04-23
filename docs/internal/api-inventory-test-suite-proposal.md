# Read API + Data Health System Proposal

## Purpose

Build a reliable operational verification system for the internal read API where:

- **Priority 1: data integrity** (fresh, populated, trustworthy daily intelligence),
- **Priority 2: endpoint correctness** (status, schema, stability, no crashes),
- failures are quickly classified into **pipeline/data failures** vs **application/API failures**,
- endpoint checks remain trivial to maintain when routes change.

This proposal is aligned with the freshness model in `docs/public/api-data-warehouse-and-pipelines.md` (daily-batch first, loop-optional, warehouse-backed reads).

---

## Primary Operational Question

Every run should answer one question clearly:

**"Is the product delivering usable market intelligence to users right now?"**

Usable means:

- critical surfaces are populated (not empty unexpectedly),
- freshness is within expected daily/loop windows,
- sentinel tickers (AAPL, SPY, QQQ) produce complete, non-stale outputs,
- API routes are reachable and payloads are valid.

---

## Current State: Are We Comprehensive Today?

Short answer: **not yet comprehensive**.

What is already good:

- We already have integration coverage for many `/api/*` routes and key auth/error paths.
- We already seed realistic SQLite data in test fixtures and validate selected payload fields.

Gaps that block "complete API testing":

- Several documented top-level endpoints are not covered by data-validating tests (`/ranking/movers`, `/ticker/{symbol}/performance`, `/admission/changes`).
- `GET /api/company/{ticker}` is not covered in existing integration tests.
- Empty-result behavior is not uniformly asserted across the full surface (some endpoints allow empty arrays legitimately, others indicate likely pipeline/data issues).
- Endpoint coverage is spread across multiple files with duplicated setup logic, making additions/removals higher-friction than needed.

---

## API Surface Inventory (Current Coverage Snapshot)

### `/api/*` endpoints

- Covered now: `/api/quote/{ticker}`, `/api/history/{ticker}`, `/api/candles/{ticker}`, `/api/stats/{ticker}`, `/api/regime/{ticker}`, recommendations endpoints, strategies/intelligence endpoints, heartbeat, latest runs.
- Missing now: `/api/company/{ticker}`, `/api/tickers` does not have robust data-quality assertions (search behavior only).

### Top-level endpoints

- Covered now: `/health`, `/ranking/top`, selected auth checks, `/ticker/{symbol}/why` not-found path.
- Missing now: success-path data validation for `/ranking/movers`, `/ticker/{symbol}/performance`, `/admission/changes`, and richer `/ticker/{symbol}/why` positive-path content.

---

## Two-Lane Architecture

## Lane A: Data Health (Primary)

Objective: detect stale, missing, incomplete, or suspicious warehouse outputs before users see empty intelligence.

Checks should cover:

- pipeline freshness (`/api/system/heartbeat`, `/api/predictions/runs/latest`),
- data presence and completeness on critical surfaces:
  - recommendations,
  - rankings,
  - quotes/stats,
  - consensus,
- sentinel-symbol integrity (AAPL, SPY, QQQ):
  - quote/stats/regime present,
  - recommendation availability where expected,
  - no unexplained empty arrays on high-value views.

Outputs:

- a `data_health_status` (`PASS`, `DEGRADED`, `FAIL`),
- machine-readable failure reasons (`stale_runs`, `empty_recommendations`, `missing_sentinel_quote`, etc.),
- freshness metadata (`latest_run_age_hours`, `latest_heartbeat_age_minutes`).

## Lane B: API Smoke (Secondary)

Objective: verify endpoint behavior and resilience independent of data quality concerns.

Checks should cover:

- expected HTTP status codes,
- normalized error payload shape (`{"error": ...}`),
- required response keys/types,
- route-level crash prevention and auth behavior.

Outputs:

- `api_smoke_status` (`PASS`, `DEGRADED`, `FAIL`),
- route-level failure map with endpoint ids.

---

## Test Suite Design (Inventory-Driven)

## 1) Single Source of Truth: Endpoint Inventory Registry

Create one registry module, for example `tests/internal_read_inventory/endpoints.py`, that declares all endpoint specs.

Each endpoint entry should include:

- `id` (stable test case id),
- `method` + `path_template`,
- required seed profile (`market`, `intelligence`, `empty_warehouse`, etc.),
- lane ownership (`data_health`, `api_smoke`, or `both`),
- happy-path expected status and minimum payload assertions,
- error-path cases (query/path auth validation),
- empty-data policy:
  - `allowed` (expected, harmless),
  - `warn` (high-priority concern),
  - `fail` (must not be empty with valid seeded data).

Result: add/remove/adjust an endpoint by editing one entry, not writing custom test plumbing.

## 2) Reusable Seed Scenarios

Refactor current seed helpers into reusable scenario builders:

- `seed_market_baseline` (price bars, profiles, ranking, consensus),
- `seed_intelligence_baseline` (strategies, stability, runs, outcomes, heartbeats),
- `seed_minimal_empty` (healthy DB but intentionally sparse tables),
- `seed_stale_job` (heartbeat/runs indicate stale or absent recent writes).

This directly supports your priority on empty responses as potential daily-job failures.

## 3) Parameterized Contract Test Harness

Build parameterized harnesses that consume endpoint specs:

- `test_data_health_inventory`:
  - executes critical endpoints in data-health mode,
  - validates freshness and minimum-population rules,
  - enforces sentinel ticker integrity,
  - applies empty-data policy with high signal/noise discipline.
- `test_api_smoke_inventory`:
  - executes smoke checks for full API surface,
  - validates status and payload contracts,
  - confirms error semantics.

Then add a dedicated parameterized suite for declared negative/error cases.

## 4) Freshness-Aware Assertions

For endpoints fed by daily jobs (recommendations, rankings, consensus, run summaries):

- assert non-empty under baseline seeded scenarios,
- assert explicit degraded/empty semantics in stale/empty scenarios,
- require diagnostics that distinguish "no signal" vs "pipeline not running" using `/api/system/heartbeat` and `/api/predictions/runs/latest`.

Add explicit freshness thresholds in one config module, for example:

- `max_run_age_hours`,
- `max_heartbeat_age_minutes`,
- `max_allowed_empty_critical_surfaces`.

These thresholds should be environment-tunable but centrally defined.

## 5) Coverage Guardrail Test

Add one meta-test that compares:

- FastAPI route inventory (`app.routes`) for `GET` API endpoints,
- declared endpoint registry ids.

Fail if any route is undocumented in the registry. This prevents silent coverage drift.

---

## Priority Model

### Priority 1: Data Integrity

Every daily validation run should fail fast on:

- stale or missing prediction runs/heartbeats beyond threshold,
- empty recommendations on baseline/sentinel scenarios,
- empty ranking outputs where data is expected,
- missing sentinel quote/stats/regime payloads.

These are user-impacting intelligence failures even if HTTP status is 200.

### Priority 2: Endpoint Validity

Every endpoint should have explicit negative-path test cases for:

- invalid path/query values,
- missing records (`404`/`422` where designed),
- auth failures for protected routes.

Also assert standardized error payload shape (`{"error": ...}`), which is guaranteed by the app exception handler.

### Priority 2a: Empty-response semantics (within endpoint validity)

Classify endpoint emptiness behavior:

- **Expected-empty**: endpoint can validly return `[]` under sparse data.
- **Suspicious-empty**: usually indicates stale or failed daily pipeline.
- **Invalid-empty**: endpoint must not be empty in baseline seeded data.

This policy avoids false alarms while still catching trading-app empty-set regressions (especially recommendations).

---

## Daily Operational Signal

Produce one compact report artifact per run (JSON, optional markdown summary):

- `overall_status`: `PASS` | `DEGRADED` | `FAIL`
- `data_health_status`: `PASS` | `DEGRADED` | `FAIL`
- `api_smoke_status`: `PASS` | `DEGRADED` | `FAIL`
- `sentinel_status`:
  - `AAPL`, `SPY`, `QQQ` with per-surface checks
- `critical_surface_status`:
  - recommendations, rankings, quotes/stats, heartbeat, latest-run
- `failure_classification`:
  - `pipeline_data_failure` vs `api_application_failure`

Alert routing should key off this classification to reduce noisy pages.

---

## Reuse Opportunities (Existing Code/Types)

Yes, we can and should reuse existing code:

- Existing test fixture patterns (`TestClient`, env setup, seeded sqlite DB).
- Existing seed logic currently in:
  - `tests/test_internal_read_api_market.py`
  - `tests/test_internal_read_api_intelligence.py`
  - `tests/test_internal_read_api_regime.py`
- Existing API behavior contracts encoded in current assertions (status codes, key fields, recommendation semantics).

There are currently no shared strict response model classes for tests. Proposal:

- Introduce lightweight test-only `TypedDict`/validator helpers for repeated payload checks (not production model rewrites).
- Keep endpoint-specific deep assertions small and focused; centralize shared assertions (status, required keys, empty policy).

---

## Proposed Roadmap

### Milestone 1: Two-lane scaffolding and inventory

- Create endpoint registry and scenario matrix.
- Add lane ownership and criticality metadata per endpoint.
- Consolidate seed/client helpers into shared test utilities.
- Add meta-test that route inventory matches registry.
- Define centralized freshness thresholds and sentinel symbols (AAPL/SPY/QQQ).

### Milestone 2: Data Health lane (primary)

- Implement data-health parameterized suite for critical surfaces.
- Add stale/empty/missing-data scenarios with explicit failure classification.
- Implement daily scorecard artifact generation.
- Enforce non-empty recommendation/ranking checks in baseline scenarios.

### Milestone 3: API Smoke lane (secondary)

- Migrate all existing endpoint tests into inventory-driven parameterized tests.
- Add missing endpoints (`/api/company/{ticker}`, `/ranking/movers`, `/ticker/{symbol}/performance`, `/admission/changes`, positive `/ticker/{symbol}/why`).
- Standardize error payload assertions.

### Milestone 4: CI gating and alert quality

- Add markers (`inventory`, `smoke`, `freshness`) for fast/slow tiers.
- Make inventory suite required in CI.
- Route alerts by failure classification (data lane vs API lane).
- Add minimal "how to add endpoint" note in test module docstring.

---

## Acceptance Criteria for "Comprehensive and Reliable"

- 100% GET endpoint inventory coverage for documented internal-read API surface.
- Data Health lane fails CI on stale critical runs and invalid empty critical surfaces.
- Sentinel symbols (AAPL/SPY/QQQ) are validated daily across required surfaces.
- Every endpoint has at least one happy-path and one error-path smoke test.
- Empty-response behavior is explicitly classified and tested.
- Consumer-critical recommendations endpoints fail CI if baseline scenario returns empty sets.
- Route-inventory drift fails CI until registry is updated.

---

## Initial Implementation Shape (Recommended File Layout)

- `tests/internal_read_inventory/endpoints.py` (registry)
- `tests/internal_read_inventory/scenarios.py` (seed scenario builders)
- `tests/internal_read_inventory/assertions.py` (shared validators)
- `tests/test_internal_read_api_inventory.py` (parameterized suite + coverage guardrail)

This keeps endpoint additions/removals to a small, obvious surface and reduces duplicated test setup.
