# Alpha Engine — Internal Read API (v1)

## Purpose

Provide fast, read-only access to computed trading intelligence from alpha-engine to trading-platform over a **private network**.

**Consumer setup (HTTP client, secrets, checklist):** see [trading-platform-alpha-engine-integration.md](./trading-platform-alpha-engine-integration.md).

## Base URL

- `http://alpha-engine.internal:<PORT>`
- **Dev:** `http://localhost:<PORT>`
- **Prod:** Railway private service URL

**Not public-facing.**

## Safety guard (not JWT)

**Recommended:** send a shared secret on every request:

```http
X-Internal-Key: <shared-secret>
```

When configured server-side, requests without a valid key are rejected. This is not a product auth layer—it prevents accidental public exposure and stray callers. Omit only in local dev if desired.

## Query parameters (explicit)

| Endpoint | Parameters |
|----------|------------|
| `GET /ranking/top` | `limit` (default 50), `tenant_id` (default `default`). **`as_of` not in v1** — latest snapshot only (see below). |
| `GET /ranking/movers` | `limit` (e.g. default 50) |
| `GET /ticker/{symbol}/why` | `limit` (e.g. default 10) — caps embedded lists such as recent predictions |
| `GET /ticker/{symbol}/performance` | `window` (e.g. `30d` — rolling window label; server maps to supported windows) |
| `GET /admission/changes` | `hours` (e.g. default 24) — lookback for admission / queue activity |

Explicit params avoid hardcoded behavior and reduce accidental breaking changes when defaults shift.

## Core endpoints (only these five)

### 1. Top rankings

`GET /ranking/top?limit=50&tenant_id=default`

Returns ranked tickers from the **latest** `ranking_snapshots` batch (`score`, `conviction`, `attribution`, `regime`, `timestamp`).

**v1 behavior:** **latest snapshot only.** Do not pass `as_of`; historical snapshot selection may come in a later version. The JSON body includes `as_of: null` and `as_of_note` explaining this.

### 2. Ranking movers

`GET /ranking/movers?limit=50`

Returns risers (rank improving), fallers (rank declining), new entrants / dropped, snapshot timestamps.

**Snapshot semantics:** movers are computed **between the two most recent `ranking_snapshots` timestamps** for the tenant—not calendar days. Documented timestamps in the response identify which pair was used.

**Rank direction:** lower **rank #** is better. **`rank_delta` = rank_today − rank_yesterday** (negative ⇒ improving). Each delta row should include a **`movement`** label (`↓ improving` / `↑ weakening` / `→ flat`). Responses include **`max_rank_depth`** (per-snapshot cap used when assigning rank). For multi-snapshot history charts, **`rank_norm` = rank / max_rank_depth** (~0 best, ~1 worst) keeps scales comparable.

### 3. Ticker explainability

`GET /ticker/{symbol}/why?limit=10`

Returns `rank_score` + components, strategy context, `multiplier_score`, admission status, temporal context (VIX, warnings), recent predictions.

### 4. Ticker performance

`GET /ticker/{symbol}/performance?window=30d`

Returns `win_rate` (rolling windows), `avg_return`, sample size (`n`), best / worst strategy, optional trend signal.

### 5. Admission changes

`GET /admission/changes?hours=24`

Returns newly admitted tickers, overrule swaps (in/out), recent `candidate_queue` changes.

## Response guarantees (contract rules)

Full TypeScript interfaces are optional; these rules are not:

- **Numbers:** all numeric fields are JSON numbers, not strings.
- **Timestamps:** ISO 8601 in **UTC** (e.g. `2026-04-18T12:00:00Z`).
- **Missing data:** use `null`, not omission of keys (so consumers can rely on stable keys).
- **Arrays:** always present; use `[]` when empty.

This removes frontend ambiguity without bloating the doc with every field name.

## Errors (minimal)

| Status | Meaning |
|--------|---------|
| `200` | Success |
| `400` | Bad request (invalid or unknown query params) |
| `404` | Ticker (or resource) not found |
| `500` | Internal error |

**Body (all error responses):**

```json
{ "error": "message" }
```

No full error envelope, codes matrix, or `requestId` required for v1.

## Latency expectation

Under normal load, **all endpoints should respond in under 200 ms** (server-side). That enforces pre-aggregated read models and stops heavy ad hoc queries from creeping in.

## Versioning stance

Path prefix `/v1` is optional. **Behavior:** breaking changes require either a **new path** (e.g. `/ranking/top-v2`) or **explicit parameter / contract versioning**—not silent changes to existing defaults.

## Design rules

1. **Read-only** — no mutations, no commands, no backtests.
2. **View-based (not raw data)** — expose computed results, not tables.
3. **Stable contract** — endpoints reflect business meaning, not schema.
4. **Low latency** — queries use pre-aggregated read models; see latency expectation above.

## Explicitly not in v1

- `GET /predictions`
- `GET /signals`
- `GET /market-data`
- `GET /risk/metrics`
- `POST /backtest`
- WebSockets
- Rate limiting
- SDKs
- Generic SQL access

## Implementation notes

**Package:** `app/internal_read_v1` — FastAPI adapter over `DashboardService` (same read models as the Streamlit UI).

**Run locally (Windows):**

```powershell
.\scripts\start_internal_read_api.ps1
```

Or: `.\.venv\Scripts\python.exe -m app.internal_read_v1` from repo root (`.env` is loaded by `__main__`).

**npm:** `npm run read-api` uses `scripts/run_read_api.cjs` so **Ctrl+C** reliably stops Python and frees the listen port on Windows (plain `npm` scripts can leave the old process bound to the port).

**Run locally (Linux / macOS):**

```bash
chmod +x scripts/start_internal_read_api.sh
./scripts/start_internal_read_api.sh
```

**Environment:**

| Variable | Default | Notes |
|----------|---------|--------|
| `ALPHA_DB_PATH` | `data/alpha.db` | Same DB your daily jobs update. |
| `INTERNAL_READ_KEY` | (none) | Required for production; use `INTERNAL_READ_INSECURE=1` only for local dev. |
| `INTERNAL_READ_HOST` | `127.0.0.1` | **Railway / public process:** set to `0.0.0.0` so the platform can route traffic. |
| `INTERNAL_READ_PORT` | `8090` | Optional explicit port. |
| `PORT` | — | Used if `INTERNAL_READ_PORT` unset (e.g. **Railway** injects `PORT`). |

**Railway / production:** deploy this service as a long-running process (see repo `Procfile`). Set `INTERNAL_READ_HOST=0.0.0.0`, `ALPHA_DB_PATH` to your persisted volume path, and `INTERNAL_READ_KEY`. Scheduled ingestion remains a **separate** job (cron / Railway cron / Task Scheduler); this service only **reads** the DB and serves HTTP to trading-platform.

**Local dev without a key (insecure):** `set INTERNAL_READ_INSECURE=1` — requests without `X-Internal-Key` are accepted; **do not use in production.**

**Health:** `GET /health` — no auth (for local probes). Other routes require `X-Internal-Key` when `INTERNAL_READ_KEY` is set.

Thin stack calling `EngineReadStore` / explainability modules via `DashboardService`.

## Evolution path

**v2 (only if needed):** stricter auth, caching, expand endpoints selectively.

**v3+:** move to Postgres, multiple consumers, consider streaming.

## One-line truth

The best internal API is boring, predictable, and hard to misuse — not an API platform, but a private data pipe exposing decisions.
