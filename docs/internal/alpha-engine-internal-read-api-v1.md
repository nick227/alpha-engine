# Alpha Engine — Internal Read API (v1)

## Purpose

Provide fast, read-only access to computed trading intelligence from alpha-engine to trading-platform over a **private network**.

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
| `GET /ranking/top` | `limit` (default server-defined, e.g. 50), `as_of` (`YYYY-MM-DD`) — as-of date for the ranking snapshot |
| `GET /ranking/movers` | `limit` (e.g. default 50) |
| `GET /ticker/{symbol}/why` | `limit` (e.g. default 10) — caps embedded lists such as recent predictions |
| `GET /ticker/{symbol}/performance` | `window` (e.g. `30d` — rolling window label; server maps to supported windows) |
| `GET /admission/changes` | `hours` (e.g. default 24) — lookback for admission / queue activity |

Explicit params avoid hardcoded behavior and reduce accidental breaking changes when defaults shift.

## Core endpoints (only these five)

### 1. Top rankings

`GET /ranking/top?limit=50&as_of=YYYY-MM-DD`

Returns ranked tickers, `rank_score`, minimal metadata.

### 2. Ranking movers

`GET /ranking/movers?limit=50`

Returns risers (rank improving), fallers (rank declining), new entrants / dropped, snapshot timestamps.

**Snapshot semantics:** movers are computed **between the two most recent `ranking_snapshots` timestamps** for the tenant—not calendar days. Documented timestamps in the response identify which pair was used.

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

Thin HTTP layer (e.g. FastAPI) calling:

- `EngineReadStore`
- `explainability_read_model`
- `explainability_rank_trends`

Runs alongside alpha-engine.

## Evolution path

**v2 (only if needed):** stricter auth, caching, expand endpoints selectively.

**v3+:** move to Postgres, multiple consumers, consider streaming.

## One-line truth

The best internal API is boring, predictable, and hard to misuse — not an API platform, but a private data pipe exposing decisions.
