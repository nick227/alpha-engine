# Alpha Engine — Internal Read API (v1)

## Purpose

Provide fast, read-only access to computed trading intelligence from alpha-engine to trading-platform over a **private network**.

## Base URL

- `http://alpha-engine.internal:<PORT>`
- **Dev:** `http://localhost:<PORT>`
- **Prod:** Railway private service URL

**Not public-facing.**

## Auth

- **None (v1)** — runs on private network only.
- **Optional later:** shared secret header.

## Core endpoints (only these five)

### 1. Top rankings

`GET /ranking/top`

Returns ranked tickers, `rank_score`, minimal metadata.

### 2. Ranking movers

`GET /ranking/movers`

Returns risers (rank improving), fallers (rank declining), new entrants / dropped, snapshot timestamps.

### 3. Ticker explainability

`GET /ticker/{symbol}/why`

Returns `rank_score` + components, strategy context, `multiplier_score`, admission status, temporal context (VIX, warnings), recent predictions.

### 4. Ticker performance

`GET /ticker/{symbol}/performance`

Returns `win_rate` (rolling windows), `avg_return`, sample size (`n`), best / worst strategy, optional trend signal.

### 5. Admission changes

`GET /admission/changes`

Returns newly admitted tickers, overrule swaps (in/out), recent `candidate_queue` changes.

## Design rules

1. **Read-only** — no mutations, no commands, no backtests.
2. **View-based (not raw data)** — expose computed results, not tables.
3. **Stable contract** — endpoints reflect business meaning, not schema.
4. **Low latency** — queries use pre-aggregated read models.

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

**v2 (only if needed):** auth, caching, expand endpoints selectively.

**v3+:** move to Postgres, multiple consumers, consider streaming.

## One-line truth

This is not an API platform — it is a private data pipe exposing decisions.
