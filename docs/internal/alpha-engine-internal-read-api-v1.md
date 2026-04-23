# Alpha Engine - Internal Read API (v1)

## Doc Sync Checklist (before release)

- Confirm every live route appears in `GET /openapi.json` and in the "Live endpoint catalog" section below.
- For any new/changed route, include method, path, query params, success shape, and error cases.
- Verify auth behavior (`X-Internal-Key`, `INTERNAL_READ_INSECURE`) is still accurate.
- Run one real call per critical endpoint (`/health`, `/ranking/top`, `/api/candles/{ticker}`, `/api/regime/{ticker}`) and update examples if shape changed.
- Keep candidate/future endpoints in "Could expose next" only; do not list them as live until implemented.

## Purpose

Provide fast, read-only access to computed trading intelligence from alpha-engine to trading-platform over a private network.

Consumer setup (HTTP client, secrets, checklist): see `trading-platform-alpha-engine-integration.md`.

## Base URL

- `http://alpha-engine.internal:<PORT>`
- Dev: `http://localhost:<PORT>`
- Prod: Railway private service URL

Not public-facing.

## Auth and access

Preferred request header:

```http
X-Internal-Key: <shared-secret>
```

Rules:

- If `INTERNAL_READ_KEY` is set, requests must include a matching `X-Internal-Key`.
- If `INTERNAL_READ_KEY` is not set, `INTERNAL_READ_INSECURE=1` allows local unsecured access.
- `/health`, `/docs`, `/openapi.json`, `/redoc` are exempt from key checks.

## Live endpoint catalog (current)

This section is the source of truth for what is currently exposed by `app/internal_read_v1/app.py` and `app/internal_read_v1/api_routes.py`.

### Root routes

| Method | Path | Query params | Description |
|---|---|---|---|
| GET | `/health` | none | DB connectivity health probe. |
| GET | `/ranking/top` | `limit` (1-500, default 50), `maxFragility` (0.0-1.0, optional), `tenant_id` (default `default`) | Latest ranking snapshot rows with score, conviction, attribution, regime, timestamp, plus edge/fragility overlays. |
| GET | `/ranking/movers` | `limit` (1-200, default 50), `tenant_id` (default `default`) | Movers between latest two ranking snapshots. |
| GET | `/ticker/{symbol}/why` | `limit` (1-100, default 10), `tenant_id` (default `default`) | Ticker explainability panel. |
| GET | `/ticker/{symbol}/performance` | `window` (`30d`, `60d`, `90d`; default `30d`), `tenant_id` | Per-ticker performance block for requested window. |
| GET | `/admission/changes` | `hours` (1-168, default 24), `tenant_id` | Recent admission/candidate-queue changes. |

### `/api/*` routes

| Method | Path | Query params | Description |
|---|---|---|---|
| GET | `/api/tickers` | `tenant_id`, `q` | List tickers, optional substring filter. |
| GET | `/api/quote/{ticker}` | `tenant_id` | Latest quote from `price_bars` (`1m` -> `1h` -> `1d`). |
| GET | `/api/history/{ticker}` | `tenant_id`, `range`, `interval` | Time series close points (`points[]`) with chosen timeframe. |
| GET | `/api/candles/{ticker}` | `tenant_id`, `range`, `interval` | OHLCV candles (`candles[]`) from `price_bars`. |
| GET | `/api/company/{ticker}` | `tenant_id` | Company profile/fundamental metadata payload. |
| GET | `/api/stats/{ticker}` | `tenant_id` | Snapshot stats (price, day change, 52w range, volume, etc). |
| GET | `/api/regime/{ticker}` | `tenant_id` | Regime read model from daily bars (`risk_on`/`risk_off` + SMA fields). |
| GET | `/api/strategies/catalog` | `tenant_id`, `status`, `track`, `active_only`, `limit` | Active strategy catalog with status/champion/score metadata. |
| GET | `/api/strategies/{strategy_id}/stability` | `tenant_id` | Stability drift metrics for one strategy (backtest vs live). |
| GET | `/api/performance/regime` | `tenant_id` | Aggregated performance by regime. |
| GET | `/api/consensus/signals` | `tenant_id`, `limit`, `min_p_final`, `ticker` | Latest consensus overlaps per ticker with `agreementBonus` and `pFinal`. |
| GET | `/api/ticker/{symbol}/attribution` | `tenant_id`, `limit` | Event attribution details from `scored_events` (`conceptTags`, `explanationTerms`, materiality). |
| GET | `/api/ticker/{symbol}/accuracy` | `tenant_id` | Historical directional hit-rate and avg residual alpha for ticker outcomes. |
| GET | `/api/system/heartbeat` | `tenant_id`, `limit` | Latest loop heartbeats per loop type (`live`/`replay`/`optimizer`). |
| GET | `/api/predictions/runs/latest` | `tenant_id`, `timeframe` | Most recent prediction run plus run health/trust diagnostics (`runStatus`, `runQuality`, reason codes). |
| GET | `/api/recommendations/latest` | `tenant_id`, `limit`, `mode`, `preference` | Top recommendation rows. |
| GET | `/api/recommendations/best` | `tenant_id`, `mode`, `preference` | Single best recommendation row. |
| GET | `/api/recommendations/{ticker}` | `tenant_id`, `mode` | Recommendation row for one ticker. |
| GET | `/api/recommendations/under/{price_cap}` | `tenant_id`, `mode`, `preference`, `limit` | Top recommendation rows with `entryZone[1] <= price_cap`. |

## Regime endpoint contract

`GET /api/regime/{ticker}`

Success response shape:

```json
{
  "ticker": "SPY",
  "regime": "risk_on",
  "score": 0.74,
  "asOf": "2026-04-21",
  "sma20": 548.2,
  "sma200": 531.7,
  "close": 552.1,
  "confirmedBars": 2
}
```

Rules:

- `regime` is exactly `risk_on` or `risk_off`.
- `asOf` is last daily bar date used (`YYYY-MM-DD`).
- `confirmedBars` is consecutive bars agreeing with current regime, capped at 5.
- If fewer than 200 daily bars are available, returns `422` with `{"error":"insufficient_history"}`.

## Intelligence endpoint examples

`GET /api/strategies/catalog?active_only=true&limit=2`

```json
{
  "tenant_id": "default",
  "count": 2,
  "strategies": [
    {
      "id": "strat_1",
      "name": "Q Momentum",
      "version": "v1",
      "strategyType": "baseline_momentum",
      "mode": "balanced",
      "track": "quant",
      "status": "ACTIVE",
      "active": true,
      "isChampion": true,
      "backtestScore": 0.61,
      "forwardScore": 0.58,
      "liveScore": 0.57,
      "stabilityScore": 0.95,
      "sampleSize": 120,
      "createdAt": "2026-04-20T00:00:00+00:00",
      "activatedAt": null,
      "deactivatedAt": null
    }
  ]
}
```

`GET /api/strategies/{strategy_id}/stability`

```json
{
  "strategyId": "strat_1",
  "name": "Q Momentum",
  "version": "v1",
  "track": "quant",
  "status": "ACTIVE",
  "backtestAccuracy": 0.64,
  "liveAccuracy": 0.6,
  "stabilityScore": 0.94,
  "updatedAt": "2026-04-21T12:00:00+00:00"
}
```

`GET /api/performance/regime`

```json
{
  "tenant_id": "default",
  "regimes": [
    {
      "regime": "risk_on",
      "predictionCount": 140,
      "accuracy": 0.62,
      "avgReturn": 0.013,
      "updatedAt": "2026-04-21T12:00:00+00:00"
    }
  ]
}
```

`GET /api/consensus/signals?limit=2&min_p_final=0.7`

```json
{
  "tenant_id": "default",
  "count": 1,
  "signals": [
    {
      "ticker": "SPY",
      "regime": "risk_on",
      "sentimentStrategyId": "sent_1",
      "quantStrategyId": "strat_1",
      "sentimentScore": 0.72,
      "quantScore": 0.76,
      "ws": 0.5,
      "wq": 0.5,
      "agreementBonus": 0.05,
      "pFinal": 0.79,
      "stabilityScore": 0.93,
      "createdAt": "2026-04-21T13:00:00+00:00"
    }
  ]
}
```

`GET /api/ticker/{symbol}/attribution?limit=2`

```json
{
  "ticker": "SPY",
  "tenant_id": "default",
  "count": 1,
  "attribution": [
    {
      "scoredEventId": "se_1",
      "category": "macro",
      "materiality": 0.82,
      "direction": "up",
      "confidence": 0.78,
      "conceptTags": ["rates", "inflation"],
      "explanationTerms": ["soft landing", "risk appetite"]
    }
  ]
}
```

`GET /api/ticker/{symbol}/accuracy`

```json
{
  "ticker": "SPY",
  "tenant_id": "default",
  "sampleCount": 1,
  "hitRate": 1.0,
  "avgResidualAlpha": 0.006
}
```

`GET /api/system/heartbeat`

```json
{
  "tenant_id": "default",
  "loops": [
    {
      "loopType": "live",
      "status": "ok",
      "notes": "alive",
      "createdAt": "2026-04-21T09:12:00+00:00"
    }
  ]
}
```

`GET /api/predictions/runs/latest`

```json
{
  "id": "run_1",
  "tenant_id": "default",
  "timeframe": "1d",
  "regime": "risk_on",
  "ingressStart": "2026-04-21T09:00:00+00:00",
  "ingressEnd": "2026-04-21T09:10:00+00:00",
  "predictionStart": "2026-04-21T09:10:00+00:00",
  "predictionEnd": "2026-04-21T09:11:00+00:00",
  "createdAt": "2026-04-21T09:11:00+00:00",
  "runStatus": "DEGRADED",
  "runQuality": 0.8,
  "degradedReasons": ["STALE_CONSENSUS"],
  "ingestLatencySec": 600,
  "predictLatencySec": 60,
  "stalenessMinutes": 85,
  "consensusStalenessMinutes": 1660,
  "expectedUniverseCount": 4500,
  "rankingUniverseCount": 4200,
  "coverageRatio": 0.9333
}
```

Reason code set for `degradedReasons` (machine-readable):

- `STALE_PREDICTION_RUN`
- `STALE_CONSENSUS`
- `INGRESS_LATENCY_HIGH`
- `PREDICTION_LATENCY_HIGH`
- `MISSING_SYMBOLS_THRESHOLD_EXCEEDED`
- `LOW_UNIVERSE_COVERAGE`

`GET /api/recommendations/under/10?mode=balanced&preference=long_only&limit=10`

```json
{
  "tenant_id": "default",
  "mode": "balanced",
  "selectionPreference": "long_only",
  "priceCap": 10.0,
  "recommendations": [
    {
      "ticker": "ABC",
      "action": "BUY",
      "confidence": 73,
      "score": 0.7341,
      "risk": "Moderate",
      "horizon": "2-6 weeks",
      "entryZone": [9.73, 9.93],
      "thesis": ["Composite model bias is bullish"],
      "avoidIf": ["Breaks below 9.4868"],
      "mode": "balanced",
      "asOf": "2026-04-22T00:00:00+00:00",
      "selectionPreference": "long_only",
      "priceCap": 10.0
    }
  ]
}
```

`GET /ranking/top?limit=5&maxFragility=0.4`

```json
{
  "tenant_id": "default",
  "as_of": null,
  "as_of_note": "v1 returns latest ranking_snapshots batch only; as_of query not implemented",
  "rankedUnderDegradedRun": false,
  "runStatus": "HEALTHY",
  "runQuality": 1.0,
  "maxFragility": 0.4,
  "rankings": [
    {
      "ticker": "SPY",
      "score": 0.81,
      "conviction": 0.74,
      "regime": "risk_on",
      "timestamp": "2026-04-22T09:11:00+00:00",
      "attribution": {"macro": 0.62, "sentiment": 0.38},
      "edgeScore": 0.7812,
      "fragilityScore": 0.214
    }
  ]
}
```

## Query parameter notes

For history/candles:

- `range`: `1D`, `1W`, `1M`, `3M`, `1Y`, `5Y`, `MAX` (plus aliases).
- `interval`: normalized values include `1m`, `5m`, `30m`, `1h`, `1D`, `1W`, `1Mo`.

For recommendations:

- `mode`: `conservative`, `balanced`, `aggressive`, `long_term`.
- `preference`: `absolute`, `long_only`.
- `under/{price_cap}`: path param `price_cap` must be `> 0`.
- For under-price recommendations, cheap-stock ranking gives higher weight to company-quality semantics for lower price bands (`<=2`, `<=10`, `<=100`).

For intelligence routes:

- `strategies/catalog.status`: optional strategy status filter (for example `ACTIVE`, `PROBATION`, `DEGRADED`).
- `strategies/catalog.track`: optional track filter (`sentiment` or `quant`).
- `strategies/catalog.active_only`: defaults to `true`.
- `consensus/signals.min_p_final`: optional minimum `pFinal` threshold.
- `predictions/runs/latest.timeframe`: optional timeframe filter before selecting latest run.
- `ranking/top.maxFragility`: optional filter in `[0.0, 1.0]`; rows with higher `fragilityScore` are excluded.

## Error semantics

| Status | Meaning |
|---|---|
| 200 | Success |
| 400 | Invalid query param / bad request |
| 401 | Missing or invalid internal key (when key configured) |
| 404 | Resource/ticker not found |
| 422 | Validation or domain constraints (for example `insufficient_history`) |
| 503 | Server key not configured and insecure mode disabled |
| 500 | Internal server error |

Error body:

```json
{ "error": "message" }
```

## Response guarantees

- Numeric fields are JSON numbers, not strings.
- Arrays should be present and use `[]` when empty.
- Timestamps are returned as either ISO 8601 datetime strings or `YYYY-MM-DD` date strings depending on route semantics.
- Missing scalar values should use `null` rather than omitting keys.

## Run locally

Windows:

```powershell
.\scripts\start_internal_read_api.ps1
```

or:

```powershell
.\.venv\Scripts\python.exe -m app.internal_read_v1
```

Linux/macOS:

```bash
chmod +x scripts/start_internal_read_api.sh
./scripts/start_internal_read_api.sh
```

`npm run read-api` uses `scripts/run_read_api.cjs` so Ctrl+C reliably stops Python and frees the port on Windows.

## Environment

| Variable | Default | Notes |
|---|---|---|
| `ALPHA_DB_PATH` | `data/alpha.db` | Read DB path. |
| `INTERNAL_READ_KEY` | none | If set, enforces `X-Internal-Key` auth. |
| `INTERNAL_READ_INSECURE` | `0` | Set `1` for local no-key mode only. |
| `INTERNAL_READ_HOST` | `127.0.0.1` | Use `0.0.0.0` on hosted/public runtime. |
| `INTERNAL_READ_PORT` | `8090` | Explicit port override. |
| `PORT` | unset | Used if `INTERNAL_READ_PORT` is unset. |

## Could expose next (candidate endpoints, not live yet)

This section captures likely extensions so consumers can plan, but these are not implemented routes today.

- `GET /api/predictions/{ticker}`: recent predictions with confidence and strategy metadata.
- `GET /api/signals/{ticker}`: latest signal stack by strategy family.
- `GET /api/consensus/{ticker}`: per-ticker consensus decomposition beyond latest signal snapshot.
- `GET /api/regime/history/{ticker}`: daily regime series (`risk_on`/`risk_off`, score, confirmations).
- `GET /api/risk/{ticker}`: volatility and drawdown diagnostics.
- `GET /api/universe/active`: active universe membership snapshot.
- `GET /api/backtest/summary/{ticker}`: read-only precomputed backtest summary.

If any candidate is promoted to live, update this document and verify presence in `/openapi.json`.

## Design rules

1. Read-only only: no mutation or command endpoints.
2. View-oriented payloads: business-ready reads, not generic SQL/table dumps.
3. Stable contract: breaking changes require a new path or explicit versioning.
4. Low latency targets by keeping compute in upstream jobs/read models.

## One-line truth

This API should be boring, predictable, and hard to misuse: a private data pipe for decisions, not a public platform surface.
