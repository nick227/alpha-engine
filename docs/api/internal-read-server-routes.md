# Internal read server — route inventory

**Stack:** FastAPI (`app/internal_read_v1`), not Fastify; this list uses a terse route-table style for quick porting to Node/Fastify clients.

**Run:** `python -m app.internal_read_v1` or `npm run read-api` · default `127.0.0.1:8090` (`INTERNAL_READ_PORT` / `PORT`).

**Auth:** Header `x-internal-key: <INTERNAL_READ_KEY>` on all routes **except** `/health`, `/docs`, `/openapi.json`, `/redoc`. If `INTERNAL_READ_KEY` is unset, set `INTERNAL_READ_INSECURE=1` for local dev (allows requests without a key).

**DB:** `ALPHA_DB_PATH` (default `data/alpha.db`). **Profiles (optional):** `COMPANY_PROFILES_DIR` (default `data/company_profiles`) for `/api/company` and `marketCap` on `/api/stats`.

---

## Code layout (read paths)

| Area | Module |
|------|--------|
| `range` / `interval` parsing | `app/internal_read_v1/chart_range_interval.py` |
| Shared FastAPI `Depends` for history + candles | `app/internal_read_v1/chart_query_dep.py` (`ChartQueryParams`) |
| OHLCV / history / candles | `app/internal_read_v1/chart_ohlcv.py` |
| Quote, company, stats | `app/internal_read_v1/chart_market.py` |
| Re-exports (compat) | `app/internal_read_v1/bars_chart.py` |
| `/api` route wiring | `app/internal_read_v1/api_routes.py` |

**Tests:** `tests/test_internal_read_api.py` (auth + legacy routes), `tests/test_internal_read_api_market.py` (seeded `/api/*`).

---

## Core

| Method | Path | Query / notes |
|--------|------|----------------|
| `GET` | `/health` | No auth. `{ status, db_path }` |
| `GET` | `/docs` | Swagger UI |
| `GET` | `/openapi.json` | OpenAPI schema |
| `GET` | `/redoc` | ReDoc |

---

## Rankings & explainability

| Method | Path | Query / notes |
|--------|------|----------------|
| `GET` | `/ranking/top` | `limit` (1–500, default 50), `tenant_id` |
| `GET` | `/ranking/movers` | `limit` (1–200, default 50), `tenant_id` |
| `GET` | `/ticker/{symbol}/why` | `limit` (1–100, default 10), `tenant_id` · 404 if no queue + no predictions |
| `GET` | `/ticker/{symbol}/performance` | `window` optional `30d` \| `60d` \| `90d`, `tenant_id` |
| `GET` | `/admission/changes` | `hours` (1–168, default 24), `tenant_id` |

---

## Market / chart API (`/api`)

| Method | Path | Query / notes |
|--------|------|----------------|
| `GET` | `/api/tickers` | `tenant_id`, optional `q` — case-insensitive substring on symbol list |
| `GET` | `/api/quote/{ticker}` | `tenant_id` · latest bar (prefers `1m` → `1h` → `1d`) |
| `GET` | `/api/history/{ticker}` | `range`, `interval`, `tenant_id` · `{ ticker, range, interval, timeframe_used, points: [{ t, c }] }` · invalid `range`/`interval` → **400** |
| `GET` | `/api/candles/{ticker}` | `range`, `interval`, `tenant_id` · OHLCV `candles[]` · same **400** rules as history |
| `GET` | `/api/company/{ticker}` | `tenant_id` · profile JSON + fundamentals snapshot merge |
| `GET` | `/api/stats/{ticker}` | `tenant_id` · `price`, `dayChangePct`, `high52`, `low52`, `avgVolume` (30-session avg of daily bars), `marketCap`, `ath`, `ipoDate`, `yearsListed` |
| `GET` | `/api/recommendations/latest` | `tenant_id`, `limit` (1–100), `mode` (`conservative` \| `balanced` \| `aggressive` \| `long_term`) |
| `GET` | `/api/recommendations/best` | `tenant_id`, `mode` · single highest-conviction house recommendation |
| `GET` | `/api/recommendations/{ticker}` | `tenant_id`, `mode` · per-ticker house verdict |

`history` and `candles` share the same query parsing (`chart_range_interval` via `Depends`).

`recommendations/*` are built from a house model layer (`app/internal_read_v1/recommendations.py`) that combines ranking snapshots, consensus signals, momentum (`dayChangePct`), and candidate admission state into a final recommendation object.

### `/api/history` and `/api/candles` — `range`

`1D` · `1W` · `1M` · `3M` · `1Y` · `5Y` · `MAX` (aliases e.g. `1y`, `1mo` for month *range*).

### `/api/history` and `/api/candles` — `interval`

Omit to use defaults (e.g. `1Y` → `1D`, `MAX` → `1Mo`). Supports values such as `1m`, `5m`, `30m`, `1h`, `1D`, `1W`, `1Mo` — see `chart_range_interval.py`.

---

## Quick examples (base `http://127.0.0.1:8090`)

```http
GET /api/tickers?q=aap
GET /api/quote/NVDA
GET /api/history/NVDA?range=1Y&interval=1D
GET /api/stats/NVDA
GET /api/company/NVDA
GET /api/candles/NVDA?range=3M&interval=1D
GET /api/recommendations/latest?limit=10&mode=balanced
GET /api/recommendations/best?mode=balanced
GET /api/recommendations/NVDA?mode=conservative
```

```http
GET /ranking/top?limit=20
GET /ticker/NVDA/why
```
