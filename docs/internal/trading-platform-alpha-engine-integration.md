# Trading platform ↔ Alpha Engine (read client guide)

This document is for **trading-platform developers** who need to **call alpha-engine’s internal read API** to consume rankings, explainability, and admission metadata. It describes how to connect, authenticate, and handle responses—**not** how alpha-engine is implemented.

For the full HTTP contract (fields, semantics, query parameters), see [alpha-engine-internal-read-api-v1.md](./alpha-engine-internal-read-api-v1.md).

---

## Architecture (what you integrate with)

```
alpha-engine (writes DB, serves read API)     trading-platform (your app)
        │                                                │
        │   HTTP GET + X-Internal-Key                    │
        └──────────────────────────────────────────────►
              loopback or private network only
```

- **You call HTTP only.** Do not open alpha-engine’s SQLite file from trading-platform; the read API is the supported integration surface.
- **Typical deployment:** both processes on the **same host**, server bound to **`127.0.0.1`** so it is not reachable from the public internet.
- **Data freshness:** alpha-engine updates the database on its schedule; your client reads **whatever snapshot** the API returns at request time.

---

## Base URL and port

| Environment | Base URL example |
|-------------|------------------|
| Local (same machine) | `http://127.0.0.1:8090` |
| Custom port | `http://127.0.0.1:<INTERNAL_READ_PORT>` |

Default listen port on the alpha-engine side is **8090** (`INTERNAL_READ_PORT`). **Coordinate the port** with whoever runs alpha-engine so your config matches.

---

## Authentication

Send the shared secret on **every** request to protected routes:

```http
X-Internal-Key: <same value as alpha-engine INTERNAL_READ_KEY>
```

- Trading-platform should store this value in **your own** secrets/config (environment variable, vault, etc.). You do **not** need `ALPHA_DB_PATH` in the trading app—that variable is for the alpha-engine process only.
- **`GET /health`** is intentionally **unauthenticated** so local probes work without the key. All **data** endpoints require the key when alpha-engine has `INTERNAL_READ_KEY` set.

**Local alpha-engine dev without a key:** if operators set `INTERNAL_READ_INSECURE=1`, the server accepts requests without `X-Internal-Key`. **Do not rely on this in production.**

---

## Quick verification

**1. Health (no key):**

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:8090/health" -UseBasicParsing | Select-Object -ExpandProperty Content
```

Expect JSON with `"status":"ok"` or `"degraded"` and a `db_path` echoing alpha-engine’s configured database path.

**2. Protected route (with key):**

```powershell
$h = @{ "X-Internal-Key" = "your-shared-secret" }
Invoke-WebRequest -Uri "http://127.0.0.1:8090/ranking/top?limit=5" -Headers $h -UseBasicParsing | Select-Object -ExpandProperty Content
```

---

## Endpoints to implement against

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness / DB reachability (no key) |
| `GET` | `/ranking/top` | Latest top rankings (**v1: no historical `as_of`**; see spec) |
| `GET` | `/ranking/movers` | Rank changes between latest snapshots |
| `GET` | `/ticker/{symbol}/why` | Explainability panel for one ticker |
| `GET` | `/ticker/{symbol}/performance` | Per-ticker performance (`window`: `30d`, `60d`, `90d`) |
| `GET` | `/admission/changes` | Admission / queue activity over `hours` |

OpenAPI is available from a running server at `/openapi.json` and interactive docs at `/docs` (localhost only when the server binds to loopback).

---

## HTTP status codes (client handling)

| Code | When | Suggested client behavior |
|------|------|---------------------------|
| `200` | Success | Parse JSON body |
| `400` | Bad query (e.g. invalid `window`) | Log, fix request |
| `401` | Missing or wrong `X-Internal-Key` | Fix secret / headers; do not retry blindly |
| `404` | No data for ticker (e.g. `/why`) | Treat as “unknown or no explainability for this symbol” |
| `503` | Server misconfigured (e.g. `INTERNAL_READ_KEY` not set and insecure mode off) | Alert ops; alpha-engine must be configured |
| `500` | Server error | Retry with backoff; alert if persistent |

Error bodies are JSON: `{ "error": "message" }` (see v1 spec).

---

## Configuration checklist (trading-platform)

1. **Base URL** — e.g. `http://127.0.0.1:8090` (or agreed host/port).
2. **Shared secret** — same value as alpha-engine `INTERNAL_READ_KEY`; inject via your env/secrets.
3. **Timeouts** — use a short client timeout (e.g. 2–5 s); the API is intended to be **sub-200 ms** server-side under normal conditions.
4. **TLS** — v1 assumes **trusted loopback or private network**; no TLS requirement on `127.0.0.1`. If you later expose over a network, use a tunnel or sidecar—**do not** bind the read API to `0.0.0.0` without additional controls.

---

## Minimal code examples

**Python (`httpx`):**

```python
import httpx

BASE = "http://127.0.0.1:8090"
KEY = "your-shared-secret"  # from env in real code

headers = {"X-Internal-Key": KEY}

with httpx.Client(base_url=BASE, headers=headers, timeout=5.0) as client:
    r = client.get("/ranking/top", params={"limit": 20})
    r.raise_for_status()
    data = r.json()
```

**Node (fetch):**

```typescript
const base = "http://127.0.0.1:8090";
const key = process.env.ALPHA_ENGINE_READ_KEY!;

const res = await fetch(`${base}/ranking/movers?limit=50`, {
  headers: { "X-Internal-Key": key },
});
if (!res.ok) throw new Error(await res.text());
const data = await res.json();
```

---

## What is explicitly out of scope for v1

- WebSockets, JWT product auth, rate limits, CORS for browsers, generic SQL, or writing back into alpha-engine through this API.

---

## Reference

- Full contract and field semantics: [alpha-engine-internal-read-api-v1.md](./alpha-engine-internal-read-api-v1.md)
- Alpha-engine operators: ensure the read API process is running, `INTERNAL_READ_KEY` is set for non-dev, and the database path matches the engine’s `ALPHA_DB_PATH`.
