# Ingestion Health Checks (Internal)

## Purpose
Provide repeatable checks for “Are sources ingesting correctly?” and “Why are events missing?”

## Audience
- Operators
- Developers on-call

## When to use this
- Live ingestion is producing fewer events than expected, or a specific source appears down.

## Prereqs
- Repo + environment access

---

## Quick smoke test (single run)
Run ingestion once and inspect per-source logs:
- `python -m app.ingest.async_runner`

What to look for in logs (`app/ingest/async_runner.py`):
- Per-source summary line:
  - `[ingest] mode=live source=... adapter=... raw=... valid=... unique=... dropped=... total_ms=...`
- If a source fails:
  - `error=unknown_adapter` (adapter name not registered)
  - `error=exception:...` (fetch/normalize issues)

## Validate source configuration shape
Sources are validated using Pydantic:
- Config: `config/sources.yaml`
- Schema: `app/ingest/source_spec.py`
- Validator: `app/ingest/validator.py` (`validate_sources_yaml`)

If ingestion fails at startup, fix YAML shape first (missing required keys, invalid types).

## Diagnose “events missing” (timestamp/text drops)
Event-level validation drops:
- Bad/missing timestamps (including epoch year 1970 fallbacks)
- Empty news text for `source_type == "news"`

Code: `app/ingest/validator.py`

## Dedupe behavior (why you see fewer inserts than rows)
Within a single run:
- `app/ingest/dedupe.py` assigns deterministic IDs as `SHA256(source_id|timestamp|text)` and drops duplicates in-memory.

Across runs:
- `app/ingest/event_store.py` uses SQLite INSERT-OR-IGNORE semantics, so re-ingesting identical IDs will not increase inserts.

## DB sanity check
Ingestion persists to:
- `data/alpha.db` (SQLite; created automatically)

The `events` table schema is created by `app/ingest/event_store.py`.

## Adapter smoke test (diagnose mode)
To quickly confirm adapter fetch + normalization for each enabled source:
- Default (network disabled): `python -m app.ingest.diagnose`
- Enable network: set `ALPHA_DIAGNOSE_ALLOW_NETWORK=1` then run `python -m app.ingest.diagnose`

Output columns include: `source_id`, `adapter`, `rows`, `timestamp_found`, `numeric_features`, `error`.

## Rate limiting (provider-level)
Provider is derived from adapter name:
- `app/ingest/runner_core.py:provider_for_adapter`

Rate limiting:
- `app/ingest/rate_limit.py:RateLimiter` (per-provider limits)

If you see chronic throttling or timeouts, increase poll intervals and reduce enabled sources while debugging.

