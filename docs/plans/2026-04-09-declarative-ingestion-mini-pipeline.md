# Proposal: Single Declarative Ingestion Mini-Pipeline (Replace Per-Adapter Logic)

Date: 2026-04-09

## Intent

Move from "many adapters with custom Python logic" to **one ingestion mini-pipeline** where each source is primarily **declarative configuration**:

- how to fetch (transport + auth + optional pagination)
- how to normalize (row selection + field mapping)
- how to filter/dedupe (timestamp bounds, empty-text drop, idempotency)
- how to map into the canonical `Event` shape

This keeps behavior consistent, removes placeholder code, and lets us progressively enable real data by **swapping configs** instead of rewriting modules.

It also cleanly separates:

- **raw content ingestion** (events ledger)
- **derived features/enrichment** (macro features, company profiles, LLM enrichment, etc.)

---

## Why this is the right direction (for this repo)

The ingestion layer is already partly declarative:

- `config/sources.yaml` defines sources
- `app/ingest/extractor.py` maps rows into `Event`
- `app/ingest/backfill_runner.py` orchestrates slices, dedupe, persistence, replay

But the fetch step is still split across many adapters (and many are mocks). This proposal makes fetch + map behavior uniform and eliminates the incentive to keep writing one-off adapter logic.

---

## Target architecture (linear, shared behavior)

```
SourceSpec (YAML)
  -> Fetcher (shared implementation; kind is small)
      -> Row selection (optional pagination)
          -> Post-fetch normalization (shared)
              -> Canonical Event mapping (field mapping only)
                  -> Canonical validation + timestamp bounds
                      -> Idempotent event hash + dedupe
                          -> Persist Event ledger
```

Derived features are strictly out-of-band:

- company profiles (yfinance `.info`) -> `data/company_profiles/*.json`
- macro snapshots (close/returns/vol) -> derived context features
- LLM enrichment (entities/sentiment/taxonomy) -> enrichment tables/files

Ingestion remains: "write the raw truth we fetched."

---

## Non-negotiables / guardrails (to prevent config becoming code)

These are requirements for the refactor to deliver the intended benefits.

1. **Fetch kinds stay extremely small:** `http_json`, `rss`, `local_file` only (no explosion).
2. **No per-source custom logic:** config must not become a loophole for bespoke behavior.
3. **`Event` schema stays minimal:** `timestamp`, `text`, `source_id`, `source_type`, optional `ticker` only. No enrichment fields in ingestion (no sentiment here).
4. **Shared post-fetch normalization:** one place for timestamp normalization, trimming, empty-text drop, and row-shape sanitization.
5. **Extraction stays simple:** mapping paths only; resist "just one more helper" creep.
6. **Content validation enforces `text`:** `text` must be non-empty for content sources, enforced in validation before persistence.
7. **Pagination defaults to none:** pagination is optional and must default to `none` to avoid accidental loops.
8. **Global rate limiter is per provider:** centralized throttling keyed by provider (not per source).
9. **Backfill ordering uses mandatory `priority`:** every source must define priority so backfills remain deterministic.
10. **Stable source identity:** keep `source_id` stable and separate from any display/source name used in UI or provider metadata.
11. **Idempotent hashing before persist:** stable event IDs/hashes computed in the pipeline prior to DB writes.
10. **Strict separation of ingestion vs derived features:** no enrichment, symbol detection, or "business meaning" in ingestion.
11. **Filters remain generic:** timestamp bounds, dedupe, empty-text drop, and basic shape validation only.
12. **Dry-run mode exists:** validate configs and request construction without hitting external APIs.
13. **Source health logging exists:** per-slice metrics like rows fetched, rows emitted, rows dropped (and why).
14. **Mocks become impossible:** once config-based pipeline is active, mock adapters are removed/banned.
15. **Pipeline stays linear:** no branching logic encoded in config; config describes data movement only.
16. **Backfill runner remains conceptually unchanged:** we swap the fetch stage implementation, not orchestration semantics. Backfill slices must pass explicit `start/end` into fetch.

---

## Config schema changes (proposal)

Extend `SourceSpec` with an explicit `fetch` block and a constrained `extract` block.

### `fetch` block (data movement only)

```yaml
fetch:
  kind: http_json | rss | local_file
  method: GET
  url: "https://..."
  headers:
    User-Agent: "..."
  params:
    limit: 50
  auth:
    kind: bearer | basic | query_param | none
    key_name: "polygon"           # key_manager lookup
    header: "Authorization"       # for bearer
    query_param: "apiKey"         # for query-param auth
  pagination:
    kind: none | cursor | page | next_url   # optional
    cursor_path: "data.after"               # for cursor
    results_path: "data.children"           # where the rows are
  retry:
    max_attempts: 3
    backoff_s: 1.0
priority: 50  # lower runs earlier in backfills (fast -> slow)
```

### `extract` block (field mapping only)

Instead of expression evaluation, `extract` should be simple mapping:

```yaml
extract:
  timestamp_path: "created_at"
  ticker_path: "symbol"          # optional (may be absent)
  text_path: "headline"          # required for content sources
  numeric_features:
    score: "score"
```

If we need to build `text` from multiple fields (headline + summary), the only allowed config-driven operations are tiny shared primitives:

- `text_first_of: ["summary", "headline", "text"]`
- `text_join_paths: ["headline", "summary"]` with a fixed joiner owned by the pipeline (not configurable per source)

No arithmetic, no templates, no scripting.

---

## Idempotency, skip logic, and pre-run validation

The ingestion pipeline must avoid redundant API calls and duplicate events. Each run should be idempotent: running the same backfill twice produces no additional API calls and no duplicate events.

### Goals

- prevent duplicate API calls
- prevent duplicate events
- skip already-ingested windows
- fail fast on invalid configs
- validate before network usage

### 1) Pre-run validation (fail fast)

Before any fetch, validate (per source):

- source is enabled
- required auth keys present
- fetch URL is syntactically valid
- extract paths are valid (path syntax only)
- timestamp path defined
- text path defined for content sources
- priority defined (mandatory)
- pagination config valid; defaults to `none`

If any fail -> abort that source before any API call.

### 2) Window idempotency (skip entire fetch)

Before fetch, compute a stable window key from:

- `source_id`
- `start_date`
- `end_date`
- `spec_hash` (hash of fetch + extract config, excluding secrets)

If `(source_id, start, end, spec_hash)` is already marked complete -> skip fetch entirely.

Store these markers in a simple ledger table (e.g. `ingest_runs`) so backfill reruns can no-op safely.

### 3) Request idempotency (prevent repeated calls)

Compute a `request_hash` from the request inputs:

```
request_hash = hash(url, params, start, end)
```

If `request_hash` was seen recently -> skip the API call. This protects retries, reruns, and overlapping slices.

### 4) Event idempotency (prevent duplicates)

Each emitted event gets a deterministic `event_hash` computed **after timezone normalization** (and before persistence):

```
event_hash = hash(source_id, timestamp_utc, text)
```

If it already exists -> drop the event.

Notes:
- normalize timestamps to UTC before hashing (required)
- keep the `Event` schema minimal; do not include enrichment fields in the hash

### 5) Skip logic levels (order)

Pipeline checks in order:

1. pre-run validation
2. window already complete -> skip
3. request duplicate -> skip
4. fetch
5. post-fetch normalization
6. map to canonical `Event`
7. validate + bounds
8. event duplicate -> drop
9. persist

### 6) Fast-fail rules

Fail immediately if:

- missing API key
- invalid extract paths
- timestamp not parseable
- response not list-like (after applying any configured row selection)
- pagination loop detected

No partial ingestion for that source in that slice.

### 7) Dry-run validation (must be meaningful)

Dry-run must validate more than request building:

- config parses and validates
- a response "row selector" (if any) would yield list-like rows (using a fixture or a recorded sample payload)
- extract paths resolve against sample rows
- timestamp + text requirements would pass for at least one row

### 8) Source health logging and drop reasons

Per slice, log (and persist if useful):

- rows fetched
- rows emitted
- rows dropped by reason: empty text, out-of-bounds, dup, invalid timestamp, invalid shape

### Result

With idempotency and skip logic:

- rerunning backfill = near zero API calls
- partial failures resume cleanly
- overlapping windows are safe
- sources can be reordered (priority keeps determinism)


---

## Minimal fetcher set (what we actually need)

To cover the current `config/sources.yaml` universe:

1. `http_json` (GET/POST, headers, params, auth, optional pagination, JSON row selection)
2. `rss` (RSS/Atom -> rows)
3. `local_file` (bundles)

Avoid adding a bespoke fetcher per provider unless it is a true protocol difference.

---

## Migration plan (removes placeholders without blocking progress)

### Phase A - Introduce fetchers + keep adapters temporarily (2-5 days)

1. Add `fetch` block to `SourceSpec` (backwards compatible).
2. Implement a shared fetch stage for:
   - `http_json`
   - `rss`
   - `local_file`
3. Update the current backfill fetch path:
   - if `spec.fetch` exists -> use shared fetch stage
   - else -> fall back to existing adapter registry (transitional only)
4. Add `--dry-run` (or env var) to validate configs without any network calls.

Definition of done: at least 2 sources moved to pure config, and dry-run validates the whole file.

### Phase B - Convert each source to declarative fetch (1-3 weeks)

Convert sources one-by-one by editing config, not writing code. After each conversion:

- disable/delete the corresponding adapter module
- keep all behavior centralized and consistent

### Phase C - Make mocks impossible (1-2 days)

1. Remove mock adapter modules entirely (or move them to `tests/fixtures`).
2. Add enforcement:
   - default to "no mocks"
   - pipeline errors if any source references a deprecated adapter path

Definition of done: `config/sources.yaml` references only config-based sources (no adapter modules).

---

## Backfills and predictability

Backfills are already slice-driven. Declarative fetch improves them because:

- bounds are handled uniformly
- all sources share retry/backoff and global rate limiting
- post-fetch normalization is centralized
- idempotent hashing happens before persistence

It becomes easier to answer:

- "why did this slice fetch 0?"
- "why did we get duplicates?"
- "why are timestamps inconsistent?"

---

## What we should NOT do

- Do not embed Python eval or arbitrary code in YAML.
- Do not let extraction become a mini language; mapping only.
- Do not mix derived features into ingestion; keep enrichment separate.
- Do not do ticker/symbol detection during ingestion; do it in enrichment.
- Do not add a yfinance fetcher here; treat yfinance-based datasets as derived builders later.

---

## Open decisions (need quick agreement)

1. **Path syntax for mapping:** dotted paths only vs minimal JSONPath; keep it simple.
2. **Auth standardization:** how KeyManager keys map to headers/params for each provider.
3. **Pagination abstraction (optional):** which patterns we support in v1 (cursor/page/next_url).
4. **Dry-run semantics:** validate-only vs request-build (no network) vs network with zero persistence.

---

## Concrete next step

If this direction matches priorities, the next implementation step is:

1) Add `fetch` + `priority` to `SourceSpec` and implement `http_json` + `local_file` fetchers (add `rss` if needed immediately)  
2) Convert `custom_bundle` and `fred_macro` to the new config-based fetch path  
3) Add dry-run + source health logging, then delete/ban mock adapters
