# Declarative Ingestion Architecture

The Alpha Engine utilizes a fully declarative, event-driven ingestion pipeline designed for parallel asynchronous execution, high resilience, and zero-code provider expansions.

This document outlines how the system transforms raw JSON from arbitrary external APIs (Reddit, Alpaca, Yahoo Finance, FRED) into standardized engine `Event` payloads.

## 1. Declarative Configuration (`sources.yaml`)
We eliminated hardcoded fetching loops. Every active data source is uniquely declared in `config/sources.yaml`. 

```yaml
sources:
  - id: "reddit_markets"
    adapter: "reddit_social_api"
    interval: 60
    enabled: true
    extract:
      expression: "data.title + ' | ' + data.selftext"
      tags: ["social", "retail"]
```
This enables dynamically adding new APIs by dropping in a configuration block rather than touching backend logic. 

## 2. Pydantic Flow Validation
Because external APIs change unexpectedly, the system enforces strict structural constraints via `app/ingest/validator.py`.
1. **Schema Validation:** At launch, the `sources.yaml` parsing maps directly into a `SourceSpec` `Pydantic` schema, immediately raising errors if required bounds are missing.
2. **Payload Protection:** Evaluated payloads yield `Event` models. Any structurally aberrant API responses are suppressed natively before causing stack traces in the trading logic.

## 3. Parallel Asynchronous Orchestration (`async_runner.py`)
All polling operates completely asynchronously.
- The pipeline utilizes `asyncio.gather()` dispatching concurrent HTTP/socket polls.
- Adapters (e.g., `app/ingest/adapters/reddit_social.py`) implement an async `fetch_raw()` interface. 
- Individual provider execution never blocks the unified processing queue, removing timing constraints across vast combinations of data providers.

## 4. The Extractor Engine (`extractor.py`)
Rather than relying on Adapters to correctly format domain knowledge, Adapters *only return raw dictionaries*.
The `Extractor` applies the YAML `expression`-driven map (e.g., `"data.headline + data.summary"`) extracting and concatenating relevant strings securely. This consolidates data normalization entirely into one location, removing formatting inconsistencies.

## 5. System Hardening & Operational Constraints
To convert simple data polling into a robust production memory-bus, several layers wrap the ingestion execution:
1. **Context & Rate Limits:** The `FetchContext` injects singletons checking global timestamp requests (`app/ingest/rate_limit.py`) mapping precise request/second constraints per provider (e.g., 200/min for Alpaca, 30/min for Reddit), avoiding API throttling penalties.
2. **Deterministic Deduplication:** Before yielding, `app/ingest/dedupe.py` subjects all arrays to an identity hash (`sha256(source + timestamp + content)`). This suppresses ghost noise caused by rapid polling cycles or RSS delays.
3. **Event Persistence:** Survived messages route via `INSERT OR IGNORE` straight into `app/ingest/event_store.py` providing a historic SQLite ledger enabling post-mortem backtests.

## 6. Granular Event Routing (`router.py`)
Finally, dropping homogenous events into massive arrays creates blind spots. The ingested unified `Event` is automatically inspected and bucketed into explicitly targeted silos:
- `news` -> `sentiment`
- `market` -> `quant` 
- `macro` -> `regime`
- `social` -> `crowd`

This allows localized signal training models to parse uniquely dominant contexts efficiently.
