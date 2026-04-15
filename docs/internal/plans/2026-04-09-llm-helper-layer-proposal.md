# LLM Helper Layer Proposal (Pipeline Enrichment + Company Touchpoints)

Date: 2026-04-09

## Executive summary

Alpha Engine already has the right loop: ingest unstructured content -> score -> measure market reaction -> generate predictions -> evaluate outcomes -> rank/promote strategies. The missing link is a reliable, structured **event understanding** layer that turns raw text into consistent, queryable fields that can power:

- better strategy features (text + quant hybridization)
- higher-quality ticker coverage (more touchpoints between companies and content)
- richer UI/Intelligence Hub experiences (heatmaps, impact panels, "why" explanations)
- deterministic exports for chat/ops tools (without requiring a heavy vector DB)

This doc proposes adding an **LLM Helper Layer** as an enrichment stage that produces strict JSON artifacts (sentiment, entity/ticker linking, event taxonomy, tags) and persists them alongside `raw_events` / `scored_events`. The layer is designed to be **optional, cacheable, schema-tolerant, and safe** (never the single point of truth for trading decisions).

---

## Current pipeline (what exists today)

At a high level (see `README.md`, `ADMIN_GUIDE.md`):

1. **Declarative ingestion** (`config/sources.yaml`, `app/ingest/`)
   - fetch raw payloads asynchronously
   - validate + extract text via YAML expressions
   - rate limit + dedupe
   - persist a historical ledger to SQLite (`data/alpha.db`)
2. **Event scoring** (`app/core/scoring.py`)
   - rule/term-cloud scoring into `ScoredEvent` (category, direction, confidence, tags)
3. **Market Reaction Analysis (MRA)** (`app/core/mra.py`)
   - quantify post-event price/volume behavior into an `MRAOutcome`
4. **Prediction + consensus** (`app/engine/runner.py`)
   - generate track signals (sentiment vs quant), then combine with regime-aware weighting
5. **Closed-loop learning**
   - evaluate realized outcomes vs predictions
   - rank strategies and evolve/promote/demote
6. **UI (Streamlit)**
   - dashboard consumes a stable read-model rather than engine internals

### Observed gap

`score_event()` is currently heuristic. It’s fast, but it can’t reliably answer:

- Which companies are mentioned (primary + secondary + peers + suppliers/customers)?
- What’s the sentiment **per company mention** (not just overall)?
- What is the event type (earnings, guidance, M&A, litigation, macro, analyst, product)?
- What’s the estimated impact horizon (intraday vs multi-day) and uncertainty?
- How do we tag content so it becomes discoverable and comparable across tickers/themes?

This is exactly where an LLM (used as a structured extractor, not a free-form writer) creates compounding value.

---

## Company goals this proposal supports

From repo docs and architecture direction, the intent is a research-first, closed-loop system that:

- transforms unstructured news/media into measurable, backtestable signals
- supports hybrid strategies (text + technical + regime)
- scales ingestion via config, not code (declarative sources)
- keeps the UI stable via a middle read model (schema volatility isolation)
- expands toward "Intelligence Hub" features (sentiment, impact, anomalies, explainability)
- stays pragmatic: deterministic retrieval + exports > overbuilt RAG for core needs

An LLM Helper Layer strengthens those goals without changing the fundamentals of the loop.

---

## Proposal: add an LLM Helper Layer as "Enrichment"

### Where it sits

Recommended placement (non-blocking):

```
Ingestion -> RawEvent persisted -> (async) LLM enrichment -> Enrichment persisted
                          \\-> (existing) score_event -> MRA -> strategies -> outcomes
```

This preserves pipeline uptime and keeps "trading loop" latency stable. Strategies can adopt enriched fields gradually (feature flags), and UI can consume enrichment as it becomes available.

### What it produces (strict JSON contracts)

The helper layer should produce deterministic, schema-validated JSON:

1. **Entity + ticker linking (touchpoints)**
   - `primary_ticker` (if missing/incorrect)
   - `mentioned_tickers[]` with per-mention confidence
   - `mention_type`: `PRIMARY|SECONDARY|PEER|SUPPLIER|CUSTOMER|COMPETITOR|MACRO_PROXY`
   - `relationship` (optional): "supplier disruption impacts ..."
   - `sector/theme tags`: e.g. `AI_INFRA`, `SEMIS`, `CLOUD`, `REGULATORY`, `EARNINGS`

2. **Sentiment and stance**
   - `overall_sentiment`: `[-1..1]` + confidence
   - `per_ticker_sentiment[]`: sentiment per mentioned ticker
   - `uncertainty`: detect speculation vs confirmation ("rumor", "reports say", "expected")
   - `drivers[]`: short extracted phrases grounded in the source text

3. **Event taxonomy (structured classification)**
   - `event_type`: `EARNINGS|GUIDANCE|M&A|REGULATORY|LITIGATION|PRODUCT|ANALYST|MACRO|OTHER`
   - `direction`: `positive|negative|neutral|mixed` (aligned to `ScoredEvent.direction`)
   - `materiality_estimate`: `[0..1]` (LLM estimate, not a market return claim)
   - `time_horizon`: `INTRADAY|SWING|LONGER` + `horizon_confidence`

4. **UI-friendly summaries (optional but high leverage)**
   - `headline`: generated from extracted facts only
   - `summary_bullets[]`: 3–5 bullets (no external facts)
   - `why_it_matters`: 1 sentence, grounded in extracted content

5. **Quality and ops metadata**
   - `model_id`, `prompt_version`, `input_hash`, `created_at`
   - `warnings[]`: missing ticker, ambiguous referent, low-confidence extraction, etc.

---

## High-value use cases (beyond "sentiment score")

### 1) Better ticker coverage and cross-company discovery (touchpoints)

Today, `RawEvent.tickers` is limited by what the adapter/extractor provides. The helper layer turns each piece of content into a graph:

- the company that is truly "about" the event (primary)
- other companies implicated (secondary)
- related companies for browsing (peer set, supplier chain, competitors)

Direct benefits:
- dashboards ("related tickers", "theme bundles", "company story feed")
- strategy research ("spillover impact" features)
- queryability ("show me all supply-chain disruptions affecting AAPL or NVDA")

### 2) Event taxonomy as a stable feature backbone

Strategy performance is often conditional on event type (earnings vs macro vs product). A consistent taxonomy enables:

- regime × event-type slices (a natural extension of current regime work)
- better ranking: compare strategies within comparable "event families"
- better strategy mutation: mutate parameters conditioned on event distributions

### 3) LLM as feature generator, not signal generator

Use enrichment fields as inputs into existing strategies rather than letting the LLM output "buy/sell".

Examples:
- `event_type=GUIDANCE` + `direction=negative` -> increase odds of negative drift on select horizons
- `uncertainty=HIGH` -> downweight text track, prefer quant track
- `mentioned_tickers` + `mention_type=SUPPLIER` -> model spillovers

This fits the platform: MRA and outcomes remain the arbiter.

### 4) Story clustering to reduce noise and improve evaluation

Ingestion dedupe prevents exact duplicates, but "same story, different headline" still floods the loop.

Proposed:
- lightweight clustering key (LLM-produced `story_id` + `story_summary`)
- or two-stage: cheap similarity (hash/TF-IDF) -> LLM merge only on collisions

Benefits:
- fewer redundant predictions
- cleaner evaluation statistics
- UI becomes a story feed rather than a firehose

### 5) Explainability artifacts that are safe and useful

Instead of letting the UI invent narratives from raw text, persist:

- extracted drivers ("guidance lowered", "FDA approval", "data center demand")
- "why it matters" grounded in drivers
- evidence snippets (short spans from original text, not paraphrased facts)

This improves trust and operator debuggability.

### 6) Data quality feedback loop for ingestion configs

The helper layer can detect when an extraction expression yields low-quality text (empty, boilerplate, nav chrome). Emit `warnings[]` that can be summarized per source:

- "source emits mostly non-financial chatter"
- "ticker missing for X% of events"
- "text too short / not in English / looks like spam"

This makes declarative ingestion self-auditing.

---

## Persistence and schema approach (Prisma + SQLite compatible)

Keep enrichment additive and schema-tolerant. Two safe options:

### Option A (fastest): store enrichment JSON in existing JSON fields

- Add `RawEvent.metadataJson` keys:
  - `enrichment: { ... }`
  - `enrichment_version`, `enrichment_created_at`

Pros: minimal schema migration, fastest to ship.  
Cons: harder to query at scale without JSON extraction.

### Option B (recommended): add first-class tables for queryability

Add models similar to:

- `EventEnrichment` keyed by `rawEventId` (and tenant)
- `CompanyMention` rows for each ticker mention
- optional `StoryCluster` keyed by cluster id

This supports:
- fast dashboard queries (heatmaps, related tickers, news impact)
- strategy training features (joins on mention type and sentiment)
- analytics (coverage rates, drift, failure modes)

---

## Execution model (how we run it safely)

### Asynchronous enrichment worker (recommended default)

Run enrichment in a background worker that:

- polls for newly inserted `raw_events` missing enrichment
- batches events by tenant/source
- calls the LLM with strict JSON schema output
- validates JSON (Pydantic) and persists
- records metrics (latency, tokens, error rates, coverage)

This keeps the trading loop deterministic and stable even if the LLM provider is slow or down.

### Synchronous enrichment (optional, gated)

Allow inline enrichment only for:

- backfill jobs where latency is acceptable
- research runs where reproducibility is more important than throughput

### Caching and idempotency

Use `input_hash = sha256(source + timestamp + normalized_text)` to:

- avoid re-enrichment duplicates
- support retries safely
- enable "re-enrich with new prompt_version" without clobbering old results

---

## Safety and anti-hallucination design

This layer must behave like a compiler, not a chatbot:

- strict JSON output only (no prose in the response channel)
- grounding rule: drivers/summaries must be traceable to text spans
- confidence + abstention: allow `unknown` / low-confidence states
- ticker whitelist mode: map to `config/target_stocks.yaml` (or allow `OTHER`) with explicit confidence
- version everything: `model_id`, `prompt_version`, `schema_version`
- never treat LLM output as truth: it’s a feature; MRA/outcomes remain the judge

---

## Success metrics (how we know it’s worth it)

### Coverage and quality

- % of events with a primary ticker in the canonical universe
- precision/recall on ticker linking (sampled human labeling)
- stability of taxonomy distribution over time (drift checks)

### Signal and ranking uplift (offline, then paper mode)

- improvement in ranking metrics when strategies can use enrichment fields
- reduction in "neutral/noise" predictions per day while maintaining recall on impactful events

### Product value

- UI engagement with "related tickers", "themes", "story feed" panels
- time-to-debug for operators (fewer "what happened?" investigations)

### Cost/latency guardrails

- tokens per event (median / p95)
- enrichment lag (time from ingest -> enrichment ready)
- error rate + retry success

---

## Implementation roadmap (phased, low-risk)

### Phase 0: contracts + plumbing (1–2 days)

- define `EventEnrichment` JSON schema + Pydantic validator
- define enrichment storage interface (Option A or B)
- add metrics and idempotency keys (`input_hash`, `prompt_version`)

### Phase 1: ticker/entity linking + sentiment (high ROI) (2–5 days)

- per-event mentioned tickers + mention types
- per-ticker sentiment + uncertainty
- UI: "related tickers" and "sentiment snippet" panels

### Phase 2: taxonomy + horizon + drivers (3–7 days)

- event type classification + horizon estimate
- drivers extracted as grounded phrases + evidence snippets
- UI: filter news by event type; "why it matters" card
- strategies: add event-type-conditioned features

### Phase 3: story clustering + dedupe augmentation (1–2 weeks)

- group near-duplicate stories; reduce prediction spam
- story-level evaluation (cluster-level outcomes)

### Phase 4: closed-loop prompt/model tuning (ongoing)

- compare enrichment versions vs realized outcomes (not for "truth", but for feature utility)
- tune prompts and thresholds to maximize strategy uplift, minimize noise

---

## Appendix A: example enrichment payload (illustrative)

```json
{
  "schema_version": "enrich.v1",
  "primary_ticker": "NVDA",
  "mentioned_tickers": [
    {"ticker": "NVDA", "mention_type": "PRIMARY", "confidence": 0.93},
    {"ticker": "SMCI", "mention_type": "CUSTOMER", "confidence": 0.62}
  ],
  "sentiment": {
    "overall": {"score": 0.41, "confidence": 0.72},
    "per_ticker": [
      {"ticker": "NVDA", "score": 0.55, "confidence": 0.76},
      {"ticker": "SMCI", "score": 0.10, "confidence": 0.48}
    ],
    "uncertainty": {"level": "LOW", "confidence": 0.66}
  },
  "taxonomy": {
    "event_type": "DATACENTER_DEMAND",
    "direction": "positive",
    "materiality_estimate": 0.63,
    "time_horizon": "SWING",
    "horizon_confidence": 0.58
  },
  "drivers": [
    {"text": "stronger than expected datacenter demand", "evidence": "datacenter demand ... stronger than expected"},
    {"text": "capex increases", "evidence": "capex increase"}
  ],
  "ui": {
    "headline": "Datacenter demand cited as stronger than expected",
    "summary_bullets": [
      "Mentions NVDA with positive tone tied to datacenter demand.",
      "Links potential spillover to SMCI as a customer/infra beneficiary."
    ],
    "why_it_matters": "The content suggests demand-driven upside with a swing-horizon bias."
  },
  "meta": {
    "model_id": "provider:model",
    "prompt_version": "2026-04-09.a",
    "input_hash": "sha256:…",
    "created_at": "2026-04-09T15:00:00Z",
    "warnings": []
  }
}
```

---

## Decision request

If this direction matches priorities, the next concrete decisions are:

1. **Persistence choice:** JSON-in-`metadataJson` (fast) vs new tables (better queries)  
2. **Execution choice:** async worker (recommended) vs inline enrichment for specific runs

Once chosen, implementation can start with Phase 0 + Phase 1 (ticker/entity linking + sentiment), which directly addresses the two initial ideas: sentiment scores and ticker meta tags/touchpoints.
