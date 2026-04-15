# Implementation Roadmap: Real Ingestion Adapters + Placeholder Removal

Date: 2026-04-09

## Goal

Turn the current POC “pipeline universe” into an end-to-end system where:

1. **All enabled ingestion adapters fetch real data** (no deterministic mocks).
2. **Backfill + replay runs without scaffolded pipeline paths**.
3. **UI surfaces real persisted artifacts** (no mock evidence tables / mock services).
4. Remaining test-only mocks are explicitly isolated behind flags and never used by default.

This roadmap is organized into phases with clear “definition of done” gates.

---

## Current state (what’s still placeholder today)

### Ingestion adapters that currently emit mock/deterministic rows

- `app/ingest/adapters/alpaca_news.py`
- `app/ingest/adapters/reddit_social.py`
- `app/ingest/adapters/options_flow.py`
- `app/ingest/adapters/etf_flows.py`
- `app/ingest/adapters/earnings_calendar.py`
- `app/ingest/adapters/fear_greed.py`
- `app/ingest/adapters/fred_macro.py`
- `app/ingest/adapters/google_trends.py`
- `app/ingest/adapters/cross_asset.py`
- `app/ingest/adapters/market_breadth.py`
- `app/ingest/adapters/market_baseline.py`

### Pipeline/engine paths that are explicitly scaffolded

- `app/runtime/pipeline.py` (mock track signals, “simple prediction”, TODO persistence)
- `app/engine/ranking_engine.py` (mock signal collection; placeholder drift/momentum)

### UI mock artifacts

- `app/ui/comparison/comparison_tables.py` (mock evidence + mock history)
- `app/ui/intelligence_hub.py` (contains mock service section)

### Acceptable test-only mock (already guarded)

- `app/core/bars/providers.py` (`MockBarsProvider`, gated via `ALLOW_MOCK_BARS=true`)

---

## Definition of done (project-wide)

We can call the placeholders “cleared out” when all of the below are true:

1. `config/sources.yaml` defaults to **real** adapters only.
2. Every adapter referenced by `config/sources.yaml`:
   - honors `ctx.start_date` / `ctx.end_date` (or the backfill runner enforces bounds cleanly)
   - returns real provider payloads (no hardcoded numbers/headlines)
   - degrades gracefully on provider failures (rate limits, empty windows)
3. Demo/backfill paths run the **real engine pipeline** (`app/engine/runner.py`) and persist to `data/alpha.db`.
4. UI pages query `data/alpha.db` via the middle/read-model (no “generate_mock_*” flows).
5. Any remaining mocks live under:
   - `tests/` (preferred), or
   - explicit feature flags that are default OFF and clearly labeled.

---

## Phase 0 — Inventory + guardrails (1–2 days)

### Tasks

1. **Add a “no-mocks” enforcement toggle** (default ON for “prod-ish” runs)
   - Example env var: `DISABLE_MOCK_ADAPTERS=true`
   - If enabled, adapters that are still mock should throw a clear error early.
2. **Add an operator-facing “provider readiness” report**
   - CLI command that prints:
     - which sources are enabled
     - which required keys are missing
     - which adapters are still mock/scaffolded
3. **Normalize adapter output expectations**
   - Ensure each adapter has a short docstring:
     - provider
     - required keys
     - row contract (fields expected by `extract:` in `config/sources.yaml`)

### Acceptance criteria

- One command can answer: “If I run backfill today, will it use any mocks?”

---

## Phase 1 — “No external dependencies” first (remove mocks by deriving from DB) (2–5 days)

These adapters can become real without adding new vendors, by computing from already-persisted price bars / existing yfinance macro series.

### 1) `market_baseline` (SPY/QQQ/IWM returns)

**Current:** mock returns.  
**Replace with:** compute returns directly from `price_bars` for the requested window.

- Input: `ctx.start_date/end_date` (or `ctx.run_timestamp`) + tickers (`SPY`, `QQQ`, `IWM`).
- Implementation: query `price_bars` for last close vs prior close at relevant horizons; emit numeric features.
- Benefit: always consistent with the same bars used by MRA/price_context.

### 2) `market_breadth` (advancers/decliners)

**Current:** mock advancers/decliners.  
**Replace with:** compute breadth over the canonical universe from `price_bars`.

- Implementation: for a given timestamp, compare each ticker’s last close vs prior close.
- Emit: `advancers`, `decliners`, `breadth_ratio`, `participation`, `risk_on_score`.

### 3) `cross_asset` (intermarket snapshot)

**Current:** mock oil/gold/dxy/vix/yields.  
**Replace with (minimal):** compute returns for configured cross-assets using existing `yfinance_macro` symbols and/or `price_bars` if present.

- If the macro series are fetched via `yfinance_macro` + persisted as events, parse the last close/return_1d.
- If you want intraday deltas (`*_return_1h`), use `BarsCache` with `HISTORICAL_BARS_PROVIDER` for those symbols (or skip intraday until available).

### Acceptance criteria

- With only bars + existing yfinance macro capabilities, these three adapters produce non-mock values.

---

## Phase 2 — Wire real providers for the remaining adapters (1–3 weeks)

This phase replaces the externally-dependent adapters with real API calls.

### Required secrets (proposed standardization)

Add to `.env.example` (and ensure `KeyManager` maps them):

- `ALPACA_API_KEY`, `ALPACA_API_SECRET`
- `POLYGON_API_KEY`
- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT` (or use unauthenticated JSON with strict rate limiting)
- `FRED_API_KEY`
- `FMP_API_KEY` (Financial Modeling Prep)
- `SERPAPI_KEY` (if using SerpApi for Google Trends) or explicitly choose `pytrends` with operational caveats

### 1) `alpaca_news`

**Replace with:** Alpaca News endpoint query with time bounds and symbol filters.

- Must: respect rate limits, pagination, and slice windows.
- Output rows should map cleanly to current `extract:` in `config/sources.yaml`:
  - `created_at`, `symbols`, `headline`, `summary`

### 2) `reddit_social`

**Replace with:** real subreddit/new posts fetch.

- Minimal viable:
  - fetch `new.json` / `hot.json` for configured subreddit
  - parse created timestamp + title + selftext + url
  - derive `detected` ticker via a simple ticker regex + whitelist (`config/target_stocks.yaml`)
- Don’t overfit detection: store the raw post; let the scoring/enrichment layers decide relevance.

### 3) `options_flow`

**Replace with:** Polygon options trades/aggregates (or another chosen provider).

- Minimal viable:
  - compute a proxy for “premium” (price * size * contract_multiplier)
  - classify `call_put`
  - emit rows consistent with `extract:` mapping (`symbol`, `call_put`, `premium`, `timestamp`)

### 4) `etf_flows`

**Replace with:** chosen provider for ETF fund flows.

Notes:
- FMP endpoint referenced in comments (`/v3/etf-holder/{symbol}`) is holdings, not flows.
- Decision needed: either
  - re-scope adapter to “ETF holdings changes / concentration signals”, or
  - integrate a real ETF flow provider and keep “inflow” semantics.

### 5) `earnings_calendar`

**Replace with:** real earnings calendar endpoint.

- Minimal viable:
  - query earnings dates per symbol window
  - emit events with `symbol`, `date`, and provider metadata.

### 6) `fear_greed`

**Replace with:** Alternative.me FNG API (or chosen provider).

- Output should include:
  - `fear_greed`, `classification`, `extreme`, `delta`, `timestamp`
- Ensure backfill windows behave (API often returns latest values; for historical, you may need a paid source or accept “latest only”).

### 7) `fred_macro`

**Replace with:** real FRED series fetch.

- Must:
  - support `spec.options.series`
  - return `date` (or `timestamp`) and `series_value`
- For yield curve spread, either:
  - fetch both `DGS10` and `DGS2` and compute spread, or
  - fetch a precomputed spread series if available.

### 8) `google_trends`

**Replace with:** real Trends provider.

- Decide the implementation:
  - SerpApi Google Trends (stable, paid), or
  - `pytrends` (free but brittle / blocks / throttling).
- Emit `keyword`, `value`, `timestamp`.

### Acceptance criteria

- All enabled sources in `config/sources.yaml` produce real data for a 24h window.
- Backfill for 7 days completes without mock adapter usage.

---

## Phase 3 — Remove scaffolded pipeline paths (2–7 days)

### 1) Deprecate `app/runtime/pipeline.py`

**Target state:** there is one canonical pipeline entrypoint for “real runs”:

- `app/engine/runner.py:run_pipeline()` for core flow + persistence

Actions:

- Mark `app/runtime/pipeline.py` as demo-only (or delete after migration).
- Update `scripts/demo_run.py` to call `app/engine/runner.run_pipeline` (or a “demo harness” wrapper) instead of `app/runtime/pipeline.run_pipeline`.

### 2) Fix `RankingEngine` placeholders

**Target state:** `app/engine/ranking_engine.py` uses real persisted signals:

- sentiment derived from latest `scored_events` or enrichment outputs
- macro derived from macro event series (yfinance_macro + fred_macro)
- drift derived from MRA continuation features (or `prediction_outcomes` aggregates)
- momentum derived from `price_bars` (returns / MA cross / RSI)

Acceptance: `RankingEngine.compute_ranking()` no longer includes “Mocking …” comments or placeholder constants.

---

## Phase 4 — Clear UI mocks by wiring to persisted artifacts (3–10 days)

### 1) Comparison views

Replace `app/ui/comparison/comparison_tables.py` mock evidence/history with:

- evidence: join `raw_events` → `scored_events` → `mra_outcomes` → latest predictions/outcomes for selected ticker/strategy
- history: query prior `prediction_scores` / `prediction_outcomes` per strategy/ticker

### 2) Intelligence Hub

Replace mock service usage with middle-layer methods backed by the DB:

- sentiment heatmap: aggregate `scored_events` / enrichment sentiment by sector/theme
- anomaly detection: start with simple Z-score rules (vol/volume spikes) using `price_bars` and MRA; add model later
- news impact: aggregate MRA vs taxonomy/event_type

Acceptance: UI pages render with an empty-state when data is missing, not fabricated data.

---

## Phase 5 — Operational hardening (ongoing)

### Reliability

- Backoff + retry strategy per provider (429 handling)
- source-level health metrics (already exists conceptually in ingest metrics)
- persist raw provider payloads (already supported via `Event.raw_payload`)

### Data quality

- enforce timestamp normalization centrally
- enforce ticker canonicalization (uppercasing + whitelist integration)
- add coverage dashboards:
  - “events per source per day”
  - “% events missing ticker”
  - “% events deduped”

### Testing

- unit tests per adapter for parsing + date bounding
- contract tests for `extract:` mappings (adapter output keys must exist)
- integration smoke: run 1-day backfill with network calls in CI (optional / nightly)

---

## Recommended execution order (highest ROI first)

1. Phase 1 DB-derived adapters (`market_baseline`, `market_breadth`, `cross_asset`)  
2. `fred_macro` + `alpaca_news` (high signal quality, relatively stable APIs)  
3. `options_flow` (use Polygon; valuable for intraday)  
4. `earnings_calendar` (use FMP or chosen provider)  
5. `reddit_social` (high noise but valuable; needs careful rate limiting + detection)  
6. `google_trends` + `fear_greed` (nice-to-have; may require vendor choice for stability)  
7. Pipeline scaffold cleanup (`app/runtime/pipeline.py`, `RankingEngine` placeholders)  
8. UI mock removal (comparison + intelligence hub)

---

## Decision points to unblock implementation

1. **ETF flows semantics:** real flows vendor vs re-scope to holdings/concentration.  
2. **Google Trends provider:** SerpApi vs `pytrends`.  
3. **Reddit access:** authenticated API vs public JSON + aggressive throttling.  
4. **CI policy:** do we allow network-backed integration tests (nightly), or keep them local-only?

