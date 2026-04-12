# Ops Monitor Streamlit Page — Backfill, Dumps, Backtests (Plan)

Date: 2026-04-11  
Repo: `alpha-engine-poc`

## Why this page exists

We need a single Streamlit page that:

1. **Monitors** backfill progress, dump freshness, and backtest/pipeline history.
2. **Educates** users on what each dataset is, how it’s populated, and what “incremental fill” means.
3. **Triggers** the existing jobs manually (without the UI directly mutating the DB).
4. Shows **coverage over the last 10 years** (month-by-month) indicating which months have data, per dataset/source.

This document is the clarifying spec + implementation plan to build that page incrementally and safely.

---

## Current inventory (what we already have)

### 1) Core backfill / ingestion “job”

**Backfill CLI** (primary operator interface today):

- Module: `app/ingest/backfill_cli.py`
- Typical commands:
  - `python -m app.ingest.backfill_cli run --days 90 --db data/alpha.db`
  - `python -m app.ingest.backfill_cli backfill-range --start YYYY-MM-DD --end YYYY-MM-DD --batch-size-days 1 --db data/alpha.db`
  - `python -m app.ingest.backfill_cli ingest-runs --start YYYY-MM-DD --end YYYY-MM-DD --db data/alpha.db`
  - `python -m app.ingest.backfill_cli ingest-runs-detail --source <source_id> --start ... --end ...`
  - `python -m app.ingest.backfill_cli ingest-runs-cleanup --db data/alpha.db [--dry-run]`
  - `python -m app.ingest.backfill_cli ingest-health --start ... --end ... --db data/alpha.db`
  - Target universe management:
    - `list-target-stocks`, `add-target-stock`, `remove-target-stock`, `enable-target-stock`, `disable-target-stock`

**Interactive launcher** (wraps/organizes operator flows):

- `start.py` (or `python -m app.cli.start`) includes preflight for bars providers and guided command prompting.

### 2) Ingestion state tables (SQLite)

The ingestion system tracks progress in `data/alpha.db` via `EventStore`:

- File: `app/ingest/event_store.py`
- Tables (relevant for monitoring):
  - `ingest_runs`: window ledger keyed by `(source_id, start_ts, end_ts, spec_hash)` with status + counts
  - `ingest_run_stats`: deeper stats for a window (timing, dropped counts, fingerprints)
  - `backfill_slice_markers`: legacy “slice done” marker keyed by `(source_id, start_ts, end_ts)`
  - `backfill_horizons`: “source has reached up to X” marker keyed by `(source_id, spec_hash)`
  - `events`: normalized ingest events emitted by adapters (separate from `raw_events` in repository schema)

### 3) Source definitions (what “backfill” actually pulls)

The backfill runner is driven by YAML specs:

- `config/sources.yaml`
  - **Dump adapters** (priority 1): serve historical coverage from files under `data/raw_dumps/*`
  - **API adapters** (priority 2+): intended for *recent windows* only (historical guarded)
- `config/keys.yaml` maps providers → env vars (note: `polygon.key` uses `ENV:POLYGON_KEY`, but the Polygon bars provider expects `POLYGON_API_KEY`).
- `config/macro_sources.yaml` for macro snapshot symbols.

### 4) Dump datasets on disk (major “data dumps”)

Current dump roots under `data/`:

- `data/raw_dumps/`:
  - `alpha_vantage/` (CSV per ticker)
  - `fnspid/` (parquet news sample in smoke test; real layout depends on dump creation)
  - `fred/` (parquet)
  - `full_history/` (CSV price dump adapter reads here)
  - `stooq/` (parquet per symbol)
  - `tiingo/` (CSV per ticker; present and populated)
  - `yahoo/` (raw Yahoo files)
  - `fmp/` (directory exists but currently empty)
  - plus a few CSVs in `data/raw_dumps/` root:
    - `raw_partner_headlines.csv`, `raw_analyst_ratings.csv`, etc.
- `data/company_profiles/` (JSON per ticker)
- `data/exports/top_ten.md`
- `outputs/` contains various pipeline artifacts and test DBs (useful for “backtest history” / run logs).

### 5) Dump download / maintenance scripts (jobs we can trigger)

Operator scripts already exist:

- `scripts/download_tiingo.py` → `data/raw_dumps/tiingo/*.csv` (needs `TIINGO_API_KEY`)
- `scripts/download_alpha_vantage.py` → `data/raw_dumps/alpha_vantage/*.csv` (uses `ALPHA_VANTAGE_API_KEY`, currently defaulting to a hardcoded key)
- `scripts/download_fmp.py` → `data/raw_dumps/fmp/*.csv` (needs `FMP_API_KEY`)
- Additional dump/pipeline verification:
  - `scripts/smoke_dump_pipeline.py` (creates synthetic dumps + runs a dump-first backfill smoke)
- One-off backfill script:
  - `scripts/backfill_aapl_bars.py` (AAPL example; not the system runner)

### 6) News + market data “providers” mentioned in your request

**Ingestion adapters (news/social/macro)** that exist today (partial list, see `app/ingest/registry.py` and `config/sources.yaml`):

- News: Alpaca (`alpaca_news`), plus dump-driven news (`fnspid_dump`, `analyst_ratings_dump`)
- Other: `reddit_social`, `google_trends`, `fred_macro`, etc.

**Financial market bars providers** used by the replay/cache layer:

- `app/core/bars/providers.py` includes `AlpacaBarsProvider`, `PolygonBarsProvider`, `YFinanceBarsProvider`
- These populate `price_bars` in `data/alpha.db` (and are needed for replay/backtests)

**Gaps vs your request:**

- SerpAPI: no adapter in repo currently (will need to add if desired).
- FMP dump: adapter exists (`app/ingest/adapters/fmp_dump.py`) but is not registered in `app/ingest/registry.py` nor defined in `config/sources.yaml` yet; `data/raw_dumps/fmp` is empty right now.

### 7) Existing UI pieces we can reuse

- Unified shell: `app/ui/app.py`
- Existing pages:
  - Dashboard: `app/ui/dashboard_compact.py`
  - Intelligence Hub: `app/ui/intelligence_hub.py`
  - Signal Audit: `app/ui/audit.py`
  - Backtest analysis page exists: `app/ui/backtest_strategy_analysis.py` (not currently in top nav)
- We already added: Paper trades page (`app/ui/paper_trades.py`) with DB-backed monitoring patterns.

---

## Design principles (important constraints)

1. **DB writes stay out of Streamlit.** The UI should *not* directly mutate `data/alpha.db` tables.  
   Instead, it should trigger existing CLI entrypoints / scripts (subprocess) which already know how to write safely.

2. **Make state explicit.** The page should explain which tables/files represent:
   - “ingestion progress”
   - “data availability”
   - “backtest/pipeline history”

3. **Incremental fill first-class.** We currently backfill in slices (e.g., 1-day windows). The UI must show:
   - what windows are complete/partial/failed
   - horizon markers (how far back we’ve reached)
   - where gaps are

4. **Operator safety.** Manual triggers need guardrails:
   - confirm prompts, dry-run modes, “check-only” coverage checks
   - strong defaults (small windows)
   - clear “what will happen” docs beside each button

---

## Proposed page: “Ops / Data Console” (reduced tabs)

### Route + navigation

- Add a top-bar route: `ops` labeled “Ops / Data”.
- Page file: `app/ui/ops_data_console.py` (name TBD).

### Layout (tabs)

We can combine the “monitoring” views into one tab and keep coverage + jobs separate. Target: **3–4 tabs**.

1. **Status (Data + Backfill + Backtests)**
   - **Explain first:** a short “How the platform gets data” panel:
     - *Dumps* (disk) feed historical windows (priority 1).
     - *APIs* fill only recent windows (priority 2+; historical guarded).
     - *Backfill runner* writes `ingest_runs` + `events`; replay/backtests write `price_bars` + `prediction_runs` etc.
   - **Backfill health (DB):** `ingest_runs` rollups + “recent failures” + “running windows”.
   - **News + non-price sources (DB):** last event timestamp + counts (24h/7d) per `source`.
   - **Backtests / pipeline history (DB + outputs):**
     - latest `prediction_runs`
     - quick link/summary to `Backtest / Strategy Analysis`
     - recent artifacts in `outputs/` (CSV/DB/logs) as “what ran recently”
   - **Dump freshness (Disk):** file counts/sizes/newest mtime for `data/raw_dumps/**` and `data/company_profiles/**`.

2. **Coverage (10-year map)**
   - Month grid for last 10 years (120 months), switchable dataset:
     - `price_bars` (market coverage by ticker/timeframe)
     - `events` (source coverage)
     - `ingest_runs` (window completion coverage)
   - Start simple: green = “has any rows this month”; yellow = “some but low”; red = “none”.
   - Tooltip drilldown: show counts + links to the underlying table slice (e.g., top windows for that month).

3. **Run Jobs (Manual triggers + logs)**
   - Curated job launcher with embedded explanations and safe defaults.
   - Group jobs by intent (not by subsystem):
     - “Backfill a date range”
     - “Check coverage / integrity”
     - “Download/refresh dumps”
     - “Cleanup stalled windows”
   - For each action:
     - show exact command
     - confirmation gate
     - stream logs
     - persist job history (`data/ops_jobs.db`)

Optional 4th tab if needed later:

4. **Learn / Docs** (only if “Status” gets too crowded)
   - Longer-form explanations, glossary, and “common failure modes” with remediation steps.
   - In Phase 1, we can keep this as expanders within “Status”.

### UX note (education without a separate tab)

Use **inline “What you’re seeing” expanders** at the top of each tab:

- Status tab: “Where does this data come from?” + “What does *complete / running / failed* mean?”
- Coverage tab: “How we classify months” + “Why APIs don’t fill old months”
- Run Jobs tab: “Safe defaults and how to not break prod”

---

## Implementation approach (incremental)

### Phase 1 — Read-only monitoring (no job triggers)

Deliver value quickly without side effects:

- Read from `data/alpha.db`:
  - `ingest_runs`, `ingest_run_stats`, `backfill_horizons`, `events`, `price_bars`
- Read from disk:
  - `data/raw_dumps/**`, `outputs/**`, `data/company_profiles/**`
- Implement:
  - Overview + Backfill Monitor + Dump Monitor
  - Minimal 10-year month grid (start with `price_bars` only)

### Phase 2 — Manual trigger framework (safe subprocess runner)

Add a reusable runner:

- A small wrapper that runs `subprocess.Popen(...)` with:
  - working directory = repo root
  - environment inherited + explicit overrides
  - line-by-line capture of stdout/stderr
  - timeout + cancel support (best-effort)
- Persist job metadata to a separate SQLite DB (avoid altering `alpha.db`):
  - `data/ops_jobs.db` with tables:
    - `ops_jobs` (id, started_at, finished_at, status, command, args_json, exit_code, log_path, initiated_by)
    - `ops_job_events` (job_id, ts, stream, line)
- UI shows:
  - recent jobs list
  - active job status
  - logs viewer

### Phase 3 — Smarter coverage + education polish

- Improve “complete/partial” thresholds per dataset.
- Add embedded documentation snippets:
  - “What is a dump adapter?”
  - “Why API sources only fill the last N days?”
  - “What does ingest_runs track?”
  - “How to interpret empty windows?”
- Add missing integrations if desired:
  - register `FMPDumpAdapter` + add to `config/sources.yaml`
  - add SerpAPI adapter (if we agree on schema + quotas)

---

## Open questions (need your decisions before Phase 2)

1. **Where should job history live?**  
   Recommendation: `data/ops_jobs.db` (separate from `alpha.db`) unless you want everything in one DB.

2. **What job triggers are allowed in production?**  
   Some scripts (e.g., smoke tests) may be “dev only”.

3. **What is the authoritative “10-year coverage” definition?**  
   For price bars: daily bars are straightforward; for news/events, expected volume is variable.  
   We can define completeness as “has at least 1 row that month” initially, then tighten later.

4. **Do we want to expose Target Stocks management on this page?**  
   It’s powerful; may belong in “Advanced” with confirmation gates.

---

## Next step (after this plan)

Implement Phase 1 as a new Streamlit route/page:

- Add route `ops` → `app/ui/ops_data_console.py`
- Extend the UI middle-layer (`EngineReadStore` / `DashboardService`) with read-only queries for:
  - `ingest_runs`, `ingest_run_stats`, `backfill_horizons`
  - `events` monthly counts
  - `price_bars` monthly counts
- Add disk scanners for `data/raw_dumps/**` + `outputs/**`
