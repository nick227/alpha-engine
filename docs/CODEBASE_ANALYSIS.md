# Alpha Engine POC — Codebase Analysis (Apr 8, 2026)

This document summarizes what the codebase is trying to become (per `/docs`), what is currently wired vs. scaffolded, and what you need to connect/finish to **actually run it end-to-end and get value** (backtests → rankings → actionable “paper/live” loop).

## TL;DR (Best Value Next Steps)

1. **Restore one runnable “vertical slice”**: `raw events + price context → scoring → MRA → strategies → predictions → outcomes → ranking → saved artifacts` (so you can iterate on models/strategies daily).
2. **Pick one canonical contract** for:
   - prediction/strategy data models (types)
   - strategy interface
   - persistence schema (SQLite vs Prisma schema)
3. **Wire the dashboard to real outputs** (read from SQLite and/or `outputs/*.csv`) so “value” is visible without digging through files.

Right now the repo contains strong building blocks and a clear intended architecture, but it has **schema/interface drift** from the v2 overlays into v3 scaffolds. That drift breaks the default demo runner and makes “paper/live” loops mostly placeholders.

## Docs Progression (Intent)

From `/docs`:

- `docs/ARCHITECTURE.md`: canonical flow: raw events → scored events → MRA outcomes → strategies → predictions → outcomes → ranking.
- `docs/ROADMAP.md`: POC focuses on dual track (news + quant), prediction-first evaluation, backtest “horse racing”, Streamlit comparison dashboard; next is paper-mode runner + more technical families + hybrid text+technical + calibration/ranking metrics; later Postgres/SaaS migration.
- v2.x overlays: add replay worker, performance aggregation, genetic optimizer, lifecycle scaffolds.
- v3.0 overlay: “Recursive Alpha Engine” loops (live/replay/optimizer), champions, consensus, stability/rollback guardrails, mission-control UI.

## Current Code Map (Where Things Live)

### Core modeling + scoring
- `app/core/types.py`: current dataclasses (RawEvent, ScoredEvent, MRAOutcome, StrategyConfig, Prediction, PredictionOutcome).
- `app/core/scoring.py`: heuristic text scoring (category/materiality/confidence/tags).
- `app/core/mra.py`: heuristic market reaction analysis from `price_context` dict.
- `app/core/repository.py`: SQLite persistence (creates tables and writes strategies/predictions/outcomes).

### Strategies (mixed maturity)
- “Newer” strategy interface (`StrategyBase`) expects: `(scored_event, mra, price_context, event_timestamp)`:
  - `app/strategies/text_mra.py`
  - `app/strategies/baseline_momentum.py`
  - `app/strategies/technical/*` (VWAP/RSI/Bollinger families)
- “Older” strategy interface is referenced but not implemented anymore:
  - `app/strategies/ma_cross.py` and `app/strategies/rsi.py` import `BaseStrategy` / `StrategyContext` that do not exist in `app/strategies/base.py` (these strategies are currently broken / dead code).

### Evaluation + ranking (multiple overlapping approaches)
- `app/engine/evaluate.py`: evaluates `Prediction` using `price_context` (e.g., `future_return_15m`) and produces `PredictionOutcome`.
- `app/engine/evaluator.py`: evaluates `Prediction` using OHLCV bars (`pandas` DataFrame) — different contract than `evaluate.py`.
- `app/engine/ranking.py`: aggregates performance from `Prediction` + `PredictionOutcome` (expects `strategy_name` field that **is not present** in `app/core/types.py:Prediction`).

### “v3 runtime” scaffolds
- `app/runtime/recursive_runtime.py`, `app/runtime/scheduler.py`: loop scaffolds.
- `app/engine/live_loop_service.py`, `app/engine/replay_loop_service.py`, `app/engine/optimizer_loop_service.py`: return placeholder dicts.
- `app/engine/recursive_alpha_engine.py`: consensus/guardrail scaffolds; doesn’t connect to real strategy execution or persistence.
- `app/ui/dashboard.py`: Streamlit “mission control” UI is currently static demo values (not reading DB/outputs).

### Duplicate module stacks (choose one)
There are parallel, partially overlapping implementations:
- `app/engine/*` vs `app/intelligence/*` (two consensus/weighting implementations)
- `app/engine/*` vs `app/evolution/*` (two mutation/tournament/promotion implementations)

This isn’t “wrong” as experimentation, but it’s currently a source of confusion and integration breakage.

### Persistence model drift (SQLite vs Prisma)
- `prisma/schema.prisma` defines v3 lifecycle/champions/consensus/heartbeat tables.
- `app/core/repository.py` defines a **different** SQLite schema (raw_events/scored_events/mra_outcomes/strategies/predictions/prediction_outcomes).

There is currently no code path that treats Prisma as the “canonical” store for the running Python system, and the Streamlit UI does not query either store.

## What Works Today (Concrete, Verifiable)

- Individual building blocks import and are reasonably self-contained:
  - `app/core/scoring.py:score_event`
  - `app/core/mra.py:compute_mra`
  - several `StrategyBase` strategies under `app/strategies/` and `app/strategies/technical/`
  - `app/engine/evaluate.py:evaluate_prediction` (price-context evaluation)
- The repo includes prior generated artifacts in `outputs/*.csv` that reflect an earlier end-to-end pipeline.

## What Is Not Wired / Currently Broken

### 1) The default demo runner is broken

Running `python scripts/demo_run.py` currently fails with:
- `ImportError: cannot import name 'run_pipeline' from 'app.engine.runner'`

Because:
- `scripts/demo_run.py` imports `run_pipeline` from `app/engine/runner.py`
- `app/engine/runner.py` currently contains a `Runner` class for v2.7 consensus building, but **no** `run_pipeline` function

### 2) Strategy API drift (some strategies can’t run)

- `app/strategies/ma_cross.py` and `app/strategies/rsi.py` reference `BaseStrategy` and `StrategyContext` which don’t exist.
- If you try to include them in any strategy registry, imports will fail.

### 3) Type/schema drift breaks ranking and backtest analytics

Examples:
- `app/engine/ranking.py` expects `Prediction.to_dict()` to include `strategy_name`, but `app/core/types.py:Prediction` does not include `strategy_name` and has no `to_dict()` method.
- `app/core/time_analysis.py` expects prediction rows to contain fields like `realized_return`, which aren’t produced by `app/engine/evaluate.py` (it outputs `PredictionOutcome.return_pct` instead).

### 4) v3 loops and UI are scaffolds

- Loop services return static “notes” and do not:
  - load active strategies
  - generate predictions
  - persist predictions/outcomes
  - update stability/performance inputs
- `app/ui/dashboard.py` does not query real data; it shows hard-coded metrics.

### 5) “Ingest” is a placeholder

- `app/ingest/` is effectively empty.
- There’s no unified data ingestion path for:
  - historical news/events
  - historical OHLCV
  - live streaming/polling

## Minimum Definition of “Usable” (So You Benefit)

If the goal is “this helps me make better decisions / test ideas”, the smallest usable system is:

1. **Backtest mode you can run in one command**
   - Input: a historical set of events + matching price context (or bars)
   - Output: predictions + outcomes + strategy ranking tables
2. **A dashboard that reads those outputs**
   - Strategy leaderboard
   - Per-ticker signal history
   - Basic calibration + stability slices
3. **A “paper mode” runner**
   - Same prediction pipeline
   - Writes predictions to DB
   - Has a replay worker that closes outcomes after horizon expiry

Everything else (genetic optimizer, champion selection, recursive loops) becomes valuable only after (1)-(3) are solid.

## Best-Value Wiring Work (What To Connect Next)

### A) Re-introduce the single source of truth pipeline entrypoint

Create/restore a function like `app/engine/pipeline.py:run_pipeline(...)` (or re-add it to `app/engine/runner.py`) that:

- takes `raw_events: list[RawEvent]` and `price_contexts: dict[event_id, dict]`
- scores events via `app/core/scoring.py`
- computes MRA via `app/core/mra.py`
- loads strategies from `experiments/strategies/*.json` into `StrategyConfig`
- runs `StrategyBase` strategies to emit `Prediction` objects
- evaluates outcomes (choose one contract: price_context OR bars; don’t keep both for the “main” path)
- writes `outputs/*.csv` and/or persists to `data/alpha.db` through `app/core/repository.py`

This is the fastest way to turn the repo into something you can iterate on weekly.

### B) Pick one strategy interface and delete/retire the other

Recommended for speed:
- Keep `app/strategies/base.py:StrategyBase` as the only interface.
- Port or retire `app/strategies/ma_cross.py` and `app/strategies/rsi.py`.

### C) Unify prediction/outcome record shape

Pick one “row schema” that downstream analytics uses. Two reasonable choices:

1) **Dataclass-first**: add `to_dict()` methods to `Prediction` and `PredictionOutcome`, and make all analytics operate on those dicts.
2) **DataFrame-first**: standardize CSV columns and let analytics always use dataframes.

Right now it’s mixed, which is why `outputs/*.csv` don’t match current code.

### D) Decide on persistence for the POC

For near-term “value”:
- Use `app/core/repository.py` SQLite as the actual store for predictions/outcomes/strategy performance.

Then later:
- Map/port to `prisma/schema.prisma` (or replace Prisma with a Python-native ORM) when you truly need Postgres + SaaS/multitenancy.

### E) Connect the Streamlit dashboard to real data

Make `app/ui/dashboard.py` read from either:
- `data/alpha.db` via `Repository.query_df(...)`, or
- `outputs/*.csv` (simplest)

and render:
- strategy performance table
- prediction stream grouped by ticker + horizon
- slice stability report if present

## Notes on v3 “Recursive Alpha Engine”

The v3 overlay is conceptually strong (loops + champions + guardrails), but it is currently “architecture-first”.

To make it real, each loop needs concrete dependencies:
- **Live loop**: active strategies + live data + persistence writer
- **Replay loop**: unscored predictions + price data + outcome writer + metric updaters
- **Optimizer loop**: mutation engine + tournament runner + promotion gate + strategy registry persistence

My recommendation is to treat v3 as a second milestone after the v2 backtest/paper vertical slice is stable.

## “Best Value” Feature Prioritization (Suggested)

1. Fix the runnable pipeline + dashboard wiring (1–2 sessions)
2. Add a paper-mode runner that reuses pipeline + replay worker (1–2 sessions)
3. Add ingestion adapters (historical first, live second) (2–4 sessions)
4. Only then: optimizer/champions/recursive loops (ongoing)

## Quick Inventory of High-Risk Drift Points

- `scripts/demo_run.py` expects `run_pipeline` but it doesn’t exist.
- `app/engine/ranking.py` expects prediction fields/methods that current dataclasses don’t provide.
- `app/strategies/ma_cross.py` and `app/strategies/rsi.py` reference missing base classes.
- `prisma/schema.prisma` is not connected to Python persistence; `app/core/repository.py` is a separate schema.

