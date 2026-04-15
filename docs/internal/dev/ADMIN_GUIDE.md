# Alpha Engine POC - Administrator Guide

Welcome to the Alpha Engine system. This guide covers the operational management, debugging, and administration of the closed-loop ingestion, prediction, and self-learning pipelines.

## 1. Core Architecture Overview

The system operates across three tightly integrated domains:

1. **Declarative Ingestion Pipeline (`app/ingest/`)**
   - **Configuration:** Managed strictly via `config/sources.yaml`. Add/Remove APIs, RSS feeds, or mock bundles here without writing code.
   - **Execution:** `async_runner.py` drives parallel fetching using Python's `asyncio.gather()`.
   - **Features:** Includes natively enforced `metrics.py` (tracking API health), `dedupe.py` (preventing duplicate events), `rate_limit.py`, and `router.py` (segregating events).

2. **Core Prediction Engine (`app/engine/runner.py`)**
   - **Signal Generation:** Routes contextualized events to text/sentiment or technical/quant tracks.
   - **Consensus:** The `consensus_engine.py` merges conflicting prediction signals using adaptive weighting relative to the current market regime (e.g., highly trusting quant in trending markets, trusting sentiment in high volatility).

3. **Closed-Loop Self-Learning (The "Optimization Layer")**
   - **Continuous Learner:** Hooks right onto the end of the `run_pipeline()` invocation, analyzing simulated "actual returns" vs "predicted returns".
   - **Genetic Optimizer & Generative Mutators:** Breeds parameters dynamically forming shadows candidates (Challengers).
   - **Promotion Engine:** A strict gating function that autonomously shifts models back and forth from active trading logic (Champions) without crashing the pipeline.

---

## 2. Managing Ingestion Sources

To map new data dependencies to the machine, edit `config/sources.yaml`:
```yaml
sources:
  - id: "my_new_data_feed"
    adapter: "reddit" # Target adapter string (e.g. reddit, alpaca, bundle)
    url: "https://reddit.com/r/investing/new.json"
    interval: 60
    enabled: true
    extract:
      expression: "data.title + ' ' + data.selftext"
      tags:
        - "social"
```
*Note: Any payload mismatch runs through `validator.py` at launch, triggering immediate fail-safes reducing runtime exceptions.*

---

## 3. The Self-Learning Workflow

The model organically tunes its configurations. You do not need to pause production to "retrain."

### How the Engine Evaluates Models
Instead of simple 'win rates', performance incorporates 4 hard-coded invariants:
1. **Correctness** (Did direction strictly match return polarity?)
2. **Alpha Magnitude** (Penalize deeply wrong calls, reward high-magnitude right calls)
3. **Stability** (Return variances akin to a rolling Sharpe ratio)
4. **Regime Adherence** (Does this strategy actually work in the current Volatility constraint?)

### Promotion and Demotion Bounds (Preventing Thrash)
The `promotion_engine.py` utilizes hard thresholds to swap active logic rules:
- **Promotion to Champion:** Win Rate > 51%, Alpha > 0.5, Stability Tracker > 0.40
- **Demotion back to Challenger:** Win Rate < 46%, Negative Alpha <-1.0, Stability collapses.

*If you need to make the machine "slower" or "faster" to adapt to market regime shifts, tune the offsets directly inside `app/engine/promotion_engine.py`.*

## 4. 90-Day Backfill Pipeline
Before live trading begins, the system can run a historical backfill to seed memory and train models.

- **Trigger:** Run `python -m app.ingest.backfill_runner` (or via scripts/test_backfill.py for rapid testing).
- **Behavior:** 
  1. Scans `config/sources.yaml` for `backfill_days` (defaults to 90).
  2. Fetches historical data in parallel chunks from all configured adapters.
  3. Validates and dedupes all historical events via the standardized pipeline.
  4. Persists the historical corpus to `data/alpha.db`.
  5. **Sequential Replay:** Iterates through every historical event chronologically.
  6. Calls `run_pipeline` for each batch, allowing the `ContinuousLearner` and `GeneticOptimizer` to evolve "Champions" from historical data before any live risk is taken.

*Resumability: Because it uses `INSERT OR IGNORE` on SHA256 hashes, you can stop and restart the backfill at any time without creating duplicate training noise.*

---

## 5. Hooking the Loop In Production

In your top-level `main.py` (or application entry point), ensure you pass instances of the learning states to the `run_pipeline` function. Because state is highly dynamic, instantiate them cleanly before entering the loop:

```python
from app.engine.continuous_learning import ContinuousLearner
from app.engine.strategy_registry import StrategyRegistry
from app.engine.promotion_engine import PromotionEngine
from app.engine.genetic_optimizer import GeneticOptimizer

# Hold these globally or attached to your daemon context 
# to ensure generational parameters persist!
registry = StrategyRegistry()
learner = ContinuousLearner()
promotion_engine = PromotionEngine(registry)
genetic_optimizer = GeneticOptimizer(registry)

# In your event loop:
run_pipeline(
    raw_events=batch,
    price_contexts=context,
    evaluate_outcomes=True,  # Mandatory to trigger learning
    learner=learner,
    registry=registry,
    promotion_engine=promotion_engine,
    genetic_optimizer=genetic_optimizer,
    generation_counter=current_gen
)
```

---

## 5. Debugging Checklist

If the engine is skipping executions or making poor choices:

1. **Verify Metrics:** Check the JSON output or memory-maps for `app/ingest/metrics.py`. Drops are explicitly logged. If the adapter hits HTTP rate limits, `rate_limit.py` logs explicit throttling offsets.
2. **Database Validations:** The ingestion pipe filters natively using `INSERT OR IGNORE` upon standard SHA256 hashes inside SQLite (`alpha.db`). If events look missing, check for identical payload text deduplication hits.
3. **Genetic Stagnation:** If predictions flatline, review the `registry.get_challengers()`. The `genetic_optimizer.py` forces a 10% explicit mutation threshold and randomly seeds new strategies automatically to prevent local minima lock-in.

---

*This document was generated automatically to map the newly completed declarative ingestion rules and native evolutionary loops within `runner.py`.*
