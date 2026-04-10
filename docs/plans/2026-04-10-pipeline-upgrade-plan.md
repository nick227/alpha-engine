# Alpha Engine Pipeline Refactor Plan (MVP)

**Date**: 2026-04-10  
**Author**: Cascade  
**Status**: Draft  
**Priority**: High  

## Executive Summary

This refactor upgrades Alpha Engine from a partially mocked research pipeline into a real-data, auditable MVP. The focus is not broad platform expansion, but hardening the core flow: real feature generation, real scoring, real rankings, regime-aware performance, and enough persisted artifacts to manually inspect strategy behavior. The work prioritizes canonical production calculations, constrained multi-timeframe support, normalized macro features, and lightweight validation. The goal is to make the system trustworthy and testable before adding deeper automation or orchestration.

**Primary product goal (this doc):** the historical backfill must fully replace the mock seeder for day-to-day development by producing the same "seed data" artifacts the UI expects (signals, consensus, performance, champions, audit trails).

**Non-goal (for MVP):** perfect model accuracy. The deliverable is *correctness + auditability + parity with seeded UI read models*.

## Core Problems

### 1. Rankings not based on real scoring
UI may show rankings derived from:
- Placeholder weights
- Mock confidence  
- Incomplete outcomes

### 2. Strategy weights not derived from performance
Weights should come from:
- Recent accuracy
- Return quality
- Stability
- Sample size

### 3. Regime performance not tracked
We need:
- Bull vs bear vs sideways performance
- Volatility regime performance
- Macro-aware strategy accuracy

### 4. Price context too thin
Need consistent:
- 15m
- 1h  
- 1d

For:
- Feature generation
- Outcome scoring
- Horizon predictions

### 5. Macro features not real yet
Need:
- VIX
- DXY
- Oil
- Gold
- BTC
- 10Y yield

## Seed / Backfill Parity (Non-Negotiable)

The backfill is "done" when a fresh DB can be populated by running backfill + replay (no `seed_mock_data.py`) and the UI has enough real rows to render the same surfaces the seeder targets.

### Backfill tenant + run mode
- Backfill runs write to `tenant_id='backfill'` (do not pollute `tenant_id='default'`).
- Backfill must be resumable/idempotent: re-running the same range must not duplicate analytics rows.

### Idempotency keys (critical for backfill)
Every write-through/read-model table must support idempotent upserts. Minimum required columns on every write table:

- `tenant_id`
- `run_id`
- `idempotency_key`

Example `idempotency_key` (stable, deterministic):
```text
idempotency_key = sha256(ticker + "|" + strategy_id + "|" + horizon + "|" + timestamp)
```

Notes:
- Use the same `run_id` for the full backfill run (or for a deterministic per-slice run ID if you checkpoint).
- Replays must upsert by `idempotency_key` to prevent duplicate rows.

Recommended idempotency inputs by table:
- `predictions` / `signals`: `ticker + strategy_id + horizon + timestamp`
- `prediction_outcomes`: `prediction_id` (or `prediction_id + evaluated_at` if multiple exit reasons are supported)
- `consensus_signals`: `ticker + horizon + timestamp`
- `strategy_performance`: `strategy_id + horizon + asof_date` (or a fixed rollup period key)
- `strategy_weights`: `strategy_id + asof_date` (or a fixed rollup period key)

### Seed parity contract (tables)
Backfill/replay must populate at least the following tables (names are the runtime/UI canonical names):

- `price_bars`: historical bars for tickers + benchmark(s); required for context + outcome evaluation.
- `raw_events`, `scored_events`, `mra_outcomes`: event pipeline artifacts; required for audit views.
- `predictions`, `prediction_outcomes`: required for audit + evaluation.
- `signals`: write-through read model (do not force UI to infer from `predictions`).
- `consensus_signals`: canonical ranking read model (replaces `ranking_snapshots` for MVP).
- `strategy_performance` (including `horizon='ALL'`): aggregated metrics for champion cards and leaderboards.
- `strategy_stability`: stability metrics used by weighting + UI.
- `strategy_weights`: persisted adaptive weights (should be upsert/idempotent).
- `regime_performance`: aggregated regime metrics (minimum: by regime; optional: by strategy+regime in a v2 table).
- `promotion_events`: promotion/demotion history (even if "manual" initially).
- `loop_heartbeats`: pipeline/backfill run health markers.

Optional but recommended for the "analytics" screens:
- `prediction_runs`, `predicted_series_points`, `actual_series_points`, `prediction_scores`
- `ranking_snapshots` (defer for MVP; `consensus_signals` is canonical ranking)

### Seed parity source of truth
The parity contract is defined by what the UI reads (read-store schema), not by what any single seeder script happens to write. Any refactor should keep table names/columns additive and prefer write-through read models (`signals`, `consensus_signals`) over forcing the UI to derive state from raw engine tables.

### Backfill parity acceptance checks
After a backfill/replay run over a non-trivial window (e.g., 30+ days):
- `predictions` and `prediction_outcomes` both have non-zero rows for the backfill tenant.
- `signals` and `consensus_signals` have rows for each supported horizon.
- `strategy_performance` contains `horizon='ALL'` rows for active strategies.
- `strategy_weights` has at least one row per active strategy (upserted, stable IDs).
- `loop_heartbeats` contains start/end markers for the run.

### Backfill parity commands (operator-facing)
Examples (choose one):
- `python -m app.ingest.backfill_cli run --days 90`
- `python -m app.ingest.backfill_cli backfill-range --start 2026-01-01 --end 2026-04-01`

Preflight expectations:
- Bars provider must be available (env: `HISTORICAL_BARS_PROVIDER=alpaca|polygon|yfinance`, or `mock` only when explicitly allowed via `ALLOW_MOCK_BARS=true`).

### Backfill parity verification (SQL)
Minimum sanity checks (tenant `backfill`):
```sql
SELECT COUNT(*) FROM predictions WHERE tenant_id='backfill';
SELECT COUNT(*) FROM prediction_outcomes WHERE tenant_id='backfill';
SELECT COUNT(*) FROM signals WHERE tenant_id='backfill';
SELECT COUNT(*) FROM consensus_signals WHERE tenant_id='backfill';
SELECT COUNT(*) FROM strategy_weights WHERE tenant_id='backfill';
SELECT COUNT(*) FROM loop_heartbeats WHERE tenant_id='backfill';
```

## Refactor Architecture

**Canonical pipeline:**

```
ingest
  -> normalize
  -> build_features
  -> generate_predictions
  -> resolve_outcomes
  -> score_strategies
  -> calculate_weights
  -> rank_tickers
  -> select_champions
  -> publish_read_models
```

**Critical**: Weights must never depend on ranking. Order is strict.

Each stage:
- Deterministic
- Persisted
- Auditable

## Final Canonical Runtime Pipeline (MVP)
This is the runtime pipeline that must exist after the refactor (matches the parity contract exactly):

```
price_bars
raw_events
scored_events
mra_outcomes
        v
predictions
        v
prediction_outcomes
        v
signals  (write-through)
        v
strategy_performance
strategy_stability
        v
strategy_weights
        v
consensus_signals  (canonical ranking read model)
        v
promotion_events
```

## Canonical Formulas

### Ranking / Consensus Read Model
For MVP, "ranking" is whatever produces the dashboard's ordered signals. Practically this is `consensus_signals` (one row per `(ticker, horizon, timestamp)`), derived from `signals` + `strategy_weights`.

**Explicit MVP rule:** `consensus_signals` is the canonical ranking table. `ranking_snapshots` is not required for MVP.

#### `consensus_signals` canonical schema (MVP)
```
consensus_signals
  tenant_id
  run_id
  idempotency_key
  ticker
  horizon
  timestamp
  score
  direction
  confidence
  regime
  strategies_json
  weights_json
```

Where:
- `score` is the ranking score used for ordering (can equal `confidence` initially, but must be persisted explicitly).
- `strategies_json` captures contributing strategies + per-strategy signal metadata.
- `weights_json` captures the weights used for this consensus row.

#### `signals` canonical schema (MVP)
`signals` is a strict write-through table derived from `predictions`. The UI must not infer this view from other tables.

```
signals
  tenant_id
  run_id
  idempotency_key
  prediction_id
  ticker
  strategy_id
  horizon
  timestamp
  direction
  confidence
  predicted_return
```

```python
ticker_score = sum(strategy_score * strategy_weight)

conviction = abs(sum(weighted_direction)) / sum(abs(weights))

attribution = {
  strategy_id: {
    weight: float,
    score: float,
    contribution: float
  }
}
```

### Strategy Score
```python
strategy_score = (
  0.4 * direction_accuracy +
  0.3 * return_quality +
  0.3 * confidence_calibration
)
```

*(Future: drawdown_penalty, volatility_penalty)*

### Regime Classification
```python
# Trend regime
if spy_20d_return > 2%:   trend_up
elif spy_20d_return < -2%: trend_down  
else:                     sideways

# Volatility regime
if vix > 20:    high_vol
else:           low_vol

# Risk regime
if spy_up and vix_down:   risk_on
else:                     risk_off
```

### Weight Stability
```python
final_weight = 0.7 * previous_weight + 0.3 * new_weight
```

## Validation Rule
**Prevent lookahead bias:**
```
prediction_time < feature_time < outcome_time
```
Fail run if violated.

## Critical Definitions

### Direction Definition
```python
direction = ["up", "down", "flat"]  # canonical internal representation
```

**Normalization rule:** if upstream strategies produce BUY/SELL/HOLD (or +/-1), normalize at ingestion to `up/down/flat` and persist only the normalized value.

### Horizon Definition
```python
horizon = ["1d", "7d", "30d"]
```

Prevents mixing incompatible predictions.

### Champion Selection Rule
```python
champion = highest_score
where:
  sample_size >= min_sample
  stability >= threshold
```

*(Future: score * stability)*

### Stability Formula
```python
stability = 1 / (1 + score_std_dev)
# or simpler: stability = 1 - volatility(score_history)
```

### Horizon-Scoped Rankings
**Ranking key:** `(ticker, horizon, timestamp)`

Results in:
- AAPL 1d
- AAPL 7d  
- AAPL 30d

Never mix horizons in rankings.

## Phase 1 - Backfill Parity (Week 1)

### Deliverables (Week 1)
- Backfill + replay produces seed-parity rows for `tenant_id='backfill'` (see contract above).
- Replay persists `signals` (write-through read model derived from `predictions`).
- `consensus_signals` is built as the canonical ranking read model (uses `strategy_weights`).
- Post-run aggregation backfills `strategy_performance` (`horizon='ALL'`), `strategy_stability`, `strategy_weights`.
- Run health markers exist in `loop_heartbeats`.

### Refactor note (backfill parity first)
If writers already materialize `signals`, `consensus_signals`, and `strategy_weights`, Phase 1 should prioritize:
- ensuring the backfill/replay path emits them for historical runs
- adding a post-replay aggregation step that backfills `strategy_performance` and `strategy_stability` from `prediction_outcomes`
- ensuring deterministic/upsert behavior (stable IDs, idempotency keys) so a resumed backfill does not duplicate analytics

## MVP Execution Order (Parity-First)
This is the direct implementation sequence. Do not proceed to the next step until the prior step's outputs exist in the DB for `tenant_id='backfill'`.

1. **Backfill ingest produces auditable events**
   - Write `raw_events` deterministically (dedupe + stable IDs).
   - Write `scored_events` and `mra_outcomes` for every replayed event.

2. **Replay produces predictions + outcomes (per horizon)**
   - For each event in chronological order, emit predictions for horizons `1d/7d/30d`.
   - Evaluate outcomes with strict time ordering (no lookahead) and persist `prediction_outcomes`.

3. **Materialize `signals` as write-through from `predictions`**
   - Persist `signals` at prediction write time (not derived in the UI).
   - Canonical `signals` schema (MVP):
     - `tenant_id`, `run_id`, `idempotency_key`
     - `prediction_id`, `ticker`, `strategy_id`, `horizon`, `timestamp`
     - `direction`, `confidence`, `predicted_return`
   - `signals.idempotency_key` should be derived from the prediction primary key inputs (same inputs used to build prediction IDs), not from auto-generated UUIDs.

4. **Backfill analytics from outcomes (post-run aggregation)**
   - Aggregate `prediction_outcomes` into `strategy_performance` (must include `horizon='ALL'`).
   - Compute `strategy_stability` from outcome history.
   - Upsert `strategy_weights` from performance + stability (stable IDs, idempotent).
   - Update `regime_performance` (minimum: by regime).

5. **Build `consensus_signals` as the canonical ranking read model**
   - Upsert one row per `(ticker,horizon,timestamp)` using current `strategy_weights` + latest participating `signals`.
   - Persist `strategies_json` + `weights_json` for auditability and reproducibility.
   - `consensus_signals.idempotency_key` must be stable and derived from `(ticker,horizon,timestamp)` (plus `tenant_id` implicitly).

6. **Champion state is derived, then persisted**
   - Pick champion strategies using `min_sample` + `stability` gates.
   - Persist state transitions (`promotion_events`) and any strategy flags needed by the UI.

7. **Run health markers exist**
   - Backfill run writes `loop_heartbeats` start/end rows (and errors) so operators can audit runs quickly.

### Optional: Ranking Engine (defer for MVP)

Generates:
- Ticker score
- Conviction
- Attribution
- Strategy contribution

**Output (optional):** `ranking_snapshots`

**Each row:**
```sql
ticker,
score,
conviction,
strategies_used,
weights_used,
regime,
timestamp
```

### Strategy Weight Engine

Calculate weights using:
```
weight = accuracy * return_quality * stability * sample_factor
```

**Inputs:**
- Strategy scores
- Regime performance
- Rolling window

**Output:** `strategy_weights`

### Regime Performance

Track:
- Trend up/down/sideways
- High/low volatility
- Risk on/off

**Store:** `regime_performance`

## Phase 2 - Multi-Timeframe Price Context (Week 2)

### Add
- `app/core/bars/multi_timeframe.py`

### Supported
- 15m
- 1h
- 1d

### Provider returns
```python
get_price_context(ticker, asof)
```

**Includes:**
- Returns 1d/7d/30d
- Volatility
- Trend slope
- Volume ratio
- ATR

**Used by:**
- Strategies
- Scoring
- Regime detection

## Phase 3 - Macro Features (Week 3)

### Add
- `app/core/macro/provider.py`

### Fetch
- VIX
- DXY
- CL (oil)
- GC (gold)
- BTC
- US10Y
- SPY benchmark

### Normalize to
`macro_snapshot`

### Features
- VIX level/change
- DXY return
- Oil/gold/BTC returns
- Yield change
- Risk-on score

**Used by:**
- Strategies
- Regime analyzer
- Ranking attribution

## Phase 4 - Auditability Layer (Week 4)

### Add
- `app/core/validation/pipeline_checks.py`

### Checks
- Missing bars
- Missing outcomes
- Zero predictions
- Stale macro
- Lookahead leakage
- Empty features
- Invalid timestamps

### Outputs
- `pipeline_run_report`

## Required Tables

This section is intentionally redundant with "Seed / Backfill Parity". If there is any conflict, the parity contract wins.

## Missing Artifact

### ranking_attribution
```json
{
  "ticker": "AAPL",
  "score": 0.71,
  "strategies": {
    "sentiment_v1": {
      "weight": 0.32,
      "score": 0.81,
      "contribution": 0.259
    },
    "mean_reversion_v2": {
      "weight": 0.21,
      "score": 0.42,
      "contribution": 0.088
    }
  }
}
```

## Multi-Timeframe Design

**Store only base timeframe:**
- 15m bars

**Derive:**
- 1h = aggregate 4 bars
- 1d = aggregate 26 bars

**Benefits:**
- Less storage
- Deterministic
- No drift

## Macro Snapshot Structure

**Standardized:**
```sql
timestamp,
vix,
vix_change_1d,
dxy_return_1d,
oil_return_1d,
gold_return_1d,
btc_return_1d,
spy_return_1d,
yield_10y,
risk_on_score
```

Single row per timestamp.

## Final Canonical Data Model

```
price_bars
macro_snapshot (optional, Phase 3)
raw_events
      |
      v
scored_events / mra_outcomes
      |
      v
predictions
      |
      v
prediction_outcomes
      |
      v
signals (write-through from predictions)
      |
      v
strategy_performance / strategy_stability
      |
      v
strategy_weights
      |
      v
consensus_signals (canonical ranking read model)
      |
      v
promotion_events
```

## Final Canonical Schema

**MVP (seed parity) tables:** see "Seed parity contract (tables)".

## Config

**config/pipeline.yaml**
```yaml
timeframes:
  - 15m
  - 1h
  - 1d

macro:
  enabled: true

ranking:
  snapshot: per_run

weights:
  min_sample: 20

regime:
  enabled: true

validation:
  enabled: true
```

## Success Criteria

After implementing this:

You will be able to:
- Run backfill instead of the mock seeder (UI renders from real rows).
- Trace every prediction
- Compare strategies by ticker
- Compare strategies by horizon
- Compare strategies by regime
- Inspect deterministic consensus math (`consensus_signals.strategies_json` + `weights_json`)
- Inspect weight evolution
- Debug bad strategies
- Verify no lookahead
- Validate scoring math
- Re-run/replay without duplicate rows (idempotent upserts)

This is the exact MVP architecture Alpha Engine needs.

## Implementation Order

### Week 1
- Backfill ingest + replay produces `raw_events/scored_events/mra_outcomes`
- Persist `predictions` + `prediction_outcomes` (no lookahead)
- Persist `signals` + `consensus_signals` during replay
- Post-run aggregation: `strategy_performance` (`horizon='ALL'`), `strategy_stability`, `strategy_weights`
- Persist `loop_heartbeats` markers for the run

### Week 2
- Regime performance tracking (`regime_performance`)
- Champion selection + `promotion_events` (minimal but auditable)

### Week 3
- Multi timeframe bars (15m base)
- Feature builder (derive 1h/1d)

### Week 4
- Macro provider (standardized snapshot)
- Validation checks (lookahead prevention)

## Result

Alpha Engine becomes:
- Real-data driven
- Strategy measurable
- Regime aware
- Fully auditable
- Deterministic calculations
- Stable for user testing

## What this version guarantees

After implementation:
- Backfill replaces mock seeder for day-to-day dev
- UI renders from real data (no derived UI logic)
- Consensus/rankings are deterministic and reproducible (`consensus_signals`)
- Weights are adaptive and persisted (`strategy_weights`)
- Champions are auditable (`promotion_events`)
- Replay is idempotent (run can resume without duplicates)
- Strategies are comparable across tickers/horizons/regimes
- Horizons are isolated (no mixing `1d/7d/30d`)
- Regimes are measurable (`regime_performance`)

---

**Document Version**: 3.0  
**Last Updated**: 2026-04-10  
**Focus**: Deterministic MVP Pipeline
