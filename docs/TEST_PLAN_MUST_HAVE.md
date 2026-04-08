# Must-Have Tests (Build These First)

This repo is moving toward an autonomous loop:

live → predictions → consensus → replay scoring → performance/stability → weighting → optimizer → promotion/rollback → repeat

The goal of this test plan is to lock down **core correctness + data integrity** before adding more features.

## Tooling

### Install (dev)

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Run tests

```bash
pytest
```

### Lint

```bash
pylint app scripts tests
```

## Must-Have Test Groups

### Pipeline — Core correctness

Build these first (fast unit tests):

1. Strategy produces prediction given event
2. Consensus combines sentiment + quant correctly (+ agreement bonus)
3. Regime weighting changes final score
4. Replay scoring computes outcome correctly (expired prediction → outcome row)
5. Stability calculation updates properly (backtest vs live ratio)
6. Optimizer mutation produces valid child config
7. Promotion gate accepts/rejects correctly
8. Rollback triggers when stability drops

Implemented in:
- `tests/test_pipeline_core.py`

### Pipeline — Data integrity

1. Prediction → outcome linkage works (outcome references prediction; prediction marks scored outcome)
2. Horizons expire correctly (no scoring before expiry)
3. No duplicate processing of raw events (queue status prevents reprocessing)
4. Regime stored per prediction (SQL column + feature snapshot)
5. Strategy lineage parent_id preserved (strategy_state / promotion_events)

Partially covered by:
- `tests/test_pipeline_core.py` (prediction→outcome)
- `tests/test_end_to_end_loops.py` (queue “process once” behavior indirectly via live tick)

### Pipeline — End-to-end

1. `run_pipeline` produces predictions
2. Live loop writes predictions
3. Replay loop updates outcomes
4. Performance/stability tables update
5. Consensus can reflect stability weights when present

Implemented in:
- `tests/test_end_to_end_loops.py` (smoke tick)

## Next Tests To Add (After Must-Have)

- Live loop deferral: enqueue event without bars → status becomes `DEFERRED` with `next_retry_at`
- Replay idempotency: rerun replay loop → scored count stays 0
- Optimizer promotion path: force PASS candidate → becomes active; parent archived; promotion_event recorded
- Consensus end-to-end: seed `strategy_stability` for track strategies and verify consensus weights stored in `consensus_metadata`

