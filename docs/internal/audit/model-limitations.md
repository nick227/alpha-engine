# Model Limitations (Internal)

## Purpose
Document what predictions mean, what they do not mean, and known failure modes.

## Audience
- Auditors
- Developers

## When to use this
- You need to bound claims and explain risk honestly.

## Prereqs
- Familiarity with the prediction lifecycle

---

## What predictions represent
- A prediction is a structured record containing:
  - **Ticker**
  - **Direction** (up/down/flat; demo path may emit `neutral`)
  - **Horizon** (when it is evaluated)
  - **Confidence** (a belief signal at prediction time, 0.0–1.0)
  - **Entry price** (used to compute a realized exit price during evaluation)

Core shapes are defined in `app/core/types.py` (`Prediction`, `PredictionOutcome`).

## Non-goals (baseline)
- Predictions are not guarantees.
- The system is not represented as a fully automated trading bot in the current tier-1 offer.
- “Confidence” is not a promise of accuracy or return; it must be validated against outcomes.

## Common failure modes
### 1) Look-ahead bias (evaluation leakage)
- The evaluation harness reads realized returns from **future-return keys** such as `future_return_15m`, `future_return_1d`, etc. (see `app/engine/evaluate.py`).
- If those keys are accidentally included in the feature set given to strategies, evaluation becomes invalid.
- Mitigation in the fuller runner path: `app/engine/runner.py` contains `_split_context()` to separate `features` vs `outcomes` and to peel off `future_return_*` keys from flat dicts.

### 2) Timestamp alignment and “1970” fallbacks
- Ingestion drops events where timestamps fail to parse or normalize to epoch-year 1970 (see `app/ingest/validator.py` and `app/core/time_utils.py`).
- Mis-mapped `extract.timestamp` fields in `config/sources.yaml` can silently reduce coverage (events will be dropped as invalid).

### 3) Regime shift + non-stationarity
- Consensus weighting is regime-aware (volatility snapshot) but still depends on stable relationships between features and outcomes.
- When volatility/trend regimes change abruptly, strategies can degrade until weights/re-ranking adapts.

### 4) Sparse/biased inputs
- Coverage can be uneven by ticker, timeframe, or source type (news vs market vs macro).
- Backtests on thin data can look deceptively strong or weak depending on window selection.

### 5) Misinterpreting demo behavior as production behavior
- The deterministic demo (`scripts/demo_run.py` → `app/runtime/pipeline.py`) uses simplified scoring + MRA + “simple” predictions.
- That path does **not** currently produce `PredictionOutcome` exports; it exports scored events, MRA outcomes, predictions, and a summary CSV.
- The fuller engine path (`app/engine/runner.py`) contains the outcome evaluation logic used for learning/ranking loops.

### 6) Rule-based scoring limitations (current scorer)
- `app/core/scoring.py` is currently rule/term-cloud driven (category rules + intensity terms + ticker term clouds).
- It is explainable, but it is also brittle to phrasing changes and may not generalize to new domains without updating rules/term clouds.

## Verification steps
- Confirm that strategies never receive `future_return_*` keys as features (use `_split_context()` conventions in `app/engine/runner.py`).
- Validate ingest coverage: run diagnose (`python -m app.ingest.diagnose`) and/or backfill coverage reports via `python -m app.ingest.backfill_cli ingest-health ...`.
- Use outcome artifacts (when produced) to validate behavior across multiple windows and regimes; avoid judging from one period.
