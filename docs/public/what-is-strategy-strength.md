# What is Strategy Strength?

## Purpose
Define “strategy strength” as an evidence-based measure of how a strategy performs, distinct from one-off confidence or single-window results.

## Audience
- End users (interpretation)
- Evaluators (due diligence)
- Developers (high level)

## When to use this
- You see strategy rankings/leaderboards and want to understand what they mean and what can make them misleading.

## Prereqs
- `docs/public/concepts/what-is-backtesting.md`

---

## Definition
**Strategy strength** is a summary of observed performance, typically derived from many outcomes:
- Accuracy / direction correctness rate
- Average return (by horizon)
- Stability / variance measures (risk-aware proxies)
- Calibration (whether confidence aligns with outcomes)

## Strategy strength vs confidence
- **Confidence**: belief at prediction time (per signal).
- **Strategy strength**: evidence from many outcomes (per strategy over time).

## Where strategy strength comes from (in this repo)
Depending on run mode, strategy performance is derived from:
- Outcome evaluation type: `app/core/types.py` (`PredictionOutcome`)
- Evaluation helper: `app/engine/evaluate.py`
- Analytics pipeline (performance → weights → consensus → promotions): `app/engine/analytics_runner.py`

Evidence is typically persisted in SQLite (`data/alpha.db`) tables such as:
- `prediction_outcomes`
- `strategy_performance` (when produced by analytics workflows)

## How to interpret it safely
- Always segment by **horizon** and (when available) **regime**.
- Be cautious of small sample sizes.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

