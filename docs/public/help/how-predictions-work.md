﻿﻿# How Predictions Work (Public Overview)

## Purpose
Explain the prediction lifecycle in a public-safe way: inputs → signals → predictions → outcomes.

## Audience
- End users
- Evaluators
- Investors

## When to use this
- You need to understand what predictions mean and how to interpret them.

## Prereqs
- `docs/public/legal/disclaimer.md`

---

## High-level flow (what happens end-to-end)
```mermaid
flowchart LR
  A[News/Media + Market Data] --> B[Event Scoring]
  B --> C[Market Context / Features]
  C --> D[Strategy Predictions]
  D --> E[Outcome Evaluation]
  E --> F[Ranking & Learning (internal)]
```

## What a prediction contains
In this repo, the core data shapes are defined in `app/core/types.py`:
- **RawEvent**: source text + timestamp + tickers
- **ScoredEvent**: category/direction/materiality/confidence + explanation terms
- **MRAOutcome**: short-horizon return features + derived `mra_score`
- **Prediction**: ticker + direction + horizon + confidence + entry price

In the deterministic demo runner (`scripts/demo_run.py`), the pipeline exports:
- `outputs/scored_events.csv`
- `outputs/mra_outcomes.csv`
- `outputs/predictions.csv`
- `outputs/strategy_performance.csv`

Note: the demo pipeline can emit a “neutral” direction for the simplified strategy; treat this as “flat/no edge” in interpretation.

## What confidence is (and is not)
- Confidence is a *self-reported belief* signal, not a promise.
- Outcomes must be measured separately; use backtests/outcome artifacts for evaluation.

## How to evaluate responsibly
- Start here: `docs/public/kb/how-to-use-and-evaluate.md`

## Where to go deeper (internal sources)
- Demo pipeline orchestration: `app/runtime/pipeline.py`
- Full engine runner path: `app/engine/runner.py`
