# What is a Signal?

## Purpose
Define what “a signal” means in Alpha Engine and how to interpret it without confusing it with outcomes or guarantees.

## Audience
- End users
- Evaluators (including investors)

## When to use this
- You see “signal” in the UI or documentation and want to understand what it represents operationally.

## Prereqs
- `docs/public/legal/disclaimer.md`

---

## Definition
A **signal** is a structured recommendation-like artifact derived from inputs (events + market context) that expresses:
- **Ticker**
- **Direction** (up/down/flat)
- **Horizon** (evaluation window)
- **Confidence** (belief at prediction time)

Signals are not outcomes. A signal is an input to a research workflow.

## Where signals come from (in this repo)
Signals are produced downstream of:
- Event scoring: `app/core/scoring.py`
- Market reaction analysis (MRA): `app/core/mra.py`
- Prediction and evaluation pipelines (full path): `app/ingest/replay_engine.py` and engine utilities

The deterministic demo path exports predictions to `outputs/predictions.csv` (`python scripts/demo_run.py`).

## How to evaluate a signal responsibly
- Treat confidence as a belief metric, not a guarantee.
- Validate using outcomes:
  - Historical replay persists outcomes to `data/alpha.db` in `prediction_outcomes`.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

