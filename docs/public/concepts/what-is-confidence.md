# What is Confidence?

## Purpose
Explain what “confidence” means in Alpha Engine and how not to misuse it as a performance promise.

## Audience
- End users
- Evaluators (including investors)

## When to use this
- You see a confidence value and want to interpret it correctly.

## Prereqs
- `docs/public/legal/disclaimer.md`

---

## Definition
**Confidence** is a prediction-time belief signal (0.0–1.0) about a direction over a horizon. It is not an outcome.

## What confidence is not
- Not a guarantee of correctness
- Not a promised return
- Not a substitute for outcome-based evaluation

## How confidence relates to outcomes
Outcomes are measured later and recorded as:
- Return (e.g., `return_pct`)
- Direction correctness (boolean)
- Risk proxies (runup/drawdown, where available)

In code, outcomes are represented by `PredictionOutcome` in `app/core/types.py`.

## How to use confidence responsibly
- Compare confidence to measured outcomes across many samples (calibration).
- Segment by horizon and regime when possible.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

