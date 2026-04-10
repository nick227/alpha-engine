# What is Backtesting?

## Purpose
Explain backtesting as a research method for validating signals/strategies with measurable outcomes.

## Audience
- End users
- Evaluators

## When to use this
- You want evidence that a signal/strategy is useful beyond a single example.

## Prereqs
- `docs/public/legal/disclaimer.md`

---

## Definition
A **backtest** is a replay-style evaluation where you:
1. Generate predictions using only information available at the time, then
2. Measure what happened after the horizon (outcomes), and
3. Summarize performance across many examples.

## What a good backtest controls for
- Look-ahead bias (future returns leaking into features)
- Regime dependency (volatility/trend changes)
- Sample size and window selection

## Backtesting in this repo (practical)
There are three levels described in:
- `docs/public/kb/how-to-use-and-evaluate.md`

In particular:
- Historical backfill + replay persists outcomes to `data/alpha.db` (`prediction_outcomes`).
- Window scoring/ranking is available via `python -m app.engine.score_predictions_cli eval-window ...`.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

