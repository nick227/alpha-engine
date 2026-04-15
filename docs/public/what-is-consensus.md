# What is Consensus?

## Purpose
Explain “consensus” as a method for combining signals from multiple tracks/strategies into a single direction/confidence.

## Audience
- End users
- Evaluators
- Developers (high level)

## When to use this
- You see multiple strategies disagreeing or you see a “consensus” signal and want to know how it’s formed.

## Prereqs
- `docs/public/concepts/what-is-a-signal.md`

---

## Definition
**Consensus** is a combining step that takes multiple signals and produces a single direction/confidence intended to be more robust than any single contributor.

## Why consensus exists
- Strategies can disagree; disagreement often indicates uncertainty.
- Different approaches work better in different regimes (volatility/trend conditions).
- A consensus layer helps avoid “single-strategy brittleness.”

## How consensus works (in this repo)
There are multiple paths in the repo, but the public-safe concept is:
- Two tracks (sentiment-like and quant-like) can be blended using regime-aware weights.
- The consensus payload includes per-track confidences and weights for audit/debugging.

Reference implementations:
- Demo consensus: `app/runtime/consensus.py`
- Fuller engine path: `app/engine/runner.py` and analytics pipeline modules

## How to evaluate consensus
- Evaluate consensus the same way you evaluate any signal: by outcomes over time, per horizon and regime.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

