﻿﻿# Help (Help-Light)

## Purpose
Provide lightweight “how it works” and component-level guides without sensitive internal detail.

## Audience
- End users
- Evaluators
- Developers who want a high-level map before going deeper

## When to use this
- You need conceptual understanding or quick troubleshooting pointers.

## Prereqs
- None

## Pages
- Glossary (below)
- How predictions work: `docs/public/help/how-predictions-work.md`
- Data sources at a glance: `docs/public/help/data-sources-at-a-glance.md`
- Component cards: `docs/public/help/components.md`

---

## Glossary (public)
- **Event**: A structured representation of an information item (news/media/etc.) with metadata.
- **Scored event**: An event annotated with classification/sentiment/impact fields.
- **Prediction**: A direction + horizon + confidence (model belief) produced by a strategy.
- **Horizon**: The evaluation window (e.g., 15m/1h/4h/1d).
- **Outcome**: What happened after the horizon (return, direction correctness, drawdown/runup).
- **Strategy**: A rule/model family that produces predictions.
- **Consensus**: A combined view from multiple strategies/signals.
- **MRA**: Market Reaction Analysis (how markets respond after events; used as features/signals).

Common confusions:
- **Confidence vs outcome**: confidence is belief at prediction time; outcomes are measured later and are the evidence layer.
- **Flat vs neutral**: core type uses `flat` for “no edge,” but some demo paths may emit `neutral`; treat both as “flat/no edge.”

## Concepts (core understanding)
- What is a signal?: `docs/public/concepts/what-is-a-signal.md`
- What is consensus?: `docs/public/concepts/what-is-consensus.md`
- What is confidence?: `docs/public/concepts/what-is-confidence.md`
- What is backtesting?: `docs/public/concepts/what-is-backtesting.md`
- What is strategy strength?: `docs/public/concepts/what-is-strategy-strength.md`
