﻿﻿# Investor FAQ

## Purpose
Answer evaluation questions about Alpha Engine: defensibility, measurement, data, and roadmap.

## Audience
- Investors
- Due diligence reviewers

## When to use this
- You are evaluating feasibility, differentiation, and go-to-market risks.

## Prereqs
- Read: `docs/public/legal/disclaimer.md`

---

## What problem are you solving?
Unstructured market information is hard to transform into repeatable, measurable signals. Alpha Engine turns information into structured artifacts and measures outcomes so research can iterate.

## What is the “product” today?
A research & analytics pipeline + dashboards:
- Data ingestion (config-driven sources)
- Structured event scoring
- Prediction generation (strategy families)
- Outcome evaluation + ranking

## How do you measure quality?
By separating:
- **Confidence** (what the model believed at prediction time)
- **Outcome metrics** (what happened after)
This keeps evaluation honest and supports iterative improvement.

In this repo, outcomes are represented as `PredictionOutcome` (`app/core/types.py`) and are persisted during historical replay to SQLite (`data/alpha.db` table `prediction_outcomes`).

## What’s defensible?
Defensibility compounds through:
- Data lineage + reproducibility
- Strategy performance histories
- Regime-aware weighting and ranking
- A unified architecture that makes iteration fast and auditable

## What is *not* claimed?
- No guaranteed returns or performance promises
- No claim that the system is currently a fully automated trading bot

## What’s the roadmap?
- Harden ingestion + evaluation
- Expand strategy families and regime controls
- Improve auditability and operator tooling
- (Future) paper trading + execution layer, once compliance and risk controls mature

## Where can I learn “how it works” without internals?
- `docs/public/help/how-predictions-work.md`
- `docs/public/kb/how-to-use-and-evaluate.md`

## What’s the “evidence trail” if I want to audit outputs?
- Demo exports: run `python scripts/demo_run.py` and inspect `outputs/*.csv`
- Historical replay: run `python -m app.ingest.backfill_cli backfill-range --start YYYY-MM-DD --end YYYY-MM-DD` and inspect `data/alpha.db` tables (notably `predictions`, `prediction_outcomes`)
- Audit UI: `streamlit run app/ui/audit.py` to inspect schema expectations and DB health
