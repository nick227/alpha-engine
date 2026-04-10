﻿﻿# Short-Form Library (Content Kit)

## Purpose
Provide a large set of small, linkable, high-signal explainers that communicate value and reduce confusion.

## Audience
- Investors
- End users
- Developers (public-safe orientation)

## When to use this
- You need a snackable explanation or a shareable snippet with a single “next link”.

## Prereqs
- None

---

## Format rules (consolidated)
Short-form is consolidated into this single page to keep the public docs set small.

Rules:
- 1-line headline per item
- 5–7 bullets max
- 1 “Next” link to a deeper KB/help page

## Backlog (to be created)
- 10× “What is X?” (MRA, consensus engine, strategy ranking, horizons, etc.)
- 10× “Why it matters” (risk controls, reproducibility, extensibility, etc.)
- 10× “Myth vs fact” (AI trading misconceptions, confidence vs performance, etc.)
- 20× “Feature bullets” (by persona: investor, operator, developer, analyst)
- 10× “Use-case snapshots” (earnings/news events, regime changes, volatility spikes)

---

## What is MRA (Market Reaction Analysis)?
- MRA describes how markets tend to react *after* specific kinds of events.
- It connects “what happened” (events/news) to “what changed” (price/volatility).
- It can be used as context/features for strategies and evaluation.
- It is not a guarantee; it’s a measurement and modeling tool.
- It becomes more useful when paired with reproducible outcomes.

Next: `docs/public/help/how-predictions-work.md`

## What is a consensus engine?
- Different strategies can disagree; consensus tries to combine them responsibly.
- It can weight signals differently depending on regime (trend/volatility context).
- It helps reduce “single-strategy brittleness.”
- It should be evaluated with outcomes, not just intuition.
- Disagreement can be informative (uncertainty), not just noise.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

## What is a “horizon”?
- A horizon is the time window a prediction is evaluated over (e.g., 15m, 1h, 1d, 7d).
- The same direction call can succeed on one horizon and fail on another.
- Horizon choice affects what “good” looks like (noise vs drift).
- Evaluation maps horizons to realized future return keys (see `app/engine/evaluate.py`).
- Compare signals within the same horizon before drawing conclusions.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

## What is “backfill + replay”?
- Backfill fetches historical inputs (events + bars) for a time window.
- Replay processes those inputs chronologically to generate predictions and outcomes.
- It produces auditable DB artifacts in `data/alpha.db` (predictions, outcomes, markers).
- It is designed to be resumable and idempotent (safe to rerun the same window).
- In this repo: `python -m app.ingest.backfill_cli backfill-range --start ... --end ...`.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

## What is deduplication (and why it matters)?
- Ingestion often sees the same payload multiple times.
- Deduplication prevents duplicates from polluting evaluation and rankings.
- This repo uses deterministic IDs: `SHA256(source_id|timestamp|text)`.
- Within-run duplicates are dropped; across runs, SQLite INSERT-OR-IGNORE prevents reinserts.
- Low “insert” counts can be correct (dedupe), not missing data.

Next: `docs/internal/audit/data-lineage.md`

## What are “target stocks”?
- Target stocks define the canonical universe the system focuses on.
- They keep coverage consistent across ingestion, bars, and analysis.
- In this repo: `config/target_stocks.yaml`.
- Backfill and replay use this universe to infer/validate tradeable tickers.
- A changing universe changes your evaluation corpus—record it with results.

Next: `docs/public/help/data-sources-at-a-glance.md`

## What is a “prediction outcome”?
- An outcome records what happened after a prediction’s horizon elapsed.
- It includes return, direction correctness, and (optionally) runup/drawdown.
- Outcomes are the evidence layer; they validate confidence and strategy quality.
- Type: `PredictionOutcome` in `app/core/types.py`.
- Persisted during historical replay to `data/alpha.db` in `prediction_outcomes`.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

## What is the replay engine?
- Replay processes historical events in time order (a backtest-style loop).
- It writes a full evidence trail: raw events → scores → predictions → outcomes.
- It is designed to be idempotent (reruns upsert/ignore vs duplicate).
- In this repo: `app/ingest/replay_engine.py`.
- Replay is invoked during backfill (`app/ingest/backfill_runner.py` + CLI).

Next: `docs/internal/ops/backfill-and-replay.md`

## Myth vs Fact: “High confidence means it will win”
- Myth: High confidence guarantees correctness.
- Fact: Confidence is belief; validate with outcomes.
- Myth: Confidence replaces backtesting.
- Fact: Outcomes/backtests are the evidence layer.
- Myth: One good chart proves a strategy.
- Fact: Regimes change; evaluate across windows/horizons.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

## Myth vs Fact: “One backtest proves the system works”
- Myth: A single good window means the strategy is “real.”
- Fact: Window selection can hide regime dependency and tail risk.
- Myth: Aggregates are enough.
- Fact: Break results down by horizon/ticker/regime.
- Myth: Confidence can replace outcomes.
- Fact: Confidence must be validated against outcomes over time.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

## Why it matters: Reproducibility beats “demo magic”
- Reproducibility makes results auditable and repeatable.
- It reduces accidental leakage and one-off tuning risk.
- It enables meaningful comparisons across strategies/regimes.
- It makes debugging and improvement faster.
- It improves stakeholder trust because claims are evidence-backed.

Next: `docs/internal/audit/reproducibility.md`

## Why it matters: Data lineage prevents “mystery metrics”
- If you can’t trace a number to inputs, you can’t audit it.
- Lineage makes debugging faster (find drops/misalignment).
- It reduces the chance of accidental leakage in evaluation.
- It improves trust because claims are evidence-backed.
- In this repo: `config/` → ingestion → `data/alpha.db` → UI/exports.

Next: `docs/internal/audit/data-lineage.md`

## Why it matters: Idempotency makes replays safe
- Backfills/replays should be safe to rerun without duplicating data.
- Idempotency prevents double counting and protects evaluation integrity.
- This repo uses deterministic hashes + SQLite upsert/ignore patterns.
- It enables resumability when runs are interrupted.
- It’s a practical control auditors care about.

Next: `docs/internal/ops/backfill-and-replay.md`

## Feature bullets (Investor lens)
- Research-first pipeline with measurable outputs and evaluation.
- Closed-loop learning/ranking design that supports iteration.
- Declarative ingestion patterns that reduce integration overhead.
- Audit-friendly artifacts (`outputs/`, DB lineage) for diligence.
- Clear separation between confidence and outcomes to avoid misleading claims.

Next: `docs/public/marketing/one-pager.md`

## Feature bullets (Operator lens)
- Config-driven ingestion (`config/sources.yaml`) with schema validation.
- Validation + dedupe to reduce noisy events.
- Provider-level rate limiting to reduce bans/timeouts.
- SQLite-first persistence (`data/alpha.db`) with audit-friendly tables.
- Diagnose and health-report CLIs to triage coverage and schema issues.

Next: `docs/internal/ops/ingestion-health-checks.md`

## Feature bullets (Developer lens)
- Clear ingestion spec model + validation (`app/ingest/source_spec.py`, `app/ingest/validator.py`).
- Deterministic demo runner (`scripts/demo_run.py`) for fast regression checks.
- Replay engine writes a complete evidence trail (events → outcomes).
- Strategy/config machinery supports extension.
- Streamlit UI surfaces for dashboarding and audit.

Next: `docs/internal/dev/README.md`

## Feature bullets (Analyst lens)
- Structured events turn unstructured information into analyzable records.
- MRA features capture short-horizon market reactions.
- Horizon-scoped evaluation avoids mixing timeframes.
- Outcome tables allow “confidence vs results” separation.
- UI and exports support repeatable analysis workflows.

Next: `docs/public/kb/how-to-use-and-evaluate.md`

## Use-case snapshot: Earnings/news-driven volatility
- A major event arrives (unstructured).
- The system structures it into an event record (classification/score).
- Strategies produce horizon-specific predictions with confidence.
- Outcomes are evaluated after the horizon to measure usefulness.
- Users learn which strategies behave best in similar regimes.

Next: `docs/public/help/how-predictions-work.md`

## Use-case snapshot: Regime shift (volatility spike)
- Volatility spikes can flip which strategy families behave well.
- Consensus weighting matters more when signals disagree.
- Backtests should include both calm and high-volatility windows.
- Evaluate by horizon; short horizons may be dominated by noise.
- Track before/after outcomes to detect drift.

Next: `docs/internal/audit/model-limitations.md`

## Use-case snapshot: Supplier disruption headlines
- A disruption headline arrives (unstructured).
- Scoring categorizes it and assigns confidence.
- MRA measures short-horizon reaction (returns/volume/VWAP distance).
- Strategies output horizon-specific predictions; outcomes are later measured.
- The system learns whether this pattern is regime-dependent.

Next: `docs/public/help/how-predictions-work.md`
