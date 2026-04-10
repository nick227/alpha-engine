﻿# How to Use Alpha Engine (Evaluate, Backtest, Troubleshoot)

## Purpose
Provide a single, action-oriented guide for evaluating signals, running backtests, and troubleshooting common issues.

## Audience
- End users
- Evaluators (including investors)
- Operators (public-safe)

## When to use this
- You want a single “playbook” for interpreting outputs and validating the system with evidence.

## Prereqs
- Read: `docs/public/help/how-predictions-work.md`
- Read: `docs/public/legal/disclaimer.md`

---

## 1) Evaluate a signal (interpretation checklist)

### Problem statement
“I have a signal — I don’t know if it’s meaningful or how much to trust it.”

### Fast checklist
- Identify the **ticker**, **direction**, and **horizon**.
- Confirm the **context** (recent events + market conditions).
- Separate **confidence** (belief at prediction time) from **outcomes** (measured results later).
- Check for disagreement across strategies/consensus (uncertainty can be informative).
- Decide next step: ignore, monitor, deeper research, or backtest.

### Practical: where to look in this repo
- Demo artifacts: `outputs/predictions.csv` (from `python scripts/demo_run.py`)
- Outcome evaluation type: `app/core/types.py` (`PredictionOutcome`)
- Historical replay outcomes: `data/alpha.db` table `prediction_outcomes` (written by `app/ingest/replay_engine.py`)

### Root causes (when signals mislead)
1. Sparse/low-quality inputs (thin news coverage, missing bars)
2. Regime shift (volatility/trend changes)
3. Overfit behavior on a narrow window
4. Misinterpreting confidence as performance

### What good looks like
- You can explain: **why** the signal exists, **what could invalidate it**, and **how you’ll measure results**.

---

## 2) Backtest (3 levels of evidence)

### A) Deterministic demo (fast, no network)
Goal: validate pipeline wiring and generate quick evidence artifacts.

- Run: `python scripts/demo_run.py`
- Confirm outputs:
  - `outputs/scored_events.csv`
  - `outputs/mra_outcomes.csv`
  - `outputs/predictions.csv`
  - `outputs/strategy_performance.csv`

### B) Historical backfill + replay (writes outcomes to SQLite)
Goal: create an auditable DB trail (events → predictions → outcomes).

1. Backfill and replay a range:
   - `python -m app.ingest.backfill_cli backfill-range --start 2024-02-20 --end 2024-03-20`
2. Inspect coverage/health:
   - `python -m app.ingest.backfill_cli ingest-health --start 2024-02-20 --end 2024-03-20`

During replay, `data/alpha.db` is populated with tables including:
- `raw_events`, `scored_events`, `mra_outcomes`, `predictions`, `prediction_outcomes`, `signals`, `consensus_signals`, `loop_heartbeats`

### C) Window scoring + ranking (series-level evaluation)
Goal: score predicted series vs actual bars in a defined prediction window and rank strategies.

- `python -m app.engine.score_predictions_cli eval-window --range 2024-03-21:2024-04-21 --timeframe 1d --rank-limit 10`

---

## 3) Troubleshoot (common failure modes)

### Problem statement
“The system outputs exist, but they don’t look right.”

### Fast checklist
- Confirm the horizon you’re interpreting matches the artifact/table you’re reading.
- Confirm data availability (events + bars) for the ticker/timeframe.
- Confirm you’re using outcomes for evidence (not confidence alone).
- Check for regime change (volatility spike, trend reversal).

### Resolution steps (practical)
- Re-run deterministic demo to isolate environment issues:
  - `python scripts/demo_run.py`
- If live ingestion is involved, run a one-shot ingest pass and inspect per-source logs:
  - `python -m app.ingest.async_runner`
- Diagnose adapters + normalization:
  - `python -m app.ingest.diagnose`
  - Enable network fetch: set `ALPHA_DIAGNOSE_ALLOW_NETWORK=1`
- Reduce scope: single ticker, single horizon, then expand.

### What good looks like
- Outputs regenerate deterministically for a fixed dataset/config.
- You can attribute changes in results to changes in inputs/regime, not randomness.

