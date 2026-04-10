# Reproducibility (Internal)

## Purpose
Provide a repeatable recipe to reproduce pipeline outputs for audit and regression checks.

## Audience
- Auditors
- Developers
- Operators

## When to use this
- You need deterministic reproduction of artifacts in `outputs/` and confirmation of end-to-end behavior.

## Prereqs
- Python environment set up (see `README.md`)
- Optional: npm install for Prisma tooling (if required by your workflow)

---

## Baseline demo run (deterministic, no external APIs)
This repo includes a self-contained demo runner that creates synthetic `RawEvent` items and synthetic `price_contexts`.

1. Ensure dependencies are installed (Python venv + requirements).
2. Run: `python scripts/demo_run.py`

## Expected artifacts
Confirm `outputs/` contains (as written by `scripts/demo_run.py`):
- `outputs/scored_events.csv`
- `outputs/mra_outcomes.csv`
- `outputs/predictions.csv`
- `outputs/strategy_performance.csv`

Notes:
- Demo entrypoint: `app/runtime/pipeline.py` (`run_pipeline` wrapper around `AlphaPipeline`).
- `scripts/demo_run.py` passes `persist=True`, but database persistence inside `app/runtime/pipeline.py` is currently stubbed behind TODOs; treat the CSVs as the primary demo artifacts.

## If outputs are missing
- Confirm the runner executed successfully (exit code, logs).
- Confirm your working directory and permissions.
- Validate that mock data or configured sources are available.

## Verification steps (audit-friendly)
- Re-run the same command twice and confirm artifacts regenerate consistently for a fixed dataset.
- Record the git commit hash and config inputs used for the run.
