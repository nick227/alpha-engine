# Daily process (Windows scheduled pipeline)

This document describes the **end-of-day batch pipeline** for this repository: what runs, in what order, where output goes, and how to verify and troubleshoot it. It matches the implementation in `run_daily_pipeline.bat` (stub) and `scripts\windows\run_daily_pipeline.bat`.

**Audience:** operators and developers responsible for keeping the daily job healthy.

**Timezone:** Schedule times below are **local Windows time** on the host (configure the task for your venue; many U.S. equity workflows use **America/Chicago**).

---

## 1. What runs

A single scheduled task should execute **one** batch file once per day:

| Field | Typical value |
|--------|----------------|
| Program | `C:\wamp64\www\alpha-engine-poc\run_daily_pipeline.bat` |
| Start in | `C:\wamp64\www\alpha-engine-poc` |
| Account | A user that can read/write the repo, database, and logs (often the same account used for development) |
| Trigger | Daily (example: **10:00 AM** local) |

The inner script sets `ROOT` from the location of `scripts\windows\run_daily_pipeline.bat` and **`cd /d` to the repository root**, so `Start In` is helpful for Task Scheduler but the batch still resolves paths correctly if `Start In` is set to the repo root.

**Python:** `%ROOT%\.venv\Scripts\python.exe` with `PYTHONPATH=.` (see the batch file).

---

## 2. Pipeline steps (in order)

All steps append to the same daily log file (see §4). Any step that exits non-zero aborts the run: the batch logs `STEP N FAILED`, then `Pipeline ABORTED`, removes the lock file, and exits with code `1`.

The batch sets **`ASOF`** to today’s calendar date (`YYYY-MM-DD`) for steps that need an explicit as-of label.

| Step | Purpose | Command (from repo root) |
|------|---------|---------------------------|
| **1** | Download / refresh price bars | `python dev_scripts\scripts\download_prices_daily.py` |
| **2** | Discovery CLI **nightly**: candidates, watchlist promotion, `prediction_queue`, outcomes, stats, plus **threshold-based** multi-strategy enqueue | `python -m app.discovery.discovery_cli nightly --db data\alpha.db --tenant-id default` |
| **3** | **Global rank + trim** pending queue rows (merit score, then keep top N across all strategies) | `python -m app.engine.queue_rank_trim --as-of %ASOF% --db data\alpha.db --tenant-id default` |
| **4** | Build **predicted series** from the queue (engine path) | `python -m app.engine.prediction_cli run-queue --as-of %ASOF% ...` (exit code `3` = partial ticker failures; the batch still treats the step as completed) |
| **5** | Materialize **`predictions`** rows for discovery queue items (needed for replay/UI that read `predictions`) | `python run_paper_trading.py --materialize-discovery-predictions --materialize-date %ASOF%` |
| **6** | **Prediction rank + trim:** set `predictions.rank_score` from confidence + strategy metrics; optional global top-N / per-strategy cap (deletes unscored rows below the cut) | `python -m app.engine.prediction_rank_sqlite --as-of %ASOF% --db data\alpha.db --tenant-id default` |
| **7** | Replay: score expired discovery predictions | `python run_paper_trading.py --replay` |
| **8** | Backfill outcomes where prices now allow scoring | `python dev_scripts\scripts\auto_backfill_outcomes.py` |
| **9** | **Real vs sim** snapshot (matched `predictions` + `prediction_outcomes` + closed `trades` by `source`) | `python -m app.analytics.learning_feedback_report --db data\alpha.db --tenant-id default` (appends one line to the batch log: avg sim / real / execution gap by source; use `--json` for full rollups) |

**Throughput tuning (optional env vars):** `ALPHA_TARGET_SIGNALS_PER_DAY`, `ALPHA_PER_STRATEGY_CAP`, `ALPHA_MIN_DISCOVERY_CONFIDENCE`, `ALPHA_INACTIVE_STRATEGIES` (comma-separated). These feed `app.engine.threshold_queue` and the supplemental enqueue after watchlist rows.

**Global competition after step 2:** `ALPHA_GLOBAL_TOP_N` (default **120**) sets how many pending rows survive step 3; each row gets `rank_score` in `metadata_json` from confidence, raw score, and latest `discovery_stats` **candidate_strategy** metrics (win rate, avg return for horizon 5d / window 30d).

**Post-materialize competition (step 6):** `ALPHA_PREDICTION_TOP_N` (default **120**) and `ALPHA_PREDICTION_MAX_PER_STRATEGY` (default **10**) control SQLite `predictions.rank_score` persistence and optional trimming after materialization (complements step 3; uses `strategy_performance`, `strategy_stability`, and `strategies.live_score`). Use `--no-trim` to only populate `rank_score` without deleting rows.

**Temporal rank modifier (steps 3 and 6):** `ALPHA_RANK_TEMPORAL` (default **on**; set `0` to disable) applies a lightweight VIX/month multiplier to rank scores after the base score is computed (`app/engine/ranking_temporal.py`). VIX is read from `price_bars` (`^VIX`, `1d`) on or before `ASOF`. The JSON summary and queue `metadata_json` include **`market_context`**: `vix`, `regime`, `vix_timestamp`, `vix_fallback_used`, `vix_age_days`, **`context_warning`** (true if fallback or VIX bar older than one calendar day vs `ASOF`).

After each **queue_rank_trim** / **prediction_rank** run, the CLI prints a one-line summary (e.g. `Market Context: VIX=18.4 | Regime=normal | Age=0d | Warning=false`) and appends a TSV row to **`logs/market_context_audit.log`** (UTC timestamp, step name, `context_warning`, `vix_fallback_used`, `vix`, `vix_age_days`) for tracking warning rates. Override path with **`ALPHA_MARKET_CONTEXT_AUDIT_LOG`**. When VIX is missing, rank is also scaled by **`ALPHA_VIX_FALLBACK_RANK_MULT`** (default **0.95**; set **1.0** to disable).

**Audit trail on predictions:** Step 6 persists **`ranking_context_json`** on each ranked `predictions` row (full `market_context`, base vs final score, temporal multiplier, and config flags including **`pipeline_version`**, short git SHA via `git rev-parse --short HEAD`, or override **`ALPHA_PIPELINE_VERSION`**) so later analysis (e.g. outcomes vs `context_warning`) does not depend on log files alone.

**CLI flags** on `discovery_cli nightly` (instead of env): `--supplement-target`, `--supplement-min-confidence`, `--supplement-per-strategy-cap`, `--no-threshold-supplement`.

**Success:** The log ends with `Pipeline finished OK` and the batch exits `0`.

---

## 3. Overlap protection

The batch uses a lock file **`pipeline.lock`** at the repository root. If it already exists, the batch appends a line to the log and exits with code **`99`** without running the steps. This prevents two overlapping runs if the scheduler fires again while a long run is still executing.

If a run crashes hard before cleanup, a **stale** `pipeline.lock` can remain and block the next run. In that case, confirm no pipeline process is active, then delete `pipeline.lock` manually.

---

## 4. Logs (primary)

**Batch / pipeline log (authoritative for this job):**

- Path: `logs\daily_pipeline_YYYY-MM-DD.log`
- Example: `logs\daily_pipeline_2026-04-17.log`

The batch also writes `pip list` to the top of each run’s section for environment auditing.

**Other logs:** Application code may write structured or per-day logs under `logs\daily\`, `logs\system\`, etc. Those complement but do not replace the batch log for answering “did the scheduled job complete all nine steps?”

---

## 5. Verifying the scheduled task (Windows)

**Query the task:**

```bat
schtasks /Query /TN "AlphaEngine - Daily Pipeline" /V /FO LIST
```

Useful fields:

- **Last Run Time** / **Last Result** — `0` usually means the last scheduled instance completed successfully from Task Scheduler’s perspective. Non-zero values need investigation (see §6).
- **Next Run Time** — confirms the trigger.
- **Stop the task if it runs longer than** — if this is shorter than a full run on your data, the scheduler may terminate the job before the batch finishes.

**Run the task on demand (same definition as the schedule):**

```bat
schtasks /Run /TN "AlphaEngine - Daily Pipeline"
```

**Run the batch manually (equivalent path exercise):**

```bat
cd /d C:\wamp64\www\alpha-engine-poc
run_daily_pipeline.bat
```

**Task Scheduler operational log:** For failures that occur before the batch writes its header (host/launch issues), use **Event Viewer** → **Applications and Services Logs** → **Microsoft** → **Windows** → **TaskScheduler** → **Operational** and filter for the task name.

---

## 6. Common issues

| Symptom | Likely cause | What to do |
|---------|----------------|------------|
| No new `daily_pipeline_*.log` lines for a scheduled time | Task never started (machine off/asleep), **on battery** if “don’t start on batteries” is set, or launch failed before the batch opened the log | Check Task Scheduler **History** / Operational log; confirm AC power and wake policy if using a laptop. |
| Last Result is a large negative number (HRESULT) | Failure in the task **host** launching the action, not necessarily Python | Event Viewer TaskScheduler channel; verify path to `.bat`, permissions, and antivirus blocks. |
| Log shows `STEP N FAILED` or `Pipeline ABORTED` | That step’s Python process exited non-zero | Read the traceback and stderr captured **above** the `STEP N FAILED` line in the same log file. |
| Log says lock exists; exit 99 | Overlapping run or stale `pipeline.lock` | Ensure only one run; remove stale lock if safe. |
| Run stops mid-stream | **Stop if task runs longer than** limit in task **Settings** | Increase the limit or shorten work per run; confirm hardware is not sleeping the disk mid-job. |

---

## 7. Health checks (recommended)

- After any deployment: run `python -m pytest tests` from the repo root (see project `pytest.ini`).
- After changing discovery or imports: confirm Step 2 starts and reaches `STEP 2 END` in `daily_pipeline_YYYY-MM-DD.log`.
- Periodically confirm `Pipeline finished OK` appears on trading days you care about.

---

## 8. Related documents

- Ops index: `docs/internal/ops/README.md`
- Higher-level job concepts (may reference different CLI entrypoints): `docs/public/jobs-and-scheduling.md`
- Backfill and replay details: `docs/internal/ops/backfill-and-replay.md`
- Incident triage: `docs/internal/ops/incident-triage.md`

---

## 9. Task registration (reference)

The repository includes `scripts\windows\fix_scheduler.ps1`, which can register a task named **AlphaEngine - Daily Pipeline** pointing at `run_daily_pipeline.bat`. Review and adjust trigger time, account, and **“stop if running longer than”** to match your environment before relying on it in production.
