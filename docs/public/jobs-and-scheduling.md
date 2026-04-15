# Jobs and Scheduling (Ops Guide)

This platform's moving parts can be simplified into **three categories**:

1. **Always-on** (loops/services)
2. **Scheduled** (daily jobs)
3. **Ad-hoc** (manual / research)

The system is **primarily daily batch-driven**. You do **not** need a 24/7 process.

Core flow:

**discovery -> prediction queue -> (optional) paper trading -> stats**

Best setup:

**one scheduled job daily** (`daily_runner`) that runs the full pipeline end-to-end.

Always-on runtime is optional, only for live/intraday behavior.

All recommended times below assume the machine is running in **America/Chicago**.

---

## 1) Always-on (optional, keep one process)

### `runtime` (optional)
**Purpose:** continuous background work (only needed for live/intraday behavior).

**Command:**
- `python scripts/run_runtime.py`

**What it runs (configurable):**
- `live` loop (optional)
- `replay` loop (recommended to move to scheduled)
- `optimizer` loop (recommended to move to scheduled)

**Enable/disable loops via env vars:**
- `ALPHA_RUNTIME_ENABLE_LIVE=true|false`
- `ALPHA_RUNTIME_ENABLE_REPLAY=true|false`
- `ALPHA_RUNTIME_ENABLE_OPTIMIZER=true|false`
- `ALPHA_RUNTIME_INTERVAL=5` (seconds)

**Recommended "minimal live" config:**
- `ALPHA_RUNTIME_ENABLE_LIVE=true`
- `ALPHA_RUNTIME_ENABLE_REPLAY=false`
- `ALPHA_RUNTIME_ENABLE_OPTIMIZER=false`

If you do not actively consume "live" events, disable the runtime entirely.

---

## 2) Scheduled jobs (core system)

The daily flow should look like:

**(optional) data refresh -> discovery -> prediction queue -> (optional) paper trading -> stats**

### Job A -- Discovery Nightly (required)
**Purpose:** generate daily intelligence and create the `prediction_queue`.

**Command:**
- `python -m app.discovery.discovery_cli nightly`

**Writes:**
- discovery candidates + watchlist
- `prediction_queue` rows (via promote)
- outcomes + stats (feedback loop)

**Recommended schedule:**
- **21:30 CT daily** (after market close, when today's daily bars are likely present)

### Job B -- Prediction Queue Runner (required)
**Purpose:** consume `prediction_queue` and materialize predicted series for queued symbols.

**Command:**
- `python -m app.engine.prediction_cli run-queue --as-of YYYY-MM-DD`

**Queue status lifecycle (for visibility + failure handling):**
- `pending -> processing -> processed`
- `pending -> processing -> failed`

**Recommended schedule:**
- **21:40 CT daily** (immediately after Discovery Nightly)

### Job C -- Full Daily Runner (recommended single-scheduler option)
**Purpose:** run the "daily chain" as a single scheduled command.

**Command:**
- `python -m app.jobs.daily_runner --date YYYY-MM-DD`

**What it runs:**
- Discovery Nightly
- Prediction Queue Runner
- (optional) Paper trading (if invoked with `--paper-trade`)

**Recommended schedule:**
- **21:30 CT daily**

If you use `daily_runner`, you typically do **not** separately schedule Job A and Job B.

### Job D -- Paper Trading (optional)
**Purpose:** simulate execution of decisions and produce a daily paper record.

**Command:**
- `python scripts/paper_trade_daily.py --date YYYY-MM-DD`

**Recommended schedule options (pick one):**
- **21:50 CT daily** (same evening; simplest operationally)
- **08:45 CT weekdays** (next morning; closer to "next-open" execution concept)

### Job E -- Data Refresh (optional, grouped)
**Purpose:** refresh external datasets (options metrics, macro dumps, etc.).

**Examples:**
- `python scripts/download_polygon_options_daily.py`

**Recommended schedule:**
- **20:30 CT daily** (before Discovery Nightly, so today's refresh is ready)

---

## 3) Ad-hoc (do not schedule)

Rule: **If it's not needed daily, don't schedule it.**

Examples:
- Backfills: `python -m app.ingest.backfill_cli backfill-range ...`
- Dataset builds: `python -m app.ml.dataset_cli build ...`
- Retraining:
  - heavy: `python scripts/expand_training_data.py`
  - quick: `python scripts/retrain_ml_recent.py`
- Validation / research scripts: anything in `scripts/validate_*.py`, `scripts/verify_*.py`, etc.

---

## Proposed scheduled times (America/Chicago)

If you want a clean, minimal schedule, use **one scheduled task**:

- **21:30 CT daily** -- `daily_runner` (Discovery -> Prediction Queue)  
  - optionally include paper trading via `--paper-trade`

If you prefer multiple tasks (more granular debugging), use:

- **20:30 CT daily** -- Data Refresh (optional)
- **21:30 CT daily** -- Discovery Nightly (required)
- **21:40 CT daily** -- Prediction Queue Runner (required)
- **21:50 CT daily** -- Paper Trading (optional)

---

## Windows Task Scheduler examples

### Option 1: Single scheduled command (recommended)

Schedule the full daily pipeline:

```bat
schtasks /Create /F /SC DAILY /ST 21:30 /TN "AlphaEngine_Daily" /TR "python -m app.jobs.daily_runner --date %DATE% --db data/alpha.db --tenant-id default"
```

Note: `%DATE%` formatting varies by Windows locale. If that's unreliable on your machine, prefer scheduling without `--date` and let the runner pick "today (UTC)", or wrap the command in a small `.cmd` file that computes `YYYY-MM-DD`.

### Option 2: Separate tasks (more granular)

```bat
schtasks /Create /F /SC DAILY /ST 21:30 /TN "AlphaEngine_DiscoveryNightly" /TR "python -m app.discovery.discovery_cli nightly --db data/alpha.db --tenant-id default"
schtasks /Create /F /SC DAILY /ST 21:40 /TN "AlphaEngine_PredictionQueue"  /TR "python -m app.engine.prediction_cli run-queue --as-of 2026-04-13 --db data/alpha.db --tenant-id default"
```

If you schedule the prediction runner separately, prefer passing `--as-of` explicitly (or compute it in a wrapper script) so the queue date is deterministic.

---

## UI visibility / debugging

In the Ops UI (`app/ui/ops_data_console.py`), you should be able to quickly answer:

- Is the queue flowing? (`pending`, `processing`, `processed`, `failed`)
- Did the prediction runner run recently? (`last run` + status)
- Can I trigger jobs manually? (`Run Discovery`, `Run Prediction Queue`, `Run Full Daily Pipeline`)


