#!/usr/bin/env bash
# ==============================================================================
# Alpha Engine — Linux / Railway Daily Processing Pipeline
# ==============================================================================
# Self-contained, POSIX-compliant runner for automated daily pipeline execution.
# Operates independently of Windows Task Scheduler.
# ==============================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT"

# Resolve Python executable
if [ -z "${PYTHON:-}" ]; then
    if [ -f "$ROOT/.venv/bin/python" ]; then
        PYTHON="$ROOT/.venv/bin/python"
    elif command -v python3 >/dev/null 2>&1; then
        PYTHON="python3"
    else
        PYTHON="python"
    fi
fi

export PYTHONPATH="."
DB_FILE="${ALPHA_DB_PATH:-data/alpha.db}"
RANKING_LOOKBACK_DAYS="${ALPHA_RANKING_SNAPSHOT_LOOKBACK_DAYS:-7}"
RANKING_MAX_TICKERS="${ALPHA_RANKING_SNAPSHOT_MAX_TICKERS:-50}"

TODAY=$(date -u +%Y-%m-%d)
LOG_FILE="logs/daily_pipeline_${TODAY}.log"
STATUS_FILE="reports/pipeline-last-status.txt"

mkdir -p logs reports

log() {
    local msg="[$(date -u '+%Y-%m-%d %H:%M:%SZ')] $1"
    echo "$msg"
    echo "$msg" >> "$LOG_FILE"
}

# Pre-flight DB check
if [ ! -f "$DB_FILE" ]; then
    log "ERROR: Database file not found at $DB_FILE"
    echo "FAILED step=0_db_check at=$(date -u +%s)" > "$STATUS_FILE"
    exit 2
fi

# Fail-closed preflight, identical to the API/CLI trigger (ALPHA_PIPELINE_ENABLED, storage headroom).
if ! PREFLIGHT=$("$PYTHON" -c "
import sys
from app.jobs.pipeline_control import preflight
ok, reason, info = preflight(sys.argv[1])
print(reason or 'ok', info)
sys.exit(0 if ok else 1)
" "$DB_FILE" 2>&1); then
    log "SKIPPED: preflight refused to run: $PREFLIGHT"
    echo "SKIPPED step=preflight at=$(date -u +%s) detail=$PREFLIGHT" > "$STATUS_FILE"
    exit 75
fi

# Acquire single-writer lock: kernel advisory flock on the same file the API/scheduler use
# (next to the database). The kernel drops it when this process dies, so a crash or redeploy
# can never leave a stale lock, and the file is never removed or tested for existence.
LOCK_FILE="${ALPHA_PIPELINE_LOCK_PATH:-$(cd "$(dirname "$DB_FILE")" && pwd)/pipeline.lock}"
exec 200>>"$LOCK_FILE"
if ! flock -n 200; then
    log "WARNING: another pipeline run holds the lock ($LOCK_FILE). Pipeline execution skipped."
    echo "SKIPPED step=lock_held at=$(date -u +%s)" > "$STATUS_FILE"
    exit 99
fi
: > "$LOCK_FILE"
echo "{\"pid\": $$, \"host\": \"$(hostname)\", \"locked_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"via\": \"run_daily_pipeline.sh\"}" > "$LOCK_FILE"

log "============================================================"
log "Alpha Engine Daily Pipeline Started"
log "ROOT=$ROOT"
log "PYTHON=$PYTHON"
log "DB=$DB_FILE"
log "============================================================"

ASOF=$($PYTHON -c "from datetime import date; print(date.today().isoformat())")

FAILED_STEP="unknown"

run_step() {
    local step_name="$1"
    shift
    log "START: $step_name"
    if "$@" >> "$LOG_FILE" 2>&1; then
        log "END: $step_name complete"
        return 0
    else
        local code=$?
        log "FAILED: $step_name (exit code $code)"
        FAILED_STEP="$step_name"
        return $code
    fi
}

# STEP 1 — Download active universe & consumer universe prices
run_step "1_download_prices" "$PYTHON" dev_scripts/scripts/download_prices_daily.py --db "$DB_FILE" --tenant-id default || {
    echo "FAILED step=$FAILED_STEP at=$(date -u +%s)" > "$STATUS_FILE"
    exit 1
}

# STEP 1b — Fundamentals snapshot
run_step "1b_sync_fundamentals" "$PYTHON" -m app.discovery.discovery_cli sync-fundamentals --db "$DB_FILE" --tenant-id default --use-active-universe || {
    log "WARNING: 1b_sync_fundamentals non-zero exit; continuing pipeline."
}

# STEP 1c — Company profiles sync
run_step "1c_company_profiles" "$PYTHON" dev_scripts/scripts/sync_company_profiles_daily.py --db "$DB_FILE" --tenant-id default || {
    log "WARNING: 1c_company_profiles non-zero exit; continuing pipeline."
}

# STEP 2 — Discovery nightly
run_step "2_discovery_nightly" "$PYTHON" -m app.discovery.discovery_cli nightly --db "$DB_FILE" --tenant-id default --admission-max 40 --admission-per-lens 6 --admission-max-overrule-swaps 5 || {
    echo "FAILED step=$FAILED_STEP at=$(date -u +%s)" > "$STATUS_FILE"
    exit 1
}

# STEP 2b — Deep price history for admitted symbols
run_step "2b_deep_fill_admitted" "$PYTHON" dev_scripts/scripts/download_prices_daily.py --db "$DB_FILE" --tenant-id default --deep-fill-admitted || {
    log "WARNING: 2b_deep_fill_admitted non-zero exit; continuing pipeline."
}

# STEP 3 — Global rank score & trim queue
run_step "3_queue_rank_trim" "$PYTHON" -m app.engine.queue_rank_trim --as-of "$ASOF" --db "$DB_FILE" --tenant-id default || {
    echo "FAILED step=$FAILED_STEP at=$(date -u +%s)" > "$STATUS_FILE"
    exit 1
}

# STEP 4 — Run prediction queue
log "START: 4_prediction_cli"
"$PYTHON" -m app.engine.prediction_cli run-queue --as-of "$ASOF" --db "$DB_FILE" --tenant-id default --limit 400 --forecast-days 30 --ingress-days 30 --freshness-hours 20 >> "$LOG_FILE" 2>&1
P_CODE=$?
if [ $P_CODE -eq 0 ] || [ $P_CODE -eq 3 ]; then
    log "END: 4_prediction_cli complete (code $P_CODE)"
else
    log "FAILED: 4_prediction_cli (exit code $P_CODE)"
    echo "FAILED step=4_prediction_cli at=$(date -u +%s)" > "$STATUS_FILE"
    exit 1
fi

# STEP 5 — Materialize discovery predictions
run_step "5_materialize_predictions" "$PYTHON" run_paper_trading.py --materialize-discovery-predictions --materialize-date "$ASOF" || {
    echo "FAILED step=$FAILED_STEP at=$(date -u +%s)" > "$STATUS_FILE"
    exit 1
}

# STEP 6 — Prediction rank sqlite
run_step "6_prediction_rank_sqlite" "$PYTHON" -m app.engine.prediction_rank_sqlite --as-of "$ASOF" --db "$DB_FILE" --tenant-id default || {
    echo "FAILED step=$FAILED_STEP at=$(date -u +%s)" > "$STATUS_FILE"
    exit 1
}

# STEP 7 — Ranking snapshots
run_step "7_ranking_snapshots" "$PYTHON" -m app.engine.ranking_snapshots_from_predictions --as-of "$ASOF" --db "$DB_FILE" --tenant-id default --lookback-days "$RANKING_LOOKBACK_DAYS" --max-tickers "$RANKING_MAX_TICKERS" || {
    echo "FAILED step=$FAILED_STEP at=$(date -u +%s)" > "$STATUS_FILE"
    exit 1
}

# STEP 8 — Replay expired predictions
run_step "8_replay" "$PYTHON" run_paper_trading.py --replay || {
    log "WARNING: 8_replay non-zero exit; continuing pipeline."
}

# STEP 9 — Backfill outcomes
run_step "9_backfill_outcomes" "$PYTHON" dev_scripts/scripts/auto_backfill_outcomes.py || {
    log "WARNING: 9_backfill_outcomes non-zero exit; continuing pipeline."
}

# STEP 10 — Learning feedback report
run_step "10_learning_feedback_report" "$PYTHON" -m app.analytics.learning_feedback_report --db "$DB_FILE" --tenant-id default || {
    log "WARNING: 10_learning_feedback_report non-zero exit; continuing."
}

# STEP 11 — Data health report
run_step "11_data_health" "$PYTHON" dev_scripts/scripts/generate_data_health_prod_report.py --db "$DB_FILE" --output reports/data-health-prod.txt || {
    log "WARNING: 11_data_health non-zero exit; continuing."
}

log "============================================================"
log "Alpha Engine Daily Pipeline Finished Successfully"
log "============================================================"

echo "OK finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$STATUS_FILE"
exit 0
