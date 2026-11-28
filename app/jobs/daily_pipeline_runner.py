"""
Daily Pipeline Runner (Python Native)
====================================
Cross-platform daily pipeline orchestrator for Linux / Railway / container deployment.
Replaces reliance on Windows Task Scheduler.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%SZ"
)
logger = logging.getLogger("daily_pipeline_runner")

def run_cmd(args: list[str], *, step_name: str, check: bool = True) -> int:
    logger.info(f"START step: {step_name}")
    t0 = time.time()
    try:
        proc = subprocess.run(
            args,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        elapsed = round(time.time() - t0, 2)
        if proc.returncode != 0 and check:
            logger.error(f"FAILED step {step_name} in {elapsed}s (exit {proc.returncode}):\n{proc.stdout[-500:]}")
            return proc.returncode
        logger.info(f"END step {step_name} in {elapsed}s (exit {proc.returncode})")
        return proc.returncode
    except Exception as e:
        elapsed = round(time.time() - t0, 2)
        logger.error(f"EXCEPTION in step {step_name} after {elapsed}s: {e}")
        if check:
            raise
        return 1

def record_stage_status(db_path: str, stage: str, status: str, error: str | None = None) -> None:
    try:
        from app.jobs import pipeline_control

        rec = pipeline_control.read_status(db_path) or {}
        rec["current_stage"] = stage
        rec["current_stage_status"] = status
        if error:
            rec["last_failed_stage"] = stage
            rec["last_error"] = str(error)[:500]
        pipeline_control._write_status(db_path, rec)
    except Exception:
        pass


def run_daily_pipeline(db_path: str = "data/alpha.db", tenant_id: str = "default") -> int:
    resolved_db = str(Path(db_path).resolve())
    os.environ["ALPHA_DB_PATH"] = resolved_db
    today_iso = date.today().isoformat()
    python_exe = sys.executable
    logger.info(f"Running daily pipeline for tenant='{tenant_id}' on db='{resolved_db}' (as_of={today_iso})")

    def _exec(args: list[str], *, step_name: str, check: bool = True) -> int:
        record_stage_status(db_path, step_name, "running")
        rc = run_cmd(args, step_name=step_name, check=check)
        if rc != 0 and check:
            record_stage_status(db_path, step_name, "failed", f"Step {step_name} exited with code {rc}")
            return rc
        record_stage_status(db_path, step_name, "succeeded" if rc == 0 else "completed_with_warnings")
        return rc

    # Step 1: Download prices for active + consumer universe
    rc = _exec([python_exe, "dev_scripts/scripts/download_prices_daily.py", "--db", db_path, "--tenant-id", tenant_id], step_name="1_download_prices")
    if rc != 0:
        return rc

    # Step 1b: Sync fundamentals
    _exec([python_exe, "-m", "app.discovery.discovery_cli", "sync-fundamentals", "--db", db_path, "--tenant-id", tenant_id, "--use-active-universe"], step_name="1b_sync_fundamentals", check=False)

    # Step 1c: Sync company profiles
    _exec([python_exe, "dev_scripts/scripts/sync_company_profiles_daily.py", "--db", db_path, "--tenant-id", tenant_id], step_name="1c_company_profiles", check=False)

    # Step 1d: Sync market events
    _exec([python_exe, "dev_scripts/scripts/sync_events_daily.py", "--db", db_path, "--tenant-id", tenant_id], step_name="1d_sync_events", check=False)

    # Step 2: Discovery nightly
    rc = _exec([python_exe, "-m", "app.discovery.discovery_cli", "nightly", "--db", db_path, "--tenant-id", tenant_id, "--admission-max", "40", "--admission-per-lens", "6", "--admission-max-overrule-swaps", "5"], step_name="2_discovery_nightly")
    if rc != 0:
        return rc

    # Step 2b: Deep fill admitted
    _exec([python_exe, "dev_scripts/scripts/download_prices_daily.py", "--db", db_path, "--tenant-id", tenant_id, "--deep-fill-admitted"], step_name="2b_deep_fill_admitted", check=False)

    # Step 3: Queue rank trim
    rc = _exec([python_exe, "-m", "app.engine.queue_rank_trim", "--as-of", today_iso, "--db", db_path, "--tenant-id", tenant_id], step_name="3_queue_rank_trim")
    if rc != 0:
        return rc

    # Step 4: Run prediction queue
    rc = _exec([python_exe, "-m", "app.engine.prediction_cli", "run-queue", "--as-of", today_iso, "--db", db_path, "--tenant-id", tenant_id, "--limit", "400", "--forecast-days", "30", "--ingress-days", "30", "--freshness-hours", "20"], step_name="4_prediction_cli", check=False)
    if rc not in (0, 3):
        logger.error(f"Prediction queue failed with code {rc}")
        record_stage_status(db_path, "4_prediction_cli", "failed", f"Exit {rc}")
        return rc

    # Step 5: Materialize predictions
    rc = _exec([python_exe, "run_paper_trading.py", "--materialize-discovery-predictions", "--materialize-date", today_iso], step_name="5_materialize_predictions")
    if rc != 0:
        return rc

    # Step 6: Prediction rank sqlite
    rc = _exec([python_exe, "-m", "app.engine.prediction_rank_sqlite", "--as-of", today_iso, "--db", db_path, "--tenant-id", tenant_id], step_name="6_prediction_rank_sqlite")
    if rc != 0:
        return rc

    # Step 7: Ranking snapshots
    rc = _exec([python_exe, "-m", "app.engine.ranking_snapshots_from_predictions", "--as-of", today_iso, "--db", db_path, "--tenant-id", tenant_id, "--lookback-days", "7", "--max-tickers", "50"], step_name="7_ranking_snapshots")
    if rc != 0:
        return rc

    # Step 7b: Refresh consensus signals
    _exec([python_exe, "dev_scripts/scripts/refresh_consensus.py", "--db", db_path, "--tenant-id", tenant_id], step_name="7b_refresh_consensus", check=False)

    # Step 8: Replay
    _exec([python_exe, "run_paper_trading.py", "--replay"], step_name="8_replay", check=False)

    # Step 9: Backfill outcomes
    _exec([python_exe, "dev_scripts/scripts/auto_backfill_outcomes.py"], step_name="9_backfill_outcomes", check=False)

    # Step 10: Learning feedback
    _exec([python_exe, "-m", "app.analytics.learning_feedback_report", "--db", db_path, "--tenant-id", tenant_id], step_name="10_learning_feedback_report", check=False)

    # Step 10b: Recompute strategy statistics
    _exec([python_exe, "dev_scripts/scripts/recompute_strategy_stats.py", "--db", db_path, "--tenant-id", tenant_id], step_name="10b_recompute_strategy_stats", check=False)

    # Step 10c: Refresh ML features
    _exec([python_exe, "dev_scripts/scripts/refresh_ml_features.py", "--db", db_path, "--tenant-id", tenant_id], step_name="10c_refresh_ml_features", check=False)

    # Step 11: Data health prod report
    _exec([python_exe, "dev_scripts/scripts/generate_data_health_prod_report.py", "--db", db_path, "--output", "reports/data-health-prod.txt"], step_name="11_data_health", check=False)

    # Update pipeline last status
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    with open(reports_dir / "pipeline-last-status.txt", "w") as f:
        f.write(f"OK finished_at={datetime.now(timezone.utc).isoformat()}\n")

    try:
        from app.jobs import pipeline_control

        rec = pipeline_control.read_status(db_path) or {}
        rec["last_successful_date"] = today_iso
        rec["last_failed_stage"] = None
        pipeline_control._write_status(db_path, rec)
    except Exception:
        pass

    logger.info("Pipeline finished successfully.")
    return 0

def main():
    parser = argparse.ArgumentParser(description="Alpha Engine Linux/Railway Daily Pipeline Runner")
    parser.add_argument("--db", default="data/alpha.db", help="Path to SQLite alpha.db")
    parser.add_argument("--tenant-id", default="default", help="Tenant ID")
    args = parser.parse_args()

    # Same preflight + single-writer lock as the API trigger, so CLI and scheduled runs never overlap.
    from app.jobs.pipeline_control import start_guarded

    result = start_guarded(
        args.db,
        trigger="cli",
        background=False,
        runner=lambda db_path: run_daily_pipeline(db_path=db_path, tenant_id=args.tenant_id),
    )
    logger.info(f"Pipeline result: {result}")
    if result.get("status") == "skipped":
        sys.exit(75)  # EX_TEMPFAIL: not run (disabled, low storage, or already running)
    sys.exit(result.get("exit_code") or 0)

if __name__ == "__main__":
    main()
