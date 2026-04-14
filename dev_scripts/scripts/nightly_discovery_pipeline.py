"""
Nightly Discovery Pipeline

Runs discovery → queues silent_compounder and balance_sheet_survivor top
candidates into prediction_queue → (optionally) runs prediction_cli.

Usage:
    python scripts/nightly_discovery_pipeline.py
    python scripts/nightly_discovery_pipeline.py --date 2026-04-13
    python scripts/nightly_discovery_pipeline.py --dry-run
    python scripts/nightly_discovery_pipeline.py --run-predictions
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date

from app.db.repository import AlphaRepository
from app.engine.discovery_integration import PROMOTED_STRATEGIES, queue_discovery_predictions


def main() -> int:
    p = argparse.ArgumentParser(description="Nightly Discovery Pipeline")
    p.add_argument("--date", default=None, help="As-of date YYYY-MM-DD (default: today)")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--tenant-id", default="default")
    p.add_argument("--min-adv", type=float, default=2_000_000)
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print candidates without writing to DB",
    )
    p.add_argument(
        "--run-predictions",
        action="store_true",
        help="After queuing, run prediction_cli run-queue automatically",
    )
    args = p.parse_args()

    as_of = date.fromisoformat(args.date) if args.date else date.today()
    as_of_str = as_of.isoformat()

    print(f"\n{'='*60}")
    print(f"  Nightly Discovery Pipeline — {as_of_str}")
    print(f"{'='*60}")
    print(f"  DB:            {args.db}")
    print(f"  Tenant:        {args.tenant_id}")
    print(f"  Min ADV:       ${args.min_adv:,.0f}")
    print(f"  Strategies:    {', '.join(PROMOTED_STRATEGIES)}")
    print(f"  Dry run:       {args.dry_run}")
    print()

    repo = AlphaRepository(db_path=args.db)
    try:
        summary = queue_discovery_predictions(
            repo=repo,
            as_of=as_of,
            tenant_id=args.tenant_id,
            min_adv=args.min_adv,
            dry_run=args.dry_run,
        )
    finally:
        repo.close()

    print(f"[Discovery] feature_rows scanned:  {summary.get('feature_rows', 'n/a')}")
    print(f"[Queue]     total queued:           {summary['total_queued']}")
    print(f"[Consensus] seeds created:          {summary.get('consensus_seeded', 'n/a (dry-run)')}")
    print()
    by_strategy = summary.get("by_strategy") or {}
    if by_strategy:
        for src, cnt in sorted(by_strategy.items()):
            print(f"  {src:<40}  {cnt:>4} predictions")
    else:
        print("  (no candidates passed filters)")
    print()

    if args.dry_run:
        print("DRY RUN complete — nothing written.")
        return 0

    if summary["total_queued"] == 0:
        print("No candidates queued — skipping prediction run.")
        return 0

    if args.run_predictions:
        print(f"[Predictions] Running prediction_cli run-queue for {as_of_str}...")
        cmd = [
            sys.executable, "-m", "app.engine.prediction_cli",
            "run-queue",
            "--as-of", as_of_str,
            "--db", args.db,
            "--tenant-id", args.tenant_id,
            "--forecast-days", "30",
            "--ingress-days", "30",
            "--limit", "200",
        ]
        result = subprocess.run(cmd, capture_output=False)
        if result.returncode not in (0, 3):
            print(f"[ERROR] prediction_cli exited with code {result.returncode}")
            return result.returncode
    else:
        print(f"Next step:")
        print(f"  python -m app.engine.prediction_cli run-queue --as-of {as_of_str} --db {args.db}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
