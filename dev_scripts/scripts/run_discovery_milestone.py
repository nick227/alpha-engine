#!/usr/bin/env python3
"""
Milestone batch: same pipeline as production, more history in one go.

  For each as-of in [end - history_days, end] (step = weekly by default):
    run_discovery  → candidate_queue + discovery_candidates
  Then once:
    run_diversity_admission  → fill dynamic slots (default max 20)

No separate seeding logic — only parameters (wide universe, higher top_n, stricter min ADV).

Example:
  python dev_scripts/scripts/run_discovery_milestone.py --db data/alpha.db --history-days 180 --step-days 7 --top-n 60
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.db.repository import AlphaRepository
from app.discovery.admission import run_diversity_admission
from app.discovery.runner import run_discovery


def _parse_date(s: str) -> date:
    return date.fromisoformat(str(s).strip())


def _iter_dates(start: date, end: date, *, step_days: int):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=step_days)


def main() -> int:
    p = argparse.ArgumentParser(description="Milestone: sweep run_discovery over a window, then admission once.")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--tenant-id", default="default")
    p.add_argument("--end-date", default=None, help="Last as-of (YYYY-MM-DD). Default: today UTC date.")
    p.add_argument("--history-days", type=int, default=180, help="How far back to start (default: 180).")
    p.add_argument("--step-days", type=int, default=7, help="Days between as-of runs (default: 7 = weekly).")
    p.add_argument("--top-n", type=int, default=60, help="Discovery top_n per strategy (milestone default higher than nightly).")
    p.add_argument(
        "--min-adv",
        type=float,
        default=2_000_000.0,
        help="Min avg $ volume 20d (default: 2_000_000).",
    )
    p.add_argument(
        "--use-target-universe",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Restrict to config/target_stocks.yaml (default: false = wide universe).",
    )
    p.add_argument("--timeframe", default="1d")
    p.add_argument("--include-experimental", action="store_true", help="Include sniper_coil / volatility_breakout if registered.")
    p.add_argument("--skip-admission", action="store_true", help="Only run discovery sweep; no run_diversity_admission.")
    p.add_argument("--admission-max", type=int, default=20)
    p.add_argument("--admission-per-lens", type=int, default=4)
    p.add_argument("--admission-per-mcap", type=int, default=5, help="Use -1 to disable mcap cap.")
    p.add_argument("--admission-no-overrule", action="store_true")
    p.add_argument("--admission-overrule-min-mult", type=float, default=0.78)
    p.add_argument("--admission-overrule-min-disc", type=float, default=0.72)
    p.add_argument("--admission-max-overrule-swaps", type=int, default=3)
    p.add_argument("--no-record-metrics", action="store_true", help="Pass through to admission metrics insert.")
    args = p.parse_args()

    end = _parse_date(args.end_date) if args.end_date else date.today()
    start = end - timedelta(days=int(args.history_days))
    step = max(1, int(args.step_days))
    mcap = int(args.admission_per_mcap)
    mcap_arg = None if mcap < 0 else mcap

    dates = list(_iter_dates(start, end, step_days=step))
    if not dates:
        print(json.dumps({"error": "empty date range", "start": start.isoformat(), "end": end.isoformat()}))
        return 1

    sweep: list[dict[str, object]] = []
    for i, asof in enumerate(dates):
        summary = run_discovery(
            db_path=str(args.db),
            tenant_id=str(args.tenant_id),
            as_of=asof,
            top_n=int(args.top_n),
            min_avg_dollar_volume_20d=float(args.min_adv),
            timeframe=str(args.timeframe),
            use_target_universe=bool(args.use_target_universe),
            symbols=None,
            use_feature_snapshot=True,
            include_experimental=bool(args.include_experimental),
        )
        sweep.append(
            {
                "as_of": asof.isoformat(),
                "feature_rows": int(summary.get("feature_rows") or 0),
                "strategies": len(summary.get("strategies") or {}),
            }
        )
        print(f"[{i + 1}/{len(dates)}] discovery as_of={asof.isoformat()} feature_rows={sweep[-1]['feature_rows']}", flush=True)

    admission: dict[str, object] = {"skipped": True}
    if not args.skip_admission:
        repo = AlphaRepository(db_path=str(args.db))
        try:
            admission = run_diversity_admission(
                repo,
                tenant_id=str(args.tenant_id),
                max_admitted=int(args.admission_max),
                per_lens_cap=int(args.admission_per_lens),
                per_mcap_cap=mcap_arg,
                overrule_at_cap=not bool(args.admission_no_overrule),
                overrule_min_multiplier=float(args.admission_overrule_min_mult),
                overrule_min_discovery_score=float(args.admission_overrule_min_disc),
                max_overrule_swaps=int(args.admission_max_overrule_swaps),
                record_metrics=not bool(args.no_record_metrics),
            )
        finally:
            repo.close()

    print(json.dumps({"window": {"start": start.isoformat(), "end": end.isoformat(), "step_days": step}, "runs": len(dates), "sweep": sweep, "admission": admission}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
