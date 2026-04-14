#!/usr/bin/env python
"""
Lightweight ML rebuild + retrain over a specified date window.

Why:
  `scripts/expand_training_data.py` loads a large universe and can take a long time.
  This script is meant for fast iteration when you add new factors (e.g. OPT:* derived series).

Example:
  python scripts/retrain_ml_recent.py --symbols AAPL,NVDA --start 2026-01-01 --end 2026-04-10 --tenant ml_train
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path


def _parse_day(x: str) -> date:
    try:
        return date.fromisoformat(str(x))
    except Exception as e:
        raise SystemExit(f"bad date '{x}' (expected YYYY-MM-DD)") from e


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/alpha.db")
    ap.add_argument("--tenant", default="ml_train")
    ap.add_argument("--symbols", default="AAPL,NVDA", help="comma-separated symbols to train on")
    ap.add_argument("--horizons", default="7d", help="comma-separated horizons (default: 7d)")
    ap.add_argument("--start", required=True, help="train start date (YYYY-MM-DD)")
    ap.add_argument("--end", required=True, help="train end date (YYYY-MM-DD)")
    ap.add_argument("--min-coverage", type=float, default=0.5)
    ap.add_argument("--train-days", type=int, default=120)
    ap.add_argument("--predict-days", type=int, default=30)
    ap.add_argument("--step-days", type=int, default=60)
    ap.add_argument("--force-rebuild", action="store_true", help="delete existing ml_learning_rows in range before rebuild")
    args = ap.parse_args()

    db_path = Path(str(args.db))
    tenant = str(args.tenant)
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    horizons = [h.strip() for h in str(args.horizons).split(",") if h.strip()]
    allowed_horizons = {"1h", "4h", "1d", "7d", "30d"}
    bad = [h for h in horizons if h not in allowed_horizons]
    if bad:
        raise SystemExit(f"bad --horizons {bad}; allowed: {sorted(allowed_horizons)}")
    start = _parse_day(str(args.start))
    end = _parse_day(str(args.end))
    if end < start:
        raise SystemExit("--end must be >= --start")

    from app.ml.dataset import build_dataset
    from app.ml.train import run_training_pipeline

    print(f"[retrain] tenant={tenant} db={db_path}")
    print(f"[retrain] symbols={symbols} horizons={horizons} range={start.isoformat()}..{end.isoformat()}")

    if bool(args.force_rebuild):
        import sqlite3

        conn = sqlite3.connect(str(db_path))
        try:
            # Delete only the rows we're about to rebuild.
            sym_q = ",".join(["?"] * len(symbols))
            hor_q = ",".join(["?"] * len(horizons))
            sql = (
                "DELETE FROM ml_learning_rows "
                f"WHERE tenant_id = ? AND symbol IN ({sym_q}) AND horizon IN ({hor_q}) "
                "  AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?"
            )
            params = [tenant] + symbols + horizons + [start.isoformat(), end.isoformat()]
            cur = conn.execute(sql, params)
            conn.commit()
            print(f"[retrain] deleted {cur.rowcount} existing ml_learning_rows")
        finally:
            conn.close()

    inserted = build_dataset(
        symbols=symbols,
        date_range=(start, end),
        horizons=horizons,
        db_path=str(db_path),
        tenant_id=tenant,
        min_feature_coverage=float(args.min_coverage),
        split="train",
    )
    print(f"[retrain] ml_learning_rows inserted: {inserted:,}")

    result = run_training_pipeline(
        symbols=symbols,
        horizons=horizons,
        data_start=start,
        data_end=end,
        db_path=str(db_path),
        tenant_id=tenant,
        min_feature_coverage=float(args.min_coverage),
        train_days=int(args.train_days),
        predict_days=int(args.predict_days),
        step_days=int(args.step_days),
    )
    total_models = sum(len(v) for v in result.values())
    print(f"[retrain] trained {total_models} passed-gate model(s)")
    for h, ids in result.items():
        print(f"  {h}: {len(ids)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
