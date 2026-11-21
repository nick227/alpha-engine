"""
Refresh ML features in ml_learning_rows from existing price bars and universe data.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.active_universe import get_active_universe_tickers
from app.ml.dataset import build_dataset


def refresh_ml_features(
    db_path: str = "data/alpha.db",
    tenant_id: str = "default",
    lookback_days: int = 14,
    horizons: list[str] | None = None,
) -> dict[str, int]:
    if horizons is None:
        horizons = ["1d", "7d"]

    end_date = date.today()
    start_date = end_date - timedelta(days=lookback_days)

    conn = sqlite3.connect(db_path)
    try:
        symbols = get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn)
    finally:
        conn.close()

    if not symbols:
        return {"inserted": 0, "symbols": 0}

    # Bounded symbol subset if universe is large to ensure quick execution
    target_symbols = symbols[:50]

    inserted = build_dataset(
        symbols=target_symbols,
        date_range=(start_date, end_date),
        horizons=horizons,
        db_path=db_path,
        tenant_id=tenant_id,
        split="train",
    )

    return {"inserted": inserted, "symbols": len(target_symbols)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh ML learning rows from price history")
    parser.add_argument("--db", default="data/alpha.db")
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument("--lookback-days", type=int, default=14)
    args = parser.parse_args()

    res = refresh_ml_features(db_path=args.db, tenant_id=args.tenant_id, lookback_days=args.lookback_days)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
