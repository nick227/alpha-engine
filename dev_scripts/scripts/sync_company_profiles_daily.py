"""
Hydrate company_profiles/*.json for the active trading universe (YAML ∪ admitted).

Supports stats fields that come from disk (e.g. ipoDate). Safe to run daily — skips
complete files unless listed keys are missing (see ensure_yfinance_company_profiles).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env", override=False)
    except ImportError:
        pass

    from app.core.active_universe import get_active_universe_tickers
    from app.core.company_profiles.yfinance_profiles import ensure_yfinance_company_profiles

    db_env = (os.environ.get("ALPHA_DB_PATH") or "").strip()
    db_default = Path(db_env) if db_env else (ROOT / "data" / "alpha.db")

    p = argparse.ArgumentParser(description="yfinance profile JSON sync for active universe")
    p.add_argument("--db", default=str(db_default))
    p.add_argument("--tenant-id", default="default")
    profiles_default = os.environ.get("COMPANY_PROFILES_DIR", str(ROOT / "data" / "company_profiles"))
    p.add_argument("--profiles-dir", default=profiles_default)
    p.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("PROFILE_FETCH_CONCURRENCY", "6")),
    )
    args = p.parse_args()

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    try:
        tickers = get_active_universe_tickers(tenant_id=args.tenant_id, sqlite_conn=conn)
    finally:
        conn.close()

    print(f"Profiles: fetching/refining for {len(tickers)} symbols (active universe)...")
    asyncio.run(
        ensure_yfinance_company_profiles(
            tickers,
            out_dir=args.profiles_dir,
            concurrency=max(1, int(args.concurrency)),
        )
    )
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
