#!/usr/bin/env python
"""
Replay sniper_coil across all historical fear-regime dates in feature_snapshot.

Runs run_discovery() for every date that is BOTH:
  1. present in feature_snapshot (has computed features), AND
  2. a fear-regime day (VIX > VIX3M)

Persists:
  - sniper candidates  -> discovery_candidates (via run_discovery)
  - near-misses        -> sniper_near_misses   (via run_discovery)

After this script completes, run the outcomes backfill to get win/loss data:
    python scripts/backfill_discovery_outcomes.py --lookback 9999 --horizons 5,20

Usage:
    python scripts/replay_fear_regime.py
    python scripts/replay_fear_regime.py --start-date 2007-01-01 --end-date 2023-12-31
    python scripts/replay_fear_regime.py --dry-run       # count dates only, no writes
    python scripts/replay_fear_regime.py --skip-existing # skip dates already in discovery_candidates
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

DB_PATH = _ROOT / "data" / "alpha.db"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_replay_dates(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    min_symbols: int = 100,
    skip_existing: bool = False,
) -> list[str]:
    """
    Return sorted list of dates that have feature_snapshot coverage AND fear regime.
    Optionally skips dates already present in discovery_candidates for sniper_coil.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT fs.as_of_date
        FROM feature_snapshot fs
        JOIN (
            SELECT DISTINCT DATE(v.timestamp) AS dt
            FROM price_bars v
            JOIN price_bars v3
              ON v3.ticker = '^VIX3M'
             AND v3.timeframe = '1d'
             AND DATE(v3.timestamp) = DATE(v.timestamp)
            WHERE v.ticker = '^VIX'
              AND v.timeframe = '1d'
              AND v.close > v3.close
        ) fear ON fear.dt = fs.as_of_date
        WHERE fs.as_of_date >= ?
          AND fs.as_of_date <= ?
        GROUP BY fs.as_of_date
        HAVING COUNT(DISTINCT fs.symbol) >= ?
        ORDER BY fs.as_of_date ASC
        """,
        (start_date, end_date, min_symbols),
    ).fetchall()
    dates = [str(r[0]) for r in rows]

    if skip_existing and dates:
        ph = ",".join("?" * len(dates))
        done = {
            str(r[0])
            for r in conn.execute(
                f"""
                SELECT DISTINCT as_of_date FROM discovery_candidates
                WHERE strategy_type = 'sniper_coil'
                  AND as_of_date IN ({ph})
                """,
                dates,
            ).fetchall()
        }
        dates = [d for d in dates if d not in done]

    return dates


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Replay sniper_coil on historical fear-regime dates")
    parser.add_argument("--db",            default=str(DB_PATH), help="Path to alpha.db")
    parser.add_argument("--start-date",    default="2007-01-01", help="Earliest date (default: 2007-01-01)")
    parser.add_argument("--end-date",      default=date.today().isoformat(), help="Latest date (default: today)")
    parser.add_argument("--skip-existing", action="store_true",  help="Skip dates already in discovery_candidates")
    parser.add_argument("--dry-run",       action="store_true",  help="Count eligible dates only, no writes")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    print(f"\n=== Fear-regime sniper replay [{args.start_date} -> {args.end_date}] ===\n")

    replay_dates = _get_replay_dates(
        conn,
        start_date=args.start_date,
        end_date=args.end_date,
        skip_existing=args.skip_existing,
    )
    conn.close()

    print(f"Eligible fear-regime dates with feature coverage: {len(replay_dates)}")
    if not replay_dates:
        print("Nothing to replay.")
        return 0

    # Year distribution
    from collections import Counter
    by_year: Counter = Counter(d[:4] for d in replay_dates)
    for yr in sorted(by_year):
        print(f"  {yr}: {by_year[yr]} days")

    if args.dry_run:
        print("\n[dry-run] No writes performed.")
        return 0

    # Import here so sys.path manipulation above takes effect first
    from app.discovery.runner import run_discovery

    print(f"\nReplaying {len(replay_dates)} dates ...\n")
    t_start = time.time()

    total_candidates = 0
    total_near_misses = 0
    errors = 0

    for i, as_of in enumerate(replay_dates, 1):
        try:
            summary = run_discovery(db_path=args.db, as_of=as_of)
            sniper_data = summary.get("strategies", {}).get("sniper_coil", {})
            n_cands = len(sniper_data.get("top", []))
            n_near = sniper_data.get("near_misses", 0)
            total_candidates += n_cands
            total_near_misses += n_near

            elapsed = time.time() - t_start
            rate = i / elapsed
            eta = (len(replay_dates) - i) / rate if rate > 0 else 0
            print(
                f"  [{i:>4}/{len(replay_dates)}] {as_of}  "
                f"candidates={n_cands}  near_misses={n_near}  "
                f"(ETA {eta/60:.0f}min)",
                flush=True,
            )

        except Exception as e:
            print(f"  [{i:>4}/{len(replay_dates)}] {as_of}  ERROR: {e}", flush=True)
            errors += 1

    elapsed_total = time.time() - t_start
    print(f"\n=== Replay complete ===")
    print(f"Dates processed:   {len(replay_dates) - errors}")
    print(f"Errors:            {errors}")
    print(f"Sniper candidates: {total_candidates}")
    print(f"Near-misses:       {total_near_misses}")
    print(f"Elapsed:           {elapsed_total/60:.1f} min")
    print()
    print("Next step: compute outcomes")
    print("  python scripts/backfill_discovery_outcomes.py --lookback 9999 --horizons 5,20")

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
