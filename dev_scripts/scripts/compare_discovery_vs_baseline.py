"""
Discovery vs Baseline Paper Trade Comparison

Reads discovery_candidate_outcomes (backfilled) and prints win rates
by strategy, horizon, and price bucket — the ground truth for whether
our promoted strategies deserve prediction queue slots.

Usage:
    python scripts/compare_discovery_vs_baseline.py
    python scripts/compare_discovery_vs_baseline.py --horizon 5
    python scripts/compare_discovery_vs_baseline.py --strategy silent_compounder
"""

from __future__ import annotations

import argparse
import sqlite3
from typing import Any


def _pct(wins: int, n: int) -> str:
    if n == 0:
        return "  n/a"
    return f"{wins / n * 100:5.1f}%"


def print_section(title: str) -> None:
    print(f"\n{'-'*65}")
    print(f"  {title}")
    print(f"{'-'*65}")


def run_comparison(
    db_path: str = "data/alpha.db",
    filter_strategy: str | None = None,
    filter_horizon: int | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # ------------------------------------------------------------------ #
    # 1. Overall win rates by strategy × horizon
    # ------------------------------------------------------------------ #
    print_section("Win Rates by Strategy × Horizon (filtered universe)")

    query = """
        SELECT
            c.strategy_type,
            o.horizon_days,
            COUNT(*)                                           AS n,
            SUM(CASE WHEN o.return_pct > 0 THEN 1 ELSE 0 END) AS wins,
            AVG(o.return_pct)                                  AS avg_ret,
            MIN(o.return_pct)                                  AS min_ret,
            MAX(o.return_pct)                                  AS max_ret
        FROM discovery_candidate_outcomes o
        JOIN discovery_candidates c
            ON  c.tenant_id   = o.tenant_id
            AND c.symbol      = o.symbol
            AND c.as_of_date  = o.entry_date
        WHERE o.return_pct IS NOT NULL
    """
    params: list[Any] = []
    if filter_strategy:
        query += " AND c.strategy_type = ?"
        params.append(filter_strategy)
    if filter_horizon:
        query += " AND o.horizon_days = ?"
        params.append(filter_horizon)

    query += " GROUP BY c.strategy_type, o.horizon_days ORDER BY c.strategy_type, o.horizon_days"

    rows = conn.execute(query, params).fetchall()

    header = f"{'Strategy':<30} {'Horizon':>7} {'N':>6} {'Win%':>6}  {'AvgRet':>7}  {'MinRet':>7}  {'MaxRet':>7}"
    print(header)
    print("  " + "-" * 63)
    for r in rows:
        print(
            f"  {r['strategy_type']:<28} {r['horizon_days']:>6}d "
            f"{r['n']:>6}  {_pct(r['wins'], r['n'])}  "
            f"{r['avg_ret']*100:>6.2f}%  {r['min_ret']*100:>6.2f}%  {r['max_ret']*100:>6.2f}%"
        )

    # ------------------------------------------------------------------ #
    # 2. Price bucket breakdown — promoted strategies only
    # ------------------------------------------------------------------ #
    print_section("Price Bucket Breakdown — silent_compounder + balance_sheet_survivor")

    bucket_rows = conn.execute("""
        SELECT
            c.strategy_type,
            CASE
                WHEN CAST(json_extract(c.metadata_json, '$.close') AS REAL) >= 20 THEN '$20+'
                WHEN CAST(json_extract(c.metadata_json, '$.close') AS REAL) >= 10 THEN '$10-$20'
                ELSE 'sub-$10'
            END AS price_bucket,
            o.horizon_days,
            COUNT(*)                                           AS n,
            SUM(CASE WHEN o.return_pct > 0 THEN 1 ELSE 0 END) AS wins,
            AVG(o.return_pct)                                  AS avg_ret
        FROM discovery_candidate_outcomes o
        JOIN discovery_candidates c
            ON  c.tenant_id  = o.tenant_id
            AND c.symbol     = o.symbol
            AND c.as_of_date = o.entry_date
        WHERE o.return_pct IS NOT NULL
          AND c.strategy_type IN ('silent_compounder', 'balance_sheet_survivor')
        GROUP BY c.strategy_type, price_bucket, o.horizon_days
        ORDER BY c.strategy_type, price_bucket DESC, o.horizon_days
    """).fetchall()

    hdr2 = f"{'Strategy':<28} {'Bucket':<10} {'Horizon':>7} {'N':>6} {'Win%':>6}  {'AvgRet':>7}"
    print(hdr2)
    print("  " + "-" * 62)
    for r in bucket_rows:
        print(
            f"  {r['strategy_type']:<26} {r['price_bucket']:<10} {r['horizon_days']:>6}d "
            f"{r['n']:>6}  {_pct(r['wins'], r['n'])}  {r['avg_ret']*100:>6.2f}%"
        )

    # ------------------------------------------------------------------ #
    # 3. Recent 30-day slice — are signals degrading?
    # ------------------------------------------------------------------ #
    print_section("Recent 30 Days — signal freshness check")

    recent_rows = conn.execute("""
        SELECT
            c.strategy_type,
            o.horizon_days,
            COUNT(*)                                           AS n,
            SUM(CASE WHEN o.return_pct > 0 THEN 1 ELSE 0 END) AS wins,
            AVG(o.return_pct)                                  AS avg_ret
        FROM discovery_candidate_outcomes o
        JOIN discovery_candidates c
            ON  c.tenant_id  = o.tenant_id
            AND c.symbol     = o.symbol
            AND c.as_of_date = o.entry_date
        WHERE o.return_pct IS NOT NULL
          AND o.entry_date >= date('now', '-30 days')
          AND c.strategy_type IN ('silent_compounder', 'balance_sheet_survivor')
        GROUP BY c.strategy_type, o.horizon_days
        ORDER BY c.strategy_type, o.horizon_days
    """).fetchall()

    hdr3 = f"{'Strategy':<28} {'Horizon':>7} {'N':>6} {'Win%':>6}  {'AvgRet':>7}"
    print(hdr3)
    print("  " + "-" * 52)
    if recent_rows:
        for r in recent_rows:
            print(
                f"  {r['strategy_type']:<26} {r['horizon_days']:>6}d "
                f"{r['n']:>6}  {_pct(r['wins'], r['n'])}  {r['avg_ret']*100:>6.2f}%"
            )
    else:
        print("  (no outcomes scored in last 30 days)")

    # ------------------------------------------------------------------ #
    # 4. Quick action summary
    # ------------------------------------------------------------------ #
    print_section("Action Summary")

    sc = conn.execute("""
        SELECT
            o.horizon_days,
            COUNT(*) AS n,
            SUM(CASE WHEN o.return_pct > 0 THEN 1 ELSE 0 END) AS wins
        FROM discovery_candidate_outcomes o
        JOIN discovery_candidates c
            ON c.tenant_id=o.tenant_id AND c.symbol=o.symbol AND c.as_of_date=o.entry_date
        WHERE o.return_pct IS NOT NULL
          AND c.strategy_type = 'silent_compounder'
          AND CAST(json_extract(c.metadata_json,'$.close') AS REAL) >= 20
        GROUP BY o.horizon_days
    """).fetchall()

    bss = conn.execute("""
        SELECT
            o.horizon_days,
            COUNT(*) AS n,
            SUM(CASE WHEN o.return_pct > 0 THEN 1 ELSE 0 END) AS wins
        FROM discovery_candidate_outcomes o
        JOIN discovery_candidates c
            ON c.tenant_id=o.tenant_id AND c.symbol=o.symbol AND c.as_of_date=o.entry_date
        WHERE o.return_pct IS NOT NULL
          AND c.strategy_type = 'balance_sheet_survivor'
          AND CAST(json_extract(c.metadata_json,'$.close') AS REAL) >= 10
          AND CAST(json_extract(c.metadata_json,'$.close') AS REAL) < 20
        GROUP BY o.horizon_days
    """).fetchall()

    def best_horizon(rows: list) -> str:
        best = max(rows, key=lambda r: r["wins"] / r["n"] if r["n"] > 0 else 0, default=None)
        if best is None:
            return "n/a"
        return f"{best['horizon_days']}d ({_pct(best['wins'], best['n']).strip()} win, n={best['n']})"

    print(f"  silent_compounder   ($20+)    best horizon: {best_horizon(list(sc))}")
    print(f"  balance_sheet_survivor ($10-$20) best horizon: {best_horizon(list(bss))}")
    print()
    print("  Recommended queue settings (from discovery_integration.py):")
    print("    silent_compounder:      UP, 20d horizon, top-15, $20+ floor")
    print("    balance_sheet_survivor: UP,  5d horizon, top-10, $10-$20 range  [!! NOT $20+]")
    print()

    conn.close()


def main() -> int:
    p = argparse.ArgumentParser(description="Discovery vs Baseline Paper Trade Comparison")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--strategy", default=None, help="Filter to one strategy")
    p.add_argument("--horizon", type=int, default=None, help="Filter to one horizon")
    args = p.parse_args()

    run_comparison(
        db_path=args.db,
        filter_strategy=args.strategy,
        filter_horizon=args.horizon,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
