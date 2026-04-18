"""
Daily learning snapshot: simulated outcomes vs executed trades (by source).

Realized return uses pnl / notional (entry_price * quantity); long-biased approximation.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

# Realized return fraction from trade row (matches sim return_pct scale for longs).
_REAL_RET = "(t.pnl / NULLIF(ABS(t.entry_price * t.quantity), 0))"


def _rollup_sql() -> str:
    return f"""
SELECT
  p.ticker AS ticker,
  COUNT(*) AS n,
  AVG(po.return_pct) AS avg_sim_return,
  AVG({_REAL_RET}) AS avg_real_return,
  AVG({_REAL_RET} - po.return_pct) AS avg_execution_gap
FROM predictions p
INNER JOIN prediction_outcomes po
  ON po.prediction_id = p.id AND po.tenant_id = p.tenant_id
INNER JOIN trades t
  ON t.prediction_id = p.id AND t.tenant_id = p.tenant_id
WHERE p.tenant_id = ?
  AND t.source = ?
  AND t.status = 'CLOSED'
GROUP BY p.ticker
ORDER BY avg_real_return DESC
"""


def _global_sql() -> str:
    return f"""
SELECT
  t.source AS source,
  COUNT(*) AS n,
  AVG(po.return_pct) AS avg_sim_return,
  AVG({_REAL_RET}) AS avg_real_return,
  AVG({_REAL_RET} - po.return_pct) AS avg_execution_gap
FROM predictions p
INNER JOIN prediction_outcomes po
  ON po.prediction_id = p.id AND po.tenant_id = p.tenant_id
INNER JOIN trades t
  ON t.prediction_id = p.id AND t.tenant_id = p.tenant_id
WHERE p.tenant_id = ?
  AND t.status = 'CLOSED'
  AND t.source IS NOT NULL AND t.source != ''
GROUP BY t.source
"""


def run_report(*, db_path: str | Path, tenant_id: str = "default") -> dict[str, Any]:
    path = Path(db_path)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    out: dict[str, Any] = {"tenant_id": tenant_id, "by_source": {}, "ticker_rollups": {}}
    try:
        glo = conn.execute(_global_sql(), (tenant_id,)).fetchall()
        for row in glo:
            src = str(row["source"])
            out["by_source"][src] = {
                "n": int(row["n"]),
                "avg_sim_return": row["avg_sim_return"],
                "avg_real_return": row["avg_real_return"],
                "avg_execution_gap": row["avg_execution_gap"],
            }
        for src in ("alpaca", "paper", "manual"):
            rows = conn.execute(_rollup_sql(), (tenant_id, src)).fetchall()
            out["ticker_rollups"][src] = [dict(r) for r in rows]
    finally:
        conn.close()
    return out


def format_oneline(summary: dict[str, Any]) -> str:
    """One line for pipeline logs (aggregate across sources with n>0)."""
    parts: list[str] = []
    for src, m in sorted((summary.get("by_source") or {}).items()):
        if not m or not m.get("n"):
            continue
        parts.append(
            f"{src}: sim={_fmt(m.get('avg_sim_return'))} real={_fmt(m.get('avg_real_return'))} gap={_fmt(m.get('avg_execution_gap'))} (n={m['n']})"
        )
    if not parts:
        return "Real vs Sim: (no matched prediction+outcome+closed trade rows)"
    return "Real vs Sim — " + " | ".join(parts)


def _fmt(x: Any) -> str:
    if x is None:
        return "n/a"
    try:
        return f"{float(x)*100:.3f}%"
    except (TypeError, ValueError):
        return str(x)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Sim vs real learning metrics (SQLite)")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--tenant-id", default="default")
    p.add_argument("--json", action="store_true", help="Print full JSON")
    args = p.parse_args(argv)
    summary = run_report(db_path=args.db, tenant_id=str(args.tenant_id))
    line = format_oneline(summary)
    print(line, flush=True)
    if args.json:
        print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
