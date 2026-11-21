"""
Recompute strategy statistics and regime performance from historical prediction outcomes.
Only valid non-quarantined outcomes are used.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def recompute_strategy_statistics(
    db_path: str = "data/alpha.db",
    tenant_id: str = "default",
) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        po_cols = {str(r["name"]) for r in conn.execute("PRAGMA table_info(prediction_outcomes)").fetchall()}
        p_cols = {str(r["name"]) for r in conn.execute("PRAGMA table_info(predictions)").fetchall()}

        if not po_cols:
            return {
                "strategy_performance_updated": 0,
                "regime_performance_updated": 0,
                "valid_outcomes_evaluated": 0,
            }

        # Select available columns dynamically
        horizon_expr = "po.horizon" if "horizon" in po_cols else ("p.horizon" if "horizon" in p_cols else "'1d'")
        regime_expr = "COALESCE(p.regime, 'NORMAL')" if "regime" in p_cols else "'NORMAL'"
        strategy_expr = "COALESCE(p.strategy_id, 'sentiment_v1_default')" if "strategy_id" in p_cols else "'sentiment_v1_default'"
        quarantine_filter = "AND (po.is_quarantined = 0 OR po.is_quarantined IS NULL)" if "is_quarantined" in po_cols else ""

        query = f"""
            SELECT 
                po.id,
                po.tenant_id,
                po.return_pct,
                po.direction_correct,
                {"po.residual_alpha" if "residual_alpha" in po_cols else "0.0 as residual_alpha"},
                {horizon_expr} as horizon,
                {strategy_expr} as strategy_id,
                {regime_expr} as regime
            FROM prediction_outcomes po
            LEFT JOIN predictions p ON p.id = po.prediction_id AND p.tenant_id = po.tenant_id
            WHERE po.tenant_id = ?
              {quarantine_filter}
        """

        rows = conn.execute(query, (tenant_id,)).fetchall()

        # Group by (strategy_id, horizon)
        strat_horizon_groups: dict[tuple[str, str], list[sqlite3.Row]] = defaultdict(list)
        regime_groups: dict[str, list[sqlite3.Row]] = defaultdict(list)

        for r in rows:
            strat = str(r["strategy_id"] or "sentiment_v1_default")
            horiz = str(r["horizon"] or "1d")
            reg = str(r["regime"] or "NORMAL")

            strat_horizon_groups[(strat, horiz)].append(r)
            strat_horizon_groups[(strat, "ALL")].append(r)
            regime_groups[reg].append(r)

        # Update strategy_performance
        strat_updates = 0
        for (strat_id, horiz), group in strat_horizon_groups.items():
            returns = [float(r["return_pct"] or 0.0) for r in group]
            accuracy = [1.0 if bool(r["direction_correct"]) else 0.0 for r in group]
            residuals = [float(r["residual_alpha"] or 0.0) for r in group]

            acc = round(mean(accuracy), 4) if accuracy else 0.0
            avg_ret = round(mean(returns), 6) if returns else 0.0
            avg_res = round(mean(residuals), 6) if residuals else 0.0
            cnt = len(group)

            existing = conn.execute(
                "SELECT id FROM strategy_performance WHERE tenant_id = ? AND strategy_id = ? AND horizon = ?",
                (tenant_id, strat_id, horiz),
            ).fetchone()
            rec_id = str(existing[0]) if existing else str(uuid4())

            conn.execute(
                """
                INSERT OR REPLACE INTO strategy_performance
                  (id, tenant_id, strategy_id, horizon, prediction_count, accuracy, avg_return, avg_residual_alpha, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (rec_id, tenant_id, strat_id, horiz, cnt, acc, avg_ret, avg_res, now_iso),
            )
            strat_updates += 1

        # Update regime_performance
        regime_updates = 0
        for reg, group in regime_groups.items():
            returns = [float(r["return_pct"] or 0.0) for r in group]
            accuracy = [1.0 if bool(r["direction_correct"]) else 0.0 for r in group]

            acc = round(mean(accuracy), 4) if accuracy else 0.0
            avg_ret = round(mean(returns), 6) if returns else 0.0
            cnt = len(group)

            existing = conn.execute(
                "SELECT id FROM regime_performance WHERE tenant_id = ? AND regime = ?",
                (tenant_id, reg),
            ).fetchone()
            rec_id = str(existing[0]) if existing else str(uuid4())

            conn.execute(
                """
                INSERT OR REPLACE INTO regime_performance
                  (id, tenant_id, regime, prediction_count, accuracy, avg_return, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (rec_id, tenant_id, reg, cnt, acc, avg_ret, now_iso),
            )
            regime_updates += 1

        conn.commit()
        return {
            "strategy_performance_updated": strat_updates,
            "regime_performance_updated": regime_updates,
            "valid_outcomes_evaluated": len(rows),
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Recompute strategy performance statistics")
    parser.add_argument("--db", default="data/alpha.db")
    parser.add_argument("--tenant-id", default="default")
    args = parser.parse_args()

    res = recompute_strategy_statistics(db_path=args.db, tenant_id=args.tenant_id)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
