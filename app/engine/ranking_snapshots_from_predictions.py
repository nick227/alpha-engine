"""
Persist ranking_snapshots from ranked discovery predictions (aligned with prediction_rank_sqlite).

One row per ticker (highest rank_score wins). Not RankingEngine — that path uses placeholder signals.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from app.core.types import TargetRanking
from app.db.repository import AlphaRepository


def persist_ranking_snapshots_from_ranked_predictions(
    *,
    db_path: str,
    tenant_id: str = "default",
    as_of_date: str,
    mode: str = "discovery",
) -> dict[str, Any]:
    """
    Insert a new ranking_snapshots batch sharing one UTC timestamp.
    Dedupes by ticker using the strongest rank_score for that day.
    """
    repo = AlphaRepository(db_path=db_path)
    ts = datetime.now(timezone.utc)
    try:
        conn = repo.conn
        rows = conn.execute(
            """
            SELECT UPPER(TRIM(ticker)) AS ticker, rank_score, confidence, regime
            FROM predictions
            WHERE tenant_id = ?
              AND mode = ?
              AND date(timestamp) = date(?)
              AND rank_score IS NOT NULL
            ORDER BY rank_score DESC
            """,
            (tenant_id, mode, as_of_date),
        ).fetchall()
        seen: set[str] = set()
        rankings: list[TargetRanking] = []
        for r in rows:
            sym = str(r["ticker"])
            if sym in seen:
                continue
            seen.add(sym)
            rs = float(r["rank_score"] or 0.0)
            conf = float(r["confidence"] or 0.0)
            regime = str(r["regime"] or "NORMAL")
            rankings.append(
                TargetRanking(
                    ticker=sym,
                    score=round(rs, 6),
                    conviction=round(min(1.0, max(0.0, conf)), 6),
                    attribution={"confidence": round(conf, 6), "strategy_weight": 1.0},
                    regime=regime,
                    timestamp=ts,
                    tenant_id=tenant_id,
                )
            )
        if not rankings:
            return {"as_of_date": as_of_date, "written": 0, "snapshot_ts": ts.isoformat()}

        repo.save_target_ranking(rankings, tenant_id)
        return {
            "as_of_date": as_of_date,
            "written": len(rankings),
            "snapshot_ts": ts.isoformat(),
        }
    finally:
        repo.close()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Write ranking_snapshots from ranked discovery predictions")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--tenant-id", default="default")
    p.add_argument("--as-of", dest="as_of", required=True, help="YYYY-MM-DD (must match prediction_rank_sqlite day)")
    p.add_argument("--mode", default="discovery", help="predictions.mode filter (default: discovery)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    out = persist_ranking_snapshots_from_ranked_predictions(
        db_path=str(args.db),
        tenant_id=str(args.tenant_id),
        as_of_date=str(args.as_of),
        mode=str(args.mode),
    )
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
