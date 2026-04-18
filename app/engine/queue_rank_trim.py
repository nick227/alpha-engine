"""
Global rank + top-N trim for prediction_queue (post-discovery, pre-run-queue).

Ranks pending rows by a simple merit score, writes rank_score into metadata_json,
sets priority from rank, deletes rows below the global cut.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from typing import Any

from app.db.repository import AlphaRepository
from app.engine.ranking_temporal import (
    append_market_context_audit,
    apply_temporal_adjustment,
    build_market_context,
    market_context_log_line,
)

# Default matches typical daily capacity for run-queue limit.
DEFAULT_GLOBAL_TOP_N = int(os.getenv("ALPHA_GLOBAL_TOP_N", "120"))
DEFAULT_STATS_WINDOW = 30
DEFAULT_STATS_HORIZON = 5


def _normalize_avg_return(avg_return: float) -> float:
    """Map trailing avg return (fraction, e.g. -0.02..0.02) into [0, 1]."""
    x = max(-0.05, min(0.05, float(avg_return)))
    return (x + 0.05) / 0.10


def fetch_candidate_strategy_stats(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    end_date: str,
    window_days: int = DEFAULT_STATS_WINDOW,
    horizon_days: int = DEFAULT_STATS_HORIZON,
) -> dict[str, dict[str, float]]:
    """Latest candidate_strategy stats row per strategy name for this end_date."""
    rows = conn.execute(
        """
        SELECT group_value, win_rate, avg_return, n
        FROM discovery_stats
        WHERE tenant_id = ?
          AND end_date = ?
          AND window_days = ?
          AND horizon_days = ?
          AND group_type = 'candidate_strategy'
        """,
        (str(tenant_id), str(end_date), int(window_days), int(horizon_days)),
    ).fetchall()
    out: dict[str, dict[str, float]] = {}
    for r in rows or []:
        gv = str(r["group_value"] or "").strip()
        if not gv:
            continue
        out[gv] = {
            "win_rate": float(r["win_rate"] or 0.0),
            "avg_return": float(r["avg_return"] or 0.0),
            "n": float(r["n"] or 0),
        }
    return out


def compute_rank_score(
    metadata: dict[str, Any],
    strategy_stats: dict[str, dict[str, float]],
) -> float:
    """
    Merit score in ~[0, 1] for sorting. Uses strategy id from metadata['strategy'].
    """
    strategy_id = str(metadata.get("strategy") or metadata.get("strategy_id") or "").strip()
    conf = float(metadata.get("avg_score") or metadata.get("confidence") or 0.5)
    raw_score = float(metadata.get("raw_score") or metadata.get("score") or conf)

    st = strategy_stats.get(strategy_id) if strategy_id else None
    if st:
        win = max(0.0, min(1.0, float(st["win_rate"])))
        ret_feat = _normalize_avg_return(float(st["avg_return"]))
    else:
        win = 0.5
        ret_feat = 0.5

    return (
        0.5 * conf
        + 0.2 * raw_score
        + 0.2 * win
        + 0.1 * ret_feat
    )


def rank_trim_pending_queue(
    *,
    db_path: str,
    as_of_date: str,
    tenant_id: str = "default",
    global_top_n: int = DEFAULT_GLOBAL_TOP_N,
    stats_window_days: int = DEFAULT_STATS_WINDOW,
    stats_horizon_days: int = DEFAULT_STATS_HORIZON,
) -> dict[str, Any]:
    """
    Load pending queue rows for as_of_date, assign rank_score, keep top global_top_n, delete the rest.
    """
    repo = AlphaRepository(db_path=db_path)
    try:
        conn = repo.conn
        strategy_stats = fetch_candidate_strategy_stats(
            conn,
            tenant_id=tenant_id,
            end_date=as_of_date,
            window_days=stats_window_days,
            horizon_days=stats_horizon_days,
        )
        market_ctx = build_market_context(conn, tenant_id=tenant_id, as_of_date=as_of_date)

        rows = conn.execute(
            """
            SELECT rowid, symbol, source, priority, metadata_json
            FROM prediction_queue
            WHERE tenant_id = ?
              AND as_of_date = ?
              AND status = 'pending'
            """,
            (tenant_id, as_of_date),
        ).fetchall()

        if not rows:
            return {
                "as_of_date": as_of_date,
                "pending_before": 0,
                "kept": 0,
                "deleted": 0,
                "global_top_n": int(global_top_n),
                "market_context": market_ctx,
            }

        scored: list[tuple[float, int, dict[str, Any], str, str]] = []
        for r in rows:
            meta = json.loads(str(r["metadata_json"] or "{}"))
            if not isinstance(meta, dict):
                meta = {}
            rank_base = compute_rank_score(meta, strategy_stats)
            strat_for_temporal = str(meta.get("strategy") or meta.get("strategy_id") or "").strip()
            m = apply_temporal_adjustment(strat_for_temporal, market_ctx)
            rank = rank_base * m
            meta["rank_score"] = round(rank, 6)
            meta["temporal_multiplier"] = round(m, 6)
            meta["market_context"] = market_ctx
            scored.append((rank, int(r["rowid"]), meta, str(r["symbol"]).upper(), str(r["source"])))

        scored.sort(key=lambda t: -t[0])
        keep_n = min(int(global_top_n), len(scored))
        keep_rowids = {t[1] for t in scored[:keep_n]}
        all_rowids = {t[1] for t in scored}

        conn.execute("BEGIN IMMEDIATE;")
        try:
            for rank, rowid, meta, _sym, _src in scored:
                if rowid not in keep_rowids:
                    continue
                # Priority for ordering inside run-queue: higher = better
                pri = int(round(float(meta.get("rank_score", 0.0)) * 10_000))
                conn.execute(
                    """
                    UPDATE prediction_queue
                    SET metadata_json = ?, priority = ?
                    WHERE rowid = ?
                    """,
                    (json.dumps(meta, sort_keys=True), pri, rowid),
                )

            to_delete = all_rowids - keep_rowids
            for rowid in to_delete:
                conn.execute("DELETE FROM prediction_queue WHERE rowid = ?", (rowid,))

            conn.commit()
        except Exception:
            conn.rollback()
            raise

        return {
            "as_of_date": as_of_date,
            "pending_before": len(scored),
            "kept": keep_n,
            "deleted": len(to_delete),
            "global_top_n": int(global_top_n),
            "strategy_stats_keys": sorted(strategy_stats.keys()),
            "market_context": market_ctx,
        }
    finally:
        repo.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Rank and globally trim prediction_queue pending rows before run-queue")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--tenant-id", default="default")
    p.add_argument("--as-of", dest="as_of", required=True, help="Queue as_of_date YYYY-MM-DD")
    p.add_argument("--top-n", type=int, default=DEFAULT_GLOBAL_TOP_N, help="Global cap after ranking (default from ALPHA_GLOBAL_TOP_N)")
    p.add_argument("--stats-window", type=int, default=DEFAULT_STATS_WINDOW)
    p.add_argument("--stats-horizon", type=int, default=DEFAULT_STATS_HORIZON)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = rank_trim_pending_queue(
        db_path=str(args.db),
        as_of_date=str(args.as_of),
        tenant_id=str(args.tenant_id),
        global_top_n=int(args.top_n),
        stats_window_days=int(args.stats_window),
        stats_horizon_days=int(args.stats_horizon),
    )
    print(json.dumps(summary, indent=2))
    mc = summary.get("market_context") or {}
    print(market_context_log_line(mc), flush=True)
    append_market_context_audit("queue_rank_trim", mc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
