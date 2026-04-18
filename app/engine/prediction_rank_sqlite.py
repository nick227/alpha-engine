"""
Persist rank_score on SQLite `predictions` using StrategyPerformance, StrategyStability,
and Strategy.live_score — complements queue_rank_trim (pre-build) with post-materialization
ranking grounded in DB metrics. Final score is multiplied by `ranking_temporal.apply_temporal_adjustment`
(VIX / month heuristics) before sort and trim.

Discovery predictions use synthetic strategy_id values (e.g. silent_compounder_v1_paper);
we resolve performance/stability by trying full id and normalized discovery base name.
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
    temporal_ranking_config_snapshot,
)

DEFAULT_TOP_N = int(os.getenv("ALPHA_PREDICTION_TOP_N", "120"))
DEFAULT_MAX_PER_STRATEGY = int(os.getenv("ALPHA_PREDICTION_MAX_PER_STRATEGY", "10"))


def _norm_return_for_rank(avg_return: float) -> float:
    x = max(-0.05, min(0.05, float(avg_return)))
    return (x + 0.05) / 0.10


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _discovery_strategy_keys(strategy_id: str, feature_json: str) -> list[str]:
    keys: list[str] = [str(strategy_id).strip()]
    try:
        meta = json.loads(feature_json or "{}")
        if isinstance(meta, dict):
            st = meta.get("strategy")
            if st and str(st) not in keys:
                keys.append(str(st))
    except Exception:
        pass
    base = str(strategy_id)
    for suf in ("_v1_paper", "_paper"):
        if base.endswith(suf):
            base = base[: -len(suf)]
    if base and base not in keys:
        keys.insert(1, base)
    return keys


def _lookup_performance(conn: sqlite3.Connection, tenant_id: str, keys: list[str]) -> tuple[float, float]:
    for sid in keys:
        for hz in ("ALL", "5d", "20d", "1d"):
            row = conn.execute(
                """
                SELECT accuracy, avg_return FROM strategy_performance
                WHERE tenant_id = ? AND strategy_id = ? AND horizon = ?
                """,
                (tenant_id, sid, hz),
            ).fetchone()
            if row:
                return float(row["accuracy"]), float(row["avg_return"])
    return 0.5, 0.0


def _lookup_stability(conn: sqlite3.Connection, tenant_id: str, keys: list[str]) -> float:
    for sid in keys:
        row = conn.execute(
            """
            SELECT stability_score FROM strategy_stability
            WHERE tenant_id = ? AND strategy_id = ?
            """,
            (tenant_id, sid),
        ).fetchone()
        if row:
            return float(row["stability_score"])
    return 0.5


def _lookup_live_score(conn: sqlite3.Connection, tenant_id: str, keys: list[str]) -> float:
    for sid in keys:
        row = conn.execute(
            "SELECT live_score FROM strategies WHERE tenant_id = ? AND id = ?",
            (tenant_id, sid),
        ).fetchone()
        if row:
            return _clamp01(float(row["live_score"]))
    for sid in keys:
        row = conn.execute(
            """
            SELECT live_score FROM strategies
            WHERE tenant_id = ? AND (strategy_type = ? OR name = ?)
            LIMIT 1
            """,
            (tenant_id, sid, sid),
        ).fetchone()
        if row:
            return _clamp01(float(row["live_score"]))
    return 0.0


def compute_prediction_rank_score(
    *,
    confidence: float,
    accuracy: float,
    avg_return: float,
    live_score: float,
    stability_score: float,
) -> float:
    ret_feat = _norm_return_for_rank(avg_return)
    return (
        0.35 * _clamp01(confidence)
        + 0.20 * _clamp01(accuracy)
        + 0.20 * ret_feat
        + 0.15 * _clamp01(live_score)
        + 0.10 * _clamp01(stability_score)
    )


def rank_predictions_for_date(
    *,
    db_path: str,
    as_of_date: str,
    tenant_id: str = "default",
    apply_trim: bool = True,
    global_top_n: int = DEFAULT_TOP_N,
    max_per_strategy: int = DEFAULT_MAX_PER_STRATEGY,
    mode_filter: str = "discovery",
) -> dict[str, Any]:
    """
    Set predictions.rank_score for the calendar day; optionally delete unscored rows below the cut.
    """
    repo = AlphaRepository(db_path=db_path)
    try:
        conn = repo.conn
        rows = conn.execute(
            """
            SELECT id, strategy_id, confidence, feature_snapshot_json
            FROM predictions
            WHERE tenant_id = ?
              AND mode = ?
              AND date(timestamp) = date(?)
            """,
            (tenant_id, mode_filter, as_of_date),
        ).fetchall()

        market_ctx = build_market_context(conn, tenant_id=tenant_id, as_of_date=as_of_date)

        if not rows:
            return {
                "as_of_date": as_of_date,
                "updated": 0,
                "trimmed": 0,
                "pending_rows": 0,
                "market_context": market_ctx,
            }

        config_snap = temporal_ranking_config_snapshot()
        scored: list[tuple[float, str, str]] = []
        for r in rows:
            keys = _discovery_strategy_keys(str(r["strategy_id"]), str(r["feature_snapshot_json"] or "{}"))
            acc, avg_ret = _lookup_performance(conn, tenant_id, keys)
            stab = _lookup_stability(conn, tenant_id, keys)
            live = _lookup_live_score(conn, tenant_id, keys)
            conf = float(r["confidence"] or 0.0)
            rs = compute_prediction_rank_score(
                confidence=conf,
                accuracy=acc,
                avg_return=avg_ret,
                live_score=live,
                stability_score=stab,
            )
            temporal_key = keys[-1] if len(keys) > 1 else keys[0]
            m = apply_temporal_adjustment(str(temporal_key), market_ctx)
            rs_adj = rs * m
            ranking_snap: dict[str, Any] = {
                "as_of_date": as_of_date,
                "market_context": market_ctx,
                "strategy_key_for_temporal": str(temporal_key),
                "temporal_multiplier": round(m, 6),
                "rank_score_base": round(rs, 6),
                "rank_score": round(rs_adj, 6),
                "config": config_snap,
            }
            conn.execute(
                """
                UPDATE predictions
                SET rank_score = ?, ranking_context_json = ?
                WHERE id = ?
                """,
                (round(rs_adj, 6), json.dumps(ranking_snap, sort_keys=True), str(r["id"])),
            )
            cap_key = keys[-1] if len(keys) > 1 else keys[0]
            scored.append((rs_adj, str(r["id"]), cap_key))

        conn.commit()

        trimmed = 0
        if apply_trim and scored:
            scored.sort(key=lambda t: -t[0])
            keep_ids: set[str] = set()
            per_count: dict[str, int] = {}
            for rs, pid, sk in scored:
                if len(keep_ids) >= int(global_top_n):
                    break
                c = per_count.get(sk, 0)
                if c >= int(max_per_strategy):
                    continue
                keep_ids.add(pid)
                per_count[sk] = c + 1

            all_ids = {p for _rs, p, _ in scored}
            drop_ids = all_ids - keep_ids
            for pid in drop_ids:
                has_out = conn.execute(
                    "SELECT 1 FROM prediction_outcomes WHERE prediction_id = ? LIMIT 1",
                    (pid,),
                ).fetchone()
                if has_out:
                    continue
                conn.execute("DELETE FROM predictions WHERE id = ?", (pid,))
                trimmed += 1
            conn.commit()

        return {
            "as_of_date": as_of_date,
            "updated": len(scored),
            "trimmed": trimmed,
            "pending_rows": len(scored),
            "global_top_n": int(global_top_n),
            "max_per_strategy": int(max_per_strategy),
            "market_context": market_ctx,
        }
    finally:
        repo.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Compute predictions.rank_score from strategy metrics; optional global trim")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--tenant-id", default="default")
    p.add_argument("--as-of", dest="as_of", required=True)
    p.add_argument("--no-trim", action="store_true")
    p.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    p.add_argument("--max-per-strategy", type=int, default=DEFAULT_MAX_PER_STRATEGY)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    out = rank_predictions_for_date(
        db_path=str(args.db),
        as_of_date=str(args.as_of),
        tenant_id=str(args.tenant_id),
        apply_trim=not bool(args.no_trim),
        global_top_n=int(args.top_n),
        max_per_strategy=int(args.max_per_strategy),
    )
    print(json.dumps(out, indent=2))
    mc = out.get("market_context") or {}
    print(market_context_log_line(mc), flush=True)
    append_market_context_audit("prediction_rank_sqlite", mc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
