"""
Read-only aggregates for the trading UI: explainability, performance, admission, diffs.

No new scoring — only SQL + JSON parse over existing tables.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    try:
        r = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        ).fetchone()
        return r is not None
    except Exception:
        return False


def _j(x: str | None) -> dict[str, Any]:
    if not x:
        return {}
    try:
        o = json.loads(str(x))
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}


def build_ticker_why_panel(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
) -> dict[str, Any]:
    sym = str(ticker).strip().upper()
    out: dict[str, Any] = {"ticker": sym, "tenant_id": tenant_id, "candidate_queue": None, "recent_predictions": []}

    if _has_table(conn, "candidate_queue"):
        try:
            row = conn.execute(
                """
                SELECT ticker, status, first_seen_at, last_seen_at, signal_count, rejection_reason,
                       primary_strategy, strategy_tags_json, discovery_lens, discovery_score,
                       price_bucket, market_cap_bucket, sector, industry, multiplier_score, metadata_json
                FROM candidate_queue
                WHERE tenant_id = ? AND UPPER(TRIM(ticker)) = ?
                """,
                (tenant_id, sym),
            ).fetchone()
            if row:
                out["candidate_queue"] = dict(row)
        except Exception:
            pass

    try:
        rows = conn.execute(
            """
            SELECT id, strategy_id, timestamp, prediction, confidence, rank_score, ranking_context_json, horizon
            FROM predictions
            WHERE tenant_id = ? AND UPPER(TRIM(ticker)) = ?
            ORDER BY timestamp DESC
            LIMIT 25
            """,
            (tenant_id, sym),
        ).fetchall()
        for r in rows:
            d = dict(r)
            ctx = _j(str(d.get("ranking_context_json") or ""))
            d["ranking_context_parsed"] = ctx
            d["temporal_multiplier"] = ctx.get("temporal_multiplier")
            d["rank_score_base"] = ctx.get("rank_score_base")
            mc = ctx.get("market_context") if isinstance(ctx.get("market_context"), dict) else {}
            d["vix_age_days"] = mc.get("vix_age_days")
            d["context_warning"] = mc.get("context_warning")
            d["vix_fallback_used"] = mc.get("vix_fallback_used")
            out["recent_predictions"].append(d)
    except Exception:
        pass

    return out


def build_per_ticker_performance(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    windows: tuple[int, ...] = (30, 60, 90),
) -> dict[str, Any]:
    sym = str(ticker).strip().upper()
    by_window: dict[str, Any] = {}
    for w in windows:
        try:
            rows = conn.execute(
                """
                SELECT p.strategy_id,
                       AVG(CASE WHEN po.direction_correct THEN 1.0 ELSE 0.0 END) AS win_rate,
                       AVG(po.return_pct) AS avg_return,
                       COUNT(*) AS n
                FROM prediction_outcomes po
                JOIN predictions p ON p.id = po.prediction_id AND p.tenant_id = po.tenant_id
                WHERE po.tenant_id = ? AND UPPER(TRIM(p.ticker)) = ?
                  AND po.evaluated_at >= datetime('now', ?)
                GROUP BY p.strategy_id
                ORDER BY n DESC
                """,
                (tenant_id, sym, f"-{int(w)} days"),
            ).fetchall()
            strat_rows = [dict(x) for x in rows]
            best = max(strat_rows, key=lambda r: (r.get("n") or 0, r.get("win_rate") or 0)) if strat_rows else None
            worst = min(strat_rows, key=lambda r: (r.get("win_rate") or 1.0, r.get("n") or 0)) if strat_rows else None
            by_window[f"{w}d"] = {
                "by_strategy": strat_rows,
                "best_strategy": (best.get("strategy_id") if best else None),
                "worst_strategy": (worst.get("strategy_id") if worst else None),
            }
        except Exception:
            by_window[f"{w}d"] = {"by_strategy": [], "best_strategy": None, "worst_strategy": None}

    return {"ticker": sym, "windows": by_window}


def build_strategy_ticker_matrix(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    tickers: list[str],
    lookback_days: int = 90,
) -> list[dict[str, Any]]:
    if not tickers:
        return []
    syms = [str(t).strip().upper() for t in tickers if str(t).strip()]
    if not syms:
        return []
    ph = ",".join(["?"] * len(syms))
    try:
        rows = conn.execute(
            f"""
            SELECT UPPER(TRIM(p.ticker)) AS ticker, p.strategy_id,
                   AVG(CASE WHEN po.direction_correct THEN 1.0 ELSE 0.0 END) AS win_rate,
                   AVG(po.return_pct) AS avg_return,
                   COUNT(*) AS n
            FROM prediction_outcomes po
            JOIN predictions p ON p.id = po.prediction_id AND p.tenant_id = po.tenant_id
            WHERE po.tenant_id = ?
              AND UPPER(TRIM(p.ticker)) IN ({ph})
              AND po.evaluated_at >= datetime('now', ?)
            GROUP BY UPPER(TRIM(p.ticker)), p.strategy_id
            """,
            (tenant_id, *syms, f"-{int(lookback_days)} days"),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def build_what_changed_recent(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    hours: int = 24,
) -> dict[str, Any]:
    out: dict[str, Any] = {"admission_runs": [], "swaps_recent": [], "candidate_touch_recent": []}
    if _has_table(conn, "admission_metrics"):
        try:
            rows = conn.execute(
                """
                SELECT run_at, newly_admitted_count, overrule_swap_count, overrule_detail_json, thresholds_json
                FROM admission_metrics
                WHERE tenant_id = ?
                  AND run_at >= datetime('now', ?)
                ORDER BY run_at DESC
                LIMIT 50
                """,
                (tenant_id, f"-{int(hours)} hours"),
            ).fetchall()
            out["admission_runs"] = [dict(r) for r in rows]
            for r in out["admission_runs"]:
                try:
                    det = json.loads(str(r.get("overrule_detail_json") or "[]"))
                    if isinstance(det, list):
                        out["swaps_recent"].extend(det)
                except Exception:
                    pass
        except Exception:
            pass

    if _has_table(conn, "candidate_queue"):
        try:
            rows = conn.execute(
                """
                SELECT ticker, status, last_seen_at, signal_count, discovery_lens, multiplier_score
                FROM candidate_queue
                WHERE tenant_id = ?
                  AND last_seen_at >= datetime('now', ?)
                ORDER BY last_seen_at DESC
                LIMIT 200
                """,
                (tenant_id, f"-{int(hours)} hours"),
            ).fetchall()
            out["candidate_touch_recent"] = [dict(r) for r in rows]
        except Exception:
            pass

    return out


def build_topn_quality_snapshot(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    top_tickers: list[str],
) -> dict[str, Any]:
    """Aggregate lens / multiplier / parsed warnings for a list of tickers (e.g. top rankings)."""
    if not top_tickers:
        return {"n": 0, "avg_multiplier": None, "lens_counts": {}, "with_context_warning": 0}
    syms = [str(t).strip().upper() for t in top_tickers]
    ph = ",".join(["?"] * len(syms))
    mults: list[float] = []
    lenses: dict[str, int] = {}
    warn_ct = 0
    if _has_table(conn, "candidate_queue"):
        try:
            rows = conn.execute(
                f"""
                SELECT UPPER(TRIM(ticker)) AS t, multiplier_score,
                       COALESCE(NULLIF(TRIM(discovery_lens),''), 'unknown') AS lens
                FROM candidate_queue
                WHERE tenant_id = ? AND UPPER(TRIM(ticker)) IN ({ph})
                """,
                (tenant_id, *syms),
            ).fetchall()
            for r in rows:
                m = r["multiplier_score"]
                if m is not None:
                    mults.append(float(m))
                lx = str(r["lens"] or "unknown")
                lenses[lx] = lenses.get(lx, 0) + 1
        except Exception:
            pass

    # context_warning: from latest prediction per ticker if present
    for s in syms:
        try:
            row = conn.execute(
                """
                SELECT ranking_context_json FROM predictions
                WHERE tenant_id = ? AND UPPER(TRIM(ticker)) = ?
                ORDER BY timestamp DESC LIMIT 1
                """,
                (tenant_id, s),
            ).fetchone()
            if not row:
                continue
            ctx = _j(str(row["ranking_context_json"] or ""))
            mc = ctx.get("market_context") if isinstance(ctx.get("market_context"), dict) else {}
            if mc.get("context_warning"):
                warn_ct += 1
        except Exception:
            pass

    n = len(syms)
    avg_m = sum(mults) / len(mults) if mults else None
    return {
        "n": n,
        "avg_multiplier": avg_m,
        "lens_counts": lenses,
        "with_context_warning": warn_ct,
        "pct_context_warning": (warn_ct / n) if n else 0.0,
    }
