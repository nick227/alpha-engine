"""
Read-only: ranking snapshot deltas, short outcome trends, weekly outcome aggregates.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from app.ui.middle.explainability_constants import MIN_SAMPLE_N


def _rank_delta_movement(rank_delta: int) -> str:
    """Lower rank # = better. Negative delta ⇒ rank # went down ⇒ improving."""
    if rank_delta < 0:
        return "↓ improving"
    if rank_delta > 0:
        return "↑ weakening"
    return "→ flat"


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    try:
        r = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        ).fetchone()
        return r is not None
    except Exception:
        return False


def _ranks_at_timestamp(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ts: str,
    max_depth: int,
) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT UPPER(TRIM(ticker)) AS ticker, score, conviction
        FROM ranking_snapshots
        WHERE tenant_id = ? AND timestamp = ?
        ORDER BY score DESC, ticker ASC
        LIMIT ?
        """,
        (tenant_id, ts, int(max_depth)),
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for i, r in enumerate(rows):
        t = str(r["ticker"])
        out[t] = {
            "rank": i + 1,
            "score": float(r["score"]),
            "conviction": float(r["conviction"]),
        }
    return out


def build_ranking_movers(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    max_rank_depth: int = 800,
    top_n: int = 20,
) -> dict[str, Any]:
    """
    Compare the two most recent distinct ranking_snapshots timestamps.
    rank_delta = rank_today - rank_yesterday (negative ⇒ rank # lower ⇒ improving).
    Each delta row includes `movement`: ↓ improving / ↑ weakening / → flat.
    """
    empty: dict[str, Any] = {
        "snapshot_ts_latest": None,
        "snapshot_ts_previous": None,
        "max_rank_depth": int(max_rank_depth),
        "message": None,
        "risers": [],
        "fallers": [],
        "new_in_latest": [],
        "dropped_from_latest": [],
        "all_deltas": [],
    }
    if not _has_table(conn, "ranking_snapshots"):
        empty["message"] = "ranking_snapshots table missing"
        return empty
    try:
        rows_ts = conn.execute(
            """
            SELECT DISTINCT timestamp FROM ranking_snapshots
            WHERE tenant_id = ?
            ORDER BY timestamp DESC
            LIMIT 2
            """,
            (tenant_id,),
        ).fetchall()
    except Exception:
        return empty

    if not rows_ts:
        empty["message"] = "No ranking snapshots"
        return empty
    ts_today = str(rows_ts[0]["timestamp"])
    empty["snapshot_ts_latest"] = ts_today
    if len(rows_ts) < 2:
        empty["message"] = "Need two distinct snapshot times for rank deltas"
        return empty
    ts_prev = str(rows_ts[1]["timestamp"])
    empty["snapshot_ts_previous"] = ts_prev

    today = _ranks_at_timestamp(conn, tenant_id=tenant_id, ts=ts_today, max_depth=max_rank_depth)
    prev = _ranks_at_timestamp(conn, tenant_id=tenant_id, ts=ts_prev, max_depth=max_rank_depth)

    all_syms = set(today) | set(prev)
    deltas: list[dict[str, Any]] = []
    new_in: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []

    for sym in sorted(all_syms):
        rt = today.get(sym)
        rp = prev.get(sym)
        if rt and rp:
            r_today = int(rt["rank"])
            r_prev = int(rp["rank"])
            rank_delta = r_today - r_prev
            deltas.append(
                {
                    "ticker": sym,
                    "rank_today": r_today,
                    "rank_yesterday": r_prev,
                    "rank_delta": rank_delta,
                    "movement": _rank_delta_movement(rank_delta),
                    "score_today": rt["score"],
                    "score_yesterday": rp["score"],
                }
            )
        elif rt and not rp:
            new_in.append(
                {
                    "ticker": sym,
                    "rank_today": rt["rank"],
                    "score_today": rt["score"],
                }
            )
        elif rp and not rt:
            dropped.append(
                {
                    "ticker": sym,
                    "rank_yesterday": rp["rank"],
                    "score_yesterday": rp["score"],
                }
            )

    # Risers: most negative delta (improved rank number)
    risers = sorted(deltas, key=lambda x: x["rank_delta"])[: int(top_n)]
    fallers = sorted(deltas, key=lambda x: -x["rank_delta"])[: int(top_n)]

    return {
        "snapshot_ts_latest": ts_today,
        "snapshot_ts_previous": ts_prev,
        "max_rank_depth": int(max_rank_depth),
        "message": None,
        "risers": risers,
        "fallers": fallers,
        "new_in_latest": new_in[: int(top_n)],
        "dropped_from_latest": dropped[: int(top_n)],
        "all_deltas": deltas,
    }


def build_outcome_trend_last_n(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    last_n: int = 10,
) -> dict[str, Any]:
    """Split last N evaluated outcomes (chronological): first half vs second half win rate."""
    sym = str(ticker).strip().upper()
    n = max(4, min(20, int(last_n)))
    half = n // 2
    out: dict[str, Any] = {
        "ticker": sym,
        "last_n": n,
        "min_sample_threshold": MIN_SAMPLE_N,
        "n_actual": 0,
        "low_sample": True,
        "half_first_n": 0,
        "half_second_n": 0,
        "outcomes": [],
        "win_rate_first_half": None,
        "win_rate_second_half": None,
        "trend": "— insufficient data",
    }
    try:
        rows = conn.execute(
            """
            SELECT po.direction_correct, po.return_pct, po.evaluated_at, p.strategy_id
            FROM prediction_outcomes po
            JOIN predictions p ON p.id = po.prediction_id AND p.tenant_id = po.tenant_id
            WHERE po.tenant_id = ? AND UPPER(TRIM(p.ticker)) = ?
            ORDER BY po.evaluated_at DESC
            LIMIT ?
            """,
            (tenant_id, sym, n),
        ).fetchall()
    except Exception:
        return out

    if len(rows) < 4:
        out["n_actual"] = len(rows)
        return out

    chrono = list(reversed([dict(r) for r in rows]))
    out["outcomes"] = chrono
    out["n_actual"] = len(chrono)

    first = chrono[:half]
    second = chrono[half:]
    out["half_first_n"] = len(first)
    out["half_second_n"] = len(second)
    out["low_sample"] = (
        len(chrono) < MIN_SAMPLE_N
        or len(first) < MIN_SAMPLE_N
        or len(second) < MIN_SAMPLE_N
    )
    if not first or not second:
        return out

    def _wr(block: list[dict[str, Any]]) -> float:
        ok = sum(1 for r in block if int(r.get("direction_correct") or 0))
        return ok / len(block) if block else 0.0

    w1, w2 = _wr(first), _wr(second)
    out["win_rate_first_half"] = w1
    out["win_rate_second_half"] = w2
    diff = w2 - w1
    if diff > 0.15:
        out["trend"] = "↑ improving"
    elif diff < -0.15:
        out["trend"] = "↓ degrading"
    else:
        out["trend"] = "→ flat"
    return out


def build_rank_history_series(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    ticker: str,
    max_snapshots: int = 10,
    max_rank_depth: int = 800,
) -> dict[str, Any]:
    """
    Rank of one ticker across the last N distinct ranking_snapshots times (chronological for charts).
    """
    sym = str(ticker).strip().upper()
    mrd = int(max_rank_depth)
    out: dict[str, Any] = {
        "ticker": sym,
        "max_snapshots": int(max_snapshots),
        "max_rank_depth": mrd,
        "series": [],
        "message": None,
    }
    if not _has_table(conn, "ranking_snapshots"):
        out["message"] = "ranking_snapshots table missing"
        return out
    try:
        ts_rows = conn.execute(
            """
            SELECT DISTINCT timestamp FROM ranking_snapshots
            WHERE tenant_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (tenant_id, int(max_snapshots)),
        ).fetchall()
    except Exception:
        return out
    if not ts_rows:
        out["message"] = "No ranking snapshots"
        return out
    chronological = list(reversed([str(r["timestamp"]) for r in ts_rows]))
    series: list[dict[str, Any]] = []
    for ts in chronological:
        ranks = _ranks_at_timestamp(
            conn, tenant_id=tenant_id, ts=ts, max_depth=max_rank_depth
        )
        if sym in ranks:
            rk = int(ranks[sym]["rank"])
            series.append(
                {
                    "snapshot_ts": ts,
                    "rank": rk,
                    "rank_norm": round(rk / float(mrd), 6) if mrd > 0 else None,
                    "score": ranks[sym]["score"],
                    "conviction": ranks[sym]["conviction"],
                }
            )
        else:
            series.append(
                {
                    "snapshot_ts": ts,
                    "rank": None,
                    "rank_norm": None,
                    "score": None,
                    "conviction": None,
                }
            )
    out["series"] = series
    return out


def build_weekly_performance_summary(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
) -> dict[str, Any]:
    """Aggregated evaluated outcomes in the last 7 days."""
    out: dict[str, Any] = {
        "window_days": 7,
        "min_sample_threshold": MIN_SAMPLE_N,
        "overall": {"n": 0, "win_rate": None, "avg_return": None, "low_sample": False},
        "by_strategy": [],
    }
    if not _has_table(conn, "prediction_outcomes"):
        return out
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n,
                   AVG(CASE WHEN po.direction_correct THEN 1.0 ELSE 0.0 END) AS win_rate,
                   AVG(po.return_pct) AS avg_return
            FROM prediction_outcomes po
            JOIN predictions p ON p.id = po.prediction_id AND p.tenant_id = po.tenant_id
            WHERE po.tenant_id = ?
              AND po.evaluated_at >= datetime('now', '-7 days')
            """,
            (tenant_id,),
        ).fetchone()
        if row:
            n_o = int(row["n"] or 0)
            out["overall"] = {
                "n": n_o,
                "win_rate": float(row["win_rate"]) if row["win_rate"] is not None else None,
                "avg_return": float(row["avg_return"]) if row["avg_return"] is not None else None,
                "low_sample": 0 < n_o < MIN_SAMPLE_N,
            }
    except Exception:
        pass
    try:
        rows = conn.execute(
            """
            SELECT p.strategy_id,
                   COUNT(*) AS n,
                   AVG(CASE WHEN po.direction_correct THEN 1.0 ELSE 0.0 END) AS win_rate,
                   AVG(po.return_pct) AS avg_return
            FROM prediction_outcomes po
            JOIN predictions p ON p.id = po.prediction_id AND p.tenant_id = po.tenant_id
            WHERE po.tenant_id = ?
              AND po.evaluated_at >= datetime('now', '-7 days')
            GROUP BY p.strategy_id
            ORDER BY n DESC
            """,
            (tenant_id,),
        ).fetchall()
        out["by_strategy"] = []
        for r in rows:
            d = dict(r)
            n_s = int(d.get("n") or 0)
            d["low_sample"] = n_s < MIN_SAMPLE_N
            out["by_strategy"].append(d)
    except Exception:
        pass
    return out
