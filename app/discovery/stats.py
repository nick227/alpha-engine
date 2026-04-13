from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

from app.discovery.promotion import assign_playbook


@dataclass(frozen=True)
class StatRow:
    end_date: str
    window_days: int
    horizon_days: int
    group_type: str
    group_value: str
    n: int
    avg_return: float
    win_rate: float
    lift: float = 0.0
    status: str = ""


def _asof(s: str | date) -> str:
    if isinstance(s, date):
        return s.isoformat()
    return date.fromisoformat(str(s).strip()).isoformat()


def _bucket_days_seen(x: int) -> str:
    if x <= 1:
        return "1"
    if x == 2:
        return "2"
    if x == 3:
        return "3"
    if x <= 5:
        return "4-5"
    if x <= 10:
        return "6-10"
    return "10+"


def _bucket_overlap(x: int) -> str:
    if x <= 1:
        return "1"
    if x == 2:
        return "2"
    if x == 3:
        return "3"
    if x == 4:
        return "4"
    return "5+"


def _agg(returns: list[float]) -> tuple[int, float, float]:
    n = len(returns)
    if n == 0:
        return 0, 0.0, 0.0
    avg = float(sum(returns) / n)
    wins = sum(1 for r in returns if r > 0)
    win_rate = float(wins) / n
    return n, avg, win_rate


def compute_discovery_stats(
    *,
    db_path: str | Path,
    tenant_id: str,
    end_date: str | date,
    window_days: int = 30,
    horizon_days: int = 5,
) -> list[StatRow]:
    """
    Aggregate watchlist outcomes over a trailing window by:
      - overlap bucket
      - days_seen bucket
      - strategy (unnested from strategies_json)

    Uses discovery_outcomes rows where return_pct is not null.
    """
    end_d = _asof(end_date)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    wl_rows = conn.execute(
        """
        SELECT watchlist_date, symbol, return_pct, overlap_count, days_seen, strategies_json
        FROM discovery_outcomes
        WHERE tenant_id = ?
          AND horizon_days = ?
          AND return_pct IS NOT NULL
          AND watchlist_date <= ?
          AND watchlist_date >= date(?, '-' || (? - 1) || ' day')
        """,
        (str(tenant_id), int(horizon_days), end_d, end_d, int(window_days)),
    ).fetchall()

    cand_rows = conn.execute(
        """
        SELECT as_of_date, symbol, strategy_type, return_pct, entry_close
        FROM discovery_candidate_outcomes
        WHERE tenant_id = ?
          AND horizon_days = ?
          AND return_pct IS NOT NULL
          AND as_of_date <= ?
          AND as_of_date >= date(?, '-' || (? - 1) || ' day')
        """,
        (str(tenant_id), int(horizon_days), end_d, end_d, int(window_days)),
    ).fetchall()

    if not wl_rows and not cand_rows:
        return []

    overlap_groups: dict[str, list[float]] = {}
    days_groups: dict[str, list[float]] = {}
    strat_groups: dict[str, list[float]] = {}
    cohort_groups: dict[str, list[float]] = {}
    price_groups: dict[str, list[float]] = {}
    sector_groups: dict[str, list[float]] = {}
    cand_strategy_groups: dict[str, list[float]] = {}
    playbook_groups: dict[str, list[float]] = {}
    playbook_candidate_groups: dict[str, list[float]] = {}
    playbook_candidate_non_promoted_groups: dict[str, list[float]] = {}

    # For promoted vs non-promoted comparisons, precompute promoted symbol set per date.
    promoted_rows = conn.execute(
        """
        SELECT as_of_date, symbol
        FROM discovery_watchlist
        WHERE tenant_id = ?
          AND as_of_date <= ?
          AND as_of_date >= date(?, '-' || (? - 1) || ' day')
        """,
        (str(tenant_id), end_d, end_d, int(window_days)),
    ).fetchall()
    promoted: set[tuple[str, str]] = {(str(r["as_of_date"]), str(r["symbol"])) for r in promoted_rows}

    playbook_rows = conn.execute(
        """
        SELECT as_of_date, symbol, playbook_id
        FROM discovery_watchlist
        WHERE tenant_id = ?
          AND as_of_date <= ?
          AND as_of_date >= date(?, '-' || (? - 1) || ' day')
        """,
        (str(tenant_id), end_d, end_d, int(window_days)),
    ).fetchall()
    playbook_by: dict[tuple[str, str], str] = {
        (str(r["as_of_date"]), str(r["symbol"]).upper()): str(r["playbook_id"] or "") for r in playbook_rows
    }

    # Sector lookup (latest fundamentals <= date)
    frows = conn.execute(
        """
        SELECT fs.ticker, fs.as_of_date, fs.sector
        FROM fundamentals_snapshot fs
        JOIN (
          SELECT tenant_id, ticker, MAX(as_of_date) as mx
          FROM fundamentals_snapshot
          WHERE tenant_id = ? AND as_of_date <= ?
          GROUP BY tenant_id, ticker
        ) latest
          ON latest.tenant_id = fs.tenant_id AND latest.ticker = fs.ticker AND latest.mx = fs.as_of_date
        """,
        (str(tenant_id), end_d),
    ).fetchall()
    sector_by_symbol = {str(r["ticker"]).upper(): (str(r["sector"]) if r["sector"] is not None else None) for r in frows}

    def _price_bucket_from_entry(entry_close: float | None) -> str | None:
        if entry_close is None:
            return None
        p = float(entry_close)
        if p < 1:
            return "<1"
        if p < 2:
            return "1-2"
        if p < 5:
            return "2-5"
        if p < 10:
            return "5-10"
        if p < 20:
            return "10-20"
        return "20+"

    # Watchlist groups (promoted cohort)
    for r in wl_rows:
        ret = float(r["return_pct"])
        sym = str(r["symbol"]).upper()
        wl_date = str(r["watchlist_date"])
        oc = int(r["overlap_count"] or 0)
        ds = int(r["days_seen"] or 0)
        overlap_groups.setdefault(_bucket_overlap(oc), []).append(ret)
        days_groups.setdefault(_bucket_days_seen(ds), []).append(ret)
        cohort_groups.setdefault("watchlist", []).append(ret)
        pb = playbook_by.get((wl_date, sym))
        if pb:
            playbook_groups.setdefault(pb, []).append(ret)

        # Price bucket from entry_close (available in outcomes table)
        # (Fetch from outcomes row itself via a second query would be expensive; instead, ignore for now)
        sec = sector_by_symbol.get(sym)
        if sec:
            sector_groups.setdefault(sec, []).append(ret)

        try:
            payload = json.loads(str(r["strategies_json"] or "[]"))
            if isinstance(payload, list):
                for s in payload:
                    st = str(s).strip()
                    if st:
                        strat_groups.setdefault(st, []).append(ret)
        except Exception:
            pass

    # Candidate cohort: compute symbol-level mean return across strategies for each day+symbol
    # Also assign a playbook per (date,symbol) by looking at that symbol's discovery strategy_types for the day.
    cand_playbook_by: dict[tuple[str, str], str] = {}
    try:
        drows = conn.execute(
            """
            SELECT as_of_date, symbol, strategy_type
            FROM discovery_candidates
            WHERE tenant_id = ?
              AND as_of_date <= ?
              AND as_of_date >= date(?, '-' || (? - 1) || ' day')
            """,
            (str(tenant_id), end_d, end_d, int(window_days)),
        ).fetchall()
        dmap: dict[tuple[str, str], set[str]] = {}
        for r in drows:
            d = str(r["as_of_date"])
            sym = str(r["symbol"]).upper()
            dmap.setdefault((d, sym), set()).add(str(r["strategy_type"]))
        for key, strats in dmap.items():
            pb, _plan = assign_playbook(sorted(strats))
            cand_playbook_by[key] = str(pb)
    except Exception:
        cand_playbook_by = {}

    cand_symbol: dict[tuple[str, str], list[float]] = {}
    for r in cand_rows:
        d = str(r["as_of_date"])
        sym = str(r["symbol"]).upper()
        ret = float(r["return_pct"])
        cand_symbol.setdefault((d, sym), []).append(ret)
        st = str(r["strategy_type"])
        cand_strategy_groups.setdefault(st, []).append(ret)

        pb = _price_bucket_from_entry(float(r["entry_close"]) if r["entry_close"] is not None else None)
        if pb:
            price_groups.setdefault(pb, []).append(ret)
        sec = sector_by_symbol.get(sym)
        if sec:
            sector_groups.setdefault(sec, []).append(ret)

    for (d, sym), rets in cand_symbol.items():
        n, avg, _wr = _agg(rets)
        if n <= 0:
            continue
        cohort_groups.setdefault("candidates", []).append(avg)
        pb = cand_playbook_by.get((d, sym))
        if pb:
            playbook_candidate_groups.setdefault(pb, []).append(avg)
        if (d, sym) not in promoted:
            cohort_groups.setdefault("candidates_non_promoted", []).append(avg)
            if pb:
                playbook_candidate_non_promoted_groups.setdefault(pb, []).append(avg)

    out: list[StatRow] = []

    for k, vals in sorted(playbook_groups.items(), key=lambda kv: kv[0]):
        n, avg, wr = _agg(vals)
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="playbook",
                group_value=str(k),
                n=n,
                avg_return=avg,
                win_rate=wr,
            )
        )

    # Playbook eval: watchlist avg + lift vs candidate cohort assigned to same playbook.
    all_playbooks = sorted(set(playbook_groups.keys()) | set(playbook_candidate_groups.keys()))
    for pb in all_playbooks:
        w = playbook_groups.get(pb, [])
        c = playbook_candidate_groups.get(pb, [])
        n_w, avg_w, wr_w = _agg(w)
        n_c, avg_c, wr_c = _agg(c)
        lift = avg_w - avg_c if n_w and n_c else 0.0
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="playbook_eval",
                group_value=str(pb),
                n=int(min(n_w or 0, n_c or 0) or (n_w or n_c)),
                avg_return=float(avg_w),
                win_rate=float(wr_w),
                lift=float(lift),
                status="",
            )
        )

    for k, vals in sorted(cohort_groups.items(), key=lambda kv: kv[0]):
        n, avg, wr = _agg(vals)
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="cohort",
                group_value=str(k),
                n=n,
                avg_return=avg,
                win_rate=wr,
            )
        )

    for k, vals in sorted(overlap_groups.items(), key=lambda kv: kv[0]):
        n, avg, wr = _agg(vals)
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="overlap_bucket",
                group_value=str(k),
                n=n,
                avg_return=avg,
                win_rate=wr,
            )
        )

    for k, vals in sorted(days_groups.items(), key=lambda kv: kv[0]):
        n, avg, wr = _agg(vals)
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="days_seen_bucket",
                group_value=str(k),
                n=n,
                avg_return=avg,
                win_rate=wr,
            )
        )

    for k, vals in sorted(strat_groups.items(), key=lambda kv: kv[0]):
        n, avg, wr = _agg(vals)
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="strategy",
                group_value=str(k),
                n=n,
                avg_return=avg,
                win_rate=wr,
            )
        )

    for k, vals in sorted(cand_strategy_groups.items(), key=lambda kv: kv[0]):
        n, avg, wr = _agg(vals)
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="candidate_strategy",
                group_value=str(k),
                n=n,
                avg_return=avg,
                win_rate=wr,
            )
        )

    for k, vals in sorted(price_groups.items(), key=lambda kv: kv[0]):
        n, avg, wr = _agg(vals)
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="price_bucket",
                group_value=str(k),
                n=n,
                avg_return=avg,
                win_rate=wr,
            )
        )

    # Keep only the top ~20 sectors by sample size to avoid flooding the table.
    sector_items = sorted(sector_groups.items(), key=lambda kv: len(kv[1]), reverse=True)[:20]
    for k, vals in sector_items:
        n, avg, wr = _agg(vals)
        out.append(
            StatRow(
                end_date=end_d,
                window_days=int(window_days),
                horizon_days=int(horizon_days),
                group_type="sector",
                group_value=str(k),
                n=n,
                avg_return=avg,
                win_rate=wr,
            )
        )

    return out


def stats_to_repo_rows(rows: list[StatRow]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "end_date": r.end_date,
                "window_days": int(r.window_days),
                "horizon_days": int(r.horizon_days),
                "group_type": r.group_type,
                "group_value": r.group_value,
                "n": int(r.n),
                "avg_return": float(r.avg_return),
                "win_rate": float(r.win_rate),
                "lift": float(r.lift),
                "status": str(r.status),
            }
        )
    return out
