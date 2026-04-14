from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OutcomeRow:
    symbol: str
    horizon_days: int
    entry_date: str
    exit_date: str | None
    entry_close: float
    exit_close: float | None
    return_pct: float | None
    overlap_count: int | None
    days_seen: int | None
    strategies: list[str]


def _asof(s: str | date) -> str:
    if isinstance(s, date):
        return s.isoformat()
    return date.fromisoformat(str(s).strip()).isoformat()


def _load_watchlist(conn: sqlite3.Connection, *, tenant_id: str, watchlist_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT symbol, overlap_count, days_seen, avg_score, strategies_json
        FROM discovery_watchlist
        WHERE tenant_id = ? AND as_of_date = ?
        ORDER BY overlap_count DESC, days_seen DESC, avg_score DESC
        """,
        (str(tenant_id), str(watchlist_date)),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        strategies: list[str] = []
        try:
            payload = json.loads(str(r["strategies_json"] or "[]"))
            if isinstance(payload, list):
                strategies = [str(x) for x in payload if str(x).strip()]
        except Exception:
            strategies = []
        out.append(
            {
                "symbol": str(r["symbol"]),
                "overlap_count": int(r["overlap_count"]),
                "days_seen": int(r["days_seen"]),
                "strategies": strategies,
            }
        )
    return out


def _close_on_or_before(conn: sqlite3.Connection, *, tenant_id: str, symbol: str, d: str) -> tuple[str, float] | None:
    r = conn.execute(
        """
        SELECT DATE(timestamp) as d, close
        FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d' AND DATE(timestamp) <= ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        (str(tenant_id), str(symbol), str(d)),
    ).fetchone()
    if r and r["close"] is not None:
        return str(r["d"]), float(r["close"])
    # Fallback: use feature_snapshot (discovery universe is much larger than price_bars)
    r = conn.execute(
        """
        SELECT as_of_date as d, close
        FROM feature_snapshot
        WHERE symbol = ? AND as_of_date <= ?
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (str(symbol), str(d)),
    ).fetchone()
    if not r or r["close"] is None:
        return None
    return str(r["d"]), float(r["close"])


def _nth_bar_after(conn: sqlite3.Connection, *, tenant_id: str, symbol: str, d: str, n: int) -> tuple[str, float] | None:
    """
    Return the close of the nth daily bar strictly after date d (1-indexed).
    """
    r = conn.execute(
        """
        SELECT DATE(timestamp) as d, close
        FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d' AND DATE(timestamp) > ?
        ORDER BY timestamp ASC
        LIMIT 1 OFFSET ?
        """,
        (str(tenant_id), str(symbol), str(d), int(n) - 1),
    ).fetchone()
    if r and r["close"] is not None:
        return str(r["d"]), float(r["close"])
    # Fallback: use feature_snapshot (discovery universe is much larger than price_bars)
    r = conn.execute(
        """
        SELECT as_of_date as d, close
        FROM feature_snapshot
        WHERE symbol = ? AND as_of_date > ?
        ORDER BY as_of_date ASC
        LIMIT 1 OFFSET ?
        """,
        (str(symbol), str(d), int(n) - 1),
    ).fetchone()
    if not r or r["close"] is None:
        return None
    return str(r["d"]), float(r["close"])


def compute_watchlist_outcomes(
    *,
    db_path: str | Path,
    tenant_id: str,
    watchlist_date: str | date,
    horizons: list[int] | None = None,
) -> list[OutcomeRow]:
    wl_date = _asof(watchlist_date)
    hs = horizons or [1, 5, 20]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    watch = _load_watchlist(conn, tenant_id=tenant_id, watchlist_date=wl_date)
    out: list[OutcomeRow] = []
    for w in watch:
        sym = str(w["symbol"])
        entry = _close_on_or_before(conn, tenant_id=tenant_id, symbol=sym, d=wl_date)
        if entry is None:
            continue
        entry_date, entry_close = entry
        for h in hs:
            exit_row = _nth_bar_after(conn, tenant_id=tenant_id, symbol=sym, d=entry_date, n=int(h))
            if exit_row is None:
                out.append(
                    OutcomeRow(
                        symbol=sym,
                        horizon_days=int(h),
                        entry_date=entry_date,
                        exit_date=None,
                        entry_close=float(entry_close),
                        exit_close=None,
                        return_pct=None,
                        overlap_count=int(w["overlap_count"]),
                        days_seen=int(w["days_seen"]),
                        strategies=list(w["strategies"]),
                    )
                )
                continue
            exit_date, exit_close = exit_row
            ret = (float(exit_close) / float(entry_close)) - 1.0 if entry_close else None
            out.append(
                OutcomeRow(
                    symbol=sym,
                    horizon_days=int(h),
                    entry_date=entry_date,
                    exit_date=exit_date,
                    entry_close=float(entry_close),
                    exit_close=float(exit_close),
                    return_pct=float(ret) if ret is not None else None,
                    overlap_count=int(w["overlap_count"]),
                    days_seen=int(w["days_seen"]),
                    strategies=list(w["strategies"]),
                )
            )
    return out


def outcomes_to_repo_rows(rows: list[OutcomeRow]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "symbol": r.symbol,
                "horizon_days": int(r.horizon_days),
                "entry_date": r.entry_date,
                "exit_date": r.exit_date,
                "entry_close": float(r.entry_close),
                "exit_close": r.exit_close,
                "return_pct": r.return_pct,
                "overlap_count": r.overlap_count,
                "days_seen": r.days_seen,
                "strategies_json": json.dumps(list(r.strategies), separators=(",", ":"), sort_keys=True),
            }
        )
    return out


def compute_candidate_outcomes(
    *,
    db_path: str | Path,
    tenant_id: str,
    as_of_date: str | date,
    horizons: list[int] | None = None,
    max_loss_pct: float | None = 0.15,
) -> list[dict[str, Any]]:
    """
    Compute forward returns for all discovery candidate rows on a given as_of_date.

    Returns repo-ready dicts for discovery_candidate_outcomes:
      {symbol, strategy_type, horizon_days, entry_date, exit_date, entry_close, exit_close, return_pct}
    """
    d0 = _asof(as_of_date)
    hs = horizons or [1, 5, 20]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT symbol, strategy_type
        FROM discovery_candidates
        WHERE tenant_id = ? AND as_of_date = ?
        """,
        (str(tenant_id), str(d0)),
    ).fetchall()
    if not rows:
        return []

    # Group by symbol to avoid recomputing entry/exit prices per strategy row.
    by_symbol: dict[str, list[str]] = {}
    for r in rows:
        sym = str(r["symbol"]).upper()
        st = str(r["strategy_type"])
        by_symbol.setdefault(sym, []).append(st)

    out: list[dict[str, Any]] = []
    for sym, strategy_types in by_symbol.items():
        entry = _close_on_or_before(conn, tenant_id=tenant_id, symbol=sym, d=d0)
        if entry is None:
            continue
        entry_date, entry_close = entry
        for h in hs:
            exit_row = _nth_bar_after(conn, tenant_id=tenant_id, symbol=sym, d=entry_date, n=int(h))
            if exit_row is None:
                for st in strategy_types:
                    out.append(
                        {
                            "symbol": sym,
                            "strategy_type": st,
                            "horizon_days": int(h),
                            "entry_date": entry_date,
                            "exit_date": None,
                            "entry_close": float(entry_close),
                            "exit_close": None,
                            "return_pct": None,
                        }
                    )
                continue
            exit_date, exit_close = exit_row
            ret = (float(exit_close) / float(entry_close)) - 1.0 if entry_close else None
            # Stop-loss: cap loss at max_loss_pct (e.g. -15%)
            # Simulates exiting when position drops that far, regardless of hold horizon.
            if ret is not None and max_loss_pct is not None and ret <= -max_loss_pct:
                exit_close = float(entry_close) * (1.0 - max_loss_pct)
                ret = -max_loss_pct
            for st in strategy_types:
                out.append(
                    {
                        "symbol": sym,
                        "strategy_type": st,
                        "horizon_days": int(h),
                        "entry_date": entry_date,
                        "exit_date": exit_date,
                        "entry_close": float(entry_close),
                        "exit_close": float(exit_close),
                        "return_pct": float(ret) if ret is not None else None,
                    }
                )
    return out
