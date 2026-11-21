"""Per-ticker daily-bar currency, relative to the market's latest bar.

A ticker is *current* when its latest valid 1d bar is within `max_lag_days` of the
latest bar of the reference set (the market). Measuring against the market rather
than the wall clock separates "this instrument stopped trading or is not ingested"
(delisted, acquired, halted) from "the pipeline has not run" — the latter is handled
by run-level degraded/stale suppression, never here.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import date

STALE_TICKER_LAG_DAYS = int(os.environ.get("PIPELINE_STALE_TICKER_LAG_DAYS", "7"))


def latest_daily_bar_dates(conn: sqlite3.Connection, *, tenant_id: str, tickers: list[str] | set[str]) -> dict[str, date]:
    """ticker (upper) -> date of its latest 1d bar with a positive close. Missing tickers are absent."""
    wanted = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    out: dict[str, date] = {}
    for i in range(0, len(wanted), 500):
        chunk = wanted[i : i + 500]
        marks = ",".join("?" * len(chunk))
        try:
            rows = conn.execute(
                f"""
                SELECT ticker, MAX(timestamp) AS ts
                FROM price_bars
                WHERE tenant_id = ? AND timeframe = '1d' AND ticker IN ({marks}) AND close > 0
                GROUP BY ticker
                """,
                (tenant_id, *chunk),
            ).fetchall()
        except sqlite3.OperationalError:
            return {}
        for row in rows:
            try:
                out[str(row[0]).strip().upper()] = date.fromisoformat(str(row[1])[:10])
            except (TypeError, ValueError):
                continue
    return out


def current_tickers(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    tickers: list[str] | set[str],
    reference: list[str] | set[str],
    max_lag_days: int = STALE_TICKER_LAG_DAYS,
) -> tuple[set[str], date | None]:
    """Return (tickers whose latest bar is within max_lag_days of the reference's latest bar, market date).

    With no reference bars at all, nothing can be judged current and the set is empty.
    """
    ref = latest_daily_bar_dates(conn, tenant_id=tenant_id, tickers=reference)
    if not ref:
        return set(), None
    market = max(ref.values())
    latest = latest_daily_bar_dates(conn, tenant_id=tenant_id, tickers=tickers)
    return {t for t, d in latest.items() if (market - d).days <= max_lag_days}, market
