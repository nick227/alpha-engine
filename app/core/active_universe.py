"""
Active trading / ranking universe: static watchlist (YAML) ∪ admitted candidates (DB).

Discovery is wide and exploratory; it writes candidate_queue (tags, multiplier_score).
Daily ranking uses only this active universe — not raw discovery scores.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path

from app.core.bar_freshness import current_tickers, latest_daily_bar_dates
from app.core.target_stocks import get_target_stocks
from app.db.repository import AlphaRepository

# Optional rejection labels for debugging (stored as plain text in candidate_queue).
REJECTION_REASONS = ("low_signal", "insufficient_history", "poor_performance", "data_gap")

CANDIDATE_QUEUE_STATUSES = ("seen", "recurring", "shortlisted", "admitted", "rejected")


def _admitted_from_sqlite(conn: sqlite3.Connection, *, tenant_id: str) -> list[str]:
    try:
        rows = conn.execute(
            """
            SELECT ticker FROM candidate_queue
            WHERE tenant_id = ? AND status = ?
            ORDER BY ticker ASC
            """,
            (tenant_id, "admitted"),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    out: list[str] = []
    for r in rows:
        t = str(r[0]).strip().upper()
        if t:
            out.append(t)
    return sorted(set(out))


def get_consumer_universe_tickers() -> list[str]:
    """Load canonical consumer universe symbols from config/consumer_universe_150.yaml."""
    import yaml
    p = Path(__file__).resolve().parent.parent.parent / "config" / "consumer_universe_150.yaml"
    if not p.exists():
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        symbols: list[str] = []
        for v in data.values():
            if isinstance(v, list):
                for s in v:
                    clean = str(s).strip().upper().replace(".", "-")
                    if clean and clean not in symbols:
                        symbols.append(clean)
        return symbols
    except Exception:
        return []


def get_active_universe_tickers(
    *,
    tenant_id: str = "default",
    asof: date | datetime | None = None,
    repository: AlphaRepository | None = None,
    sqlite_conn: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[str]:
    """
    static_watchlist (config/target_stocks.yaml) ∪ candidate_queue where status='admitted' ∪ consumer_universe_150.yaml.
    """
    static = set(get_target_stocks(asof=asof))
    consumer = set(get_consumer_universe_tickers())
    if repository is not None:
        conn = repository.conn
        admitted = set(repository.list_admitted_candidate_tickers(tenant_id))
    elif sqlite_conn is not None:
        conn = sqlite_conn
        admitted = set(_admitted_from_sqlite(sqlite_conn, tenant_id=tenant_id))
    else:
        repo = AlphaRepository(db_path=db_path or "data/alpha.db")
        conn = repo.conn
        admitted = set(repo.list_admitted_candidate_tickers(tenant_id))
    if asof is None and admitted:
        admitted -= stopped_admitted_tickers(conn, tenant_id=tenant_id, admitted=admitted, reference=static | consumer)
    return sorted(static | admitted | consumer)


def stopped_admitted_tickers(
    conn: sqlite3.Connection, *, tenant_id: str, admitted: set[str], reference: set[str]
) -> set[str]:
    """Admitted (dynamic) tickers whose bars stopped while the market's continued
    (delisted, acquired, halted). Never-ingested admissions are kept so they get ingested;
    static and consumer lists are curated and never dropped here. Current universe only:
    historical backfills (asof) keep every admission."""
    latest = latest_daily_bar_dates(conn, tenant_id=tenant_id, tickers=admitted)
    current, market = current_tickers(conn, tenant_id=tenant_id, tickers=set(latest), reference=reference)
    if market is None:
        return set()
    return set(latest) - current
