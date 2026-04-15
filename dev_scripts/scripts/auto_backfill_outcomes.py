"""
Auto Backfill Outcomes

Fills return_pct for any discovery_candidate_outcomes and prediction_outcomes
rows that were NULL because forward prices weren't available yet when they
were first written. Run this daily after download_prices_daily.py.

Exit codes:
  0 — success (or nothing to do)
  1 — fatal error
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "alpha.db"
TENANT_ID = "default"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def backfill_candidate_outcomes(conn: sqlite3.Connection) -> int:
    """
    Fill return_pct for discovery_candidate_outcomes rows where exit_close IS NULL
    but sufficient forward price data now exists.
    Returns number of rows filled.
    """
    pending = conn.execute("""
        SELECT o.rowid, o.symbol, o.strategy_type, o.horizon_days,
               o.entry_date, o.entry_close
        FROM discovery_candidate_outcomes o
        WHERE o.tenant_id = ?
          AND o.return_pct IS NULL
          AND o.entry_close IS NOT NULL
          AND o.entry_close > 0
        ORDER BY o.entry_date ASC
    """, (TENANT_ID,)).fetchall()

    if not pending:
        return 0

    filled = 0
    for row in pending:
        symbol = str(row["symbol"])
        horizon = int(row["horizon_days"])
        entry_date = str(row["entry_date"])
        entry_close = float(row["entry_close"])

        # First daily bar at least horizon calendar days after entry
        exit_row = conn.execute("""
            SELECT DATE(timestamp) AS d, close
            FROM price_bars
            WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
              AND DATE(timestamp) > ?
            ORDER BY timestamp ASC
            LIMIT 1 OFFSET ?
        """, (TENANT_ID, symbol, entry_date, horizon - 1)).fetchone()

        if not exit_row:
            # Fallback: feature_snapshot
            exit_row = conn.execute("""
                SELECT as_of_date AS d, close
                FROM feature_snapshot
                WHERE symbol = ? AND as_of_date > ?
                ORDER BY as_of_date ASC
                LIMIT 1 OFFSET ?
            """, (symbol, entry_date, horizon - 1)).fetchone()

        if not exit_row or exit_row["close"] is None:
            continue

        exit_close = float(exit_row["close"])
        exit_date = str(exit_row["d"])
        return_pct = (exit_close / entry_close) - 1.0

        conn.execute("""
            UPDATE discovery_candidate_outcomes
            SET exit_date  = ?,
                exit_close = ?,
                return_pct = ?
            WHERE rowid = ?
        """, (exit_date, exit_close, return_pct, row["rowid"]))
        filled += 1

    if filled:
        conn.commit()
    return filled


def backfill_prediction_outcomes(conn: sqlite3.Connection) -> int:
    """
    Score prediction_outcomes for discovery predictions whose horizon has
    now elapsed but weren't scored yet (e.g. price data arrived after last replay).
    Returns number of rows written.
    """
    today = date.today()
    cutoff_dt = datetime.combine(today, datetime.min.time()).replace(
        hour=23, minute=59, tzinfo=timezone.utc
    )

    unscored = conn.execute("""
        SELECT p.id, p.ticker, p.prediction, p.entry_price, p.horizon, p.timestamp
        FROM predictions p
        LEFT JOIN prediction_outcomes po
            ON po.prediction_id = p.id AND po.tenant_id = p.tenant_id
        WHERE p.tenant_id = ?
          AND p.mode = 'discovery'
          AND po.id IS NULL
    """, (TENANT_ID,)).fetchall()

    if not unscored:
        return 0

    scored = 0
    for row in unscored:
        horizon_str = str(row["horizon"]).strip().lower()
        try:
            horizon_days = int(horizon_str.rstrip("d"))
        except (ValueError, AttributeError):
            horizon_days = 5

        try:
            created_at = datetime.fromisoformat(str(row["timestamp"]))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        expiry_dt = created_at + __import__("datetime").timedelta(days=horizon_days)
        if expiry_dt > cutoff_dt:
            continue

        expiry_date = expiry_dt.date()

        exit_row = conn.execute("""
            SELECT close FROM price_bars
            WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
              AND DATE(timestamp) >= ?
            ORDER BY timestamp ASC LIMIT 1
        """, (TENANT_ID, str(row["ticker"]), expiry_date.isoformat())).fetchone()

        if not exit_row:
            exit_row = conn.execute("""
                SELECT close FROM feature_snapshot
                WHERE symbol = ? AND as_of_date >= ?
                ORDER BY as_of_date ASC LIMIT 1
            """, (str(row["ticker"]), expiry_date.isoformat())).fetchone()

        if not exit_row:
            continue

        exit_price = float(exit_row["close"])
        entry_price = float(row["entry_price"])
        if entry_price <= 0:
            continue

        return_pct = (exit_price / entry_price) - 1.0
        direction = str(row["prediction"])
        direction_correct = (return_pct > 0 and direction == "BUY") or \
                            (return_pct < 0 and direction == "SELL")

        outcome_id = str(uuid.uuid4())
        conn.execute("""
            INSERT OR REPLACE INTO prediction_outcomes
              (id, tenant_id, prediction_id, exit_price, return_pct, direction_correct,
               max_runup, max_drawdown, evaluated_at, exit_reason, residual_alpha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            outcome_id, TENANT_ID, str(row["id"]),
            exit_price, return_pct, 1 if direction_correct else 0,
            max(return_pct, 0.0), min(return_pct, 0.0),
            _now_iso(), "horizon", return_pct,
        ))
        scored += 1

    if scored:
        conn.commit()
    return scored


def main() -> int:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")

    try:
        c1 = backfill_candidate_outcomes(conn)
        print(f"discovery_candidate_outcomes filled: {c1}")

        c2 = backfill_prediction_outcomes(conn)
        print(f"prediction_outcomes scored: {c2}")

        print(f"Backfill complete  (candidate_outcomes={c1}, prediction_outcomes={c2})")
        return 0
    except Exception as e:
        print(f"[ERROR] auto_backfill_outcomes failed: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
