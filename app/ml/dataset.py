"""
Training dataset builder.

For each (symbol, date, horizon) in a date range, computes a point-in-time
feature vector and the corresponding forward excess return vs SPY.
Rows are written to ml_learning_rows (idempotent — skips existing rows).
"""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
from uuid import uuid4

from app.ml.feature_builder import FeatureBuilder


# Horizon string → calendar days (used for future-return calculation)
HORIZON_DAYS: dict[str, float] = {
    "1h":  1 / 24,
    "4h":  4 / 24,
    "1d":  1.0,
    "7d":  7.0,
    "30d": 30.0,
}

# Tolerance window when searching for an exit-price bar near the target date
_EXIT_TOLERANCE_DAYS = 5


def build_dataset(
    symbols: list[str],
    date_range: tuple[date, date],
    horizons: list[str],
    db_path: str | Path = "data/alpha.db",
    dumps_root: str | Path = "data/raw_dumps",
    min_feature_coverage: float = 0.8,
    tenant_id: str = "default",
    split: str = "train",
    factors_path: str = "config/factors.yaml",
) -> int:
    """
    Build and persist training rows for every (symbol, date, horizon) combination.

    Already-existing rows (same timestamp + symbol + horizon + tenant_id) are
    skipped so repeated calls are safe.

    Returns the number of new rows inserted.
    """
    db_path = Path(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    fb = FeatureBuilder(
        db_path=db_path,
        dumps_root=dumps_root,
        tenant_id=tenant_id,
        factors_path=factors_path,
    )

    start, end = date_range

    # Bulk-load price bars for all needed symbols into memory.
    # This replaces O(n_dates × n_factors) DB queries with one fetch per symbol.
    all_price_symbols: list[str] = list(symbols)
    for spec in fb.config.factors:
        if spec.source in ("price", "price_relative") and spec.symbol:
            sym = spec.symbol
            if "{ticker}" not in sym:
                all_price_symbols.append(sym)
        if spec.source == "price_relative" and spec.benchmark:
            all_price_symbols.append(spec.benchmark)
    fb.prefetch_bars(list(set(all_price_symbols)), start, end)

    # Also prefetch the symbol variants with {ticker} substituted for each ticker
    for ticker in symbols:
        for spec in fb.config.factors:
            if spec.source in ("price", "price_relative") and spec.symbol:
                resolved = spec.resolve_symbol(ticker)
                if resolved and resolved not in fb._bars_cache:
                    fb.prefetch_bars([resolved], start, end)

    inserted = 0
    current = start

    try:
        while current <= end:
            for symbol in symbols:
                for horizon in horizons:
                    h_days = HORIZON_DAYS.get(horizon)
                    if h_days is None:
                        continue

                    # Idempotency check
                    exists = conn.execute(
                        """
                        SELECT 1 FROM ml_learning_rows
                        WHERE tenant_id = ? AND timestamp = ? AND symbol = ? AND horizon = ?
                        """,
                        (tenant_id, current.isoformat(), symbol, horizon),
                    ).fetchone()
                    if exists:
                        continue

                    # Build point-in-time feature vector (pass horizon string for set lookup)
                    features, coverage = fb.build(symbol, current, horizon)
                    if coverage < min_feature_coverage:
                        continue

                    # Forward excess return (strictly after current date — training only)
                    future_ret = _compute_excess_return(conn, symbol, current, h_days, tenant_id)

                    conn.execute(
                        """
                        INSERT OR IGNORE INTO ml_learning_rows
                            (id, tenant_id, timestamp, symbol, horizon,
                             features_json, future_return, coverage_ratio, split)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid4()),
                            tenant_id,
                            current.isoformat(),
                            symbol,
                            horizon,
                            json.dumps(features),
                            future_ret,
                            round(coverage, 4),
                            split,
                        ),
                    )
                    inserted += 1

            current += timedelta(days=1)

        conn.commit()
    finally:
        conn.close()
        fb.close()

    return inserted


def _compute_excess_return(
    conn: sqlite3.Connection,
    symbol: str,
    entry_date: date,
    horizon_days: float,
    tenant_id: str,
) -> Optional[float]:
    """
    Compute log excess return of symbol vs SPY over the horizon.

    excess = log(stock_exit / stock_entry) - log(spy_exit / spy_entry)

    Returns None if any price is missing or non-positive.
    This value is forward-looking and must only be computed for training rows,
    never at inference time.
    """
    h_calendar = math.ceil(horizon_days)
    exit_target = entry_date + timedelta(days=h_calendar)

    stock_entry = _close_on_or_before(conn, symbol, entry_date, tenant_id)
    stock_exit = _close_on_or_after(conn, symbol, exit_target, tenant_id)
    spy_entry = _close_on_or_before(conn, "SPY", entry_date, tenant_id)
    spy_exit = _close_on_or_after(conn, "SPY", exit_target, tenant_id)

    prices = [stock_entry, stock_exit, spy_entry, spy_exit]
    if any(p is None or p <= 0 for p in prices):
        return None

    stock_ret = math.log(stock_exit / stock_entry)   # type: ignore[arg-type]
    spy_ret = math.log(spy_exit / spy_entry)          # type: ignore[arg-type]
    return round(stock_ret - spy_ret, 6)


def _close_on_or_before(
    conn: sqlite3.Connection,
    ticker: str,
    target: date,
    tenant_id: str,
) -> Optional[float]:
    row = conn.execute(
        """
        SELECT close FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
          AND DATE(timestamp) <= ?
        ORDER BY timestamp DESC LIMIT 1
        """,
        (tenant_id, ticker, target.isoformat()),
    ).fetchone()
    return float(row["close"]) if row else None


def _close_on_or_after(
    conn: sqlite3.Connection,
    ticker: str,
    target: date,
    tenant_id: str,
) -> Optional[float]:
    """Fetch close on or just after target (within tolerance)."""
    ceiling = target + timedelta(days=_EXIT_TOLERANCE_DAYS)
    row = conn.execute(
        """
        SELECT close FROM price_bars
        WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
          AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
        ORDER BY timestamp ASC LIMIT 1
        """,
        (tenant_id, ticker, target.isoformat(), ceiling.isoformat()),
    ).fetchone()
    return float(row["close"]) if row else None
