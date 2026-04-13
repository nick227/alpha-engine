"""
Daily setup scanner for the three-condition mean-reversion strategy.

Entry conditions (all must be true, using prior-day values only):
  1. VIX > VIX3M       — fear regime active
  2. candle_body <= p33 — bear candle, bottom third of fear distribution
  3. volume_zscore_20 < 1.0 — NOT a volume spike (counterintuitive: spike = continuation)

Universe: Q4/Q5 by avg dollar-volume (large + mega cap only).

candle_body is clipped to [-1, 1] before thresholding.
p33 threshold is supplied externally (from most recent validation run) or computed
from a rolling 252-day fear-day sample if not supplied.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

_ROOT = Path(__file__).resolve().parent.parent.parent
DB = str(_ROOT / "data" / "alpha.db")

# candle_body p33 threshold for the bear-candle condition.
# Source: clean yfinance 2018-2026 validation (4,778 tickers, 8.7M obs, 141k setups).
# The full-history CSV data has systemic OHLC errors (close/open outside high-low range)
# that corrupt the p33 calculation — that run's cutoff of -1.000 hit the clip boundary
# and is not usable as an operational threshold. Use the clean-data value instead.
# 2010s IC with this threshold: -0.028 (t=-24.0, n=746k); setup dn%=43.2%.
# Update this by re-running the yfinance-only subset validation after each data refresh.
DEFAULT_P33 = -0.80   # from clean 2018-2026 yfinance validation, 2026-04-12

# Volume z-score ceiling — observations above this are excluded (continuation risk)
VOL_ZSCORE_CAP = 1.0

# Minimum avg daily dollar-volume to be considered Q4/Q5
# (computed dynamically; this is just a fallback label)
_Q4Q5_LABELS = {"Q4 large", "Q5 mega"}

# Module-level caches — avoids re-running expensive queries on every scan() call.
# Qualifying tickers: stable (only changes with new data ingestion).
# Size map: recomputed at most once per SMAP_TTL_DAYS.
# Invalidate both by calling clear_caches().
_qual_cache: dict[str, list[str]] = {}
_smap_cache: dict[str, tuple[str, dict[str, str]]] = {}  # db -> (date_computed, size_map)
SMAP_TTL_DAYS = 30   # recompute size quintiles if last computation > 30 days ago


def clear_caches() -> None:
    """Clear all module-level caches (e.g., after a data refresh)."""
    _qual_cache.clear()
    _smap_cache.clear()


# Keep old name for backwards compatibility
def clear_qual_cache() -> None:
    clear_caches()


@dataclass
class Setup:
    ticker: str
    date: str           # YYYY-MM-DD of the signal candle
    candle_body: float
    volume_zscore: float
    vix_term: float
    close: float        # closing price of the signal candle (entry reference)
    size_quintile: str


def _get_vix_term(conn: sqlite3.Connection, date: str) -> Optional[float]:
    """Return VIX - VIX3M for a specific date. Positive = fear.
    Uses timestamp range to hit the (tenant_id, ticker, timeframe, timestamp) index.
    """
    row = conn.execute(
        "SELECT close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d' "
        "AND timestamp >= ? AND timestamp < date(?, '+1 day') "
        "ORDER BY timestamp DESC LIMIT 1",
        (date, date),
    ).fetchone()
    row3m = conn.execute(
        "SELECT close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX3M' AND timeframe='1d' "
        "AND timestamp >= ? AND timestamp < date(?, '+1 day') "
        "ORDER BY timestamp DESC LIMIT 1",
        (date, date),
    ).fetchone()
    if row and row3m:
        return float(row[0]) - float(row3m[0])
    return None


def _build_size_map(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, str]:
    """
    Compute avg dollar-volume (last 252 bars) for each ticker, then assign
    quintile labels Q1..Q5. Returns {ticker: quintile_label}.
    """
    ph = ",".join("?" * len(tickers))
    rows = conn.execute(f"""
        SELECT ticker, AVG(close * volume) as avg_dv
        FROM (
            SELECT ticker, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
            FROM price_bars
            WHERE tenant_id='ml_train' AND timeframe='1d' AND ticker IN ({ph})
        ) WHERE rn <= 252
        GROUP BY ticker
    """, tickers).fetchall()

    size_map = {r[0]: float(r[1]) for r in rows if r[1] and r[1] > 0}
    vals = np.array(list(size_map.values()))
    if len(vals) == 0:
        return {}
    pcts = np.percentile(vals, [20, 40, 60, 80])

    result: dict[str, str] = {}
    for ticker, dv in size_map.items():
        if dv < pcts[0]:
            result[ticker] = "Q1 micro"
        elif dv < pcts[1]:
            result[ticker] = "Q2 small"
        elif dv < pcts[2]:
            result[ticker] = "Q3 mid"
        elif dv < pcts[3]:
            result[ticker] = "Q4 large"
        else:
            result[ticker] = "Q5 mega"
    return result


def _compute_rolling_p33(
    conn: sqlite3.Connection,
    tickers_q45: list[str],
    as_of_date: str,
    lookback_days: int = 504,
) -> float:
    """
    Compute the p33 of clipped candle_body on fear days over the last
    `lookback_days` calendar days ending at `as_of_date`.
    Used when DEFAULT_P33 override is not supplied.
    """
    ph = ",".join("?" * len(tickers_q45))
    rows = conn.execute(f"""
        SELECT DATE(timestamp) as dt, open, high, low, close
        FROM price_bars
        WHERE tenant_id='ml_train' AND timeframe='1d'
          AND ticker IN ({ph})
          AND DATE(timestamp) BETWEEN DATE(?, '-{lookback_days} days') AND DATE(?)
        ORDER BY dt
    """, tickers_q45 + [as_of_date, as_of_date]).fetchall()

    vix_rows = conn.execute(
        "SELECT DATE(timestamp), close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d' "
        "AND DATE(timestamp) BETWEEN DATE(?, '-? days') AND DATE(?)",
    ).fetchall()
    # simplified: pull all vix/vix3m and compute term
    vix_d = {r[0]: float(r[1]) for r in conn.execute(
        "SELECT DATE(timestamp), close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX' AND timeframe='1d'"
    ).fetchall()}
    vix3m_d = {r[0]: float(r[1]) for r in conn.execute(
        "SELECT DATE(timestamp), close FROM price_bars "
        "WHERE tenant_id='default' AND ticker='^VIX3M' AND timeframe='1d'"
    ).fetchall()}

    fear_bodies: list[float] = []
    for dt, op, hi, lo, cl in rows:
        vt = vix_d.get(dt, np.nan) - vix3m_d.get(dt, np.nan)
        if not np.isfinite(vt) or vt <= 0:
            continue
        rng = float(hi) - float(lo)
        if rng <= 0:
            continue
        body = float(np.clip((float(cl) - float(op)) / rng, -1.0, 1.0))
        fear_bodies.append(body)

    if len(fear_bodies) < 100:
        return DEFAULT_P33
    return float(np.percentile(fear_bodies, 33))


def scan(
    date: str,
    p33_threshold: Optional[float] = None,
    vol_zscore_cap: float = VOL_ZSCORE_CAP,
    db: str = DB,
    compute_p33_if_missing: bool = False,
) -> list[Setup]:
    """
    Scan for setups on `date` (the signal candle date — entry is next open).

    Parameters
    ----------
    date : str
        YYYY-MM-DD of the candle to evaluate.
    p33_threshold : float, optional
        candle_body <= this value triggers the bear-candle condition.
        Defaults to DEFAULT_P33 if not supplied.
    vol_zscore_cap : float
        volume_zscore must be BELOW this to qualify (default 1.0).
    db : str
        Path to SQLite database.
    compute_p33_if_missing : bool
        If True, compute p33 dynamically from a rolling 504-day window.
        Slower but adaptive. Useful for paper trading warm-up.

    Returns
    -------
    List of Setup objects sorted by candle_body ascending (most bearish first).
    """
    threshold = p33_threshold if p33_threshold is not None else DEFAULT_P33

    conn = sqlite3.connect(db)

    # ── 1. Check regime ──────────────────────────────────────────────
    vix_term = _get_vix_term(conn, date)
    if vix_term is None or vix_term <= 0:
        conn.close()
        return []  # not a fear day

    # ── 2. Universe: tickers with enough history ─────────────────────
    # Qualifying tickers (COUNT >= 1250) is stable across a backfill run —
    # cache it at module level to avoid the expensive GROUP BY on 25M rows.
    if db not in _qual_cache:
        qual_rows = conn.execute("""
            SELECT ticker FROM price_bars
            WHERE tenant_id='ml_train' AND timeframe='1d'
            GROUP BY ticker
            HAVING COUNT(*) >= 1250
        """).fetchall()
        _qual_cache[db] = [r[0] for r in qual_rows]
    tickers = _qual_cache[db]

    if not tickers:
        conn.close()
        return []

    # ── 3. Assign size quintiles (cached, recomputed every SMAP_TTL_DAYS) ─
    from datetime import datetime, timedelta
    cached = _smap_cache.get(db)
    if cached is not None:
        last_date_str, cached_smap = cached
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
        signal_date = datetime.strptime(date, "%Y-%m-%d")
        stale = (signal_date - last_date).days > SMAP_TTL_DAYS
    else:
        stale = True

    if stale:
        size_map_raw = _build_size_map(conn, tickers)
        _smap_cache[db] = (date, size_map_raw)
    else:
        size_map_raw = _smap_cache[db][1]

    q45_tickers = [t for t, q in size_map_raw.items() if q in _Q4Q5_LABELS]

    if not q45_tickers:
        conn.close()
        return []

    # ── 4. Optional: compute p33 dynamically ─────────────────────────
    if compute_p33_if_missing and p33_threshold is None:
        threshold = _compute_rolling_p33(conn, q45_tickers, date)

    # ── 5. Pull last 30 bars per ticker ending on signal date ────────
    # 30-bar window gives the 22 bars needed for signals plus a buffer for
    # weekends/holidays. Bounding from below avoids loading full history.
    ph = ",".join("?" * len(q45_tickers))
    rows = conn.execute(f"""
        SELECT ticker, DATE(timestamp) as dt, open, high, low, close, volume
        FROM (
            SELECT ticker, timestamp, open, high, low, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
            FROM price_bars
            WHERE tenant_id='ml_train' AND timeframe='1d'
              AND ticker IN ({ph})
              AND timestamp <= ?
        )
        WHERE rn <= 30
        ORDER BY ticker, dt
    """, q45_tickers + [date]).fetchall()
    conn.close()

    # ── 6. Compute signals per ticker ────────────────────────────────
    from collections import defaultdict
    ticker_rows: dict[str, list] = defaultdict(list)
    for row in rows:
        ticker_rows[row[0]].append(row)

    setups: list[Setup] = []
    for ticker, trws in ticker_rows.items():
        # need at least 21 bars for volume z-score window
        if len(trws) < 22:
            continue

        # get the signal candle (last row that is <= date)
        # ensure we are exactly on `date`
        last = trws[-1]
        if last[1] != date:
            continue  # no bar for this ticker on signal date

        op, hi, lo, cl, vl = (float(last[2]), float(last[3]),
                               float(last[4]), float(last[5]), float(last[6]))

        # candle body (clipped to [-1, 1])
        # Reject candles where open or close sits outside the high-low range —
        # this indicates a data integrity issue (unadjusted split, bad bar).
        # Legitimate candles with body == -1.0 exactly (close == low) are kept;
        # what we're catching here is when the raw ratio was below -1.0 before clip.
        rng = hi - lo
        if rng <= 0:
            continue
        raw_body = (cl - op) / rng
        if raw_body < -1.0 or raw_body > 1.0:
            continue   # OHLC integrity violation — skip
        body = float(raw_body)

        # volume z-score (20-bar window, using the 21 bars before signal candle)
        prior_vols = np.array([float(r[6]) for r in trws[-22:-1]], dtype=float)
        if len(prior_vols) < 20:
            continue
        vstd = float(np.std(prior_vols[-20:]))
        if vstd <= 0:
            continue
        vz = (vl - float(np.mean(prior_vols[-20:]))) / vstd

        # apply three conditions
        if body > threshold:
            continue   # not a bear candle
        if vz >= vol_zscore_cap:
            continue   # volume spike — skip (continuation risk)

        setups.append(Setup(
            ticker=ticker,
            date=date,
            candle_body=body,
            volume_zscore=vz,
            vix_term=vix_term,
            close=cl,
            size_quintile=size_map_raw[ticker],
        ))

    setups.sort(key=lambda s: s.candle_body)   # most bearish first
    return setups
