"""Price-only daily ingestion for the live universe (Railway's job).

Independent of the full research pipeline and its 3 GB storage guard. Bounded on its
own terms: a small free-space preflight, the shared single-writer lock, and a write of
only a few thousand rows. Every run re-fetches the full window (split/dividend
adjusted) and upserts it, so provider re-adjustments of older bars are reconciled —
change detection alone misses dividend re-adjustments. Never deletes rows.

    python -m app.jobs.live_bars --once        # one run (e.g. via railway ssh)
    ALPHA_LIVE_BARS_ENABLED=true               # daily in-process schedule (after US close)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from app.jobs import pipeline_control
from app.research_publication.live_universe import live_universe

logger = logging.getLogger("live_bars")

TENANT = "default"
FETCH_PERIOD_DAYS = int(os.environ.get("ALPHA_LIVE_BARS_PERIOD_DAYS", "1100"))  # ~3 years: 2y window + warm-up
MIN_FREE_MB = int(os.environ.get("ALPHA_LIVE_BARS_MIN_FREE_MB", "64"))
UTC_HOUR = int(os.environ.get("ALPHA_LIVE_BARS_UTC_HOUR", "22"))
REL_EPS = 1e-9


def status_path(db_path: str) -> Path:
    return Path(db_path).resolve().parent / "live-bars-status.json"


def read_status(db_path: str) -> dict[str, Any] | None:
    try:
        return json.loads(status_path(db_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_status(db_path: str, record: dict[str, Any]) -> None:
    p = status_path(db_path)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


def fetch_yfinance(tickers: list[str], days: int) -> dict[str, list[tuple[str, float, float, float, float, float]]]:
    """Daily bars, split & dividend adjusted (auto_adjust=True), canonical midnight-UTC timestamps."""
    import yfinance as yf

    end = datetime.now(timezone.utc).date() + timedelta(days=1)
    raw = yf.download(" ".join(tickers), start=(end - timedelta(days=days)).isoformat(), end=end.isoformat(),
                      auto_adjust=True, progress=False, threads=True, group_by="ticker")
    out: dict[str, list[tuple[str, float, float, float, float, float]]] = {}
    for t in tickers:
        try:
            df = raw[t] if len(tickers) > 1 else raw
        except KeyError:
            continue
        rows = []
        for ts, r in df.iterrows():
            vals = [r.get(k) for k in ("Open", "High", "Low", "Close")]
            try:
                o, h, lo, c = (float(v) for v in vals)
            except (TypeError, ValueError):
                continue
            if not all(x == x and x > 0 for x in (o, h, lo, c)):
                continue
            vol = r.get("Volume")
            vol = float(vol) if vol == vol and vol is not None else 0.0
            rows.append((f"{ts.date().isoformat()}T00:00:00+00:00", o, h, lo, c, vol))
        if rows:
            out[t] = rows
    return out


def refresh(db_path: str, *, fetch: Callable[[list[str], int], dict[str, list[tuple]]] = fetch_yfinance, trigger: str = "manual") -> dict[str, Any]:
    tickers = live_universe()
    free_mb = int(shutil.disk_usage(Path(db_path).resolve().parent).free / (1024 * 1024))
    base = {"trigger": trigger, "tickers": tickers, "free_mb": free_mb, "min_free_mb": MIN_FREE_MB,
            "started_at": datetime.now(timezone.utc).isoformat()}
    if free_mb < MIN_FREE_MB:
        rec = {**base, "state": "skipped", "reason": "insufficient_storage"}
        _write_status(db_path, rec)
        return rec
    lock = pipeline_control.try_lock(db_path)
    if lock is None:
        rec = {**base, "state": "skipped", "reason": "writer_lock_held"}
        _write_status(db_path, rec)
        return rec
    try:
        fetched = fetch(tickers, FETCH_PERIOD_DAYS)
        missing = sorted(set(tickers) - set(fetched))
        conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
        try:
            inserted = corrected = unchanged = 0
            conn.execute("BEGIN IMMEDIATE")
            for t, rows in fetched.items():
                existing = {r[0]: r[1:] for r in conn.execute(
                    "SELECT timestamp, open, high, low, close, volume FROM price_bars WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d' AND timestamp >= ?",
                    (TENANT, t, rows[0][0]))}
                for row in rows:
                    old = existing.get(row[0])
                    if old is None:
                        inserted += 1
                    elif any(abs(float(a) - b) > REL_EPS * max(1.0, abs(b)) for a, b in zip(old, row[1:])):
                        corrected += 1
                    else:
                        unchanged += 1
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO price_bars (tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume) VALUES (?, ?, '1d', ?, ?, ?, ?, ?, ?)",
                        (TENANT, t, *row))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        market = max((rows[-1][0][:10] for rows in fetched.values()), default=None)
        rec = {**base, "state": "succeeded" if not missing else "partial", "missing": missing, "market_date": market,
               "inserted": inserted, "corrected": corrected, "unchanged": unchanged,
               "finished_at": datetime.now(timezone.utc).isoformat()}
    except Exception as exc:  # recorded, never silent
        rec = {**base, "state": "failed", "error": f"{type(exc).__name__}: {exc}"[:500], "finished_at": datetime.now(timezone.utc).isoformat()}
    finally:
        pipeline_control._release(lock)
    _write_status(db_path, rec)
    return rec


class LiveBarsScheduler:
    """Once per UTC day after UTC_HOUR. Idempotent via the status file."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @staticmethod
    def is_enabled() -> bool:
        return os.environ.get("ALPHA_LIVE_BARS_ENABLED", "").strip().lower() == "true"

    def _due(self, now: datetime) -> bool:
        s = read_status(self.db_path) or {}
        done_today = s.get("state") in ("succeeded", "partial") and str(s.get("finished_at", "")).startswith(now.date().isoformat())
        return now.hour >= UTC_HOUR and not done_today

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                if self._due(datetime.now(timezone.utc)):
                    logger.info("live_bars refresh: %s", refresh(self.db_path, trigger="scheduler"))
            except Exception as exc:
                logger.error("live_bars scheduler error: %s", exc)
            self._stop.wait(300)

    def start(self) -> None:
        if self._thread is None or not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True, name="live_bars_scheduler")
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true", required=True)
    ap.add_argument("--db", default=os.environ.get("ALPHA_DB_PATH", "data/alpha.db"))
    rec = refresh(ap.parse_args().db)
    print(json.dumps(rec, indent=2))
    return 0 if rec.get("state") in ("succeeded", "partial") else 1


if __name__ == "__main__":
    raise SystemExit(main())
