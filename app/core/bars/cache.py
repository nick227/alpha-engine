from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as pd

from app.core.repository import Repository
from app.core.time_utils import normalize_timestamp, to_utc_datetime
from app.core.bars.providers import HistoricalBarsProvider, OHLCVBar


@dataclass(slots=True)
class BarsRange:
    start: datetime
    end: datetime


class BarsCache:
    """
    SQLite-backed OHLCV cache using Repository.price_bars.
    """

    def __init__(
        self,
        *,
        db_path: str,
        provider: HistoricalBarsProvider,
        tenant_id: str = "default",
        min_rows_expected_per_day: int = 50,
    ) -> None:
        self.db_path = str(db_path)
        self.provider = provider
        self.tenant_id = str(tenant_id)
        self.min_rows_expected_per_day = int(min_rows_expected_per_day)

    def _repo(self) -> Repository:
        return Repository(db_path=self.db_path)

    def fetch_bars_df(self, *, timeframe: str, tickers: Iterable[str], start: datetime, end: datetime) -> pd.DataFrame:
        start_s = normalize_timestamp(start)
        end_s = normalize_timestamp(end)
        tickers_l = [str(t) for t in tickers if t]
        if not tickers_l:
            return pd.DataFrame()

        tf = str(timeframe).strip().lower()
        repo = self._repo()
        try:
            placeholders = ",".join(["?"] * len(tickers_l))
            sql = f"""
            SELECT tenant_id, ticker, timeframe, timestamp, open, high, low, close, volume
            FROM price_bars
            WHERE tenant_id = ?
              AND timeframe = ?
              AND ticker IN ({placeholders})
              AND timestamp >= ?
              AND timestamp < ?
            ORDER BY ticker ASC, timestamp ASC
            """
            params = tuple([self.tenant_id, tf] + tickers_l + [start_s, end_s])
            return repo.query_df(sql, params)
        finally:
            repo.close()

    def _expected_rows_per_day(self, *, timeframe: str) -> int:
        tf = str(timeframe).strip().lower()
        if tf == "1m":
            # Be forgiving on missing market hours and sparse providers.
            return max(1, int(self.min_rows_expected_per_day))
        if tf == "1h":
            return 4
        if tf == "1d":
            return 1
        return 1

    def _has_coverage(self, *, repo: Repository, ticker: str, timeframe: str, start: datetime, end: datetime) -> bool:
        start_s = normalize_timestamp(start)
        end_s = normalize_timestamp(end)
        tf = str(timeframe).strip().lower()
        row = repo.conn.execute(
            """
            SELECT COUNT(1) AS n
            FROM price_bars
            WHERE tenant_id = ?
              AND ticker = ?
              AND timeframe = ?
              AND timestamp >= ?
              AND timestamp < ?
            """,
            (self.tenant_id, str(ticker), tf, start_s, end_s),
        ).fetchone()
        n = int(row["n"]) if row and row["n"] is not None else 0
        expected_per_day = self._expected_rows_per_day(timeframe=tf)
        expected = max(
            1,
            int(((to_utc_datetime(end) - to_utc_datetime(start)).total_seconds() / 86400.0) * expected_per_day),
        )
        return n >= expected

    def ensure_bars(self, *, ticker: str, timeframe: str, start: datetime, end: datetime) -> int:
        """
        Ensure the cache has bars for [start,end). Returns number of bars inserted/updated.
        """
        start_utc = to_utc_datetime(start)
        end_utc = to_utc_datetime(end)
        if start_utc >= end_utc:
            return 0

        tf = str(timeframe).strip().lower()
        repo = self._repo()
        try:
            if self._has_coverage(repo=repo, ticker=ticker, timeframe=tf, start=start_utc, end=end_utc):
                return 0

            bars: list[OHLCVBar] = self.provider.fetch_bars(timeframe=tf, ticker=str(ticker), start=start_utc, end=end_utc)
            if not bars:
                return 0

            rows = []
            for b in bars:
                rows.append(
                    (
                        self.tenant_id,
                        str(ticker),
                        tf,
                        normalize_timestamp(b.timestamp),
                        float(b.open),
                        float(b.high),
                        float(b.low),
                        float(b.close),
                        float(b.volume),
                    )
                )

            with repo.transaction():
                repo.persist_price_bars(rows)
            return len(rows)
        finally:
            repo.close()

    def ensure_many(self, *, tickers: Iterable[str], timeframe: str, start: datetime, end: datetime) -> dict[str, int]:
        inserted: dict[str, int] = {}
        for t in sorted({str(x) for x in tickers if x}):
            inserted[t] = self.ensure_bars(ticker=t, timeframe=timeframe, start=start, end=end)
        return inserted

    def ensure_policy(
        self,
        *,
        tickers: Iterable[str],
        start: datetime,
        end: datetime,
        now: datetime | None = None,
    ) -> dict[str, dict[str, int]]:
        """
        Ensure multi-timeframe coverage for the requested window using policy:
          - 1m for last 5d
          - 1h for last 90d
          - 1d for older-than-90d remainder
        """
        now_utc = to_utc_datetime(now or datetime.now(timezone.utc)).replace(microsecond=0)
        start_utc = to_utc_datetime(start).replace(microsecond=0)
        end_utc = to_utc_datetime(end).replace(microsecond=0)
        if start_utc >= end_utc:
            return {}

        d5 = now_utc - timedelta(days=5)
        d90 = now_utc - timedelta(days=90)

        out: dict[str, dict[str, int]] = {}

        # 1d: [start, min(end, d90))
        older_end = min(end_utc, d90)
        if start_utc < older_end:
            out["1d"] = self.ensure_many(tickers=tickers, timeframe="1d", start=start_utc, end=older_end)

        # 1h: [max(start, d90), end)
        h_start = max(start_utc, d90)
        if h_start < end_utc:
            out["1h"] = self.ensure_many(tickers=tickers, timeframe="1h", start=h_start, end=end_utc)

        # 1m: [max(start, d5), end)
        m_start = max(start_utc, d5)
        if m_start < end_utc:
            out["1m"] = self.ensure_many(tickers=tickers, timeframe="1m", start=m_start, end=end_utc)

        return out

    def coverage_pct(
        self,
        *,
        tickers: Iterable[str],
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> float:
        tickers_l = [str(t) for t in tickers if t]
        if not tickers_l:
            return 0.0
        tf = str(timeframe).strip().lower()
        repo = self._repo()
        try:
            covered = 0
            for t in tickers_l:
                if self._has_coverage(repo=repo, ticker=t, timeframe=tf, start=start, end=end):
                    covered += 1
            return covered / len(tickers_l) * 100.0
        finally:
            repo.close()


def bar_window_for_events(
    *,
    event_times: Iterable[datetime],
    lookback: timedelta = timedelta(days=2),
    lookahead: timedelta = timedelta(days=2),
) -> BarsRange:
    times = [to_utc_datetime(t) for t in event_times]
    if not times:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        return BarsRange(start=now - timedelta(days=1), end=now)
    start = min(times) - lookback
    end = max(times) + lookahead
    return BarsRange(start=start.replace(microsecond=0), end=end.replace(microsecond=0))
