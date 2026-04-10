from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.time_utils import normalize_timestamp, to_utc_datetime


@dataclass(frozen=True, slots=True)
class OHLCVBar:
    timestamp: str  # UTC ISO
    open: float
    high: float
    low: float
    close: float
    volume: float


class HistoricalBarsProvider(Protocol):
    name: str

    def fetch_bars(
        self,
        *,
        timeframe: str,
        ticker: str,
        start: datetime,
        end: datetime,
    ) -> list[OHLCVBar]:
        """
        Fetch OHLCV bars for [start, end) in UTC.

        timeframe: '1m' | '1h' | '1d'
        """


class FallbackBarsProvider:
    """
    Provider wrapper that tries multiple providers in order.

    This is intended for backfills where "some bars" are better than stalling the replay.
    """

    def __init__(self, providers: list[HistoricalBarsProvider]) -> None:
        self.providers = [p for p in (providers or []) if p is not None]
        names = [getattr(p, "name", "unknown") for p in self.providers]
        self.name = "->".join([str(n) for n in names if n]) or "fallback"

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        last_exc: Exception | None = None
        for p in self.providers:
            try:
                bars = p.fetch_bars(timeframe=timeframe, ticker=ticker, start=start, end=end)
                if bars:
                    return bars
            except Exception as e:
                last_exc = e
                continue
        if last_exc is not None:
            # Swallow provider errors; the cache layer will fall back to whatever is already persisted.
            return []
        return []


def _http_json(url: str, *, headers: dict[str, str] | None = None, timeout_s: int = 30) -> Any:
    req = Request(url, headers=headers or {}, method="GET")
    with urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def _chunk_range(start: datetime, end: datetime, chunk: timedelta) -> list[tuple[datetime, datetime]]:
    out: list[tuple[datetime, datetime]] = []
    cur = start
    while cur < end:
        nxt = min(cur + chunk, end)
        out.append((cur, nxt))
        cur = nxt
    return out


class AlpacaBarsProvider:
    name = "alpaca"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str = "https://data.alpaca.markets",
        max_chunk_days: int = 7,
        sleep_s: float = 0.0,
    ) -> None:
        self.api_key = api_key or os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
        self.api_secret = api_secret or os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
        if not self.api_key or not self.api_secret:
            raise ValueError("Missing Alpaca credentials (ALPACA_API_KEY/ALPACA_API_SECRET or APCA_API_KEY_ID/APCA_API_SECRET_KEY).")
        self.base_url = str(base_url).rstrip("/")
        self.max_chunk_days = int(max_chunk_days)
        self.sleep_s = float(sleep_s)

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        start_utc = to_utc_datetime(start)
        end_utc = to_utc_datetime(end)
        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        }

        tf = str(timeframe).strip().lower()
        tf_map = {"1m": "1Min", "1h": "1Hour", "1d": "1Day"}
        alpaca_tf = tf_map.get(tf)
        if alpaca_tf is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        out: list[OHLCVBar] = []
        for s, e in _chunk_range(start_utc, end_utc, timedelta(days=self.max_chunk_days)):
            # Alpaca bars endpoint: /v2/stocks/{symbol}/bars
            params = {
                "timeframe": alpaca_tf,
                "start": normalize_timestamp(s),
                "end": normalize_timestamp(e),
                "limit": 10000,
                "adjustment": "all",
            }
            url = f"{self.base_url}/v2/stocks/{ticker}/bars?{urlencode(params)}"
            payload = _http_json(url, headers=headers)
            bars = payload.get("bars") if isinstance(payload, dict) else None
            if not isinstance(bars, list):
                continue
            for b in bars:
                try:
                    ts = normalize_timestamp(b.get("t"))
                    out.append(
                        OHLCVBar(
                            timestamp=ts,
                            open=float(b.get("o", 0.0)),
                            high=float(b.get("h", 0.0)),
                            low=float(b.get("l", 0.0)),
                            close=float(b.get("c", 0.0)),
                            volume=float(b.get("v", 0.0)),
                        )
                    )
                except Exception:
                    continue
            if self.sleep_s > 0:
                time.sleep(self.sleep_s)
        return out


class PolygonBarsProvider:
    name = "polygon"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.polygon.io",
        max_chunk_days: int = 7,
        sleep_s: float = 0.0,
    ) -> None:
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise ValueError("Missing Polygon credentials (POLYGON_API_KEY).")
        self.base_url = str(base_url).rstrip("/")
        self.max_chunk_days = int(max_chunk_days)
        self.sleep_s = float(sleep_s)

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        start_utc = to_utc_datetime(start)
        end_utc = to_utc_datetime(end)
        out: list[OHLCVBar] = []

        tf = str(timeframe).strip().lower()
        tf_map = {"1m": "minute", "1h": "hour", "1d": "day"}
        polygon_tf = tf_map.get(tf)
        if polygon_tf is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        for s, e in _chunk_range(start_utc, end_utc, timedelta(days=self.max_chunk_days)):
            # Polygon requires YYYY-MM-DD for the range endpoint; keep it inclusive by day and let caching de-dupe.
            from_day = s.date().isoformat()
            to_day = (e - timedelta(seconds=1)).date().isoformat()
            params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": self.api_key}
            url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/{polygon_tf}/{from_day}/{to_day}?{urlencode(params)}"
            payload = _http_json(url)
            results = payload.get("results") if isinstance(payload, dict) else None
            if not isinstance(results, list):
                continue
            for r in results:
                try:
                    # "t" is ms epoch.
                    ts = normalize_timestamp(int(r.get("t")))
                    out.append(
                        OHLCVBar(
                            timestamp=ts,
                            open=float(r.get("o", 0.0)),
                            high=float(r.get("h", 0.0)),
                            low=float(r.get("l", 0.0)),
                            close=float(r.get("c", 0.0)),
                            volume=float(r.get("v", 0.0)),
                        )
                    )
                except Exception:
                    continue
            if self.sleep_s > 0:
                time.sleep(self.sleep_s)
        return out


class YFinanceBarsProvider:
    name = "yfinance"

    def __init__(self) -> None:
        try:
            import yfinance  # type: ignore
        except Exception as e:
            raise ImportError("yfinance is not installed. Install it to use the yfinance fallback provider.") from e
        self._yf = yfinance

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        # yfinance intraday availability is limited; this is best-effort fallback.
        import pandas as pd  # type: ignore

        start_utc = to_utc_datetime(start)
        end_utc = to_utc_datetime(end)
        tf = str(timeframe).strip().lower()
        tf_map = {"1m": "1m", "1h": "1h", "1d": "1d"}
        yf_tf = tf_map.get(tf)
        if yf_tf is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        df = self._yf.download(
            tickers=str(ticker),
            start=start_utc,
            end=end_utc,
            interval=yf_tf,
            progress=False,
            auto_adjust=False,
            actions=False,
            threads=False,
        )
        if df is None or getattr(df, "empty", True):
            return []
        try:
            df = df.reset_index()
        except Exception:
            return []

        # yfinance can return MultiIndex columns (e.g. ("Open","SPY") / ("Price","Open") variants).
        try:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join([str(x) for x in col if x]) for col in df.columns]
        except Exception:
            pass

        def _pick(row: Any, base: str) -> Any:
            if base in row:
                return row[base]
            # Common flattened patterns: "Open_SPY", "Price_Open", etc.
            for k in getattr(row, "index", []):
                try:
                    ks = str(k)
                except Exception:
                    continue
                if ks == base:
                    return row[k]
                if ks.startswith(base + "_") or ks.endswith("_" + base):
                    return row[k]
            return None

        def _scalar(v: Any) -> Any:
            try:
                if isinstance(v, pd.Series):
                    return v.iloc[0]
            except Exception:
                pass
            return v

        out: list[OHLCVBar] = []
        for _, row in df.iterrows():
            try:
                dt_val = None
                if "Datetime" in row:
                    dt_val = row["Datetime"]
                elif "Date" in row:
                    dt_val = row["Date"]
                elif "index" in row:
                    dt_val = row["index"]
                else:
                    dt_val = _pick(row, "Datetime") or _pick(row, "Date")
                if dt_val is None:
                    continue
                ts = normalize_timestamp(dt_val)

                o = _scalar(_pick(row, "Open"))
                h = _scalar(_pick(row, "High"))
                l = _scalar(_pick(row, "Low"))
                c = _scalar(_pick(row, "Close"))
                vol = _scalar(_pick(row, "Volume")) or 0.0
                out.append(
                    OHLCVBar(
                        timestamp=ts,
                        open=float(o),
                        high=float(h),
                        low=float(l),
                        close=float(c),
                        volume=float(vol),
                    )
                )
            except Exception:
                continue
        return out


class MockBarsProvider:
    name = "mock"

    def fetch_bars(self, *, timeframe: str, ticker: str, start: datetime, end: datetime) -> list[OHLCVBar]:
        # Generate fake bars for POC stability
        tf = str(timeframe).strip().lower()
        step = timedelta(minutes=1)
        if tf == "1h":
            step = timedelta(hours=1)
        elif tf == "1d":
            step = timedelta(days=1)
        elif tf != "1m":
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        out: list[OHLCVBar] = []
        cur = start
        while cur < end:
            ts = normalize_timestamp(cur)
            out.append(
                OHLCVBar(
                    timestamp=ts,
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.5,
                    volume=1000.0,
                )
            )
            cur += step
        return out


def build_bars_provider(name: str) -> HistoricalBarsProvider:
    key = str(name or "").strip().lower()
    if key in {"alpaca", "alpaca_v2"}:
        return AlpacaBarsProvider()
    if key in {"polygon"}:
        return PolygonBarsProvider()
    if key in {"yfinance", "yf"}:
        return YFinanceBarsProvider()
    if key in {"mock"}:
        return MockBarsProvider()
    raise ValueError(f"Unknown bars provider: {name}")
