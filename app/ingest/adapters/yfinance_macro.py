from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.ingest.fetch_context import FetchContext
from app.ingest.source_spec import SourceSpec
from app.core.time_utils import to_utc_datetime, normalize_timestamp


class YFinanceMacroAdapter:
    """
    Fetch daily macro asset snapshots via yfinance.

    Intended for Oil/Gold/DXY/BTC context features.
    """

    async def fetch_raw(self, spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
        await ctx.rate_limiter.throttle()

        try:
            import yfinance as yf  # type: ignore
        except Exception as e:
            raise ImportError("yfinance is not installed; cannot use yfinance_macro adapter.") from e

        start_dt = to_utc_datetime(ctx.start_date or ctx.run_timestamp).replace(microsecond=0)
        end_dt = to_utc_datetime(ctx.end_date or (start_dt + timedelta(days=1))).replace(microsecond=0)

        # yfinance often needs a wider window to get a prior close for returns.
        lookback_start = start_dt - timedelta(days=10)

        symbols = []
        try:
            symbols = list((spec.options or {}).get("symbols") or [])
        except Exception:
            symbols = []

        if not symbols:
            return []

        out: list[dict[str, Any]] = []
        for sym in symbols:
            yf_sym = str(sym).strip()
            if not yf_sym:
                continue
            try:
                df = yf.download(
                    tickers=yf_sym,
                    start=lookback_start,
                    end=end_dt,
                    interval="1d",
                    progress=False,
                    auto_adjust=False,
                    actions=False,
                    threads=False,
                )
            except Exception:
                continue

            if df is None or getattr(df, "empty", True):
                continue

            try:
                df = df.reset_index()
            except Exception:
                continue

            # Try to find the last close before end_dt and the prior close for return_1d.
            closes: list[tuple[datetime, float]] = []
            for _, row in df.iterrows():
                try:
                    ts = row["Date"] if "Date" in row else row.get("Datetime")
                    if ts is None:
                        continue
                    if not isinstance(ts, datetime):
                        ts = datetime.fromisoformat(str(ts))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    ts = ts.astimezone(timezone.utc).replace(microsecond=0)
                    close = float(row["Close"])
                    closes.append((ts, close))
                except Exception:
                    continue

            closes.sort(key=lambda x: x[0])
            closes = [c for c in closes if c[0] < end_dt]
            if not closes:
                continue

            last_ts, last_close = closes[-1]
            prev_close = closes[-2][1] if len(closes) >= 2 else None
            ret_1d = None
            if prev_close is not None and prev_close != 0:
                ret_1d = (last_close / prev_close) - 1.0

            out.append(
                {
                    "timestamp": normalize_timestamp(start_dt),
                    "symbol": yf_sym,
                    "close": float(last_close),
                    "return_1d": float(ret_1d) if ret_1d is not None else None,
                    "asof": normalize_timestamp(last_ts),
                    "provider": "yfinance",
                }
            )

        return out

