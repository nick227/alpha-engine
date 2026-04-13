"""
FeatureBuilder — point-in-time feature vector construction.

Fetches full OHLCV bars from the price_bars SQLite table and macro values
from FRED dump parquets. Applies publication lags, horizon eligibility
filtering, and computes all supported transforms.

All data is fetched as-of (as_of_date - lag), ensuring strict no-lookahead.
"""
from __future__ import annotations

import math
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from app.ml.factor_spec import (
    HORIZON_DAYS,
    FactorConfig,
    FactorSpec,
    load_factor_config,
)

# Minimum bars required for transforms that need more than window+1
_MIN_BARS_SLACK = 5
# FRED parquet cache — shared across instances
_fred_cache: dict[str, pd.DataFrame] = {}


class FeatureBuilder:
    """
    Builds a {factor_name: float} feature vector for a given (ticker, as_of, horizon).

    Interface:
        fb = FeatureBuilder()
        features, coverage = fb.build("AAPL", date(2024, 3, 15), horizon="1d")
        # coverage: fraction of eligible factors that returned a non-null value
    """

    def __init__(
        self,
        db_path: str | Path = "data/alpha.db",
        dumps_root: str | Path = "data/raw_dumps",
        tenant_id: str = "default",
        factors_path: str = "config/factors.yaml",
    ) -> None:
        self.db_path = Path(db_path)
        self.dumps_root = Path(dumps_root)
        self.tenant_id = tenant_id
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.config: FactorConfig = load_factor_config(factors_path)
        # In-memory bar cache: {symbol: sorted DataFrame} populated by prefetch_bars()
        self._bars_cache: dict[str, pd.DataFrame] = {}

    def prefetch_bars(
        self,
        symbols: list[str],
        start: date,
        end: date,
        lookback_days: int = 300,
    ) -> None:
        """
        Bulk-load daily OHLCV bars for all symbols into memory.

        Call this once before a dataset build loop to replace O(n_dates × n_factors)
        DB queries with a single fetch per symbol.

        lookback_days: extra history before `start` needed for rolling transforms
                       (e.g. 252-day percentile window).
        """
        fetch_start = start - timedelta(days=lookback_days)
        for symbol in symbols:
            rows = self.conn.execute(
                """
                SELECT timestamp, open, high, low, close, volume
                FROM price_bars
                WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
                  AND DATE(timestamp) >= ? AND DATE(timestamp) <= ?
                ORDER BY timestamp ASC
                """,
                (self.tenant_id, symbol, fetch_start.isoformat(), end.isoformat()),
            ).fetchall()
            if rows:
                df = pd.DataFrame([dict(r) for r in rows])
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
                df = df.sort_values("timestamp")
                for col in ("open", "high", "low", "close", "volume"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                # Store with DatetimeIndex so loc[:cutoff] is O(log n)
                self._bars_cache[symbol] = df.set_index("timestamp")

    # ── Public API ──────────────────────────────────────────────────────────

    def build(
        self,
        ticker: str,
        as_of: date,
        horizon: str,
    ) -> tuple[dict[str, float], float]:
        """
        Build a feature vector for (ticker, as_of) for the given horizon.

        Uses horizon_sets whitelist if configured; falls back to window guard.

        Returns:
            features:  {name: value} for each factor that yielded a result
            coverage:  fraction of eligible factors present (0.0–1.0)
        """
        horizon_days = HORIZON_DAYS.get(horizon, 1.0)
        eligible = self.config.get_eligible_specs(horizon, horizon_days)

        features: dict[str, float] = {}
        present = 0
        denom = 0  # coverage denominator (derived-series factors don't penalize coverage when missing)

        for spec in eligible:
            sym = spec.resolve_symbol(ticker) if spec.source in ("price", "price_relative") else None
            value = self._compute(spec, ticker, as_of)
            if value is not None and math.isfinite(value):
                features[spec.name] = value
                present += 1
                denom += 1
            else:
                # Derived series (OPT/SHORT/EARN/INT) are additive: don't reduce coverage when absent.
                if sym and any(sym.startswith(pfx) for pfx in ("OPT:", "SHORT:", "EARN:", "INT:")):
                    continue
                denom += 1

        coverage = present / denom if denom > 0 else 0.0
        return features, coverage

    # ── Dispatch ────────────────────────────────────────────────────────────

    def _compute(self, spec: FactorSpec, ticker: str, as_of: date) -> Optional[float]:
        lag = spec.effective_lag()

        if spec.source == "fred":
            return self._fred_value(spec.series or "", as_of, spec.window, lag, spec.transform)

        sym = spec.resolve_symbol(ticker)
        if not sym:
            return None

        if spec.source == "price_relative":
            bench = spec.benchmark
            if not bench:
                return None
            return self._relative_value(sym, bench, as_of, spec.window, lag, spec.transform)

        # source == "price"
        return self._price_transform(sym, as_of, spec.window, lag, spec.transform)

    # ── Price transforms ─────────────────────────────────────────────────────

    def _price_transform(
        self,
        symbol: str,
        as_of: date,
        window: int,
        lag: int,
        transform: str,
    ) -> Optional[float]:
        n_needed = _bars_needed(window, transform)
        df = self._get_bars(symbol, as_of, n_needed, lag)
        if df is None or len(df) < n_needed:
            return None
        return _apply_transform(df, transform, window)

    def _relative_value(
        self,
        symbol: str,
        benchmark: str,
        as_of: date,
        window: int,
        lag: int,
        transform: str,
    ) -> Optional[float]:
        n_needed = _bars_needed(window, transform)
        df_sym = self._get_bars(symbol, as_of, n_needed, lag)
        df_ben = self._get_bars(benchmark, as_of, n_needed, lag)

        if df_sym is None or df_ben is None:
            return None

        if transform == "relative_return":
            r_sym = _apply_transform(df_sym, "return", window)
            r_ben = _apply_transform(df_ben, "return", window)
            if r_sym is None or r_ben is None:
                return None
            return r_sym - r_ben

        if transform == "beta":
            return _compute_beta(df_sym, df_ben, window)

        if transform == "level_diff":
            # Raw level difference: sym.close - bench.close (e.g. VIX - VIX3M)
            if len(df_sym) < 1 or len(df_ben) < 1:
                return None
            c_sym = float(df_sym["close"].values[-1])
            c_ben = float(df_ben["close"].values[-1])
            return c_sym - c_ben

        return None

    # ── Bar fetching ─────────────────────────────────────────────────────────

    def _get_bars(
        self,
        symbol: str,
        as_of: date,
        n_bars: int,
        lag_days: int,
    ) -> Optional[pd.DataFrame]:
        """
        Return the last n_bars daily OHLCV rows for symbol up to (as_of - lag_days).

        Uses in-memory cache if available (populated by prefetch_bars), otherwise
        falls back to a DB query.
        """
        effective = as_of - timedelta(days=lag_days)
        need = n_bars + _MIN_BARS_SLACK

        if symbol in self._bars_cache:
            df = self._bars_cache[symbol]  # DatetimeIndex, sorted ascending
            cutoff = (pd.Timestamp(effective.isoformat()) + pd.Timedelta(hours=23, minutes=59)).tz_localize("UTC")
            sub = df.loc[:cutoff]          # O(log n) with sorted DatetimeIndex
            if sub.empty:
                return None
            sliced = sub.iloc[-need:]
            return sliced.reset_index()    # bring timestamp back as column

        # DB fallback (used when prefetch_bars was not called)
        rows = self.conn.execute(
            """
            SELECT timestamp, open, high, low, close, volume
            FROM price_bars
            WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
              AND DATE(timestamp) <= ?
            ORDER BY timestamp DESC LIMIT ?
            """,
            (self.tenant_id, symbol, effective.isoformat(), need),
        ).fetchall()

        if not rows:
            return None

        df = pd.DataFrame([dict(r) for r in rows])
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True)
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    # ── FRED macro ───────────────────────────────────────────────────────────

    def _fred_value(
        self,
        series: str,
        as_of: date,
        window: int,
        lag: int,
        transform: str,
    ) -> Optional[float]:
        effective = as_of - timedelta(days=lag)
        path = self.dumps_root / "fred" / f"{series}.parquet"

        if not path.exists():
            return None

        cache_key = str(path)
        if cache_key not in _fred_cache:
            try:
                df = pd.read_parquet(path)
                df["_date"] = pd.to_datetime(df["date"]).dt.date
                _fred_cache[cache_key] = df
            except Exception:
                return None

        df = _fred_cache[cache_key]
        sub = df[df["_date"] <= effective].sort_values("_date")

        if sub.empty:
            return None

        vals = sub["value"].tolist()

        if transform == "level":
            return float(vals[-1])
        if transform == "diff":
            return float(vals[-1] - vals[-2]) if len(vals) >= 2 else None
        if transform == "return":
            if len(vals) < window + 1:
                return None
            curr, prior = vals[-1], vals[-(window + 1)]
            return float((curr - prior) / abs(prior)) if prior != 0 else None

        return None

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "FeatureBuilder":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ═══════════════════════════════════════════════════════════════════════════
# Transform implementations (pure functions on DataFrames)
# ═══════════════════════════════════════════════════════════════════════════

def _bars_needed(window: int, transform: str) -> int:
    """How many bars to fetch for a given window/transform."""
    # Most transforms need window+1 bars; some need more.
    # Keep this tight: it drives whether derived-series "level" factors work when only a few points exist.
    if transform == "level":
        return 1
    if transform == "return":
        return window + 1
    if transform == "diff":
        return 2
    if transform in ("rsi", "atr_ratio", "stochastic"):
        return window + 2
    if transform in ("beta", "trend_slope", "dollar_volume_trend"):
        return window + 5
    if transform in ("volume_zscore", "dollar_volume_zscore"):
        return window + 1
    if transform in ("gap_open", "gap_follow_through", "level_diff"):
        return 2
    if transform == "candle_body":
        return 1
    if transform == "intraday_trend":
        return window
    return window + 1


def _apply_transform(df: pd.DataFrame, transform: str, window: int) -> Optional[float]:
    """
    Apply a transform to a sorted OHLCV DataFrame and return a scalar.
    All transforms are computed using only rows present in df (newest = last row).
    """
    if df.empty:
        return None

    closes = df["close"].values
    n = len(closes)

    # ── Simple price ──────────────────────────────────────────────────────
    if transform == "return":
        if n < window + 1:
            return None
        curr, prior = closes[-1], closes[-window - 1]
        return math.log(curr / prior) if curr > 0 and prior > 0 else None

    if transform == "level":
        return float(closes[-1])

    if transform == "diff":
        return float(closes[-1] - closes[-2]) if n >= 2 else None

    # ── Volatility ───────────────────────────────────────────────────────
    if transform == "volatility":
        if n < window + 1:
            return None
        window_closes = closes[-window - 1:]
        rets = np.log(window_closes[1:] / window_closes[:-1])
        std = float(np.std(rets))
        return std * math.sqrt(252)  # annualized

    # ── Z-score ──────────────────────────────────────────────────────────
    if transform == "zscore":
        if n < window:
            return None
        w = closes[-window:]
        std = float(np.std(w))
        if std == 0:
            return 0.0
        return float((closes[-1] - np.mean(w)) / std)

    # ── Percentile rank ───────────────────────────────────────────────────
    if transform == "percentile":
        if n < window:
            return None
        w = closes[-window:]
        return float(np.mean(w < closes[-1]))

    # ── ATR ratio ────────────────────────────────────────────────────────
    if transform == "atr_ratio":
        if n < window + 1 or "high" not in df.columns:
            return None
        highs = df["high"].values[-window - 1:]
        lows = df["low"].values[-window - 1:]
        prev_closes = closes[-window - 1:-1]
        tr = np.maximum(
            highs[1:] - lows[1:],
            np.maximum(
                np.abs(highs[1:] - prev_closes),
                np.abs(lows[1:] - prev_closes),
            ),
        )
        atr = float(np.mean(tr))
        close_t = float(closes[-1])
        return atr / close_t if close_t > 0 else None

    # ── Range expansion ───────────────────────────────────────────────────
    if transform == "range_expansion":
        if n < window + 1 or "high" not in df.columns:
            return None
        highs = df["high"].values[-window:]
        lows = df["low"].values[-window:]
        daily_ranges = highs - lows
        avg_range = float(np.mean(daily_ranges[:-1]))  # prior n-1 days
        today_range = float(daily_ranges[-1])
        return (today_range / avg_range) if avg_range > 0 else None

    # ── RSI ──────────────────────────────────────────────────────────────
    if transform == "rsi":
        if n < window + 1:
            return None
        window_closes = closes[-window - 1:]
        deltas = np.diff(window_closes)
        gains = np.maximum(deltas, 0)
        losses = np.abs(np.minimum(deltas, 0))
        avg_gain = float(np.mean(gains))
        avg_loss = float(np.mean(losses))
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    # ── Stochastic %K ────────────────────────────────────────────────────
    if transform == "stochastic":
        if n < window or "high" not in df.columns:
            return None
        h = float(np.max(df["high"].values[-window:]))
        l = float(np.min(df["low"].values[-window:]))
        c = float(closes[-1])
        denom = h - l
        return (c - l) / denom if denom > 0 else 0.5

    # ── MA distance ──────────────────────────────────────────────────────
    if transform == "ma_distance":
        if n < window:
            return None
        sma = float(np.mean(closes[-window:]))
        return (closes[-1] - sma) / sma if sma > 0 else None

    # ── Drawdown from rolling high ────────────────────────────────────────
    if transform == "drawdown":
        if n < window:
            return None
        rolling_max = float(np.max(closes[-window:]))
        return (closes[-1] - rolling_max) / rolling_max if rolling_max > 0 else None

    # ── OLS trend slope (normalized) ──────────────────────────────────────
    if transform == "trend_slope":
        if n < window:
            return None
        log_prices = np.log(closes[-window:])
        x = np.arange(window, dtype=float)
        slope = float(np.polyfit(x, log_prices, 1)[0])
        # Normalize by current price magnitude → fractional daily drift
        return slope

    # ── Volume surge ──────────────────────────────────────────────────────
    if transform == "volume_surge":
        if n < window or "volume" not in df.columns:
            return None
        vols = df["volume"].values[-window:]
        avg_vol = float(np.mean(vols[:-1]))  # prior window-1 bars
        today_vol = float(vols[-1])
        return (today_vol / avg_vol) if avg_vol > 0 else None

    # ── Dollar volume trend (OLS slope) ──────────────────────────────────
    if transform == "dollar_volume_trend":
        if n < window or "volume" not in df.columns:
            return None
        dvol = closes[-window:] * df["volume"].values[-window:]
        x = np.arange(window, dtype=float)
        slope = float(np.polyfit(x, dvol, 1)[0])
        # Normalize by mean dollar volume so it's scale-free
        mean_dvol = float(np.mean(dvol))
        return slope / mean_dvol if mean_dvol > 0 else None

    # ── Volume z-score (participation signal) ────────────────────────────
    if transform == "volume_zscore":
        if n < window or "volume" not in df.columns:
            return None
        vols = df["volume"].values[-window:].astype(float)
        std = float(np.std(vols))
        if std == 0:
            return 0.0
        return float((vols[-1] - float(np.mean(vols))) / std)

    # ── Dollar volume z-score (institutional flow proxy) ─────────────────
    if transform == "dollar_volume_zscore":
        if n < window or "volume" not in df.columns:
            return None
        dvol = np.log1p(closes[-window:] * df["volume"].values[-window:])
        std = float(np.std(dvol))
        if std == 0:
            return 0.0
        return float((dvol[-1] - float(np.mean(dvol))) / std)

    # ── Gap open (overnight gap as fraction of prev close) ────────────────
    if transform == "gap_open":
        if n < 2 or "open" not in df.columns:
            return None
        open_t = float(df["open"].values[-1])
        close_prev = float(closes[-2])
        return (open_t - close_prev) / close_prev if close_prev > 0 else None

    # ── Gap follow-through (did price continue in gap direction?) ─────────
    if transform == "gap_follow_through":
        if n < 2 or "open" not in df.columns:
            return None
        open_t = float(df["open"].values[-1])
        close_prev = float(closes[-2])
        close_t = float(closes[-1])
        gap = open_t - close_prev
        if gap == 0:
            return 0.0
        # Positive = price continued in gap direction; negative = gap filled
        return (close_t - open_t) / abs(gap)

    # ── Candle body position (opening range break proxy) ──────────────────
    # (close - open) / (high - low): where close sits within the day's range
    # +1 = opened at low, closed at high (full bull candle); -1 = full bear; 0 = doji
    if transform == "candle_body":
        if n < 1 or "open" not in df.columns or "high" not in df.columns:
            return None
        open_t  = float(df["open"].values[-1])
        high_t  = float(df["high"].values[-1])
        low_t   = float(df["low"].values[-1])
        close_t = float(closes[-1])
        rng = high_t - low_t
        if rng == 0:
            return 0.0
        return (close_t - open_t) / rng

    # ── Intraday trend strength (rolling candle body efficiency) ──────────
    # Rolling mean of candle_body over window days — captures whether intraday
    # moves have been consistently directional (trending) vs choppy (mean-reverting)
    if transform == "intraday_trend":
        if n < window or "open" not in df.columns or "high" not in df.columns:
            return None
        opens = df["open"].values[-window:]
        highs = df["high"].values[-window:]
        lows  = df["low"].values[-window:]
        clss  = closes[-window:]
        rngs  = highs - lows
        with np.errstate(invalid="ignore", divide="ignore"):
            bodies = np.where(rngs > 0, (clss - opens) / rngs, 0.0)
        return float(np.mean(bodies))

    return None


def _compute_beta(
    df_stock: pd.DataFrame,
    df_bench: pd.DataFrame,
    window: int,
) -> Optional[float]:
    """Rolling OLS beta of stock vs benchmark over window days."""
    n_stock = len(df_stock)
    n_bench = len(df_bench)
    if n_stock < window + 1 or n_bench < window + 1:
        return None

    stock_closes = df_stock["close"].values[-window - 1:]
    bench_closes = df_bench["close"].values[-window - 1:]

    stock_rets = np.log(stock_closes[1:] / stock_closes[:-1])
    bench_rets = np.log(bench_closes[1:] / bench_closes[:-1])

    var_bench = float(np.var(bench_rets))
    if var_bench == 0:
        return None

    cov = float(np.cov(stock_rets, bench_rets)[0, 1])
    return cov / var_bench
