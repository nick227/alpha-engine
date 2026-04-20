"""
Backward-compatible re-exports for internal read chart/market helpers.

Implementation lives in chart_*.py modules.
"""

from __future__ import annotations

from app.internal_read_v1.chart_market import (
    build_company_payload,
    build_quote_payload,
    build_stats_payload,
    load_company_profile_json,
)
from app.internal_read_v1.chart_ohlcv import (
    MAX_POINTS_CAP,
    build_candles_payload,
    build_history_payload,
    pick_timeframe,
    read_ohlcv_df,
)
from app.internal_read_v1.chart_range_interval import (
    DEFAULT_INTERVAL,
    RANGE_ALIASES,
    parse_interval_key,
    parse_range_key,
    window_start_end,
)
from app.internal_read_v1.chart_symbols import normalize_ticker

__all__ = [
    "DEFAULT_INTERVAL",
    "MAX_POINTS_CAP",
    "RANGE_ALIASES",
    "build_candles_payload",
    "build_company_payload",
    "build_history_payload",
    "build_quote_payload",
    "build_stats_payload",
    "load_company_profile_json",
    "normalize_ticker",
    "parse_interval_key",
    "parse_range_key",
    "pick_timeframe",
    "read_ohlcv_df",
    "window_start_end",
]
