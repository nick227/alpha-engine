"""Parse `range` and `interval` query params for chart endpoints."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.time_utils import to_utc_datetime

UTC = timezone.utc

RANGE_ALIASES = {
    "1d": "1D",
    "1w": "1W",
    "1mo": "1M",
    "3mo": "3M",
    "1y": "1Y",
    "5y": "5Y",
    "max": "MAX",
}

DEFAULT_INTERVAL: dict[str, str] = {
    "1D": "5m",
    "1W": "30m",
    "1M": "1D",
    "3M": "1D",
    "1Y": "1D",
    "5Y": "1W",
    "MAX": "1Mo",
}

_INTERVAL_RE = re.compile(r"^(?P<n>\d+)(?P<u>m|mo|min|h|d|w|y)$", re.I)


def parse_range_key(raw: str | None) -> str:
    if not raw:
        return "1Y"
    low = str(raw).strip().lower()
    if low in RANGE_ALIASES:
        return RANGE_ALIASES[low]
    s = str(raw).strip().upper()
    if s in ("1D", "1W", "1M", "3M", "1Y", "5Y", "MAX"):
        return s
    raise ValueError(f"invalid range: {raw}")


def parse_interval_key(raw: str | None, range_key: str) -> str:
    if raw is None or str(raw).strip() == "":
        return DEFAULT_INTERVAL[range_key]
    s = str(raw).strip()
    u = s.upper()
    if u in ("1MO", "1MO.", "MONTH", "MONTHLY"):
        return "1Mo"
    if u in ("1W", "WEEK", "WEEKLY"):
        return "1W"
    if u in ("1D", "DAY", "DAILY"):
        return "1D"
    if u in ("1H", "60M", "60MIN"):
        return "1h"
    if u in ("30M", "30MIN"):
        return "30m"
    if u in ("5M", "5MIN"):
        return "5m"
    if u == "1M":
        if range_key in ("MAX", "5Y", "1Y", "3M"):
            return "1Mo"
        return "1m"
    m = _INTERVAL_RE.match(s.strip())
    if m:
        n, unit = m.group("n"), m.group("u").lower()
        if unit == "mo":
            return "1Mo"
        if unit == "m" and n == "1":
            return "1m"
        if unit == "m" and n == "5":
            return "5m"
        if unit == "m" and n == "30":
            return "30m"
        if unit == "h" and n == "1":
            return "1h"
        if unit == "d" and n == "1":
            return "1D"
        if unit == "w" and n == "1":
            return "1W"
    raise ValueError(f"invalid interval: {raw}")


def window_start_end(*, range_key: str, now: datetime) -> tuple[datetime | None, datetime]:
    end = to_utc_datetime(now)
    if range_key == "MAX":
        return None, end
    deltas = {
        "1D": timedelta(days=2),
        "1W": timedelta(days=8),
        "1M": timedelta(days=32),
        "3M": timedelta(days=95),
        "1Y": timedelta(days=370),
        "5Y": timedelta(days=365 * 5 + 5),
    }
    return end - deltas[range_key], end
