from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def normalize_timestamp(value: Any) -> str:
    """
    Normalize timestamps into a stable UTC ISO-8601 string with an explicit offset.

    Accepts:
    - datetime (naive treated as UTC)
    - ISO strings (supports trailing 'Z')
    - epoch seconds / milliseconds as int/float or numeric strings
    """
    dt = to_utc_datetime(value)
    # Keep seconds precision; drop microseconds to stabilize IDs / ordering.
    dt = dt.replace(microsecond=0)
    return dt.isoformat()


def to_utc_datetime(value: Any) -> datetime:
    if value is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        return _epoch_to_dt(value)

    s = str(value).strip()
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)

    # Numeric strings -> epoch seconds/millis.
    if _looks_numeric(s):
        try:
            return _epoch_to_dt(float(s))
        except Exception:
            pass

    # Accept common "Z" suffix.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # datetime.fromisoformat supports offsets but not 'Z'.
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        # Attempt to coerce space separator or missing timezone.
        try:
            parsed = datetime.fromisoformat(s.replace(" ", "T"))
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _epoch_to_dt(value: float) -> datetime:
    # Heuristic: > 1e12 is likely milliseconds.
    seconds = float(value) / 1000.0 if float(value) > 1.0e12 else float(value)
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _looks_numeric(s: str) -> bool:
    # Allow leading '-' and a single '.'.
    if s.startswith("-"):
        s = s[1:]
    if not s:
        return False
    dot = 0
    for ch in s:
        if ch == ".":
            dot += 1
            if dot > 1:
                return False
            continue
        if ch < "0" or ch > "9":
            return False
    return True

