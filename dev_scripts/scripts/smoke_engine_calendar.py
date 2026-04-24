"""
Quick smoke test for /api/engine/calendar contract.

Usage:
  python dev_scripts/scripts/smoke_engine_calendar.py
  python dev_scripts/scripts/smoke_engine_calendar.py --minimum 10 --limit 50
  python dev_scripts/scripts/smoke_engine_calendar.py --key <INTERNAL_READ_KEY>
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def _load_env_key_from_dotenv() -> str | None:
    root = Path(__file__).resolve().parents[2]
    env_file = root / ".env"
    if not env_file.exists():
        return None
    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() != "INTERNAL_READ_KEY":
                continue
            val = v.strip().strip('"').strip("'")
            return val or None
    except Exception:
        return None
    return None


def _http_get_json(url: str, *, key: str | None = None, timeout_sec: float = 10.0) -> dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    if key:
        req.add_header("X-Internal-Key", key)
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("expected top-level JSON object")
    return payload


def _validate_event(event: dict[str, Any], idx: int) -> list[str]:
    errors: list[str] = []
    pref = f"events[{idx}]"
    required = ("date", "type", "symbol", "direction", "confidence", "source")
    for key in required:
        if key not in event:
            errors.append(f"{pref}.{key} missing")
    e_type = str(event.get("type", ""))
    if e_type not in {"prediction", "ranking", "consensus"}:
        errors.append(f"{pref}.type invalid")
    direction = str(event.get("direction", ""))
    if direction not in {"BUY", "HOLD", "SELL"}:
        errors.append(f"{pref}.direction invalid")
    return errors


def main() -> int:
    default_port = str(os.environ.get("INTERNAL_READ_PORT") or os.environ.get("PORT") or "8090").strip()
    default_base = f"http://127.0.0.1:{default_port}"
    parser = argparse.ArgumentParser(description="Smoke test /api/engine/calendar contract")
    parser.add_argument("--base-url", default=default_base, help="API base URL")
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument("--month", default=None, help="YYYY-MM (defaults to current UTC month)")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--minimum", type=int, default=10, help="Minimum expected events")
    parser.add_argument("--key", default=None, help="Optional X-Internal-Key")
    parser.add_argument("--allow-below-minimum", action="store_true")
    args = parser.parse_args()

    month = args.month
    if not month:
        from datetime import datetime, UTC

        month = datetime.now(UTC).strftime("%Y-%m")

    resolved_key = (
        str(args.key).strip()
        if args.key is not None and str(args.key).strip()
        else (os.environ.get("INTERNAL_READ_KEY") or "").strip() or _load_env_key_from_dotenv()
    )

    query = urllib.parse.urlencode({"tenant_id": args.tenant_id, "month": month, "limit": args.limit})
    url = f"{args.base_url.rstrip('/')}/api/engine/calendar?{query}"

    try:
        payload = _http_get_json(url, key=resolved_key)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        print(f"[calendar-smoke] FAIL: HTTP {exc.code} for {url}")
        if body:
            print(body[:600])
        return 2
    except Exception as exc:
        print(f"[calendar-smoke] FAIL: request error: {exc}")
        return 2

    failures: list[str] = []
    for k in (
        "month",
        "distribution",
        "minimumDaysTarget",
        "distinctDays",
        "meetsDayTarget",
        "eventCount",
        "meetsMinimum",
        "countsByType",
        "events",
    ):
        if k not in payload:
            failures.append(f"payload missing {k}")

    events = payload.get("events")
    if not isinstance(events, list):
        failures.append("payload.events must be a list")
        events = []

    count = int(payload.get("eventCount") or 0)
    if count != len(events):
        failures.append(f"eventCount mismatch: eventCount={count}, len(events)={len(events)}")

    meets_minimum = bool(payload.get("meetsMinimum"))
    if len(events) < int(args.minimum) and not args.allow_below_minimum:
        failures.append(f"events below minimum: {len(events)} < {int(args.minimum)}")
    if len(events) >= int(args.minimum) and not meets_minimum:
        failures.append("meetsMinimum false but events satisfy minimum")

    counts_by_type = payload.get("countsByType")
    if not isinstance(counts_by_type, dict):
        failures.append("countsByType missing/invalid")
    if payload.get("distribution") not in {"actual", "uniform"}:
        failures.append("distribution invalid")
    try:
        distinct_days = int(payload.get("distinctDays") or 0)
        min_days = int(payload.get("minimumDaysTarget") or 0)
    except (TypeError, ValueError):
        failures.append("distinctDays/minimumDaysTarget invalid")
        distinct_days = 0
        min_days = 0
    if distinct_days > 0 and min_days > 0:
        meets_day_target = bool(payload.get("meetsDayTarget"))
        if (distinct_days >= min_days) != meets_day_target:
            failures.append("meetsDayTarget inconsistent with distinctDays/minimumDaysTarget")

    else:
        for t in ("prediction", "ranking", "consensus"):
            if t not in counts_by_type:
                failures.append(f"countsByType.{t} missing")

    for idx, event in enumerate(events):
        if not isinstance(event, dict):
            failures.append(f"events[{idx}] must be object")
            continue
        failures.extend(_validate_event(event, idx))

    if failures:
        print("[calendar-smoke] FAIL")
        for f in failures[:50]:
            print(f" - {f}")
        if len(failures) > 50:
            print(f" - ... and {len(failures) - 50} more")
        return 1

    print("[calendar-smoke] PASS")
    print(
        json.dumps(
            {
                "month": payload.get("month"),
                "eventCount": payload.get("eventCount"),
                "meetsMinimum": payload.get("meetsMinimum"),
                "countsByType": payload.get("countsByType"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

