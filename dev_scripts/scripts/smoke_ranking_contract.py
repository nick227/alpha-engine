"""
Quick smoke test for /ranking/top contract quality.

Usage examples:
  python dev_scripts/scripts/smoke_ranking_contract.py
  python dev_scripts/scripts/smoke_ranking_contract.py --base-url http://127.0.0.1:8000 --key my-secret
  python dev_scripts/scripts/smoke_ranking_contract.py --allow-empty-rankings
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


def _http_get_json(url: str, *, key: str | None = None, timeout_sec: float = 10.0) -> dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    if key:
        req.add_header("X-Internal-Key", key)
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("expected top-level JSON object")
    return parsed


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


def _validate_row(row: dict[str, Any], idx: int) -> list[str]:
    errs: list[str] = []
    rank = f"rankings[{idx}]"

    if row.get("rankingKind") != "relative_priority":
        errs.append(f"{rank}.rankingKind missing/invalid")
    if row.get("notActionable") is not True:
        errs.append(f"{rank}.notActionable missing/invalid")

    for key in ("drivers", "risks", "changes"):
        value = row.get(key)
        if not isinstance(value, list):
            errs.append(f"{rank}.{key} must be a list")
        elif len(value) == 0:
            errs.append(f"{rank}.{key} is empty")

    rc = row.get("rankContext")
    if not isinstance(rc, dict):
        errs.append(f"{rank}.rankContext missing/invalid")
        return errs

    required_scalar = (
        "status",
        "horizon",
        "fit",
        "durability",
        "freshness",
        "spread",
        "pressure",
        "trigger",
    )
    for key in required_scalar:
        if key not in rc:
            errs.append(f"{rank}.rankContext.{key} missing")

    required_lists = ("basis", "timing", "risks", "invalidators", "history")
    for key in required_lists:
        value = rc.get(key)
        if not isinstance(value, list):
            errs.append(f"{rank}.rankContext.{key} must be a list")
        elif len(value) == 0:
            errs.append(f"{rank}.rankContext.{key} is empty")

    scope = rc.get("scope")
    if not isinstance(scope, dict):
        errs.append(f"{rank}.rankContext.scope missing/invalid")
    else:
        for key in ("window", "cutoff", "median", "edge", "peers"):
            if key not in scope:
                errs.append(f"{rank}.rankContext.scope.{key} missing")

    return errs


def main() -> int:
    default_port = str(
        os.environ.get("INTERNAL_READ_PORT")
        or os.environ.get("PORT")
        or "8090"
    ).strip()
    default_base_url = f"http://127.0.0.1:{default_port}"
    parser = argparse.ArgumentParser(description="Smoke test /ranking/top contract")
    parser.add_argument("--base-url", default=default_base_url, help="API base URL")
    parser.add_argument("--tenant-id", default="default", help="Tenant id query value")
    parser.add_argument("--limit", type=int, default=10, help="Ranking limit")
    parser.add_argument("--key", default=None, help="Optional X-Internal-Key")
    parser.add_argument(
        "--allow-empty-rankings",
        action="store_true",
        help="Pass even if rankings list is empty (still validates envelope)",
    )
    args = parser.parse_args()
    resolved_key = (
        str(args.key).strip()
        if args.key is not None and str(args.key).strip()
        else (os.environ.get("INTERNAL_READ_KEY") or "").strip() or _load_env_key_from_dotenv()
    )

    query = urllib.parse.urlencode({"tenant_id": args.tenant_id, "limit": args.limit})
    url = f"{args.base_url.rstrip('/')}/ranking/top?{query}"

    try:
        payload = _http_get_json(url, key=resolved_key)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        print(f"[smoke] FAIL: HTTP {exc.code} for {url}")
        if body:
            print(body[:600])
        return 2
    except Exception as exc:
        print(f"[smoke] FAIL: request error: {exc}")
        return 2

    failures: list[str] = []
    for key in ("rankingKind", "notActionable", "factorVersion", "rankings"):
        if key not in payload:
            failures.append(f"payload missing {key}")

    rankings = payload.get("rankings")
    if not isinstance(rankings, list):
        failures.append("payload.rankings must be a list")
        rankings = []

    if not rankings and not args.allow_empty_rankings:
        failures.append("payload.rankings is empty (use --allow-empty-rankings to bypass)")

    for idx, row in enumerate(rankings):
        if not isinstance(row, dict):
            failures.append(f"rankings[{idx}] must be an object")
            continue
        failures.extend(_validate_row(row, idx))

    if failures:
        print("[smoke] FAIL")
        for item in failures[:50]:
            print(f" - {item}")
        if len(failures) > 50:
            print(f" - ... and {len(failures) - 50} more")
        return 1

    print("[smoke] PASS")
    print(
        json.dumps(
            {
                "rowsValidated": len(rankings),
                "rankingProvenance": payload.get("rankingProvenance"),
                "runStatus": payload.get("runStatus"),
                "runQuality": payload.get("runQuality"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

