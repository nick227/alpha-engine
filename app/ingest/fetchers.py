from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.ingest.fetch_context import FetchContext
from app.ingest.source_spec import SourceSpec


def _coerce_fetch_kind(fetch: Any) -> str:
    """
    Accept either a FetchSpec model or a dict payload and infer kind when omitted.

    Many adapters pass a minimal payload like {"url": "...", "params": {...}}.
    """
    if isinstance(fetch, dict):
        kind = str(fetch.get("kind") or "").strip()
        if kind:
            return kind
        if fetch.get("file") and not fetch.get("url"):
            return "local_file"
        return "http_json"
    # Pydantic model
    kind = str(getattr(fetch, "kind", "") or "").strip()
    return kind or "http_json"


def _get_fetch_field(fetch: Any, key: str, default: Any = None) -> Any:
    if isinstance(fetch, dict):
        return fetch.get(key, default)
    return getattr(fetch, key, default)


def _select_rows(payload: Any, *, rows_path: str | None) -> list[dict[str, Any]]:
    if rows_path is None or str(rows_path).strip() == "":
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            # Common pattern: a dict response that is itself a row.
            return [payload]
        return []

    cur: Any = payload
    for part in str(rows_path).split("."):
        key = part.strip()
        if not key:
            continue
        if isinstance(cur, dict):
            cur = cur.get(key)
        else:
            return []

    if isinstance(cur, list):
        return [r for r in cur if isinstance(r, dict)]
    if isinstance(cur, dict):
        return [cur]
    return []


async def fetch_rows(spec: SourceSpec, ctx: FetchContext) -> list[dict[str, Any]]:
    """
    Shared declarative fetch stage.

    Keep fetch kinds small:
    - local_file: JSON list of dict rows
    - http_json: JSON response (optional rows_path selection)
    - rss: reserved (not implemented yet)
    """
    fetch = getattr(spec, "fetch", None)
    if fetch is None:
        # Allow callers to pass a bare fetch payload dict directly.
        if isinstance(spec, dict):
            fetch = spec
        else:
            raise ValueError("fetch_rows requires spec.fetch (or a fetch dict)")

    kind = _coerce_fetch_kind(fetch)
    if kind == "local_file":
        options = getattr(spec, "options", {}) if not isinstance(spec, dict) else {}
        file = _get_fetch_field(fetch, "file") or (options.get("file") if isinstance(options, dict) else None)
        if not file:
            return []
        path = Path(str(file))
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            return []
        raw = path.read_text(encoding="utf-8")
        try:
            payload = json.loads(raw)
        except Exception:
            return []
        return _select_rows(payload, rows_path=_get_fetch_field(fetch, "rows_path"))

    if kind == "http_json":
        url_val = _get_fetch_field(fetch, "url")
        if not url_val:
            return []

        method = str(_get_fetch_field(fetch, "method", "GET") or "GET").upper()
        params = dict(_get_fetch_field(fetch, "params", {}) or {})
        headers = dict(_get_fetch_field(fetch, "headers", {}) or {})

        url = str(url_val)
        if method == "GET" and params:
            joiner = "&" if "?" in url else "?"
            url = f"{url}{joiner}{urlencode(params)}"

        def _do() -> Any:
            req = Request(url, headers=headers, method=method)
            with urlopen(req, timeout=int(_get_fetch_field(fetch, "timeout_s", 30) or 30)) as resp:
                body = resp.read()
            return json.loads(body.decode("utf-8"))

        payload = await asyncio.to_thread(_do)
        return _select_rows(payload, rows_path=_get_fetch_field(fetch, "rows_path"))

    if kind == "rss":
        raise NotImplementedError("rss fetch kind not implemented yet")

    raise ValueError(f"Unknown fetch kind: {kind}")
