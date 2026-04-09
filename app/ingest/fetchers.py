from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.ingest.fetch_context import FetchContext
from app.ingest.source_spec import SourceSpec


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
    if spec.fetch is None:
        raise ValueError("fetch_rows requires spec.fetch")

    kind = str(spec.fetch.kind)
    if kind == "local_file":
        file = spec.fetch.file or (spec.options.get("file") if isinstance(spec.options, dict) else None)
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
        return _select_rows(payload, rows_path=spec.fetch.rows_path)

    if kind == "http_json":
        if not spec.fetch.url:
            return []

        method = str(spec.fetch.method or "GET").upper()
        params = dict(spec.fetch.params or {})
        headers = dict(spec.fetch.headers or {})

        url = str(spec.fetch.url)
        if method == "GET" and params:
            joiner = "&" if "?" in url else "?"
            url = f"{url}{joiner}{urlencode(params)}"

        def _do() -> Any:
            req = Request(url, headers=headers, method=method)
            with urlopen(req, timeout=int(spec.fetch.timeout_s or 30)) as resp:
                body = resp.read()
            return json.loads(body.decode("utf-8"))

        payload = await asyncio.to_thread(_do)
        return _select_rows(payload, rows_path=spec.fetch.rows_path)

    if kind == "rss":
        raise NotImplementedError("rss fetch kind not implemented yet")

    raise ValueError(f"Unknown fetch kind: {kind}")

