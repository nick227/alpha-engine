from __future__ import annotations

from pathlib import Path

import pytest

from app.ingest.adapter_helpers import fetch_json
from app.ingest.fetch_context import FetchContext
from app.ingest.key_manager import KeyManager
from app.ingest.rate_limit import RateLimiter
from app.ingest.source_spec import SourceSpec


def _ctx() -> FetchContext:
    return FetchContext(
        provider="test",
        key_manager=KeyManager(),
        rate_limiter=RateLimiter("test"),
        cache_handle={},
    )


def test_fetch_json_local_file(tmp_path: Path) -> None:
    p = tmp_path / "rows.json"
    p.write_text('[{"a": 1}, {"b": 2}]', encoding="utf-8")

    spec = SourceSpec(
        id="t",
        type="news",
        adapter="x",
        enabled=True,
        priority=1,
        fetch={"kind": "local_file", "file": str(p)},
    )

    rows = __import__("asyncio").run(fetch_json(spec, _ctx()))
    assert rows == [{"a": 1}, {"b": 2}]


def test_fetch_json_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    async def _fake_fetch_rows(_spec, _ctx):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RuntimeError("boom")
        return [{"ok": True}]

    import app.ingest.adapter_helpers as helpers

    monkeypatch.setattr(helpers.fetchers, "fetch_rows", _fake_fetch_rows)

    spec = SourceSpec(
        id="t",
        type="news",
        adapter="x",
        enabled=True,
        priority=1,
        fetch={"kind": "http_json", "url": "http://example.invalid", "timeout_s": 1},
    )

    rows = __import__("asyncio").run(fetch_json(spec, _ctx(), retries=2))
    assert rows == [{"ok": True}]
    assert calls["n"] == 3

