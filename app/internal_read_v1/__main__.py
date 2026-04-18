"""Run: python -m app.internal_read_v1 — see INTERNAL_READ_HOST / INTERNAL_READ_PORT / PORT."""

from __future__ import annotations

import os

import uvicorn

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def _listen_host() -> str:
    # Local default: loopback only. Railway/production: set INTERNAL_READ_HOST=0.0.0.0
    return os.environ.get("INTERNAL_READ_HOST", "127.0.0.1")


def _listen_port() -> int:
    # Railway injects PORT; INTERNAL_READ_PORT overrides when set explicitly.
    raw = os.environ.get("INTERNAL_READ_PORT") or os.environ.get("PORT") or "8090"
    return int(raw)


if __name__ == "__main__":
    uvicorn.run(
        "app.internal_read_v1.app:app",
        host=_listen_host(),
        port=_listen_port(),
        factory=False,
    )
