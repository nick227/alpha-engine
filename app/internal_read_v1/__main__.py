"""Run: python -m app.internal_read_v1 (127.0.0.1 — see INTERNAL_READ_PORT)."""

from __future__ import annotations

import os

import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("INTERNAL_READ_PORT", "8090"))
    uvicorn.run(
        "app.internal_read_v1.app:app",
        host="127.0.0.1",
        port=port,
        factory=False,
    )
