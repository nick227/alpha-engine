from __future__ import annotations

import os
from pathlib import Path

from app.cli.start import main


def _load_dotenv_min(dotenv_path: str | os.PathLike = ".env") -> None:
    """
    Minimal `.env` loader so `python start.py` works even when python-dotenv isn't installed.

    - Supports `KEY=VALUE` with optional single/double quotes
    - Ignores blank lines and `#` comments
    - Does not overwrite existing environment variables
    """
    path = Path(dotenv_path)
    if not path.exists():
        return

    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if (len(value) >= 2) and ((value[0] == value[-1]) and value[0] in {"'", '"'}):
            value = value[1:-1]
        os.environ[key] = value


# Best-effort load .env early for interactive runs.
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(override=False)
except Exception:
    _load_dotenv_min()


if __name__ == "__main__":
    raise SystemExit(main())
