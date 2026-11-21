"""Configurable operational (inference) universe — kept separate from ML training data."""
from __future__ import annotations

import os
import re
from pathlib import Path

_CONFIG = Path(__file__).resolve().parents[2] / "config" / "live_universe.yaml"
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def live_universe() -> list[str]:
    raw = os.environ.get("ALPHA_LIVE_UNIVERSE", "").strip()
    if raw:
        symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    else:
        import yaml

        data = yaml.safe_load(_CONFIG.read_text(encoding="utf-8")) or {}
        symbols = [str(s).strip().upper() for s in data.get("symbols") or []]
    bad = [s for s in symbols if not _SYMBOL.match(s)]
    if bad or not symbols:
        raise ValueError(f"invalid live universe: {bad or 'empty'}")
    return sorted(set(symbols))
