from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True, slots=True)
class MacroSeriesSpec:
    name: str
    symbol: str
    provider: str = "yfinance"


def load_macro_series_specs(path: str | Path = "config/macro_sources.yaml") -> list[MacroSeriesSpec]:
    p = Path(path)
    if not p.exists():
        return []

    raw: Any
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception:
        return []

    if not isinstance(raw, dict):
        return []

    root = raw.get("macro_sources")
    if not isinstance(root, dict):
        return []

    yf = root.get("yfinance")
    if not isinstance(yf, list):
        return []

    out: list[MacroSeriesSpec] = []
    for row in yf:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip().lower()
        symbol = str(row.get("symbol") or "").strip()
        if not name or not symbol:
            continue
        out.append(MacroSeriesSpec(name=name, symbol=symbol, provider="yfinance"))

    return out

