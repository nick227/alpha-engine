"""
Resolve paths for `data/raw_dumps/full_history/{SYMBOL}.csv` with ticker renames.

Some archives use older listing symbols (e.g. FB) while configs use current tickers (META).
"""
from __future__ import annotations

from pathlib import Path
from typing import Final

# Requested ticker -> filename stem on disk
FULL_HISTORY_CSV_ALIASES: Final[dict[str, str]] = {"META": "FB"}


def resolve_full_history_csv_path(dump_dir: Path, symbol: str) -> Path | None:
    """Return path to an existing CSV for *symbol*, applying known filename aliases."""
    u = symbol.upper().strip()
    direct = dump_dir / f"{u}.csv"
    if direct.is_file():
        return direct
    alt = FULL_HISTORY_CSV_ALIASES.get(u)
    if alt:
        p = dump_dir / f"{alt}.csv"
        if p.is_file():
            return p
    return None
