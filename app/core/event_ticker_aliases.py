"""
Raw `stock` values in analyst/partner CSVs vs configured tickers (e.g. FB vs META).
"""
from __future__ import annotations

from typing import Final

# Requested symbol -> acceptable raw `stock` values (uppercase)
EVENT_STOCK_ALIASES: Final[dict[str, frozenset[str]]] = {
    "META": frozenset({"META", "FB"}),
    "FB": frozenset({"META", "FB"}),
}
