"""
Map discovery strategy types to high-level "lenses" (multi-bucket, diversity-friendly).

Ranking does not use these; they tag candidate_queue rows for admission and analysis.
"""

from __future__ import annotations

# strategy_type -> discovery_lens
STRATEGY_DEFAULT_LENS: dict[str, str] = {
    "realness_repricer": "undervalued",
    "narrative_lag": "undervalued",
    "silent_compounder": "small_cap_asymmetry",
    "balance_sheet_survivor": "blue_chip_stability",
    "ownership_vacuum": "sector_pattern",
    "sniper_coil": "top_signal",
    "volatility_breakout": "top_signal",
}


def default_lens_for_strategy(strategy_type: str) -> str:
    return STRATEGY_DEFAULT_LENS.get(str(strategy_type).strip(), "top_signal")
