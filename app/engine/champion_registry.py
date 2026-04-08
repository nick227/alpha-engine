from __future__ import annotations


def champion_snapshot(sentiment: dict | None, quant: dict | None) -> dict:
    return {
        "sentiment_champion": sentiment,
        "quant_champion": quant,
        "timestamped": True,
    }

