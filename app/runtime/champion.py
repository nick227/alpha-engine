from __future__ import annotations

from typing import Any, Dict


class ChampionRegistry:
    """
    Registry for tracking champion strategies from sentiment and quant tracks.
    
    Canonical implementation - replaces app/engine/champion_registry.py and app/intelligence/champion_registry.py
    """

    def __init__(self):
        self.sentiment = None
        self.quant = None

    def update(self, sentiment: Dict[str, Any] | None, quant: Dict[str, Any] | None) -> None:
        """Update the current champions for each track."""
        self.sentiment = sentiment
        self.quant = quant

    def snapshot(self) -> Dict[str, Any]:
        """Get the current champion snapshot."""
        return {
            "sentiment_champion": self.sentiment,
            "quant_champion": self.quant,
            "timestamped": True,
        }

    def get_sentiment_champion(self) -> Dict[str, Any] | None:
        """Get the current sentiment champion."""
        return self.sentiment

    def get_quant_champion(self) -> Dict[str, Any] | None:
        """Get the current quant champion."""
        return self.quant

    def clear(self) -> None:
        """Clear all champions."""
        self.sentiment = None
        self.quant = None

    def is_empty(self) -> bool:
        """Check if both champions are empty."""
        return self.sentiment is None and self.quant is None

    def has_sentiment_champion(self) -> bool:
        """Check if sentiment champion exists."""
        return self.sentiment is not None

    def has_quant_champion(self) -> bool:
        """Check if quant champion exists."""
        return self.quant is not None


# Legacy function for backward compatibility
def champion_snapshot(sentiment: Dict[str, Any] | None, quant: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Legacy function - creates a champion snapshot without state.
    
    Deprecated: Use ChampionRegistry class for new code.
    """
    return {
        "sentiment_champion": sentiment,
        "quant_champion": quant,
        "timestamped": True,
    }
