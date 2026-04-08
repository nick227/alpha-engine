from __future__ import annotations

from app.engine.runner import Runner


class HybridDualTrackV27Strategy:
    name = "hybrid_dual_track_v2_7"

    def __init__(self) -> None:
        self.runner = Runner()

    def predict(
        self,
        ticker: str,
        sentiment_direction: str,
        sentiment_confidence: float,
        quant_direction: str,
        quant_confidence: float,
        realized_volatility: float,
        historical_volatility_window: list[float],
        adx_value: float | None = None,
    ) -> dict:
        return self.runner.build_prediction(
            ticker=ticker,
            sentiment_direction=sentiment_direction,
            sentiment_confidence=sentiment_confidence,
            quant_direction=quant_direction,
            quant_confidence=quant_confidence,
            realized_volatility=realized_volatility,
            historical_volatility_window=historical_volatility_window,
            adx_value=adx_value,
        )
