from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from app.core.types import Prediction, PredictionDirection, ScoredEvent, MRAOutcome
from app.strategies.base import StrategyBase


class TextMRAStrategy(StrategyBase):
    def maybe_predict(self, scored_event: ScoredEvent, mra: MRAOutcome, price_context: dict, event_timestamp: datetime) -> Prediction | None:
        cfg = self.config.config
        allowed_categories = set(cfg.get("required_categories", []))
        if allowed_categories and scored_event.category not in allowed_categories:
            return None
        if scored_event.materiality < cfg.get("min_materiality", 0.0):
            return None
        if scored_event.company_relevance < cfg.get("min_company_relevance", 0.0):
            return None
        if mra.mra_score < cfg.get("min_mra_score", 0.0):
            return None

        direction: PredictionDirection = "flat"
        if scored_event.direction == "positive":
            direction = "up"
        elif scored_event.direction == "negative":
            direction = "down"

        confidence = max(0.0, min(
            cfg.get("text_weight", 0.6) * scored_event.confidence +
            cfg.get("mra_weight", 0.4) * mra.mra_score,
            1.0,
        ))

        return Prediction(
            id=str(uuid4()),
            strategy_id=self.config.id,
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=event_timestamp,
            prediction=direction,
            confidence=confidence,
            horizon=cfg.get("horizon", "15m"),
            entry_price=float(price_context.get("entry_price", 100.0)),
            mode=self.config.mode,
            feature_snapshot={
                "category": scored_event.category,
                "materiality": scored_event.materiality,
                "mra_score": mra.mra_score,
            },
        )
