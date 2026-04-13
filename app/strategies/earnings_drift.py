"""
Earnings Drift (PEAD) strategy.

Fires the morning after an earnings announcement when the EPS surprise is
large enough and the day-of price action confirms the surprise direction.

Signal: surprise_z (cross-sectional z-score of EPS surprise magnitude)
Hold:   5 trading days
Filter: |surprise_z| >= tail_threshold (default Q1/Q4 quartile)
        price confirmation (return_1d same sign as surprise)
        size >= Q3 (injected via raw_event metadata)

Confidence is clamped to [0.30, 0.95] and scales linearly with abs(surprise_z).
"""
from __future__ import annotations

from uuid import uuid4

from app.core.types import MRAOutcome, Prediction, ScoredEvent
from app.strategies.base import StrategyBase, StrategyConfig

# Default tail threshold — Q1/Q4 quartile boundary.
# Cross-sectional z-score at the first/fourth quartile boundary is typically ~0.67
# (half-sigma), but empirically the PEAD IC is driven by the tails (validated at
# Q1+Q4 split). 0.67 keeps ~50 % of events which is the quartile split intent.
_DEFAULT_TAIL_THRESHOLD = 0.67

# Confidence range
_CONF_MIN = 0.30
_CONF_MAX = 0.95

# Max |surprise_z| used for confidence scaling (saturation point)
_Z_SATURATION = 3.0

# Minimum size quintile (1-indexed): Q3 = 3
_MIN_SIZE_QUINTILE = 3


def _size_quintile_int(size_q: str | int | None) -> int:
    """Parse 'Q3 mid' or '3' or 3 → integer quintile number."""
    if size_q is None:
        return 0
    if isinstance(size_q, int):
        return size_q
    s = str(size_q).strip()
    if s and s[0] == "Q" and s[1:2].isdigit():
        return int(s[1])
    if s[:1].isdigit():
        return int(s[:1])
    return 0


class EarningsDriftStrategy(StrategyBase):
    """Post-Earnings Announcement Drift (PEAD) strategy."""

    def __init__(self, config: StrategyConfig) -> None:
        super().__init__(config)
        cfg = config.config or {}
        self.tail_threshold: float = float(cfg.get("tail_threshold", _DEFAULT_TAIL_THRESHOLD))
        self.min_size_quintile: int = int(cfg.get("min_size_quintile", _MIN_SIZE_QUINTILE))
        self.horizon: str = str(cfg.get("horizon", "5d"))
        self.require_confirmation: bool = bool(cfg.get("require_confirmation", True))

    # ------------------------------------------------------------------
    # StrategyBase interface
    # ------------------------------------------------------------------

    def maybe_predict(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        price_context: dict,
        event_timestamp,
    ) -> Prediction | None:

        # Gate 1: earnings event only (metadata injected by runner from raw.metadata)
        if not price_context.get("earnings_announcement"):
            return None

        # Gate 2: surprise_z must exceed tail threshold
        surprise_z = price_context.get("surprise_z")
        if surprise_z is None:
            return None
        try:
            surprise_z = float(surprise_z)
        except (TypeError, ValueError):
            return None
        if abs(surprise_z) < self.tail_threshold:
            return None

        # Gate 3: size quintile filter (Q3+)
        size_q = price_context.get("size_quintile")
        if _size_quintile_int(size_q) < self.min_size_quintile:
            return None

        # Gate 4: price confirmation filter
        # Announcement-day return must agree with surprise direction.
        if self.require_confirmation:
            return_1d = price_context.get("return_1d")
            if return_1d is not None:
                try:
                    r1d = float(return_1d)
                    # Positive surprise must have positive day-of return (and vice versa)
                    if surprise_z > 0 and r1d < 0:
                        return None
                    if surprise_z < 0 and r1d > 0:
                        return None
                except (TypeError, ValueError):
                    pass  # missing return_1d — proceed without confirmation filter

        # Direction
        direction = "up" if surprise_z > 0 else "down"

        # Confidence: linear scale from _CONF_MIN at threshold to _CONF_MAX at saturation
        z_abs = abs(surprise_z)
        z_range = max(_Z_SATURATION - self.tail_threshold, 1e-6)
        raw_conf = (z_abs - self.tail_threshold) / z_range
        confidence = _CONF_MIN + raw_conf * (_CONF_MAX - _CONF_MIN)
        confidence = max(_CONF_MIN, min(_CONF_MAX, confidence))

        entry_price = float(price_context.get("entry_price", 100.0))

        return Prediction(
            id=str(uuid4()),
            strategy_id=self.config.id,
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=event_timestamp,
            prediction=direction,  # type: ignore[arg-type]
            confidence=round(confidence, 4),
            horizon=self.horizon,
            entry_price=entry_price,
            mode=self.config.mode,
            feature_snapshot={
                "surprise_z": round(surprise_z, 4),
                "size_quintile": str(size_q or ""),
                "return_1d": price_context.get("return_1d"),
                "eps_actual": price_context.get("eps_actual"),
                "eps_estimated": price_context.get("eps_estimated"),
                "surprise_raw": price_context.get("surprise_raw"),
                "family": "event",
                "signal": "pead",
            },
        )
