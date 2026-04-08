from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ConsensusSignal:
    ticker: str
    regime: str
    sentiment_strategy_id: str
    quant_strategy_id: str
    sentiment_score: float
    quant_score: float
    ws: float
    wq: float
    agreement_bonus: float
    p_final: float
    stability_score: float
