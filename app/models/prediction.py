
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Prediction:
    id: str
    ticker: str
    strategy_id: str
    score: float
    horizon_minutes: int
    created_at: datetime
