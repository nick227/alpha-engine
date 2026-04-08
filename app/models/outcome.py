
from dataclasses import dataclass

@dataclass
class Outcome:
    prediction_id: str
    return_pct: float
    correct: bool
