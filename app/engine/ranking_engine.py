from __future__ import annotations

import numpy as np
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dataclasses import asdict

from app.core.types import TargetRanking, SignalScore
from app.core.active_universe import get_active_universe_tickers
from app.db.repository import AlphaRepository


class RankingEngine:
    """
    Autonomous Dynamic Target Ranking Engine
    Synthesizes multiple signal tracks into a single conviction score.
    """

    # Regime-aware weight matrix [signals x regimes]
    # Volatility Regimes: LOW, NORMAL, HIGH
    WEIGHT_MATRIX = {
        "LOW": {
            "sentiment": 0.20,
            "macro": 0.30,
            "drift": 0.10,
            "momentum": 0.40
        },
        "NORMAL": {
            "sentiment": 0.40,
            "macro": 0.20,
            "drift": 0.20,
            "momentum": 0.20
        },
        "HIGH": {
            "sentiment": 0.60,
            "macro": 0.10,
            "drift": 0.25,
            "momentum": 0.05
        }
    }

    # Signal half-lives (in minutes) for staleness decay
    SIGNAL_HALF_LIVES = {
        "sentiment": 60,    # 1 hour
        "macro": 1440,     # 1 day
        "drift": 15,       # 15 minutes
        "momentum": 30     # 30 minutes
    }

    def __init__(self, repository: Optional[AlphaRepository] = None) -> None:
        self.repository = repository or AlphaRepository()

    def _get_decay_factor(self, signal_name: str, timestamp: datetime) -> float:
        """Compute exponential decay based on signal age"""
        now = datetime.now(timezone.utc)
        age_minutes = (now - timestamp).total_seconds() / 60.0
        half_life = self.SIGNAL_HALF_LIVES.get(signal_name, 60)
        return float(np.exp(-np.log(2) * age_minutes / half_life))

    def _collect_signals(self, ticker: str) -> List[SignalScore]:
        """
        Collect latest signals for a ticker.
        In a real scenario, this would query the DB for the latest events/metrics.
        """
        signals: List[SignalScore] = []
        now = datetime.now(timezone.utc)

        # 1. Sentiment Signal
        # Query latest scored event for this ticker
        try:
            # Mocking signal collection logic - in production this queries AlphaRepository
            # For brevity in this implementation, we simulate the retrieval
            sentiment_val = 0.5 # Default neutral
            sentiment_conf = 0.5
            
            # TODO: self.repository.get_latest_scored_event(ticker)
            signals.append(SignalScore(
                name="sentiment",
                value=sentiment_val,
                timestamp=now,
                confidence=sentiment_conf
            ))
        except Exception:
            pass

        # 2. Macro Sensitivity
        # Simplified: Higher sensitivity for mega-caps
        mega_caps = {"NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA"}
        macro_val = 0.8 if ticker.upper() in mega_caps else 0.4
        signals.append(SignalScore(
            name="macro",
            value=macro_val,
            timestamp=now, # Macro changes slowly, but we treat current knowledge as 'now'
            confidence=1.0
        ))

        # 3. News Drift (MRA continuation slope)
        signals.append(SignalScore(
            name="drift",
            value=0.0, # Placeholder
            timestamp=now,
            confidence=0.5
        ))

        # 4. Momentum (Technical trend)
        signals.append(SignalScore(
            name="momentum",
            value=0.0, # Placeholder
            timestamp=now,
            confidence=0.5
        ))

        return signals

    def compute_ranking(self, tenant_id: str = "default") -> List[TargetRanking]:
        """
        Compute rankings for all enabled Target Stocks.
        """
        tickers = get_active_universe_tickers(repository=self.repository)
        if not tickers:
            return []

        # 1. Determine current global regime
        # In a real scenario, we'd get this from RegimeManager
        regime = "NORMAL" # Fallback
        
        # 2. Collect raw signal table [Ticker x Signal]
        raw_data = []
        for ticker in tickers:
            signals = self._collect_signals(ticker)
            row = {"ticker": ticker}
            for s in signals:
                decayed_value = s.value * self._get_decay_factor(s.name, s.timestamp)
                row[s.name] = decayed_value
            raw_data.append(row)

        df = pd.DataFrame(raw_data).set_index("ticker")

        # 3. Normalize signals (Z-Score across universe)
        # Avoid division by zero if std is 0
        norm_df = (df - df.mean()) / df.std().replace(0, 1.0)
        norm_df = norm_df.fillna(0.0) # Handle NaN if universe is size 1

        # 4. Apply weight matrix
        weights = self.WEIGHT_MATRIX.get(regime, self.WEIGHT_MATRIX["NORMAL"])
        
        rankings: List[TargetRanking] = []
        now = datetime.now(timezone.utc)

        for ticker in tickers:
            ticker_signals = norm_df.loc[ticker]
            
            composite_score = 0.0
            attribution = {}
            
            for sig_name, sig_weight in weights.items():
                if sig_name in ticker_signals:
                    contribution = float(ticker_signals[sig_name] * sig_weight)
                    composite_score += contribution
                    attribution[sig_name] = round(contribution, 4)
            
            # Conviction is the absolute magnitude of the composite score
            conviction = float(abs(composite_score))
            
            rankings.append(TargetRanking(
                ticker=ticker,
                score=round(float(composite_score), 4),
                conviction=round(conviction, 4),
                attribution=attribution,
                regime=regime,
                timestamp=now,
                tenant_id=tenant_id
            ))

        # Sort by score descending
        return sorted(rankings, key=lambda x: x.score, reverse=True)

    def recompute(self, tenant_id: str = "default") -> List[TargetRanking]:
        """Recompute and persist rankings"""
        rankings = self.compute_ranking(tenant_id)
        if rankings:
            self.repository.save_target_ranking(rankings, tenant_id)
        return rankings
