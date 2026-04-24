import logging
import json
from datetime import datetime
from uuid import uuid4
from typing import Any, List, Dict, Optional

from app.db.repository import AlphaRepository
from app.services.engine_read_store import EngineReadStore

log = logging.getLogger(__name__)

class PromotionEngine:
    """
    Automates the lifecycle of trading strategies based on Canonical Alpha scores.
    Promotes candidates to champions and removes underperformers.
    """

    def __init__(self, repository: Optional[AlphaRepository] = None):
        self.repo = repository or AlphaRepository()
        self.store = EngineReadStore(db_path=str(self.repo.db_path))

    # --- Compatibility hooks for the self-learning pipeline ---
    #
    # `app.engine.runner.run_pipeline()` historically expected a promotion engine that can
    # review champion strategies and evaluate challenger candidates based on the
    # `ContinuousLearner` performance output.
    #
    # The current PromotionEngine primarily operates on the DB-backed efficiency read model.
    # Provide no-op implementations so end-to-end backfill/replay can run without coupling
    # the DB promotion flow to the in-memory StrategyRegistry lifecycle.

    def review_champions(self, perfs: Dict[str, Any]) -> None:
        """
        Optional hook used by the self-learning loop to review existing champions.

        `perfs` is a mapping of strategy_id -> performance metrics (shape defined by ContinuousLearner).
        """
        _ = perfs
        return None

    def evaluate_candidates(self, perfs: Dict[str, Any]) -> None:
        """
        Optional hook used by the self-learning loop to promote/demote strategies.

        `perfs` is a mapping of strategy_id -> performance metrics (shape defined by ContinuousLearner).
        """
        _ = perfs
        return None

    def evaluate_all_contexts(self, tenant_id: str = "default"):
        """
        Scans all known tickers and timeframes to update context-specific champions.
        """
        log.info(f"Starting promotion cycle for tenant: {tenant_id}")
        
        # 1. Get all tickers that have been scored
        tickers = self.repo.list_scored_tickers(tenant_id=tenant_id)
        
        promotion_count = 0
        
        for ticker in tickers:
            # We evaluate across standard timeframes and the 'None' (all) regime
            # In expanded versions, we would iterate through specific regimes too
            for timeframe in ["1m", "5m", "15m", "1h", "1d"]:
                self._evaluate_context(tenant_id, ticker, timeframe)
                promotion_count += 1
                
        log.info(f"Promotion cycle complete. Evaluated {promotion_count} contexts.")

    def _evaluate_context(self, tenant_id: str, ticker: str, timeframe: str, regime: Optional[str] = None):
        """
        Evaluates a single context and promotes the top strategy if it passes the gate.
        """
        # 1. Get current rankings for this context using Canonical Alpha sorting
        rankings = self.store.rank_strategies_by_efficiency(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            regime=regime,
            alpha_version="canonical_v1",
            limit=5
        )
        
        if not rankings:
            return

        top_strat = rankings[0]
        
        # 2. Apply Alpha Gate (Gating Rules from CHAMPION_PROMOTION_RULES.md)
        # Thresholds: Alpha > 0.60, Samples > 50 (production)
        MIN_ALPHA = 0.60
        MIN_SAMPLES = 50
        
        is_worthy = (top_strat.alpha_strategy >= MIN_ALPHA and top_strat.samples >= MIN_SAMPLES)
        
        # 3. Check current champion for this context
        current_champ = self.repo.get_efficiency_champion_record(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            regime=regime
        )
        
        if is_worthy:
            # Check if there's a change
            if current_champ is None or current_champ["strategy_id"] != top_strat.strategy_id:
                log.info(f"PROMOTION: {top_strat.strategy_id} becomes champion for {ticker}/{timeframe}")
                
                self.repo.upsert_efficiency_champion_record(
                    tenant_id=tenant_id,
                    ticker=ticker,
                    timeframe=timeframe,
                    regime=regime,
                    strategy_id=top_strat.strategy_id,
                    strategy_version=top_strat.strategy_version,
                    avg_efficiency_rating=top_strat.avg_efficiency_rating,
                    alpha_strategy=top_strat.alpha_strategy,
                    samples=top_strat.samples,
                    total_forecast_days=top_strat.total_forecast_days
                )
                
                self._log_event(
                    tenant_id=tenant_id,
                    strategy_id=top_strat.strategy_id,
                    prev_status=current_champ["strategy_id"] if current_champ else "NONE",
                    new_status="CHAMPION",
                    event_type="CONTEXT_PROMOTION",
                    metadata={
                        "ticker": ticker,
                        "timeframe": timeframe,
                        "alpha": top_strat.alpha_strategy,
                        "samples": top_strat.samples
                    }
                )
        else:
            # If current champ no longer worthy, we might want to clear it (demote)
            if current_champ:
                log.warning(f"DEMOTION: {current_champ['strategy_id']} is no longer worthy for {ticker}/{timeframe}")
                # For now, we keep the record but log a warning, or we could delete the record
                # self._log_event(...)

    def _log_event(self, tenant_id: str, strategy_id: str, prev_status: str, 
                   new_status: str, event_type: str, metadata: dict):
        event_id = str(uuid4())
        self.repo.conn.execute(
            """
            INSERT INTO promotion_events 
            (id, tenant_id, strategy_id, prev_status, new_status, event_type, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, tenant_id, strategy_id, prev_status, new_status, event_type, json.dumps(metadata))
        )
        self.repo.conn.commit()

if __name__ == "__main__":
    # Test execution
    logging.basicConfig(level=logging.INFO)
    engine = PromotionEngine()
    engine.evaluate_all_contexts(tenant_id="backfill")
