from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from functools import lru_cache

from app.ui.middle.engine_read_store import (
    ChampionRow,
    ChampionMatrixRow,
    ConsensusRow,
    EngineReadStore,
    LoopHealthRow,
    SignalRow,
    RankingSnapshotRow,
    StrategyEfficiencyRow,
    StrategyTimelineRow,
    PredictionRunRow,
    SeriesPointRow,
    PredictionScoreRow,
)
from app.core.target_stocks import get_target_stocks, get_target_stocks_registry, load_target_stock_specs


@dataclass(frozen=True)
class ChampionView:
    strategy_id: str
    track: str
    win_rate: float
    alpha: float
    stability: float
    confidence_weight: float


@dataclass(frozen=True)
class ConsensusView:
    ticker: str
    direction: str
    confidence: float
    total_weight: float
    participating_strategies: int
    active_regime: str | None
    high_vol_strength: float | None
    low_vol_strength: float | None
    trust: float | None = None


@dataclass(frozen=True)
class LoopHealthView:
    last_write_at: str | None
    signal_rate_per_min: float | None
    consensus_rate_per_min: float | None
    learner_update_rate_per_min: float | None
    heartbeats: list["LoopHeartbeatView"]


@dataclass(frozen=True)
class LoopHeartbeatView:
    loop_type: str
    status: str
    last_heartbeat_at: str
    notes: str | None = None


@dataclass(frozen=True)
class SignalView:
    time: str
    ticker: str
    direction: str
    strategy: str
    regime: str | None
    confidence: float
    trust: float | None = None


@dataclass(frozen=True)
class RankingView:
    ticker: str
    score: float
    conviction: float
    attribution: dict[str, float]
    regime: str
    timestamp: str


@dataclass(frozen=True)
class StrategyEfficiencyView:
    strategy_id: str
    strategy_version: str | None
    forecast_days: int
    samples: int
    total_forecast_days: int
    avg_efficiency_rating: float
    alpha_strategy: float
    win_rate: float
    avg_return: float
    drawdown: float
    stability: float


@dataclass(frozen=True)
class ChampionSummary:
    """Typed champion summary for horizon-based champions"""
    horizon: int
    strategy_id: str
    efficiency: float
    alpha: float
    samples: int


@dataclass(frozen=True)
class IntelligenceStateData:
    """Typed return value for cached intelligence state data"""
    matrix: list[ChampionMatrixView]
    rankings: list[StrategyEfficiencyView]
    overlay_series: dict | None
    timeline: list[StrategyTimelineView] | None
    consensus: ConsensusView | None
    champions: list[ChampionSummary]


@dataclass(frozen=True)
class PredictionRunView:
    id: str
    label: str
    timeframe: str
    created_at: str


@dataclass(frozen=True)
class SeriesPointView:
    timestamp: str
    value: float


@dataclass(frozen=True)
class ScoreView:
    strategy_id: str
    ticker: str
    efficiency_rating: float
    hit_rate: float
    sync_rate: float
    return_error: float
    alpha_prediction: float
    attribution: dict[str, float]


@dataclass(frozen=True)
class ChampionMatrixView:
    """One cell in the ticker × strategy comparison matrix, ready for the UI."""
    ticker: str
    timeframe: str
    forecast_days: int
    strategy_id: str
    regime: str
    alpha_strategy: float
    avg_pred_return_pct: float   # e.g. 0.038 = +3.8%
    avg_actual_return_pct: float
    direction_accuracy: float    # 0.0–1.0
    entry_price: float | None
    target_price: float | None   # entry * (1 + avg_pred_return)
    samples: int


@dataclass(frozen=True)
class StrategyTimelineView:
    """One historical snapshot for the strategy autopsy chart."""
    run_date: str
    ticker: str
    strategy_id: str
    forecast_days: int
    alpha_prediction: float
    pred_return_pct: float
    actual_return_pct: float
    direction_correct: bool
    entry_price: float | None
    target_price: float | None


@dataclass(frozen=True)
class PredictionAnalyticsQuery:
    tenant_id: str = "default"
    run_id: str | None = None
    ticker: str | None = None
    strategy_id: str | None = None


@dataclass(frozen=True)
class PredictionAnalyticsResult:
    run_view: PredictionRunView | None
    chart_card: dict[str, Any] | None
    metric_cards: list[dict[str, Any]]
    leaderboard_card: dict[str, Any] | None
    details_table_card: dict[str, Any] | None


def arrow(direction: str) -> str:
    d = str(direction).strip().lower()
    if d in ("up", "long", "buy", "1", "+1"):
        return "↑"
    if d in ("down", "short", "sell", "-1"):
        return "↓"
    return "→"


class DashboardService: 
    """
    Stable read-model API for Streamlit.

    Architecture:
      Streamlit -> DashboardService -> EngineReadStore (SQL) -> alpha.db

    Rule: UI remains read-only and never queries the DB directly.
    """

    def __init__(self, db_path: str | Path = "data/alpha.db") -> None:
        self.store = EngineReadStore(db_path=db_path)

    def close(self) -> None:
        self.store.close()

    def list_tenants(self) -> list[str]:
        return self.store.list_tenants()

    def list_tickers(self, *, tenant_id: str = "default") -> list[str]:
        """Get available tickers for the specified tenant"""
        try:
            # Try tenant-aware target stocks first
            return get_target_stocks(tenant_id=tenant_id)
        except Exception:
            # Fallback to store-based ticker listing
            return self.store.list_tickers(tenant_id=tenant_id)

    def get_target_stocks_panel(self, *, asof: str | None = None) -> dict:
        """
        UI helper: return Target Stocks metadata for display.
        """
        reg = get_target_stocks_registry()
        specs = load_target_stock_specs()
        active = set(get_target_stocks(asof=asof))
        rows = []
        for s in sorted(specs, key=lambda x: x.symbol):
            rows.append(
                {
                    "symbol": s.symbol,
                    "enabled": bool(s.enabled),
                    "active": (s.symbol in active),
                    "group": s.group,
                    "active_from": (s.active_from.isoformat() if s.active_from else None),
                }
            )
        return {"target_universe_version": reg.target_universe_version, "rows": rows}

    @staticmethod
    def _champion_view(row: ChampionRow) -> ChampionView:
        return ChampionView(
            strategy_id=row.strategy_id,
            track=row.track,
            win_rate=row.win_rate,
            alpha=row.alpha,
            stability=row.stability,
            confidence_weight=row.confidence_weight,
        )

    def get_champions(self, *, tenant_id: str = "default", min_predictions: int = 5) -> dict[str, ChampionView]:
        rows = self.store.get_champions(tenant_id=tenant_id, min_predictions=min_predictions)
        return {k: self._champion_view(v) for k, v in rows.items()}

    def get_challengers(self, *, tenant_id: str = "default", min_predictions: int = 5) -> dict[str, ChampionView]:
        rows = self.store.get_challengers(tenant_id=tenant_id, min_predictions=min_predictions)
        return {k: self._champion_view(v) for k, v in rows.items()}

    @staticmethod
    def _consensus_view(row: ConsensusRow) -> ConsensusView:
        return ConsensusView(
            ticker=row.ticker,
            direction=row.direction,
            confidence=row.confidence,
            total_weight=row.total_weight,
            participating_strategies=row.participating_strategies,
            active_regime=row.active_regime,
            high_vol_strength=row.high_vol_strength,
            low_vol_strength=row.low_vol_strength,
            trust=row.trust,
        )

    def get_latest_consensus(self, *, tenant_id: str = "default", ticker: str) -> ConsensusView | None: 
        row = self.store.get_latest_consensus(tenant_id=tenant_id, ticker=ticker) 
        return None if row is None else self._consensus_view(row) 

    def get_consensus_by_horizon(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        horizons: list[str],
    ) -> dict[str, ConsensusView | None]:
        rows = self.store.get_latest_consensus_by_horizon(tenant_id=tenant_id, ticker=ticker, horizons=horizons)
        return {h: (None if r is None else self._consensus_view(r)) for h, r in rows.items()}

    @staticmethod
    def _signal_view(row: SignalRow) -> SignalView:
        return SignalView(
            time=row.time,
            ticker=row.ticker,
            direction=row.direction,
            strategy=row.strategy,
            regime=row.regime,
            confidence=row.confidence,
            trust=row.trust,
        )

    def get_recent_signals(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
        limit: int = 25,
    ) -> list[SignalView]:
        rows = self.store.get_recent_signals(tenant_id=tenant_id, ticker=ticker, limit=limit)
        return [self._signal_view(r) for r in rows]

    @staticmethod
    def _heartbeat_view(row: LoopHealthRow) -> LoopHeartbeatView:
        return LoopHeartbeatView(
            loop_type=row.loop_type,
            status=row.status,
            last_heartbeat_at=row.last_heartbeat_at,
            notes=row.notes,
        )

    def get_loop_health(self, *, tenant_id: str = "default") -> LoopHealthView:
        summary = self.store.get_loop_health(tenant_id=tenant_id)
        return LoopHealthView(
            last_write_at=summary.last_write_at,
            signal_rate_per_min=summary.signal_rate_per_min,
            consensus_rate_per_min=summary.consensus_rate_per_min,
            learner_update_rate_per_min=summary.learner_update_rate_per_min,
            heartbeats=[self._heartbeat_view(hb) for hb in summary.heartbeats],
        )

    @staticmethod
    def _ranking_view(row: RankingSnapshotRow) -> RankingView:
        return RankingView(
            ticker=row.ticker,
            score=row.score,
            conviction=row.conviction,
            attribution=row.attribution,
            regime=row.regime,
            timestamp=row.timestamp,
        )

    def get_target_rankings(self, *, tenant_id: str = "default", limit: int = 10) -> list[RankingView]:
        rows = self.store.get_latest_rankings(tenant_id=tenant_id, limit=limit)
        return [self._ranking_view(r) for r in rows]

    def get_top_ten_signals(self, *, tenant_id: str = "default", limit: int = 10) -> list[dict]:
        """
        Get top ten signals with ranking, direction, expected move, alpha, and strategy.
        Returns a list of dictionaries formatted for the top ten signals display.
        """
        # Get latest rankings for top signals
        rankings = self.store.get_latest_rankings(tenant_id=tenant_id, limit=limit)
        
        # Get recent signals for strategy information
        recent_signals = self.store.get_recent_signals(tenant_id=tenant_id, limit=50)
        
        # Get consensus data for confidence information
        consensus_data = {}
        for signal in recent_signals:
            if signal.ticker not in consensus_data:
                consensus = self.store.get_latest_consensus(tenant_id=tenant_id, ticker=signal.ticker)
                if consensus:
                    consensus_data[signal.ticker] = {
                        'confidence': consensus.confidence,
                        'direction': consensus.direction,
                        'participating_strategies': consensus.participating_strategies
                    }
        
        # Combine rankings with consensus data to create top ten signals
        top_signals = []
        for i, ranking in enumerate(rankings):
            # Get consensus info for this ticker
            consensus_info = consensus_data.get(ranking.ticker, {})
            
            # Determine direction from consensus or ranking score
            direction = "BUY" if ranking.score > 0 else "SELL"
            if consensus_info.get('direction'):
                direction = consensus_info['direction'].upper()
            
            # Calculate expected move based on score and conviction
            expected_move = abs(ranking.score * ranking.conviction * 100)
            
            # Get alpha from ranking score
            alpha = abs(ranking.score)
            
            # Find a recent strategy for this ticker
            strategy = "unknown"
            for signal in recent_signals:
                if signal.ticker == ranking.ticker:
                    strategy = signal.strategy
                    break
            
            # Format the signal
            signal_data = {
                'rank': i + 1,
                'direction': direction,
                'ticker': ranking.ticker,
                'expected_move': f"{expected_move:+.1f}%",
                'alpha': alpha,
                'strategy': strategy,
                'confidence': consensus_info.get('confidence', ranking.conviction),
                'score': ranking.score,
                'conviction': ranking.conviction,
                'regime': ranking.regime,
                'timestamp': ranking.timestamp
            }
            
            top_signals.append(signal_data)
        
        # Sort by alpha (score) and return top 10
        top_signals.sort(key=lambda x: x['alpha'], reverse=True)
        return top_signals[:10]

    def _efficiency_view(self, r: StrategyEfficiencyRow) -> StrategyEfficiencyView:
        return StrategyEfficiencyView(
            strategy_id=r.strategy_id,
            strategy_version=r.strategy_version,
            forecast_days=r.forecast_days,
            samples=r.samples,
            total_forecast_days=r.total_forecast_days,
            avg_efficiency_rating=r.avg_efficiency_rating,
            alpha_strategy=r.alpha_strategy,
            win_rate=r.win_rate,
            avg_return=r.avg_return,
            drawdown=r.drawdown,
            stability=r.stability,
        )

    def get_efficiency_rankings(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str | None = None,
        forecast_days: int | None = None,
        regime: str | None = None,
        min_samples: int | None = None,
        min_total_forecast_days: int | None = None,
        limit: int = 20,
    ) -> list[StrategyEfficiencyView]:
        rows = self.store.rank_strategies_by_efficiency(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            forecast_days=forecast_days,
            regime=regime,
            min_samples=min_samples,
            min_total_forecast_days=min_total_forecast_days,
            alpha_version="canonical_v1",
            limit=limit,
        )
        return [self._efficiency_view(r) for r in rows]

    def get_efficiency_champion(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        timeframe: str = "1d",
        forecast_days: int | None = None,
        regime: str | None = None,
        min_samples: int = 20,
        min_total_forecast_days: int = 200,
    ) -> StrategyEfficiencyView | None:
        row = self.store.get_efficiency_champion(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            forecast_days=forecast_days,
            regime=regime,
            min_samples=min_samples,
            min_total_forecast_days=min_total_forecast_days,
        )
        return None if row is None else self._efficiency_view(row)

    def list_prediction_runs(self, *, tenant_id: str = "default") -> list[PredictionRunView]:
        rows = self.store.list_prediction_runs(tenant_id=tenant_id)
        return [
            PredictionRunView(
                id=r.id,
                label=f"Run {r.id[:8]} ({r.prediction_start} to {r.prediction_end})",
                timeframe=r.timeframe,
                created_at=r.created_at,
            )
            for r in rows
        ]

    def list_run_tickers(self, *, run_id: str, tenant_id: str = "default") -> list[str]:
        return self.store.list_run_tickers(run_id=run_id, tenant_id=tenant_id)

    def list_run_strategies(self, *, run_id: str, tenant_id: str = "default") -> list[str]:
        return self.store.list_run_strategies(run_id=run_id, tenant_id=tenant_id)

    def get_prediction_analytics(self, query: PredictionAnalyticsQuery) -> PredictionAnalyticsResult:
        """
        Main orchestration for the Prediction Analytics module.
        Builds a full result DTO containing cards for the UI.
        """
        if not query.run_id:
            return PredictionAnalyticsResult(None, None, [], None, None)

        # 1. Fetch scores for leaderboard
        scores = self.store.get_strategy_scores_for_run(run_id=query.run_id, tenant_id=query.tenant_id)
        
        # Sort by alpha_prediction per user's request for overlays/sorting
        scores.sort(key=lambda x: x.alpha_prediction, reverse=True)

        leaderboard_data = [
            {
                "Strategy": s.strategy_id,
                "Ticker": s.ticker,
                "Alpha": f"{s.alpha_prediction:.3f}",
                "Hit Rate": f"{s.direction_hit_rate:.1%}",
                "Efficiency": f"{s.efficiency_rating:.3f}"
            }
            for s in scores[:10]
        ]
        
        leaderboard_card = {
            "title": f"Top Predictions in {query.run_id[:8]}",
            "type": "table",
            "data": leaderboard_data
        }

        # 2. Build variance chart if ticker and strategy selected
        chart_card = None
        metric_cards = []
        
        if query.ticker and query.strategy_id:
            series = self.store.get_series_comparison(
                run_id=query.run_id,
                strategy_id=query.strategy_id,
                ticker=query.ticker,
                tenant_id=query.tenant_id
            )
            
            if series["predicted"] or series["actual"]:
                chart_card = {
                    "title": f"Forecast Variance: {query.ticker} ({query.strategy_id})",
                    "type": "variance_chart",
                    "alpha": 0.0, # Placeholder
                    "predicted": [{"x": p.timestamp, "y": p.value} for p in series["predicted"]],
                    "actual": [{"x": a.timestamp, "y": a.value} for a in series["actual"]]
                }
            
            # Find specific score for metrics
            match = next((s for s in scores if s.ticker == query.ticker and s.strategy_id == query.strategy_id), None)
            if match:
                if chart_card:
                    chart_card["alpha"] = match.alpha_prediction
                
                metric_cards = [
                    {"label": "Alpha Score", "value": f"{match.alpha_prediction:.3f}", "icon": "Star"},
                    {"label": "Hit Rate", "value": f"{match.direction_hit_rate:.1%}", "icon": "Target"},
                    {"label": "Return", "value": f"{match.total_return_actual:.2%}", "icon": "TrendingUp"},
                    {"label": "Efficiency", "value": f"{match.efficiency_rating:.3f}", "icon": "Zap"}
                ]

        return PredictionAnalyticsResult(
            run_view=None,
            chart_card=chart_card,
            metric_cards=metric_cards,
            leaderboard_card=leaderboard_card,
            details_table_card=None
        )

    def get_multi_strategy_overlay(
        self,
        *,
        run_id: str,
        ticker: str,
        strategy_ids: list[str],
        tenant_id: str = "default",
    ) -> dict[str, Any]:
        """
        Orchestrate multiple strategy overlays for a single ticker.
        Returns raw series data only. UI merges with metadata.
        """
        # Optimized multi-series comparison (single query)
        series_data = self.store.get_multi_series_comparison(
            run_id=run_id,
            ticker=ticker,
            strategy_ids=strategy_ids,
            tenant_id=tenant_id
        )
        
        actual_series = [{"x": a.timestamp, "y": a.value} for a in series_data["actual"]]
        strategies_data = []
        
        for i, sid in enumerate(strategy_ids):
            if i < len(series_data["strategies"]):
                predicted = series_data["strategies"][i]["predicted"]
                strategies_data.append({
                    "strategy_id": sid,
                    "predicted": [{"x": p.timestamp, "y": p.value} for p in predicted]
                })
        
        return {
            "ticker": ticker,
            "actual": actual_series,
            "strategies": strategies_data
        }

    def get_champion_matrix(
        self,
        *,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str | None = None,
    ) -> list[ChampionMatrixView]:
        """
        Returns all (ticker × strategy) prediction comparisons for the Intelligence Hub matrix view.

        The row's headline is always price-first:
          entry_price, target_price, and return % are primary.
          alpha_strategy is a secondary comparator.
        """
        rows: list[ChampionMatrixRow] = self.store.get_champion_comparison_matrix(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            alpha_version="canonical_v1",
        )

        out: list[ChampionMatrixView] = []
        for r in rows:
            target = (
                r.entry_price * (1.0 + r.avg_pred_return)
                if r.entry_price is not None
                else None
            )
            out.append(
                ChampionMatrixView(
                    ticker=r.ticker,
                    timeframe=r.timeframe,
                    forecast_days=r.forecast_days,
                    strategy_id=r.strategy_id,
                    regime=r.regime,
                    alpha_strategy=r.alpha_strategy,
                    avg_pred_return_pct=r.avg_pred_return,
                    avg_actual_return_pct=r.avg_actual_return,
                    direction_accuracy=r.direction_accuracy,
                    entry_price=r.entry_price,
                    target_price=target,
                    samples=r.samples,
                )
            )
        return out

    def get_strategy_timeline(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        strategy_id: str,
        limit: int = 90,
        run_id: str | None = None,
    ) -> list[StrategyTimelineView]:
        """
        Per-run autopsy timeline for one strategy/ticker pair.

        Every row expresses its prediction in price terms:
          - entry_price: the base price when forecast was made
          - target_price: where the strategy predicted price would go
          - target_price_label: formatted for display (e.g. "$547.20 (+3.2%)")
        """
        rows: list[StrategyTimelineRow] = self.store.get_strategy_timeline(
            tenant_id=tenant_id,
            ticker=ticker,
            strategy_id=strategy_id,
            limit=limit,
            run_id=run_id,
        )

        out: list[StrategyTimelineView] = []
        for r in rows:
            out.append(
                StrategyTimelineView(
                    run_date=r.run_date,
                    ticker=r.ticker,
                    strategy_id=r.strategy_id,
                    forecast_days=r.forecast_days,
                    alpha_prediction=r.alpha_prediction,
                    pred_return_pct=r.total_return_pred,
                    actual_return_pct=r.total_return_actual,
                    direction_correct=(r.direction_hit_rate >= 0.5),
                    entry_price=r.entry_price,
                    target_price=r.target_price,
                )
            )
        return out

    @lru_cache(maxsize=128)
    def _cached_intelligence_state(
        self,
        *,
        version: tuple,
        tenant_id: str,
        ticker: str,
        timeframe: str,
        run_id: str | None = None,
        strategy_ids: tuple,
        selected_strategy: str
    ):
        """
        Cached intelligence state with immutable parameters.
        Version key ensures cache invalidation when new data arrives.
        """
        # Load champion matrix (real data only)
        matrix = self.get_champion_matrix(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe
        )
        
        # Load efficiency rankings (real data only)
        # Note: Get all horizons for champion computation, UI can filter by horizon if needed
        rankings = self.get_efficiency_rankings(
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            forecast_days=None,  # Get all horizons for comprehensive champion computation
            limit=200,  # Limit to prevent unbounded cache growth
            min_samples=20  # Only include strategies with sufficient samples
        )
        
        # Load strategy overlays (real data only)
        overlays = (
            self.get_multi_strategy_overlay(
                run_id=run_id,
                ticker=ticker,
                strategy_ids=list(strategy_ids),
                tenant_id=tenant_id
            ) if strategy_ids and run_id else None
        )
        
        # Load strategy timeline (optional selected)
        timeline = self.get_strategy_timeline(
            tenant_id=tenant_id,
            ticker=ticker,
            strategy_id=selected_strategy,
            limit=90,
            run_id=run_id
        ) if selected_strategy else None
        
        # Load consensus data (optional)
        consensus = self.get_latest_consensus(tenant_id=tenant_id, ticker=ticker)
        
        # Prepare champions summary from efficiency rankings
        champions_summary = self._compute_champions(rankings)
        
        return IntelligenceStateData(
            matrix=matrix,
            rankings=rankings,
            overlay_series=overlays,
            timeline=timeline,
            consensus=consensus,
            champions=champions_summary
        )
    
    def get_intelligence_state(self, state):
        """
        Get complete intelligence hub state with real data only.
        """
        from app.ui.intelligence.intelligence_hub_state import IntelligenceHubState
        from app.ui.intelligence.intelligence_hub_dto import IntelligenceHubDTO
        
        # Compute tenant_id once for consistency
        tenant_id = getattr(state, 'tenant_id', 'default')
        
        # Resolve available tickers with tenant support
        tickers = self.list_tickers(tenant_id=tenant_id)
        
        # Resolve prediction runs for ticker with tenant support
        runs = self.list_prediction_runs(tenant_id=tenant_id)
        
        # Sort runs first to get newest first
        runs = sorted(runs, key=lambda r: r.created_at, reverse=True)
        
        # Resolve active run_id (now from sorted runs)
        latest_run = runs[0].id if runs else None
        run_id = state.run_id or latest_run
        
        # Compute strategy_ids once for consistency
        strategy_ids = tuple(sorted(state.strategy_ids))
        
        # Normalize values to avoid double cache entries
        selected_strategy = state.selected_strategy or ""
        normalized_run = run_id or ""
        
        # Map UI timeframe to database timeframe
        # UI shows '1M', '3M', '6M', '1Y' but database only has '1d'
        # For now, we always use '1d' since that's all the data we have
        db_timeframe = '1d'
        
        # Get data-driven cache invalidation timestamp
        last_write = self.store.get_last_prediction_write(
            tenant_id=tenant_id,
            ticker=state.ticker
        )
        
        # Get composite version for cache invalidation
        version_key = (
            tenant_id,  # Include tenant_id to prevent cross-tenant cache collision
            state.ticker,
            db_timeframe,  # Use database timeframe for cache key
            normalized_run,  # Use normalized value
            strategy_ids,  # Include strategy_ids for proper invalidation
            selected_strategy,  # Use normalized value
            last_write  # Data-driven invalidation - put last to avoid cache churn
        )
        
        # Get cached data with immutable parameters
        cached_data = self._cached_intelligence_state(
            version=version_key,
            tenant_id=tenant_id,
            ticker=state.ticker,
            timeframe=db_timeframe,  # Use database timeframe
            run_id=run_id,  # Pass original run_id (can be None)
            strategy_ids=strategy_ids,  # Use pre-computed
            selected_strategy=selected_strategy  # Use normalized value
        )
        
        return IntelligenceHubDTO(
            state=state,
            tickers=tickers,
            runs=runs,
            matrix_rows=cached_data.matrix,
            strategy_rankings=cached_data.rankings,
            overlay_series=cached_data.overlay_series,
            timeline=cached_data.timeline,
            consensus=cached_data.consensus,
            champions=cached_data.champions
        )
    
    def _compute_champions(self, rankings: list[StrategyEfficiencyView]) -> list[ChampionSummary]:
        """
        Compute champions by horizon from efficiency rankings.
        Returns sorted list of champions (1d, 7d, 30d).
        """
        from collections import defaultdict
        
        champions_summary = []
        if rankings:
            # Group by forecast_days and find best efficiency per horizon
            horizon_groups = defaultdict(list)
            for ranking in rankings:
                horizon_groups[ranking.forecast_days].append(ranking)
            
            for horizon in sorted(horizon_groups.keys()):  # Consistent ordering: 1d, 7d, 30d
                rows = horizon_groups[horizon]
                # Use composite metric for more stable champion selection
                best = max(rows, key=lambda r: (
                    r.avg_efficiency_rating,
                    r.alpha_strategy,
                    r.stability,
                    r.samples
                ))
                champions_summary.append(ChampionSummary(
                    horizon=horizon,
                    strategy_id=best.strategy_id,
                    efficiency=best.avg_efficiency_rating,
                    alpha=best.alpha_strategy,
                    samples=best.samples
                ))
        
        return champions_summary

    
