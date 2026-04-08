from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.ui.middle.engine_read_store import (
    ChampionRow,
    ConsensusRow,
    EngineReadStore,
    LoopHealthRow,
    SignalRow,
    RankingSnapshotRow,
    StrategyEfficiencyRow,
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
    samples: int
    total_forecast_days: int
    avg_efficiency_rating: float
    alpha_strategy: float
    win_rate: float
    avg_return: float
    drawdown: float
    stability: float


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
        try:
            return get_target_stocks()
        except Exception:
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
        )

    def get_latest_consensus(self, *, tenant_id: str = "default", ticker: str) -> ConsensusView | None:
        row = self.store.get_latest_consensus(tenant_id=tenant_id, ticker=ticker)
        return None if row is None else self._consensus_view(row)

    @staticmethod
    def _signal_view(row: SignalRow) -> SignalView:
        return SignalView(
            time=row.time,
            ticker=row.ticker,
            direction=row.direction,
            strategy=row.strategy,
            regime=row.regime,
            confidence=row.confidence,
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

    @staticmethod
    def _efficiency_view(r: StrategyEfficiencyRow) -> StrategyEfficiencyView:
        return StrategyEfficiencyView(
            strategy_id=r.strategy_id,
            strategy_version=r.strategy_version,
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
        """
        actual_series = []
        strategies_data = []
        
        for sid in strategy_ids:
            series = self.store.get_series_comparison(
                run_id=run_id,
                strategy_id=sid,
                ticker=ticker,
                tenant_id=tenant_id
            )
            if not actual_series:
                actual_series = [{"x": a.timestamp, "y": a.value} for a in series["actual"]]
            
            strategies_data.append({
                "strategy_id": sid,
                "predicted": [{"x": p.timestamp, "y": p.value} for p in series["predicted"]]
            })
            
        return {
            "title": f"Multi-Strategy Forecast Overlay: {ticker}",
            "actual": actual_series,
            "strategies": strategies_data
        }
