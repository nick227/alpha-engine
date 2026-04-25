from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from functools import lru_cache
import json

from app.services.engine_read_store import (
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
from app.core.active_universe import get_active_universe_tickers
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

    def get_meta_ranker_latest(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        n = max(1, min(2000, int(limit)))
        if as_of_date is None:
            row = self.store.conn.execute(
                """
                SELECT MAX(as_of_date) AS as_of_date
                FROM prediction_queue
                WHERE tenant_id = ?
                """,
                (str(tenant_id),),
            ).fetchone()
            as_of = str(row["as_of_date"]) if row and row["as_of_date"] is not None else None
        else:
            as_of = str(as_of_date)
        if not as_of:
            return {"tenant_id": tenant_id, "as_of_date": None, "rows": []}

        rows = self.store.conn.execute(
            """
            SELECT symbol, source, status, metadata_json
            FROM prediction_queue
            WHERE tenant_id = ? AND as_of_date = ?
            ORDER BY priority DESC, created_at DESC, symbol ASC
            LIMIT ?
            """,
            (str(tenant_id), str(as_of), int(n)),
        ).fetchall()

        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                md = json.loads(str(r["metadata_json"] or "{}"))
            except Exception:
                md = {}
            if not isinstance(md, dict):
                md = {}
            ml = md.get("ml_challenger")
            if not isinstance(ml, dict):
                continue
            penalties = ml.get("penalties") if isinstance(ml.get("penalties"), dict) else {}
            out.append(
                {
                    "symbol": str(r["symbol"]),
                    "source": str(r["source"]),
                    "status": str(r["status"]),
                    "strategy": md.get("strategy") or md.get("primary_strategy"),
                    "experimentClass": ml.get("experiment_class"),
                    "experimentKey": ml.get("experiment_key"),
                    "baseScore": ml.get("base_score"),
                    "pOutperform": ml.get("p_outperform"),
                    "pFail": ml.get("p_fail"),
                    "finalRankScore": ml.get("final_rank_score") if ml.get("final_rank_score") is not None else ml.get("score"),
                    "crowdingPenalty": penalties.get("crowding"),
                    "regimeMismatchPenalty": penalties.get("regime_mismatch"),
                    "as_of_date": ml.get("as_of_date") or as_of,
                    "mode": ml.get("mode"),
                    "nonReplacing": bool(ml.get("non_replacing", True)),
                }
            )

        out.sort(
            key=lambda x: (
                -999999.0 if x["finalRankScore"] is None else -float(x["finalRankScore"]),
                str(x["symbol"]),
            )
        )
        return {
            "tenant_id": tenant_id,
            "as_of_date": as_of,
            "rows": out[:n],
        }

    def get_meta_ranker_intents_latest(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        n = max(1, min(2000, int(limit)))
        if as_of_date is None:
            row = self.store.conn.execute(
                """
                SELECT MAX(as_of_date) AS as_of_date
                FROM trade_intents
                WHERE tenant_id = ?
                """,
                (str(tenant_id),),
            ).fetchone()
            as_of = str(row["as_of_date"]) if row and row["as_of_date"] is not None else None
        else:
            as_of = str(as_of_date)
        if not as_of:
            return {"tenant_id": tenant_id, "as_of_date": None, "rows": []}

        rows = self.store.conn.execute(
            """
            SELECT symbol, horizon_days, entry_date, entry_price, exit_date, exit_price,
                   entry_price_model, exit_price_model, intent_status, score_json, metadata_json,
                   class_key, experiment_key, run_id, created_at
            FROM trade_intents
            WHERE tenant_id = ? AND as_of_date = ?
            ORDER BY created_at DESC, symbol ASC, horizon_days ASC
            LIMIT ?
            """,
            (str(tenant_id), str(as_of), int(n)),
        ).fetchall()

        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                score = json.loads(str(r["score_json"] or "{}"))
            except Exception:
                score = {}
            if not isinstance(score, dict):
                score = {}
            try:
                meta = json.loads(str(r["metadata_json"] or "{}"))
            except Exception:
                meta = {}
            if not isinstance(meta, dict):
                meta = {}
            out.append(
                {
                    "symbol": str(r["symbol"]),
                    "horizonDays": int(r["horizon_days"]),
                    "entryDate": r["entry_date"],
                    "entryPrice": (float(r["entry_price"]) if r["entry_price"] is not None else None),
                    "exitDate": r["exit_date"],
                    "exitPrice": (float(r["exit_price"]) if r["exit_price"] is not None else None),
                    "entryPriceModel": str(r["entry_price_model"]),
                    "exitPriceModel": str(r["exit_price_model"]),
                    "intentStatus": str(r["intent_status"]),
                    "classKey": str(r["class_key"]),
                    "experimentKey": str(r["experiment_key"]),
                    "runId": str(r["run_id"]),
                    "score": score,
                    "metadata": meta,
                    "createdAt": str(r["created_at"] or ""),
                }
            )
        return {"tenant_id": tenant_id, "as_of_date": as_of, "rows": out[:n]}

    def get_meta_ranker_intent_replay(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        n = max(1, min(2000, int(limit)))
        base = self.get_meta_ranker_intents_latest(
            tenant_id=tenant_id,
            as_of_date=as_of_date,
            limit=n,
        )
        if not base.get("as_of_date"):
            return {
                **base,
                "summary": {
                    "rows": 0,
                    "rowsWithRealized": 0,
                    "avgRealizedReturn": None,
                    "winRate": None,
                },
            }

        out: list[dict[str, Any]] = []
        realized_vals: list[float] = []
        wins = 0
        for row in base.get("rows", []):
            symbol = str(row.get("symbol") or "").strip().upper()
            hz = int(row.get("horizonDays") or 0)
            res = self.store.conn.execute(
                """
                SELECT return_pct
                FROM discovery_outcomes
                WHERE tenant_id = ? AND watchlist_date = ? AND symbol = ? AND horizon_days = ?
                LIMIT 1
                """,
                (str(tenant_id), str(base["as_of_date"]), symbol, hz),
            ).fetchone()
            realized = float(res["return_pct"]) if res and res["return_pct"] is not None else None
            if realized is not None:
                realized_vals.append(realized)
                if realized > 0.0:
                    wins += 1
            out.append({**row, "realizedReturn": realized})

        summary = {
            "rows": len(out),
            "rowsWithRealized": len(realized_vals),
            "avgRealizedReturn": ((sum(realized_vals) / len(realized_vals)) if realized_vals else None),
            "winRate": ((wins / len(realized_vals)) if realized_vals else None),
        }
        return {
            "tenant_id": tenant_id,
            "as_of_date": base["as_of_date"],
            "rows": out,
            "summary": summary,
        }

    def get_experiment_leaderboard(
        self,
        *,
        tenant_id: str = "default",
        horizon: str = "5d",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        hz = str(horizon).strip().lower()
        if hz not in {"5d", "20d"}:
            hz = "5d"
        n = max(1, min(500, int(limit)))
        metric_col = "metric_5d_return" if hz == "5d" else "metric_20d_return"

        ml_rows = self.store.conn.execute(
            f"""
            WITH latest_runs AS (
              SELECT class_key, experiment_key, MAX(started_at) AS max_started_at
              FROM experiment_runs
              WHERE tenant_id = ?
              GROUP BY class_key, experiment_key
            ),
            latest_run_ids AS (
              SELECT er.id, er.class_key, er.experiment_key
              FROM experiment_runs er
              JOIN latest_runs lr
                ON lr.class_key = er.class_key
               AND lr.experiment_key = er.experiment_key
               AND lr.max_started_at = er.started_at
              WHERE er.tenant_id = ?
            )
            SELECT
              r.class_key,
              r.experiment_key,
              r."""
            + metric_col
            + """ AS metric_return,
              r.win_rate,
              r.drawdown,
              r.turnover,
              r.metadata_json,
              r.created_at AS updated_at
            FROM experiment_results r
            JOIN latest_run_ids lid ON lid.id = r.run_id
            WHERE r.tenant_id = ?
            """,
            (str(tenant_id), str(tenant_id), str(tenant_id)),
        ).fetchall()

        det_rows = self.store.conn.execute(
            """
            SELECT
              'deterministic_strategy' AS class_key,
              sp.strategy_id AS experiment_key,
              sp.avg_return AS metric_return,
              sp.accuracy AS win_rate,
              NULL AS drawdown,
              NULL AS turnover,
              '{}' AS metadata_json,
              sp.updated_at AS updated_at
            FROM strategy_performance sp
            WHERE sp.tenant_id = ? AND LOWER(sp.horizon) = ?
            """,
            (str(tenant_id), hz),
        ).fetchall()

        out: list[dict[str, Any]] = []
        for row in [*(ml_rows or []), *(det_rows or [])]:
            out.append(
                {
                    "classKey": str(row["class_key"]),
                    "experimentKey": str(row["experiment_key"]),
                    "metricReturn": (float(row["metric_return"]) if row["metric_return"] is not None else None),
                    "winRate": (float(row["win_rate"]) if row["win_rate"] is not None else None),
                    "drawdown": (float(row["drawdown"]) if row["drawdown"] is not None else None),
                    "turnover": (float(row["turnover"]) if row["turnover"] is not None else None),
                    "metadataJson": str(row["metadata_json"] or "{}"),
                    "updatedAt": str(row["updated_at"] or ""),
                    "horizon": hz,
                }
            )
        out.sort(
            key=lambda r: (
                -999999.0 if r["metricReturn"] is None else -float(r["metricReturn"]),
                -999999.0 if r["winRate"] is None else -float(r["winRate"]),
            )
        )
        return out[:n]

    def get_experiment_trends(
        self,
        *,
        tenant_id: str = "default",
        horizon: str = "5d",
        class_key: str | None = None,
        experiment_key: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        hz = str(horizon).strip().lower()
        if hz not in {"5d", "20d"}:
            hz = "5d"
        n = max(1, min(1000, int(limit)))
        forecast_days = 5 if hz == "5d" else 20
        metric_col = "metric_5d_return" if hz == "5d" else "metric_20d_return"

        where_parts = ["er.tenant_id = ?"]
        params: list[Any] = [str(tenant_id)]
        if class_key:
            where_parts.append("er.class_key = ?")
            params.append(str(class_key))
        if experiment_key:
            where_parts.append("er.experiment_key = ?")
            params.append(str(experiment_key))

        ml_rows = self.store.conn.execute(
            f"""
            SELECT
              substr(COALESCE(erun.completed_at, erun.started_at, er.created_at), 1, 10) AS day,
              er.class_key AS class_key,
              er.experiment_key AS experiment_key,
              er.{metric_col} AS metric_return,
              er.win_rate AS win_rate,
              er.drawdown AS drawdown,
              er.turnover AS turnover,
              er.created_at AS created_at
            FROM experiment_results er
            LEFT JOIN experiment_runs erun
              ON erun.id = er.run_id AND erun.tenant_id = er.tenant_id
            WHERE {" AND ".join(where_parts)}
            ORDER BY day DESC, er.created_at DESC
            LIMIT ?
            """,
            (*params, n),
        ).fetchall()

        det_where = ["ps.tenant_id = ?", "ps.forecast_days = ?"]
        det_params: list[Any] = [str(tenant_id), int(forecast_days)]
        if class_key and str(class_key) != "deterministic_strategy":
            det_rows = []
        else:
            if experiment_key:
                det_where.append("ps.strategy_id = ?")
                det_params.append(str(experiment_key))
            det_rows = self.store.conn.execute(
                f"""
                SELECT
                  substr(ps.created_at, 1, 10) AS day,
                  'deterministic_strategy' AS class_key,
                  ps.strategy_id AS experiment_key,
                  AVG(ps.total_return_actual) AS metric_return,
                  AVG(ps.direction_hit_rate) AS win_rate,
                  AVG(ps.magnitude_error) AS drawdown,
                  NULL AS turnover,
                  MAX(ps.created_at) AS created_at
                FROM prediction_scores ps
                WHERE {" AND ".join(det_where)}
                GROUP BY substr(ps.created_at, 1, 10), ps.strategy_id
                ORDER BY day DESC, created_at DESC
                LIMIT ?
                """,
                (*det_params, n),
            ).fetchall()

        out: list[dict[str, Any]] = []
        for row in [*(ml_rows or []), *(det_rows or [])]:
            out.append(
                {
                    "day": str(row["day"] or ""),
                    "classKey": str(row["class_key"]),
                    "experimentKey": str(row["experiment_key"]),
                    "metricReturn": (float(row["metric_return"]) if row["metric_return"] is not None else None),
                    "winRate": (float(row["win_rate"]) if row["win_rate"] is not None else None),
                    "drawdown": (float(row["drawdown"]) if row["drawdown"] is not None else None),
                    "turnover": (float(row["turnover"]) if row["turnover"] is not None else None),
                    "horizon": hz,
                    "createdAt": str(row["created_at"] or ""),
                }
            )

        out.sort(
            key=lambda r: (
                str(r["day"]),
                str(r["classKey"]),
                str(r["experimentKey"]),
            ),
            reverse=True,
        )
        return out[:n]

    def get_experiment_summary(
        self,
        *,
        tenant_id: str = "default",
        horizon: str = "5d",
        lookback_days: int = 14,
        limit: int = 200,
    ) -> dict[str, Any]:
        rows = self.get_experiment_trends(
            tenant_id=tenant_id,
            horizon=horizon,
            class_key=None,
            experiment_key=None,
            limit=max(limit, lookback_days * 50),
        )

        day_cutoff = None
        day_values = sorted({str(r.get("day") or "") for r in rows if str(r.get("day") or "")})
        if day_values and lookback_days > 0 and len(day_values) > lookback_days:
            day_cutoff = day_values[-lookback_days]

        filtered = [
            r for r in rows
            if r.get("day")
            and (day_cutoff is None or str(r["day"]) >= str(day_cutoff))
        ]

        by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for r in filtered:
            key = (str(r.get("classKey") or ""), str(r.get("experimentKey") or ""))
            by_key.setdefault(key, []).append(r)

        movers: list[dict[str, Any]] = []
        for (class_key, experiment_key), series in by_key.items():
            if not series:
                continue
            ordered = sorted(series, key=lambda x: str(x.get("day") or ""))
            first = ordered[0]
            last = ordered[-1]
            first_ret = first.get("metricReturn")
            last_ret = last.get("metricReturn")
            delta_ret = None
            if first_ret is not None and last_ret is not None:
                delta_ret = float(last_ret) - float(first_ret)
            last_win = last.get("winRate")
            movers.append(
                {
                    "classKey": class_key,
                    "experimentKey": experiment_key,
                    "startDay": first.get("day"),
                    "endDay": last.get("day"),
                    "startReturn": first_ret,
                    "endReturn": last_ret,
                    "deltaReturn": delta_ret,
                    "latestWinRate": last_win,
                    "points": len(ordered),
                }
            )

        movers.sort(
            key=lambda x: (
                -999999.0 if x["deltaReturn"] is None else -float(x["deltaReturn"]),
                -999999.0 if x["endReturn"] is None else -float(x["endReturn"]),
            )
        )

        def _avg(vals: list[float | None]) -> float | None:
            kept = [float(v) for v in vals if v is not None]
            return (sum(kept) / len(kept)) if kept else None

        overall = {
            "rows": len(filtered),
            "seriesCount": len(by_key),
            "avgReturn": _avg([r.get("metricReturn") for r in filtered]),
            "avgWinRate": _avg([r.get("winRate") for r in filtered]),
        }
        return {
            "tenant_id": tenant_id,
            "horizon": horizon,
            "lookbackDays": int(lookback_days),
            "overall": overall,
            "bestMovers": movers[:10],
            "worstMovers": list(reversed(movers[-10:])) if movers else [],
        }

    def list_tickers(self, *, tenant_id: str = "default") -> list[str]:
        """Active universe: static (YAML) ∪ candidate_queue status=admitted."""
        try:
            return get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=self.store.conn)
        except Exception:
            return self.store.list_tickers(tenant_id=tenant_id)

    def list_discovery_dates(self, *, tenant_id: str = "default", limit: int = 120) -> list[str]:
        return self.store.list_discovery_dates(tenant_id=tenant_id, limit=limit)

    def list_discovery_strategy_types(self, *, tenant_id: str = "default") -> list[str]:
        return self.store.list_discovery_strategy_types(tenant_id=tenant_id)

    def list_discovery_candidates(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str,
        strategy_type: str | None = None,
        price_bucket: str | None = None,
        min_score: float = 0.0,
        symbol: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return self.store.list_discovery_candidates(
            tenant_id=tenant_id,
            as_of_date=as_of_date,
            strategy_type=strategy_type,
            price_bucket=price_bucket,
            min_score=min_score,
            symbol=symbol,
            limit=limit,
        )

    def list_discovery_overlap(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str,
        min_strategies: int = 2,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return self.store.list_discovery_overlap(
            tenant_id=tenant_id,
            as_of_date=as_of_date,
            min_strategies=min_strategies,
            limit=limit,
        )

    def list_discovery_momentum(
        self,
        *,
        tenant_id: str = "default",
        end_date: str,
        window_days: int = 10,
        strategy_type: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return self.store.list_discovery_momentum(
            tenant_id=tenant_id,
            end_date=end_date,
            window_days=window_days,
            strategy_type=strategy_type,
            limit=limit,
        )

    def list_watchlist_dates(self, *, tenant_id: str = "default", limit: int = 120) -> list[str]:
        return self.store.list_watchlist_dates(tenant_id=tenant_id, limit=limit)

    def list_watchlist(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return self.store.list_watchlist(tenant_id=tenant_id, as_of_date=as_of_date, limit=limit)

    def list_daily_top_picks(
        self,
        *,
        tenant_id: str = "default",
        as_of_date: str,
        limit: int = 15,
    ) -> list[dict[str, Any]]:
        return self.store.list_daily_top_picks(tenant_id=tenant_id, as_of_date=as_of_date, limit=limit)

    def get_last_discovery_job(
        self,
        *,
        tenant_id: str = "default",
        job_type: str,
    ) -> dict[str, Any] | None:
        return self.store.get_last_discovery_job(tenant_id=tenant_id, job_type=job_type)

    def list_watchlist_outcomes(
        self,
        *,
        tenant_id: str = "default",
        watchlist_date: str,
        horizon_days: int = 5,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        return self.store.list_watchlist_outcomes(
            tenant_id=tenant_id,
            watchlist_date=watchlist_date,
            horizon_days=horizon_days,
            limit=limit,
        )

    def list_discovery_stats(
        self,
        *,
        tenant_id: str = "default",
        end_date: str,
        window_days: int = 30,
        horizon_days: int = 5,
        group_type: str | None = None,
        latest_only: bool = True,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return self.store.list_discovery_stats(
            tenant_id=tenant_id,
            end_date=end_date,
            window_days=window_days,
            horizon_days=horizon_days,
            group_type=group_type,
            latest_only=latest_only,
            limit=limit,
        )

    def list_paper_trades(
        self,
        *,
        tenant_id: str = "default",
        mode: str | None = "paper",
        ticker: str | None = None,
        strategy_id: str | None = None,
        status: str | None = None,
        since_iso: str | None = None,
        limit: int = 250,
    ) -> list[dict[str, Any]]:
        rows = self.store.list_trades(
            tenant_id=tenant_id,
            mode=mode,
            ticker=ticker,
            strategy_id=strategy_id,
            status=status,
            since_iso=since_iso,
            limit=limit,
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            pnl_pct = None
            try:
                denom = float(r.entry_price) * float(r.quantity)
                if r.pnl is not None and denom:
                    pnl_pct = float(r.pnl) / denom
            except Exception:
                pnl_pct = None

            out.append(
                {
                    "id": r.id,
                    "timestamp": r.timestamp,
                    "ticker": r.ticker,
                    "direction": r.direction,
                    "quantity": r.quantity,
                    "entry_price": r.entry_price,
                    "exit_price": r.exit_price,
                    "pnl": r.pnl,
                    "pnl_pct": pnl_pct,
                    "status": r.status,
                    "mode": r.mode,
                    "strategy_id": r.strategy_id,
                    "analysis": r.analysis,
                    "llm_prediction": r.llm_prediction,
                    "engine_decision": r.engine_decision,
                    "llm_status": r.llm_status,
                    "llm_agrees": r.llm_agrees,
                }
            )
        return out

    def list_paper_positions(
        self,
        *,
        tenant_id: str = "default",
        mode: str | None = "paper",
        ticker: str | None = None,
        price_timeframe: str = "1d",
    ) -> list[dict[str, Any]]:
        rows = self.store.list_positions(tenant_id=tenant_id, mode=mode, ticker=ticker)
        tickers = sorted({r.ticker for r in rows})
        last_prices = self.store.get_latest_close_prices(
            tenant_id=tenant_id,
            timeframe=price_timeframe,
            tickers=tickers,
        )

        out: list[dict[str, Any]] = []
        for r in rows:
            last = last_prices.get(r.ticker)
            unreal = None
            unreal_pct = None
            if last is not None:
                try:
                    if str(r.direction).lower() == "short":
                        unreal = float(r.quantity) * (float(r.average_entry_price) - float(last))
                    else:
                        unreal = float(r.quantity) * (float(last) - float(r.average_entry_price))
                    denom = float(r.average_entry_price) * float(r.quantity)
                    if denom:
                        unreal_pct = float(unreal) / denom
                except Exception:
                    unreal = None
                    unreal_pct = None

            out.append(
                {
                    "ticker": r.ticker,
                    "direction": r.direction,
                    "quantity": r.quantity,
                    "avg_entry": r.average_entry_price,
                    "last_price": last,
                    "unrealized_pnl": unreal,
                    "unrealized_pnl_pct": unreal_pct,
                    "mode": r.mode,
                }
            )
        return out

    def get_paper_overview(
        self,
        *,
        tenant_id: str = "default",
        mode: str | None = "paper",
        ticker: str | None = None,
        limit: int = 250,
    ) -> dict[str, Any]:
        trades = self.list_paper_trades(tenant_id=tenant_id, mode=mode, ticker=ticker, limit=limit)
        closed = [t for t in trades if str(t.get("status", "")).upper() == "CLOSED" and t.get("pnl") is not None]
        open_trades = [t for t in trades if str(t.get("status", "")).upper() != "CLOSED"]

        realized = float(sum(float(t["pnl"]) for t in closed if t.get("pnl") is not None) or 0.0)
        wins = len([t for t in closed if float(t.get("pnl") or 0.0) > 0.0])
        win_rate = (wins / len(closed)) if closed else None

        return {
            "realized_pnl": realized,
            "closed_trades": len(closed),
            "open_trades": len(open_trades),
            "win_rate": win_rate,
        }

    def get_ingest_run_summary(
        self,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
        source_id: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.store.summarize_ingest_runs(start_ts=start_ts, end_ts=end_ts, source_id=source_id)
        return [
            {
                "source_id": r.source_id,
                "windows": r.windows,
                "complete": r.complete_windows,
                "running": r.running_windows,
                "failed": r.failed_windows,
                "empty": r.empty_windows,
                "retries": r.retries_total,
                "fetched": r.fetched_rows,
                "emitted": r.emitted_rows,
                "skipped": r.skipped_windows,
            }
            for r in rows
        ]

    def get_recent_ingest_runs(
        self,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
        source_id: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = self.store.list_recent_ingest_runs(
            start_ts=start_ts, end_ts=end_ts, source_id=source_id, status=status, limit=limit
        )
        return [
            {
                "source_id": r.source_id,
                "start_ts": r.start_ts,
                "end_ts": r.end_ts,
                "status": r.status,
                "provider": r.provider,
                "ok": r.ok,
                "retry_count": r.retry_count,
                "fetched_count": r.fetched_count,
                "emitted_count": r.emitted_count,
                "empty_count": r.empty_count,
                "newest_event_ts": r.newest_event_ts,
                "last_error": r.last_error,
                "updated_at": r.updated_at,
            }
            for r in rows
        ]

    def get_backfill_horizons(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.store.list_backfill_horizons(limit=limit)
        return [
            {
                "source_id": r.source_id,
                "spec_hash": r.spec_hash,
                "backfilled_until_ts": r.backfilled_until_ts,
                "updated_at": r.updated_at,
            }
            for r in rows
        ]

    def get_events_freshness(
        self,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return self.store.summarize_events_freshness(start_ts=start_ts, end_ts=end_ts, limit=limit)

    def get_coverage_month_counts(
        self,
        *,
        dataset: str,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str = "1d",
        source_id: str | None = None,
        start_ym: str,
        end_ym: str,
    ) -> dict[str, int]:
        ds = str(dataset).strip().lower()
        if ds == "price_bars":
            return self.store.monthly_counts_price_bars(
                tenant_id=tenant_id, ticker=ticker, timeframe=timeframe, start_ym=start_ym, end_ym=end_ym
            )
        if ds == "events":
            return self.store.monthly_counts_events(source_id=source_id, start_ym=start_ym, end_ym=end_ym)
        if ds == "ingest_runs":
            return self.store.monthly_counts_ingest_runs(source_id=source_id, start_ym=start_ym, end_ym=end_ym)
        if ds in {"ml", "ml_7d", "ml_learning_rows"}:
            return self.store.monthly_counts_ml_learning_rows(
                tenant_id=tenant_id,
                symbol=ticker,
                horizon="7d",
                labeled_only=True,
                start_ym=start_ym,
                end_ym=end_ym,
            )
        return {}

    def list_ml_symbols(self, *, tenant_id: str = "backfill", horizon: str = "7d", limit: int = 200) -> list[str]:
        return self.store.list_ml_symbols(tenant_id=tenant_id, horizon=horizon, limit=limit)

    def get_ml_readiness_per_ticker(
        self,
        *,
        tenant_id: str = "backfill",
        horizon: str = "7d",
        start_date: str,
        end_date: str,
        min_feature_coverage: float = 0.8,
        symbol: str | None = None,
        limit_symbols: int = 80,
    ) -> list[dict[str, Any]]:
        rows = self.store.summarize_ml_readiness_per_ticker(
            tenant_id=tenant_id,
            horizon=horizon,
            start_date=start_date,
            end_date=end_date,
            min_feature_coverage=min_feature_coverage,
            symbol_filter=symbol,
            limit_symbols=limit_symbols,
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "symbol": r.symbol,
                    "horizon": r.horizon,
                    "train_rows_total": r.train_rows_total,
                    "train_rows_labeled": r.train_rows_labeled,
                    "label_null_rate": r.label_null_rate,
                    "coverage_p10": r.coverage_p10,
                    "coverage_median": r.coverage_median,
                    "pct_cov_ge_min": r.pct_coverage_ge_min,
                    "n_features_est": r.n_features_est,
                    "min_rows_required": r.min_rows_required,
                    "ready": r.ready,
                    "top_blocker": r.top_blocker,
                    "suggested_action": r.suggested_action,
                    "suggested_action_kind": r.suggested_action_kind,
                }
            )
        return out

    def get_ml_dataset_state(
        self,
        *,
        tenant_id: str = "backfill",
        horizon: str = "7d",
        start_date: str,
        end_date: str,
        symbol: str | None = None,
    ) -> dict[str, int]:
        return self.store.ml_dataset_state(
            tenant_id=tenant_id, horizon=horizon, start_date=start_date, end_date=end_date, symbol=symbol
        )

    def get_ml_low_coverage_window(
        self,
        *,
        tenant_id: str = "backfill",
        symbol: str,
        horizon: str = "7d",
        start_date: str,
        end_date: str,
        min_feature_coverage: float = 0.8,
    ) -> dict[str, Any]:
        w = self.store.ml_low_coverage_window(
            tenant_id=tenant_id,
            symbol=symbol,
            horizon=horizon,
            start_date=start_date,
            end_date=end_date,
            min_feature_coverage=min_feature_coverage,
        )
        return {
            "symbol": w.symbol,
            "horizon": w.horizon,
            "min_bad_date": w.min_bad_date,
            "max_bad_date": w.max_bad_date,
            "bad_rows": w.bad_rows,
            "total_rows": w.total_rows,
        }

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
        Get top ten signals with ranking, direction, expected move, alpha, strategy, and attribution.
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
            
            # Determine forecast horizon based on strategy or default to 7d
            forecast_horizon = "7d"  # Default
            if "1d" in strategy.lower() or "intraday" in strategy.lower():
                forecast_horizon = "1d"
            elif "30d" in strategy.lower() or "monthly" in strategy.lower():
                forecast_horizon = "30d"
            
            # Format the signal with enhanced data
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
                'timestamp': ranking.timestamp,
                'forecast_horizon': forecast_horizon,
                'attribution': ranking.attribution if hasattr(ranking, 'attribution') else {},
                'participating_strategies': consensus_info.get('participating_strategies', 1)
            }
            
            top_signals.append(signal_data)
        
        # Sort by confidence instead of alpha as requested
        top_signals.sort(key=lambda x: x['confidence'], reverse=True)
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
        from app.services.intelligence_hub_state import IntelligenceHubState
        from app.services.intelligence_hub_dto import IntelligenceHubDTO
        
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

    # --- Explainability read models (existing tables only; see explainability_read_model.py) ---

    def get_explain_ticker_panel(self, *, tenant_id: str = "default", ticker: str) -> dict[str, Any]:
        from app.services.explainability_read_model import build_ticker_why_panel

        return build_ticker_why_panel(self.store.conn, tenant_id=tenant_id, ticker=ticker)

    def get_explain_per_ticker_performance(self, *, tenant_id: str = "default", ticker: str) -> dict[str, Any]:
        from app.services.explainability_read_model import build_per_ticker_performance

        return build_per_ticker_performance(self.store.conn, tenant_id=tenant_id, ticker=ticker)

    def get_explain_strategy_ticker_matrix(
        self,
        *,
        tenant_id: str = "default",
        tickers: list[str] | None = None,
        lookback_days: int = 90,
    ) -> list[dict[str, Any]]:
        from app.services.explainability_read_model import build_strategy_ticker_matrix

        t = list(tickers) if tickers else get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=self.store.conn)
        return build_strategy_ticker_matrix(self.store.conn, tenant_id=tenant_id, tickers=t, lookback_days=lookback_days)

    def get_explain_what_changed(self, *, tenant_id: str = "default", hours: int = 24) -> dict[str, Any]:
        from app.services.explainability_read_model import build_what_changed_recent

        return build_what_changed_recent(self.store.conn, tenant_id=tenant_id, hours=hours)

    def get_explain_topn_quality(self, *, tenant_id: str = "default", limit: int = 20) -> dict[str, Any]:
        from app.services.explainability_read_model import build_topn_quality_snapshot

        rankings = self.get_target_rankings(tenant_id=tenant_id, limit=limit)
        tickers = [r.ticker for r in rankings]
        base = build_topn_quality_snapshot(self.store.conn, tenant_id=tenant_id, top_tickers=tickers)
        base["rankings_head"] = [
            {"ticker": r.ticker, "score": r.score, "conviction": r.conviction, "regime": r.regime}
            for r in rankings
        ]
        return base

    def get_explain_ranking_movers(self, *, tenant_id: str = "default", top_n: int = 20) -> dict[str, Any]:
        from app.services.explainability_rank_trends import build_ranking_movers

        return build_ranking_movers(self.store.conn, tenant_id=tenant_id, top_n=int(top_n))

    def get_explain_outcome_trend(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        last_n: int = 10,
    ) -> dict[str, Any]:
        from app.services.explainability_rank_trends import build_outcome_trend_last_n

        return build_outcome_trend_last_n(
            self.store.conn, tenant_id=tenant_id, ticker=ticker, last_n=int(last_n)
        )

    def get_explain_weekly_performance(self, *, tenant_id: str = "default") -> dict[str, Any]:
        from app.services.explainability_rank_trends import build_weekly_performance_summary

        return build_weekly_performance_summary(self.store.conn, tenant_id=tenant_id)

    def get_explain_rank_history(
        self,
        *,
        tenant_id: str = "default",
        ticker: str,
        max_snapshots: int = 10,
    ) -> dict[str, Any]:
        from app.services.explainability_rank_trends import build_rank_history_series

        return build_rank_history_series(
            self.store.conn,
            tenant_id=tenant_id,
            ticker=ticker,
            max_snapshots=int(max_snapshots),
        )
