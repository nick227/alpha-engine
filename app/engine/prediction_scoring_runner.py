from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any

from app.db.repository import AlphaRepository
from app.engine.prediction_sync import EfficiencyConfig, score_sync

log = logging.getLogger(__name__)


def _align_by_timestamp(
    predicted: list[tuple[str, float]],
    actual: list[tuple[str, float]],
) -> tuple[list[str], list[float], list[float]]:
    pred_map = {str(ts): float(v) for ts, v in predicted}
    act_map = {str(ts): float(v) for ts, v in actual}
    common = sorted(set(pred_map.keys()) & set(act_map.keys()))
    return common, [pred_map[t] for t in common], [act_map[t] for t in common]


class PredictionScoringRunner:
    def __init__(self, *, repository: AlphaRepository | None = None) -> None:
        self.repo = repository or AlphaRepository()

    def materialize_actual_series_from_price_bars(
        self,
        *,
        run_id: str,
        ticker: str,
        timeframe: str,
        tenant_id: str = "default",
    ) -> int:
        run = self.repo.get_prediction_run(run_id=run_id, tenant_id=tenant_id)
        if not run:
            return 0
        start = str(run["prediction_start"])
        end = str(run["prediction_end"])

        # Prefer exact timeframe bars; fallback to deriving daily closes from intraday if needed.
        rows = self.repo.conn.execute(
            """
            SELECT timestamp, close as value
            FROM price_bars
            WHERE tenant_id = ?
              AND ticker = ?
              AND timeframe = ?
              AND timestamp >= ?
              AND timestamp <= ?
            ORDER BY timestamp ASC
            """,
            (tenant_id, str(ticker), str(timeframe), start, end),
        ).fetchall()

        if not rows and str(timeframe).strip().lower() == "1d":
            rows = self.repo.conn.execute(
                """
                WITH daily AS (
                  SELECT substr(timestamp, 1, 10) as day, MAX(timestamp) as ts
                  FROM price_bars
                  WHERE tenant_id = ?
                    AND ticker = ?
                    AND timeframe IN ('1m','1h')
                    AND timestamp >= ?
                    AND timestamp <= ?
                  GROUP BY substr(timestamp, 1, 10)
                  ORDER BY day ASC
                )
                SELECT pb.timestamp as timestamp, pb.close as value
                FROM daily d
                JOIN price_bars pb
                  ON pb.tenant_id = ?
                 AND pb.ticker = ?
                 AND pb.timestamp = d.ts
                ORDER BY pb.timestamp ASC
                """,
                (tenant_id, str(ticker), start, end, tenant_id, str(ticker)),
            ).fetchall()
        points = [(str(r["timestamp"]), float(r["value"])) for r in rows]
        return self.repo.upsert_actual_series_points(
            run_id=run_id,
            ticker=ticker,
            timeframe=timeframe,
            points=points,
            tenant_id=tenant_id,
        )

    def score_run(
        self,
        *,
        run_id: str,
        tenant_id: str = "default",
        ticker: str | None = None,
        timeframe: str | None = None,
        strategy_id: str | None = None,
        config: EfficiencyConfig | None = None,
        materialize_actual: bool = True,
        autobuild_predicted_series: bool = False,
    ) -> list[dict[str, Any]]:
        cfg = config or EfficiencyConfig()

        if autobuild_predicted_series:
            try:
                from app.engine.predicted_series_builder import PredictedSeriesBuilder, BuildConfig

                builder = PredictedSeriesBuilder(repository=self.repo)
                builder.build_for_run(run_id=run_id, tickers=([ticker] if ticker else None), config=BuildConfig(tenant_id=tenant_id))
            except Exception:
                # Scoring should remain best-effort; failing to autobuild should not crash the run.
                pass

        run = self.repo.get_prediction_run(run_id=run_id, tenant_id=tenant_id) or {}
        run_regime = run.get("regime")

        targets = self.repo.list_score_targets_for_run(
            run_id=run_id,
            tenant_id=tenant_id,
            ticker=ticker,
            timeframe=timeframe,
            strategy_id=strategy_id,
        )
        if not targets:
            log.info("no predicted series found for run_id=%s", run_id)
            return []

        out: list[dict[str, Any]] = []
        for t in targets:
            sid = str(t["strategy_id"])
            sver = str(t.get("strategy_version") or "") or None
            tk = str(t["ticker"])
            tf = str(t["timeframe"])

            actual = self.repo.fetch_actual_series(run_id=run_id, ticker=tk, timeframe=tf, tenant_id=tenant_id)
            if (not actual) and materialize_actual:
                inserted = self.materialize_actual_series_from_price_bars(
                    run_id=run_id,
                    ticker=tk,
                    timeframe=tf,
                    tenant_id=tenant_id,
                )
                if inserted:
                    actual = self.repo.fetch_actual_series(run_id=run_id, ticker=tk, timeframe=tf, tenant_id=tenant_id)

            predicted = self.repo.fetch_predicted_series(
                run_id=run_id,
                strategy_id=sid,
                ticker=tk,
                timeframe=tf,
                tenant_id=tenant_id,
            )
            if not predicted or not actual:
                log.warning(
                    "skip scoring run_id=%s strategy_id=%s ticker=%s timeframe=%s (pred=%s actual=%s)",
                    run_id,
                    sid,
                    tk,
                    tf,
                    len(predicted),
                    len(actual),
                )
                continue

            _, pred_vals, act_vals = _align_by_timestamp(predicted, actual)
            score = score_sync(pred_vals, act_vals, config=cfg)

            weights = cfg.weights
            scales = cfg.scales
            # Explainability: store the component contributions used by efficiency_rating.
            daily_scale = float(scales.daily_return_scale)
            magnitude_score = 1.0 - (score.magnitude_error / daily_scale) if daily_scale > 0 else 0.0
            total_scale = daily_scale * (max(1, score.forecast_days) ** 0.5)
            total_return_score = 1.0 - (score.total_return_error / total_scale) if total_scale > 0 else 0.0
            attribution = {
                "sync": weights.sync * score.sync_rate,
                "direction": weights.direction * score.direction_hit_rate,
                "horizon": weights.horizon * score.horizon_weight,
                "magnitude": weights.magnitude * magnitude_score,
                "total_return": weights.total_return * total_return_score,
            }

            row = {
                "run_id": str(run_id),
                "strategy_id": sid,
                "strategy_version": sver,
                "ticker": tk,
                "timeframe": tf,
                "regime": run_regime,
                **asdict(score),
                "attribution": attribution,
            }
            self.repo.save_prediction_score(row, tenant_id=tenant_id)
            out.append(row)

        log.info("scored %s series for run_id=%s", len(out), run_id)
        return out
