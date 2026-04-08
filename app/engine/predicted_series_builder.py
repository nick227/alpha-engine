from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from math import log
from typing import Any

import numpy as np

from app.core.time_utils import normalize_timestamp
from app.db.repository import AlphaRepository


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


@dataclass(frozen=True)
class Signal:
    direction: str  # up | down | flat
    confidence: float  # 0..1 (best-effort)


class CurveModel(ABC):
    @abstractmethod
    def build_curve(
        self,
        *,
        start_level: float,
        timestamps: list[str],
        signal: Signal,
        features: dict[str, Any],
        config: "BuildConfig",
    ) -> list[float]:
        raise NotImplementedError


class FlatHoldModel(CurveModel):
    def build_curve(
        self,
        *,
        start_level: float,
        timestamps: list[str],
        signal: Signal,
        features: dict[str, Any],
        config: "BuildConfig",
    ) -> list[float]:
        return [float(start_level) for _ in timestamps]


class DirectionalDriftModel(CurveModel):
    """
    MVP curve model:
      step_return ~= sign(direction) * confidence * vol_scale (clamped)
      level[t] = level[t-1] * (1 + step_return)
    """

    def build_curve(
        self,
        *,
        start_level: float,
        timestamps: list[str],
        signal: Signal,
        features: dict[str, Any],
        config: "BuildConfig",
    ) -> list[float]:
        direction = str(signal.direction).strip().lower()
        if direction == "up":
            sign = 1.0
        elif direction == "down":
            sign = -1.0
        else:
            sign = 0.0

        confidence = _clamp(float(signal.confidence), 0.0, 1.0)
        vol_scale = float(features.get("vol_scale", 0.01))
        cap = float(config.cap_daily_return)

        step = _clamp(sign * confidence * vol_scale, -cap, cap)

        out: list[float] = []
        level = float(start_level)
        for _ in timestamps:
            out.append(level)
            level = level * (1.0 + step)
        return out


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(str(value)[:10])


def _daily_midnight_utc(d: date) -> str:
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return normalize_timestamp(dt)


@dataclass(frozen=True)
class BuildConfig:
    model: str = "directional_drift"
    signal_source: str = "consensus"  # only consensus for now
    consensus_strategy_id: str = "consensus-v1"
    vol_lookback: int = 20
    cap_daily_return: float = 0.05
    skip_if_exists: bool = True
    tenant_id: str = "default"


@dataclass(frozen=True)
class BuildResult:
    run_id: str
    strategy_id: str
    ticker: str
    timeframe: str
    points_written: int
    skipped: bool
    skip_reason: str | None
    model_used: str
    vol_estimate: float | None
    start_level: float | None


class PredictedSeriesBuilder:
    def __init__(self, *, repository: AlphaRepository | None = None) -> None:
        self.repo = repository or AlphaRepository()

    def _model(self, name: str) -> CurveModel:
        n = str(name).strip().lower()
        if n in ("flat", "flat_hold", "hold"):
            return FlatHoldModel()
        if n in ("directional_drift", "drift"):
            return DirectionalDriftModel()
        raise ValueError(f"unknown curve model: {name}")

    def _resolve_consensus_signal(
        self,
        *,
        ticker: str,
        ingress_start: str,
        ingress_end: str,
        tenant_id: str,
    ) -> Signal | None:
        # Prefer consensus signals that were produced during the ingress window.
        row = self.repo.conn.execute(
            """
            SELECT p_final, created_at
            FROM consensus_signals
            WHERE tenant_id = ?
              AND ticker = ?
              AND created_at >= ?
              AND created_at <= ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (tenant_id, str(ticker), str(ingress_start), str(ingress_end)),
        ).fetchone()

        if row is None:
            # Fallback: latest available for ticker.
            row = self.repo.conn.execute(
                """
                SELECT p_final, created_at
                FROM consensus_signals
                WHERE tenant_id = ?
                  AND ticker = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (tenant_id, str(ticker)),
            ).fetchone()

        if row is None:
            return None

        try:
            p_final = float(row["p_final"])
        except Exception:
            return None

        # Map p_final to direction; confidence is abs(p_final) clamped.
        if p_final > 0.05:
            direction = "up"
        elif p_final < -0.05:
            direction = "down"
        else:
            direction = "flat"
        confidence = _clamp(abs(p_final), 0.0, 1.0)
        return Signal(direction=direction, confidence=confidence)

    def _resolve_prediction_signal(
        self,
        *,
        ticker: str,
        ingress_start: str,
        ingress_end: str,
        tenant_id: str,
    ) -> Signal | None:
        """
        Fallback when consensus_signals are unavailable:
        use the most recent prediction in the ingress window (best-effort proxy).
        """
        row = self.repo.conn.execute(
            """
            SELECT prediction, confidence, timestamp
            FROM predictions
            WHERE tenant_id = ?
              AND ticker = ?
              AND timestamp >= ?
              AND timestamp <= ?
            ORDER BY timestamp DESC, confidence DESC
            LIMIT 1
            """,
            (tenant_id, str(ticker), str(ingress_start), str(ingress_end)),
        ).fetchone()
        if row is None:
            row = self.repo.conn.execute(
                """
                SELECT prediction, confidence, timestamp
                FROM predictions
                WHERE tenant_id = ?
                  AND ticker = ?
                ORDER BY timestamp DESC, confidence DESC
                LIMIT 1
                """,
                (tenant_id, str(ticker)),
            ).fetchone()
        if row is None:
            return None
        direction = str(row["prediction"]).strip().lower()
        if direction not in ("up", "down", "flat", "neutral"):
            direction = "flat"
        if direction == "neutral":
            direction = "flat"
        try:
            conf = _clamp(float(row["confidence"]), 0.0, 1.0)
        except Exception:
            conf = 0.5
        return Signal(direction=direction, confidence=conf)

    def _resolve_start_level(
        self,
        *,
        ticker: str,
        timeframe: str,
        prediction_start: str,
        tenant_id: str,
    ) -> float | None:
        row = self.repo.conn.execute(
            """
            SELECT close
            FROM price_bars
            WHERE tenant_id = ?
              AND ticker = ?
              AND timeframe = ?
              AND timestamp < ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (tenant_id, str(ticker), str(timeframe), str(prediction_start)),
        ).fetchone()
        if row is None:
            # Fallback for daily curves when only intraday bars are present.
            if str(timeframe).strip().lower() == "1d":
                row = self.repo.conn.execute(
                    """
                    SELECT close
                    FROM price_bars
                    WHERE tenant_id = ?
                      AND ticker = ?
                      AND timeframe IN ('1m', '1h')
                      AND timestamp < ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (tenant_id, str(ticker), str(prediction_start)),
                ).fetchone()

            # If we still didn't find a prior bar, use the first bar at/after prediction_start.
            if row is None:
                row = self.repo.conn.execute(
                    """
                    SELECT close
                    FROM price_bars
                    WHERE tenant_id = ?
                      AND ticker = ?
                      AND (
                        timeframe = ?
                        OR (? = '1d' AND timeframe IN ('1m','1h'))
                      )
                      AND timestamp >= ?
                    ORDER BY timestamp ASC
                    LIMIT 1
                    """,
                    (tenant_id, str(ticker), str(timeframe), str(timeframe).strip().lower(), str(prediction_start)),
                ).fetchone()
            if row is None:
                return None
        try:
            return float(row["close"])
        except Exception:
            return None

    def _estimate_vol_scale(
        self,
        *,
        ticker: str,
        timeframe: str,
        ingress_end: str,
        vol_lookback: int,
        tenant_id: str,
    ) -> float:
        rows = self.repo.conn.execute(
            """
            SELECT close
            FROM price_bars
            WHERE tenant_id = ?
              AND ticker = ?
              AND timeframe = ?
              AND timestamp <= ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (tenant_id, str(ticker), str(timeframe), str(ingress_end), int(max(2, vol_lookback + 1))),
        ).fetchall()

        # Fallback: if we asked for daily bars but don't have them, estimate vol from daily closes derived from intraday bars.
        if not rows and str(timeframe).strip().lower() == "1d":
            rows = self.repo.conn.execute(
                """
                WITH daily AS (
                  SELECT substr(timestamp, 1, 10) as day, MAX(timestamp) as ts
                  FROM price_bars
                  WHERE tenant_id = ?
                    AND ticker = ?
                    AND timeframe IN ('1m','1h')
                    AND timestamp <= ?
                  GROUP BY substr(timestamp, 1, 10)
                  ORDER BY day DESC
                  LIMIT ?
                )
                SELECT pb.close
                FROM daily d
                JOIN price_bars pb
                  ON pb.tenant_id = ?
                 AND pb.ticker = ?
                 AND pb.timestamp = d.ts
                ORDER BY pb.timestamp ASC
                """,
                (
                    tenant_id,
                    str(ticker),
                    str(ingress_end),
                    int(max(2, vol_lookback + 1)),
                    tenant_id,
                    str(ticker),
                ),
            ).fetchall()
        closes = []
        for r in rows:
            try:
                closes.append(float(r["close"]))
            except Exception:
                continue
        closes = list(reversed(closes))
        if len(closes) < 2:
            return 0.01
        rets = []
        for a, b in zip(closes[:-1], closes[1:]):
            if a <= 0 or b <= 0:
                continue
            rets.append(log(b / a))
        if len(rets) < 1:
            return 0.01
        return float(np.std(np.asarray(rets, dtype=float), ddof=0))

    def _prediction_timestamps(
        self,
        *,
        ticker: str,
        timeframe: str,
        prediction_start: str,
        prediction_end: str,
        tenant_id: str,
    ) -> list[str]:
        # Best-effort: if we have bars in the prediction window, use their exact timestamps.
        rows = self.repo.conn.execute(
            """
            SELECT timestamp
            FROM price_bars
            WHERE tenant_id = ?
              AND ticker = ?
              AND timeframe = ?
              AND timestamp >= ?
              AND timestamp <= ?
            ORDER BY timestamp ASC
            """,
            (tenant_id, str(ticker), str(timeframe), str(prediction_start), str(prediction_end)),
        ).fetchall()
        stamps = [str(r["timestamp"]) for r in rows if r and r["timestamp"] is not None]
        if stamps:
            return stamps

        if str(timeframe).strip().lower() == "1d":
            # Derive daily timestamps from intraday bars (last bar per day).
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
                SELECT ts as timestamp
                FROM daily
                ORDER BY timestamp ASC
                """,
                (tenant_id, str(ticker), str(prediction_start), str(prediction_end)),
            ).fetchall()
            stamps = [str(r["timestamp"]) for r in rows if r and r["timestamp"] is not None]
            if stamps:
                return stamps

        # Fallback: day-by-day midnight UTC (keeps curve stable even without bars).
        start_d = _parse_iso_date(prediction_start)
        end_d = _parse_iso_date(prediction_end)
        out: list[str] = []
        d = start_d
        while d <= end_d:
            out.append(_daily_midnight_utc(d))
            d += timedelta(days=1)
        return out

    def build(
        self,
        *,
        run_id: str,
        ticker: str,
        config: BuildConfig | None = None,
    ) -> BuildResult:
        cfg = config or BuildConfig()
        tenant_id = str(cfg.tenant_id)

        run = self.repo.get_prediction_run(run_id=str(run_id), tenant_id=tenant_id)
        if not run:
            return BuildResult(
                run_id=str(run_id),
                strategy_id=str(cfg.consensus_strategy_id),
                ticker=str(ticker),
                timeframe="1d",
                points_written=0,
                skipped=True,
                skip_reason="run_not_found",
                model_used=str(cfg.model),
                vol_estimate=None,
                start_level=None,
            )

        timeframe = str(run.get("timeframe") or "1d")
        ingress_start = str(run["ingress_start"])
        ingress_end = str(run["ingress_end"])
        prediction_start = str(run["prediction_start"])
        prediction_end = str(run["prediction_end"])

        strategy_id = str(cfg.consensus_strategy_id)

        if cfg.skip_if_exists:
            existing = self.repo.fetch_predicted_series(
                run_id=str(run_id),
                strategy_id=strategy_id,
                ticker=str(ticker),
                timeframe=timeframe,
                tenant_id=tenant_id,
            )
            if existing:
                return BuildResult(
                    run_id=str(run_id),
                    strategy_id=strategy_id,
                    ticker=str(ticker),
                    timeframe=timeframe,
                    points_written=0,
                    skipped=True,
                    skip_reason="already_exists",
                    model_used=str(cfg.model),
                    vol_estimate=None,
                    start_level=None,
                )

        if str(cfg.signal_source).strip().lower() != "consensus":
            return BuildResult(
                run_id=str(run_id),
                strategy_id=strategy_id,
                ticker=str(ticker),
                timeframe=timeframe,
                points_written=0,
                skipped=True,
                skip_reason="unsupported_signal_source",
                model_used=str(cfg.model),
                vol_estimate=None,
                start_level=None,
            )

        signal = self._resolve_consensus_signal(
            ticker=str(ticker),
            ingress_start=ingress_start,
            ingress_end=ingress_end,
            tenant_id=tenant_id,
        )
        if signal is None:
            # Fallback to predictions-based proxy if consensus_signals haven't been materialized yet.
            signal = self._resolve_prediction_signal(
                ticker=str(ticker),
                ingress_start=ingress_start,
                ingress_end=ingress_end,
                tenant_id=tenant_id,
            )
        if signal is None:
            return BuildResult(
                run_id=str(run_id),
                strategy_id=strategy_id,
                ticker=str(ticker),
                timeframe=timeframe,
                points_written=0,
                skipped=True,
                skip_reason="no_signal",
                model_used=str(cfg.model),
                vol_estimate=None,
                start_level=None,
            )

        start_level = self._resolve_start_level(
            ticker=str(ticker),
            timeframe=timeframe,
            prediction_start=prediction_start,
            tenant_id=tenant_id,
        )
        if start_level is None:
            return BuildResult(
                run_id=str(run_id),
                strategy_id=strategy_id,
                ticker=str(ticker),
                timeframe=timeframe,
                points_written=0,
                skipped=True,
                skip_reason="no_start_level",
                model_used=str(cfg.model),
                vol_estimate=None,
                start_level=None,
            )

        vol = self._estimate_vol_scale(
            ticker=str(ticker),
            timeframe=timeframe,
            ingress_end=ingress_end,
            vol_lookback=int(cfg.vol_lookback),
            tenant_id=tenant_id,
        )
        stamps = self._prediction_timestamps(
            ticker=str(ticker),
            timeframe=timeframe,
            prediction_start=prediction_start,
            prediction_end=prediction_end,
            tenant_id=tenant_id,
        )

        model = self._model(cfg.model)
        levels = model.build_curve(
            start_level=float(start_level),
            timestamps=stamps,
            signal=signal,
            features={"vol_scale": float(vol)},
            config=cfg,
        )
        points = list(zip(stamps, [float(x) for x in levels]))
        written = self.repo.upsert_predicted_series_points(
            run_id=str(run_id),
            strategy_id=strategy_id,
            ticker=str(ticker),
            timeframe=timeframe,
            points=points,
            tenant_id=tenant_id,
            strategy_version=None,
        )

        return BuildResult(
            run_id=str(run_id),
            strategy_id=strategy_id,
            ticker=str(ticker),
            timeframe=timeframe,
            points_written=int(written),
            skipped=False,
            skip_reason=None,
            model_used=str(cfg.model),
            vol_estimate=float(vol),
            start_level=float(start_level),
        )

    def build_for_run(
        self,
        *,
        run_id: str,
        tickers: list[str] | None = None,
        config: BuildConfig | None = None,
    ) -> list[BuildResult]:
        cfg = config or BuildConfig()
        tenant_id = str(cfg.tenant_id)

        if tickers is None:
            # Prefer consensus_signals tickers; fallback to predictions; then price_bars in range.
            rows = self.repo.conn.execute(
                """
                SELECT DISTINCT ticker
                FROM consensus_signals
                WHERE tenant_id = ?
                ORDER BY ticker ASC
                """,
                (tenant_id,),
            ).fetchall()
            tickers = [str(r["ticker"]) for r in rows if r and r["ticker"]]
            if not tickers:
                run = self.repo.get_prediction_run(run_id=str(run_id), tenant_id=tenant_id) or {}
                is_ = run.get("ingress_start")
                ie = run.get("ingress_end")
                ps = run.get("prediction_start")
                pe = run.get("prediction_end")
                if is_ and ie:
                    rows = self.repo.conn.execute(
                        """
                        SELECT DISTINCT ticker
                        FROM predictions
                        WHERE tenant_id = ?
                          AND timestamp >= ?
                          AND timestamp <= ?
                        ORDER BY ticker ASC
                        """,
                        (tenant_id, str(is_), str(ie)),
                    ).fetchall()
                    tickers = [str(r["ticker"]) for r in rows if r and r["ticker"]]

                if not tickers and ps and pe:
                    rows = self.repo.conn.execute(
                        """
                        SELECT DISTINCT ticker
                        FROM predictions
                        WHERE tenant_id = ?
                          AND timestamp >= ?
                          AND timestamp <= ?
                        ORDER BY ticker ASC
                        """,
                        (tenant_id, str(ps), str(pe)),
                    ).fetchall()
                    tickers = [str(r["ticker"]) for r in rows if r and r["ticker"]]

                if not tickers:
                    rows = self.repo.conn.execute(
                        """
                        SELECT DISTINCT ticker
                        FROM predictions
                        WHERE tenant_id = ?
                        ORDER BY ticker ASC
                        """,
                        (tenant_id,),
                    ).fetchall()
                    tickers = [str(r["ticker"]) for r in rows if r and r["ticker"]]

            if not tickers:
                run = self.repo.get_prediction_run(run_id=str(run_id), tenant_id=tenant_id) or {}
                ps = run.get("prediction_start")
                pe = run.get("prediction_end")
                tf = str(run.get("timeframe") or "1d")
                if ps and pe:
                    rows = self.repo.conn.execute(
                        """
                        SELECT DISTINCT ticker
                        FROM price_bars
                        WHERE tenant_id = ?
                          AND (
                            timeframe = ?
                            OR (? = '1d' AND timeframe IN ('1m','1h'))
                          )
                          AND timestamp >= ?
                          AND timestamp <= ?
                        ORDER BY ticker ASC
                        """,
                        (tenant_id, tf, tf, str(ps), str(pe)),
                    ).fetchall()
                    tickers = [str(r["ticker"]) for r in rows if r and r["ticker"]]

        out: list[BuildResult] = []
        for t in sorted({str(x).strip().upper() for x in (tickers or []) if str(x).strip()}):
            out.append(self.build(run_id=str(run_id), ticker=t, config=cfg))
        return out
