from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from app.core.types import MRAOutcome, Prediction, PredictionDirection, ScoredEvent
from app.strategies.base import StrategyBase


def _safe_float(x: object, default: float = 0.0) -> float:
    try:
        return float(x)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float(default)


def _rel_key(horizon: str, benchmark: str) -> str:
    return f"rel_return_{str(horizon).strip().lower()}_vs_{str(benchmark).strip().upper()}"


class RelativeStrengthVsBenchmarkStrategy(StrategyBase):
    """
    Cross-asset / relative strength strategy.

    Emits a signal when the target has clear leadership (or weakness) vs a benchmark.
    Requires upstream price_context to include either:
    - `rel_return_{1d|7d|30d}_vs_{BENCH}` keys, or
    - `benchmarks[{BENCH}][return_*]` + `return_*` on the target.
    """

    def maybe_predict(
        self,
        scored_event: ScoredEvent,
        mra: MRAOutcome,
        price_context: dict,
        event_timestamp: datetime,
    ) -> Prediction | None:
        cfg = dict(self.config.config or {})
        benchmark = str(cfg.get("benchmark", "SPY")).strip().upper()
        horizon = str(cfg.get("horizon", "30d")).strip().lower()

        # Signal horizon for computing leadership can differ from prediction horizon.
        signal_h = str(cfg.get("signal_horizon", "7d")).strip().lower()
        if signal_h not in {"1d", "7d", "30d"}:
            signal_h = "7d"

        rel = price_context.get(_rel_key(signal_h, benchmark))
        if rel is None:
            # Try compute from benchmarks payload.
            bmarks = price_context.get("benchmarks")
            if isinstance(bmarks, dict):
                b = bmarks.get(benchmark)
                if isinstance(b, dict):
                    tr = price_context.get(f"return_{signal_h}")
                    br = b.get(f"return_{signal_h}")
                    if tr is not None and br is not None:
                        rel = _safe_float(tr, 0.0) - _safe_float(br, 0.0)

        if rel is None:
            return None

        rel = float(_safe_float(rel, 0.0))
        min_rel = float(_safe_float(cfg.get("min_rel_return", 0.01), 0.01))
        if abs(rel) < min_rel:
            return None

        target_abs = _safe_float(price_context.get(f"return_{signal_h}"), 0.0)
        min_abs = float(_safe_float(cfg.get("min_abs_return", 0.0), 0.0))
        if abs(target_abs) < min_abs:
            return None

        # Optional: require benchmark trend alignment (avoid long vs falling market, etc.)
        require_bench_align = bool(cfg.get("require_benchmark_alignment", False))
        if require_bench_align:
            bmarks = price_context.get("benchmarks")
            if isinstance(bmarks, dict) and isinstance(bmarks.get(benchmark), dict):
                bench_r = _safe_float(bmarks[benchmark].get(f"return_{signal_h}"), 0.0)
                if rel > 0 and bench_r < 0:
                    return None
                if rel < 0 and bench_r > 0:
                    return None

        direction: PredictionDirection = "up" if rel > 0 else "down"

        # Confidence: based on magnitude of relative return and a small boost from regime/trend.
        rel_boost = max(0.0, min((abs(rel) - min_rel) / max(min_rel * 3.0, 1e-6), 1.0))
        conf = 0.55 + 0.25 * rel_boost

        trend_strength = str(price_context.get("trend_strength") or price_context.get("trend") or "UNKNOWN")
        if trend_strength == "STRONG":
            conf += 0.05
        elif trend_strength == "WEAK":
            conf -= 0.03

        conf = max(0.1, min(conf, 0.90))

        return Prediction(
            id=str(uuid4()),
            strategy_id=self.config.id,
            scored_event_id=scored_event.id,
            ticker=scored_event.primary_ticker,
            timestamp=event_timestamp,
            prediction=direction,
            confidence=float(conf),
            horizon=horizon,
            entry_price=float(price_context.get("entry_price", 100.0)),
            mode=self.config.mode,
            feature_snapshot={
                "family": "cross_asset",
                "setup": "relative_strength",
                "benchmark": benchmark,
                "signal_horizon": signal_h,
                "rel_return": float(rel),
                "target_return": float(target_abs),
            },
        )

