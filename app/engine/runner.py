from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable
import json

from app.engine.consensus_engine import ConsensusEngine, TrackSignal
from app.core.regime_manager import RegimeManager
from app.core.mra import compute_mra
from app.core.repository import Repository
from app.core.scoring import score_event
from app.core.types import RawEvent, StrategyConfig, Prediction, PredictionOutcome
from app.engine.evaluate import evaluate_prediction, summarize_outcomes
from app.engine.strategy_factory import build_strategy_instance


class Runner:
    """
    v2.7 runner excerpt:
    - accepts sentiment + quant track outputs
    - computes regime-aware weighted consensus
    - returns a prediction payload that can be stored by existing persistence code
    """

    def __init__(self) -> None:
        self.consensus_engine = ConsensusEngine()

    def build_prediction(
        self,
        ticker: str,
        sentiment_direction: str,
        sentiment_confidence: float,
        quant_direction: str,
        quant_confidence: float,
        realized_volatility: float,
        historical_volatility_window: list[float],
        adx_value: float | None = None,
        sentiment_stability: float | None = None,
        quant_stability: float | None = None,
    ) -> dict[str, Any]:
        sentiment_signal = TrackSignal(
            ticker=ticker,
            direction=sentiment_direction,
            confidence=sentiment_confidence,
            track="sentiment",
            metadata={"source": "sentiment_track"},
        )
        quant_signal = TrackSignal(
            ticker=ticker,
            direction=quant_direction,
            confidence=quant_confidence,
            track="quant",
            metadata={"source": "quant_track"},
        )

        consensus = self.consensus_engine.combine(
            sentiment_signal=sentiment_signal,
            quant_signal=quant_signal,
            realized_volatility=realized_volatility,
            historical_volatility_window=historical_volatility_window,
            adx_value=adx_value,
            sentiment_stability=sentiment_stability,
            quant_stability=quant_stability,
        )

        return {
            "ticker": ticker,
            "timestamp": datetime.utcnow().isoformat(),
            "prediction": consensus.direction,
            "confidence": consensus.confidence,
            "track": "hybrid",
            "regime": consensus.regime["volatility_regime"],
            "regime_snapshot": consensus.regime,
            "sentiment_confidence": consensus.sentiment_confidence,
            "quant_confidence": consensus.quant_confidence,
            "weighted_consensus": consensus.weighted_consensus,
            "metadata": consensus.metadata,
        }


def _strategy_track(strategy_type: str) -> str:
    st = strategy_type.lower()
    if st.startswith("text_") or st.startswith("sentiment"):
        return "sentiment"
    if st.startswith("technical_") or st.startswith("baseline_") or st.startswith("quant"):
        return "quant"
    if st == "consensus":
        return "consensus"
    return "unknown"


def _strategy_display_name(config: StrategyConfig) -> str:
    return f"{config.strategy_type}:{config.version}"


def _estimate_realized_volatility(price_context: dict) -> float:
    if "realized_volatility" in price_context:
        try:
            return float(price_context["realized_volatility"])
        except (TypeError, ValueError):
            return 0.0

    candidates = []
    for key in ("return_1m", "return_5m", "return_15m", "return_1h"):
        try:
            candidates.append(abs(float(price_context.get(key, 0.0))))
        except (TypeError, ValueError):
            continue
    if not candidates:
        return 0.0
    return max(candidates)


def load_strategy_configs(
    strategy_dir: str | Path = "experiments/strategies",
) -> list[StrategyConfig]:
    """
    Loads StrategyConfig-like JSON files (id/name/version/strategy_type/mode/config/active)
    and ignores overlay scaffolds that don't match that shape.
    """
    base = Path(strategy_dir)
    if not base.is_absolute():
        project_root = Path(__file__).resolve().parents[2]
        base = project_root / base
    if not base.exists():
        return []

    configs: list[StrategyConfig] = []
    for path in sorted(base.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        if not isinstance(payload, dict):
            continue
        if "strategy_type" not in payload:
            continue
        if "id" not in payload or "name" not in payload or "version" not in payload:
            continue
        if "mode" not in payload or "config" not in payload:
            continue

        try:
            configs.append(
                StrategyConfig(
                    id=str(payload["id"]),
                    name=str(payload["name"]),
                    version=str(payload["version"]),
                    strategy_type=str(payload["strategy_type"]),
                    mode=str(payload["mode"]),
                    config=dict(payload.get("config") or {}),
                    active=bool(payload.get("active", True)),
                )
            )
        except Exception:
            continue

    return configs


def _build_strategy_instance(config: StrategyConfig):
    """
    Returns a StrategyBase instance for configs that map to the current StrategyBase API.
    Unknown strategy_type returns None so pipeline can skip it safely.
    """
    return build_strategy_instance(config)


def run_pipeline(
    raw_events: list[RawEvent],
    price_contexts: dict[str, dict],
    *,
    persist: bool = False,
    db_path: str | Path = "data/alpha.db",
    strategy_configs: list[StrategyConfig] | None = None,
    mode_override: str | None = None,
    evaluate_outcomes: bool = True,
) -> dict[str, Any]:
    """
    End-to-end POC vertical slice:
    raw events + price_context -> scoring -> MRA -> strategies -> predictions -> outcomes -> summary (+ optional DB writes).

    Returns dicts that are easy to write to CSV or render in Streamlit.
    """
    configs = strategy_configs if strategy_configs is not None else load_strategy_configs()
    active_configs = [c for c in configs if c.active]

    strategies: list[tuple[StrategyConfig, Any]] = []
    for cfg in active_configs:
        instance = _build_strategy_instance(cfg)
        if instance is not None:
            strategies.append((cfg, instance))

    consensus_config = StrategyConfig(
        id="consensus-v1",
        name="consensus_v1",
        version="v1",
        strategy_type="consensus",
        mode="backtest",
        config={},
        active=True,
    )

    repo: Repository | None = None
    if persist:
        repo = Repository(db_path=db_path)
        with repo.transaction():
            for cfg, _ in strategies:
                repo.persist_strategy(cfg)
            repo.persist_strategy(consensus_config)

    raw_event_rows: list[dict[str, Any]] = []
    scored_event_rows: list[dict[str, Any]] = []
    mra_rows: list[dict[str, Any]] = []
    prediction_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []

    predictions: list[Prediction] = []
    outcomes: list[PredictionOutcome] = []

    runner = Runner()
    regime_manager = RegimeManager()
    summary_rows: list[dict[str, Any]] = []

    with (repo.transaction() if repo is not None else nullcontext()):
        for raw in raw_events:
            price_context = price_contexts.get(raw.id, {})
            if repo is not None:
                repo.persist_raw_event(raw)

            raw_event_rows.append(
                {
                    "id": raw.id,
                    "timestamp": raw.timestamp.isoformat(),
                    "source": raw.source,
                    "text": raw.text,
                    "tickers": ",".join(raw.tickers),
                }
            )

            scored = score_event(raw)
            if repo is not None:
                repo.persist_scored_event(scored, raw_event_id=raw.id)

            scored_event_rows.append(
                {
                    "id": scored.id,
                    "raw_event_id": scored.raw_event_id,
                    "primary_ticker": scored.primary_ticker,
                    "category": scored.category,
                    "materiality": scored.materiality,
                    "direction": scored.direction,
                    "confidence": scored.confidence,
                    "company_relevance": scored.company_relevance,
                    "concept_tags": json.dumps(scored.concept_tags),
                    "explanation_terms": json.dumps(scored.explanation_terms),
                    "scorer_version": scored.scorer_version,
                    "taxonomy_version": scored.taxonomy_version,
                }
            )

            mra = compute_mra(scored, price_context)
            if repo is not None:
                repo.persist_mra_outcome(mra, scored_event_id=scored.id)

            mra_rows.append(
                {
                    "id": mra.id,
                    "scored_event_id": mra.scored_event_id,
                    "return_1m": mra.return_1m,
                    "return_5m": mra.return_5m,
                    "return_15m": mra.return_15m,
                    "return_1h": mra.return_1h,
                    "volume_ratio": mra.volume_ratio,
                    "vwap_distance": mra.vwap_distance,
                    "range_expansion": mra.range_expansion,
                    "continuation_slope": mra.continuation_slope,
                    "pullback_depth": mra.pullback_depth,
                    "mra_score": mra.mra_score,
                }
            )

            event_predictions: list[tuple[str, StrategyConfig, Prediction]] = []

            realized_vol = _estimate_realized_volatility(price_context)
            hist_window = price_context.get("historical_volatility_window")
            if not isinstance(hist_window, list) or not hist_window:
                hist_window = [realized_vol for _ in range(20)]

            adx_value: float | None = None
            for key in ("adx", "adx_14", "adx_value"):
                if key in price_context:
                    try:
                        adx_value = float(price_context[key])
                    except (TypeError, ValueError):
                        adx_value = None
                    break

            regime_snapshot = regime_manager.classify(
                realized_volatility=float(realized_vol),
                historical_volatility_window=[float(x) for x in hist_window],
                adx_value=adx_value,
            )
            regime_payload = asdict(regime_snapshot)
            vol_regime = str(regime_snapshot.volatility_regime)

            for cfg, strat in strategies:
                pred = strat.maybe_predict(scored, mra, price_context, raw.timestamp)
                if pred is None:
                    continue

                if mode_override is not None:
                    pred.mode = mode_override

                pred.feature_snapshot.setdefault("regime", vol_regime)
                pred.feature_snapshot.setdefault("trend_strength", regime_snapshot.trend_strength)
                pred.feature_snapshot.setdefault("regime_snapshot", regime_payload)

                track = _strategy_track(cfg.strategy_type)
                strategy_name = _strategy_display_name(cfg)
                predictions.append(pred)
                event_predictions.append((track, cfg, pred))

                if repo is not None:
                    repo.persist_prediction(pred)

                prediction_rows.append(
                    {
                        "id": pred.id,
                        "strategy_id": pred.strategy_id,
                        "strategy_name": strategy_name,
                        "strategy_type": cfg.strategy_type,
                        "track": track,
                        "scored_event_id": pred.scored_event_id,
                        "ticker": pred.ticker,
                        "timestamp": pred.timestamp.isoformat(),
                        "prediction": pred.prediction,
                        "confidence": pred.confidence,
                        "horizon": pred.horizon,
                        "entry_price": pred.entry_price,
                        "mode": pred.mode,
                        "regime": vol_regime,
                        "trend_strength": regime_snapshot.trend_strength,
                    }
                )

            sentiment = [t for t in event_predictions if t[0] == "sentiment"]
            quant = [t for t in event_predictions if t[0] == "quant"]
            if sentiment and quant:
                best_sentiment = sorted(sentiment, key=lambda r: r[2].confidence, reverse=True)[0][2]
                best_quant = sorted(quant, key=lambda r: r[2].confidence, reverse=True)[0][2]

                sentiment_stability = repo.get_strategy_stability_score(best_sentiment.strategy_id) if repo is not None else None
                quant_stability = repo.get_strategy_stability_score(best_quant.strategy_id) if repo is not None else None

                consensus_payload = runner.build_prediction(
                    ticker=scored.primary_ticker,
                    sentiment_direction=best_sentiment.prediction,
                    sentiment_confidence=float(best_sentiment.confidence),
                    quant_direction=best_quant.prediction,
                    quant_confidence=float(best_quant.confidence),
                    realized_volatility=float(realized_vol),
                    historical_volatility_window=[float(x) for x in hist_window],
                    adx_value=adx_value,
                    sentiment_stability=sentiment_stability,
                    quant_stability=quant_stability,
                )

                consensus_pred = Prediction(
                    id=f"cons_{best_sentiment.id[:8]}_{best_quant.id[:8]}",
                    strategy_id=consensus_config.id,
                    scored_event_id=scored.id,
                    ticker=scored.primary_ticker,
                    timestamp=raw.timestamp,
                    prediction=str(consensus_payload["prediction"]),  # type: ignore[arg-type]
                    confidence=float(consensus_payload["confidence"]),
                    horizon=str(best_sentiment.horizon or best_quant.horizon),
                    entry_price=float(price_context.get("entry_price", 100.0)),
                    mode=mode_override or "backtest",
                    feature_snapshot={
                        "regime": consensus_payload.get("regime"),
                        "regime_snapshot": consensus_payload.get("regime_snapshot"),
                        "trend_strength": regime_snapshot.trend_strength,
                        "sentiment_confidence": consensus_payload.get("sentiment_confidence"),
                        "quant_confidence": consensus_payload.get("quant_confidence"),
                        "weighted_consensus": consensus_payload.get("weighted_consensus"),
                        "consensus_metadata": consensus_payload.get("metadata", {}),
                    },
                )
                predictions.append(consensus_pred)

                if repo is not None:
                    repo.persist_prediction(consensus_pred)

                prediction_rows.append(
                    {
                        "id": consensus_pred.id,
                        "strategy_id": consensus_pred.strategy_id,
                        "strategy_name": _strategy_display_name(consensus_config),
                        "strategy_type": consensus_config.strategy_type,
                        "track": "consensus",
                        "scored_event_id": consensus_pred.scored_event_id,
                        "ticker": consensus_pred.ticker,
                        "timestamp": consensus_pred.timestamp.isoformat(),
                        "prediction": consensus_pred.prediction,
                        "confidence": consensus_pred.confidence,
                        "horizon": consensus_pred.horizon,
                        "entry_price": consensus_pred.entry_price,
                        "mode": consensus_pred.mode,
                        "regime": vol_regime,
                        "trend_strength": regime_snapshot.trend_strength,
                    }
                )

    if not evaluate_outcomes:
        if repo is not None:
            repo.close()
        return {
            "raw_event_rows": raw_event_rows,
            "scored_event_rows": scored_event_rows,
            "mra_rows": mra_rows,
            "prediction_rows": prediction_rows,
            "outcome_rows": [],
            "summary": [],
            "db_path": str(db_path),
        }

    raw_to_scored: dict[str, str] = {row["raw_event_id"]: row["id"] for row in scored_event_rows}
    scored_to_raw: dict[str, str] = {scored_id: raw_id for raw_id, scored_id in raw_to_scored.items()}

    with (repo.transaction() if repo is not None else nullcontext()):
        for pred in predictions:
            raw_event_id = scored_to_raw.get(pred.scored_event_id)
            if raw_event_id is None:
                continue
            price_context = price_contexts.get(raw_event_id, {})
            out = evaluate_prediction(pred, price_context)
            outcomes.append(out)

            if repo is not None:
                repo.persist_outcome(out)

            outcome_rows.append(
                {
                    "id": out.id,
                    "prediction_id": out.prediction_id,
                    "exit_price": out.exit_price,
                    "return_pct": out.return_pct,
                    "direction_correct": out.direction_correct,
                    "max_runup": out.max_runup,
                    "max_drawdown": out.max_drawdown,
                    "evaluated_at": out.evaluated_at.isoformat(),
                    "exit_reason": out.exit_reason,
                }
            )

    pred_by_id: dict[str, dict[str, Any]] = {row["id"]: row for row in prediction_rows}
    joined: list[dict[str, Any]] = []
    for out in outcomes:
        pred_row = pred_by_id.get(out.prediction_id)
        if not pred_row:
            continue
        joined.append(
            {
                "strategy_name": pred_row.get("strategy_name"),
                "strategy_type": pred_row.get("strategy_type"),
                "mode": pred_row.get("mode", "backtest"),
                "horizon": pred_row.get("horizon"),
                "confidence": float(pred_row.get("confidence", 0.0)),
                "return_pct": float(out.return_pct),
                "direction_correct": bool(out.direction_correct),
            }
        )

    summary_rows = summarize_outcomes(joined)

    if repo is not None:
        repo.close()

    return {
        "raw_event_rows": raw_event_rows,
        "scored_event_rows": scored_event_rows,
        "mra_rows": mra_rows,
        "prediction_rows": prediction_rows,
        "outcome_rows": outcome_rows,
        "summary": summary_rows,
        "db_path": str(db_path),
    }
