from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.db.repository import AlphaRepository
from app.engine.efficiency_champion_promotion import decide_efficiency_champion
from app.engine.predicted_series_builder import BuildConfig, PredictedSeriesBuilder
from app.engine.prediction_scoring_runner import PredictionScoringRunner


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_date_or_iso(value: str, *, end_of_day: bool = False) -> str:
    s = str(value).strip()
    if "T" in s:
        # Assume ISO-ish; normalize Z to +00:00 then back to Z.
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return _isoz(dt)
    dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt + timedelta(hours=23, minutes=59, seconds=59)
    return _isoz(dt)


def _parse_range(range_str: str) -> tuple[str, str]:
    s = str(range_str).strip()
    if ":" not in s:
        raise ValueError("range must be formatted like YYYY-MM-DD:YYYY-MM-DD")
    a, b = s.split(":", 1)
    start = _parse_date_or_iso(a, end_of_day=False)
    end = _parse_date_or_iso(b, end_of_day=True)
    return start, end


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prediction Sync / Efficiency Rating CLI",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m app.engine.score_predictions_cli eval-window --range 2024-03-21:2024-04-21 --timeframe 1d --rank-limit 10\n"
            "  python -m app.engine.score_predictions_cli score-predictions --range 2024-03-21:2024-04-21 --timeframe 1d\n"
            "  python -m app.engine.score_predictions_cli rank-strategies --ticker AAPL --timeframe 1d --min-samples 20\n"
            "\n"
            "Tip: Prefer the interactive launcher:\n"
            "  python start.py\n"
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # 1. score-predictions
    score = sub.add_parser(
        "score-predictions",
        help="Score predicted series vs actuals for a run",
        description="Score predicted series vs actuals for a run",
    )
    score.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    score.add_argument("--tenant-id", default="default")
    score.add_argument("--timeframe", default="1d")
    score.add_argument("--regime", default=None, help="Optional regime tag for this run (e.g. HIGH/NORMAL/LOW)")
    score.add_argument("--range", dest="prediction_range", required=True, help="Prediction range: YYYY-MM-DD:YYYY-MM-DD")
    score.add_argument("--ingress-range", default=None, help="Ingress range: YYYY-MM-DD:YYYY-MM-DD (default: 30d before prediction start)")
    score.add_argument("--run-id", default=None, help="Optional existing run id to reuse")
    score.add_argument("--ticker", default=None)
    score.add_argument("--strategy-id", default=None)
    score.add_argument(
        "--autobuild-predicted-series",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Attempt to build consensus predicted series for this run before scoring",
    )
    score.add_argument(
        "--materialize-actual",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Populate actual series from price_bars when missing",
    )

    # 2. backfill-scores
    backfill = sub.add_parser(
        "backfill-scores",
        help="Score any unscored prediction runs",
        description="Score any unscored prediction runs",
    )
    backfill.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    backfill.add_argument("--tenant-id", default="default")
    backfill.add_argument("--limit", type=int, default=200)
    backfill.add_argument(
        "--materialize-actual",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Populate actual series from price_bars when missing",
    )

    # 3. rank-strategies
    rank = sub.add_parser(
        "rank-strategies",
        help="Rank strategies by efficiency rating",
        description="Rank strategies by efficiency rating",
    )
    rank.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    rank.add_argument("--tenant-id", default="default")
    rank.add_argument("--ticker", default=None)
    rank.add_argument("--timeframe", default=None)
    rank.add_argument("--forecast-days", type=int, default=None)
    rank.add_argument("--regime", default=None)
    rank.add_argument("--min-samples", type=int, default=None)
    rank.add_argument("--min-total-forecast-days", type=int, default=None)
    rank.add_argument("--limit", type=int, default=20)

    # 4. promote-strategies (Autonomous Engine)
    promote_auto = sub.add_parser(
        "promote-strategies",
        help="Evaluate and promote strategies to champion status (Autonomous)",
        description="Evaluate and promote strategies to champion status (Autonomous)",
    )
    promote_auto.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    promote_auto.add_argument("--tenant-id", default="default")

    # 5. build-predicted-series
    build = sub.add_parser(
        "build-predicted-series",
        help="Build consensus predicted series points for a run",
        description="Build consensus predicted series points for a run",
    )
    build.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    build.add_argument("--tenant-id", default="default")
    build.add_argument("--run-id", required=True)
    build.add_argument("--ticker", action="append", default=None, help="Repeatable; omit to build for all tickers discovered")
    build.add_argument("--model", default="directional_drift")
    build.add_argument("--source", default="consensus")
    build.add_argument("--force", action="store_true", help="Rebuild even if points already exist")
    build.add_argument("--cap", type=float, default=0.05)
    build.add_argument("--vol-lookback", type=int, default=20)

    # 6. promote-champions (Manual/Granular)
    promote_man = sub.add_parser(
        "promote-champions",
        help="Dry-run/apply: persist an active efficiency champion per ticker (Manual controls)",
        description="Dry-run/apply: persist an active efficiency champion per ticker (Manual controls)",
    )
    promote_man.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    promote_man.add_argument("--tenant-id", default="default")
    promote_man.add_argument("--run-id", default=None)
    promote_man.add_argument("--ticker", action="append", default=None)
    promote_man.add_argument("--timeframe", default="1d")
    promote_man.add_argument("--forecast_days", type=int, default=None)
    promote_man.add_argument("--regime", default=None)
    promote_man.add_argument("--min-samples", type=int, default=10)
    promote_man.add_argument("--min-total-forecast-days", type=int, default=0)
    promote_man.add_argument("--min-efficiency", type=float, default=0.1)
    promote_man.add_argument("--min-delta", type=float, default=0.01)
    promote_man.add_argument("--apply", action="store_true", help="Persist the champion to the database")

    # 7. eval-window
    evalw = sub.add_parser(
        "eval-window",
        help="One-shot: create run -> build series -> score -> rank (within run)",
        description="One-shot: create run -> build series -> score -> rank (within run)",
    )
    evalw.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    evalw.add_argument("--tenant-id", default="default")
    evalw.add_argument("--timeframe", default="1d")
    evalw.add_argument("--regime", default=None, help="Optional regime tag for this run")
    evalw.add_argument("--range", dest="prediction_range", required=True, help="Prediction range: YYYY-MM-DD:YYYY-MM-DD")
    evalw.add_argument("--ingress-range", default=None, help="Ingress range: YYYY-MM-DD:YYYY-MM-DD")
    evalw.add_argument("--ticker", action="append", default=None)
    evalw.add_argument("--model", default="directional_drift")
    evalw.add_argument("--source", default="consensus")
    evalw.add_argument("--cap", type=float, default=0.05)
    evalw.add_argument("--vol-lookback", type=int, default=20)
    evalw.add_argument(
        "--materialize-actual",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    evalw.add_argument("--rank-limit", type=int, default=10)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    repo = AlphaRepository(args.db)
    runner = PredictionScoringRunner(repository=repo)

    try:
        if args.command == "score-predictions":
            pred_start, pred_end = _parse_range(args.prediction_range)

            if args.ingress_range:
                ing_start, ing_end = _parse_range(args.ingress_range)
            else:
                dt_start = datetime.fromisoformat(pred_start.replace("Z", "+00:00"))
                ing_end = _isoz(dt_start - timedelta(seconds=1))
                ing_start = _isoz(dt_start - timedelta(days=30))

            run_id = repo.create_prediction_run(
                tenant_id=args.tenant_id,
                timeframe=args.timeframe,
                regime=args.regime,
                ingress_start=ing_start,
                ingress_end=ing_end,
                prediction_start=pred_start,
                prediction_end=pred_end,
            )

            rows = runner.score_run(
                run_id=run_id,
                tenant_id=args.tenant_id,
                ticker=args.ticker,
                timeframe=args.timeframe,
                materialize_actual=bool(args.materialize_actual),
                autobuild_predicted_series=bool(args.autobuild_predicted_series),
            )
            print(f"run_id={run_id} scored={len(rows)}")
            return 0 if rows else 2

        if args.command == "backfill-scores":
            runs = repo.list_unscored_runs(tenant_id=args.tenant_id, limit=args.limit)
            total = 0
            for r in runs:
                rid = str(r["id"])
                rows = runner.score_run(
                    run_id=rid,
                    tenant_id=args.tenant_id,
                    materialize_actual=bool(args.materialize_actual),
                )
                total += len(rows)
            print(f"runs_scored={len(runs)} series_scored={total}")
            return 0

        if args.command == "promote-strategies":
            from app.engine.promotion_engine import PromotionEngine
            engine = PromotionEngine(repository=repo)
            engine.evaluate_all_contexts(tenant_id=args.tenant_id)
            print(f"Promotion evaluation complete for tenant={args.tenant_id}")
            return 0

        if args.command == "rank-strategies":
            rows = repo.rank_strategies(
                tenant_id=args.tenant_id,
                ticker=args.ticker,
                timeframe=args.timeframe,
                forecast_days=args.forecast_days,
                regime=args.regime,
                min_samples=args.min_samples,
                min_total_forecast_days=args.min_total_forecast_days,
                limit=args.limit,
            )
            for r in rows:
                print(
                    f"{r['strategy_id']} {r.get('strategy_version','')} "
                    f"avg={float(r['avg_efficiency_rating']):.4f} samples={int(r['samples'])} days={int(r['total_forecast_days'] or 0)}"
                )
            return 0

        if args.command == "build-predicted-series":
            builder = PredictedSeriesBuilder(repository=repo)
            cfg = BuildConfig(
                model=str(args.model),
                signal_source=str(args.source),
                cap_daily_return=float(args.cap),
                vol_lookback=int(args.vol_lookback),
                skip_if_exists=(not bool(args.force)),
                tenant_id=str(args.tenant_id),
            )
            results = builder.build_for_run(run_id=str(args.run_id), tickers=(args.ticker if args.ticker else None), config=cfg)
            for r in results:
                status = "skipped" if r.skipped else "built"
                reason = f" ({r.skip_reason})" if r.skip_reason else ""
                print(f"{r.ticker} {status} points={r.points_written}{reason}")
            return 0

        if args.command == "promote-champions":
            if args.ticker:
                tickers = [str(t).strip().upper() for t in args.ticker if str(t).strip()]
            elif args.run_id:
                tickers = repo.list_run_tickers(run_id=str(args.run_id), tenant_id=str(args.tenant_id))
            else:
                tickers = repo.list_scored_tickers(
                    tenant_id=str(args.tenant_id),
                    timeframe=(str(args.timeframe) if args.timeframe else None),
                    forecast_days=(int(args.forecast_days) if args.forecast_days is not None else None),
                    regime=(str(args.regime) if args.regime else None),
                )

            promoted = 0
            kept = 0
            skipped = 0

            for tk in tickers:
                incumbent = repo.get_efficiency_champion_record(
                    tenant_id=str(args.tenant_id),
                    ticker=str(tk),
                    timeframe=str(args.timeframe),
                    forecast_days=(int(args.forecast_days) if args.forecast_days is not None else None),
                    regime=(str(args.regime) if args.regime else None),
                )
                challenger = repo.select_efficiency_champion(
                    tenant_id=str(args.tenant_id),
                    ticker=str(tk),
                    timeframe=str(args.timeframe),
                    forecast_days=(int(args.forecast_days) if args.forecast_days is not None else None),
                    regime=(str(args.regime) if args.regime else None),
                    min_samples=int(args.min_samples),
                    min_total_forecast_days=int(args.min_total_forecast_days),
                )

                decision = decide_efficiency_champion(
                    incumbent=incumbent,
                    challenger=challenger,
                    min_efficiency=float(args.min_efficiency),
                    min_delta_vs_incumbent=float(args.min_delta),
                )

                inc_id = str((incumbent or {}).get("strategy_id") or "-")
                ch_id = str((challenger or {}).get("strategy_id") or "-")

                print(f"{tk} {decision.action} reason={decision.reason} incumbent={inc_id} challenger={ch_id}")

                if decision.action == "promote":
                    promoted += 1
                    if bool(args.apply) and challenger:
                        repo.upsert_efficiency_champion_record(
                            tenant_id=str(args.tenant_id),
                            ticker=str(tk),
                            timeframe=str(args.timeframe),
                            forecast_days=(int(args.forecast_days) if args.forecast_days is not None else None),
                            regime=(str(args.regime) if args.regime else None),
                            strategy_id=str(challenger["strategy_id"]),
                            strategy_version=(str(challenger.get("strategy_version") or "") or None),
                            avg_efficiency_rating=float(challenger["avg_efficiency_rating"]),
                            samples=int(challenger["samples"]),
                            total_forecast_days=int(challenger.get("total_forecast_days") or 0),
                        )
                elif decision.action == "keep":
                    kept += 1
                else:
                    skipped += 1

            mode = "applied" if bool(args.apply) else "dry_run"
            print(f"mode={mode} tickers={len(tickers)} promoted={promoted} kept={kept} skipped={skipped}")
            return 0

        if args.command == "eval-window":
            effective_tenant = str(args.tenant_id)
            # Automatic tenant detection logic (simplified for clear repo)
            pred_start, pred_end = _parse_range(args.prediction_range)
            if args.ingress_range:
                ing_start, ing_end = _parse_range(args.ingress_range)
            else:
                dt_start = datetime.fromisoformat(pred_start.replace("Z", "+00:00"))
                ing_end = _isoz(dt_start - timedelta(seconds=1))
                ing_start = _isoz(dt_start - timedelta(days=30))

            run_id = repo.create_prediction_run(
                tenant_id=effective_tenant,
                timeframe=args.timeframe,
                regime=args.regime,
                ingress_start=ing_start,
                ingress_end=ing_end,
                prediction_start=pred_start,
                prediction_end=pred_end,
            )

            builder = PredictedSeriesBuilder(repository=repo)
            cfg = BuildConfig(
                model=str(args.model),
                signal_source=str(args.source),
                cap_daily_return=float(args.cap),
                vol_lookback=int(args.vol_lookback),
                skip_if_exists=True,
                tenant_id=effective_tenant,
            )
            build_results = builder.build_for_run(run_id=str(run_id), tickers=(args.ticker if args.ticker else None), config=cfg)
            
            scored_rows = runner.score_run(
                run_id=str(run_id),
                tenant_id=effective_tenant,
                timeframe=str(args.timeframe),
                materialize_actual=bool(args.materialize_actual),
                autobuild_predicted_series=False,
            )
            print(f"run_id={run_id} scored={len(scored_rows)}")

            ranked = sorted(scored_rows, key=lambda r: float(r.get("efficiency_rating", 0.0)), reverse=True)
            for r in ranked[: int(args.rank_limit)]:
                print(
                    f"{r['ticker']} {r['strategy_id']} "
                    f"eff={float(r['efficiency_rating']):.4f} "
                    f"dir={float(r['direction_hit_rate']):.3f} days={int(r['forecast_days'])}"
                )
            return 0 if scored_rows else 2

        return 1
    finally:
        repo.close()


if __name__ == "__main__":
    raise SystemExit(main())
