from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

from app.db.repository import AlphaRepository
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Prediction Sync / Efficiency Rating CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    score = sub.add_parser("score-predictions", help="Score predicted series vs actuals for a run")
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
        "--materialize-actual",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Populate actual series from price_bars when missing (default: true)",
    )

    backfill = sub.add_parser("backfill-scores", help="Score any unscored prediction runs")
    backfill.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    backfill.add_argument("--tenant-id", default="default")
    backfill.add_argument("--limit", type=int, default=200)
    backfill.add_argument(
        "--materialize-actual",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Populate actual series from price_bars when missing (default: true)",
    )

    rank = sub.add_parser("rank-strategies", help="Rank strategies by efficiency rating")
    rank.add_argument("--db", default="data/alpha.db", help="SQLite path (default: data/alpha.db)")
    rank.add_argument("--tenant-id", default="default")
    rank.add_argument("--ticker", default=None)
    rank.add_argument("--timeframe", default=None)
    rank.add_argument("--forecast-days", type=int, default=None)
    rank.add_argument("--regime", default=None)
    rank.add_argument("--min-samples", type=int, default=None)
    rank.add_argument("--min-total-forecast-days", type=int, default=None)
    rank.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

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
                run_id=args.run_id,
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
                strategy_id=args.strategy_id,
                materialize_actual=bool(args.materialize_actual),
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

        return 1
    finally:
        repo.close()


if __name__ == "__main__":
    raise SystemExit(main())
