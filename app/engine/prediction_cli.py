from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone

from app.core.time_utils import normalize_timestamp
from app.db.repository import AlphaRepository
from app.engine.predicted_series_builder import BuildConfig, PredictedSeriesBuilder


def _isoz(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_date(value: str) -> datetime:
    # Treat YYYY-MM-DD as midnight UTC.
    s = str(value).strip()
    if "T" in s:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Prediction queue runner (build predicted series for queued symbols)",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m app.engine.prediction_cli run-queue --as-of 2026-04-12\n"
            "  python -m app.engine.prediction_cli run-queue --forecast-days 30 --ingress-days 30 --limit 50\n"
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    rq = sub.add_parser(
        "run-queue",
        help="Consume prediction_queue(pending) and write predicted_series_points",
        description="Consume prediction_queue(pending) and write predicted_series_points",
    )
    rq.add_argument("--db", default="data/alpha.db")
    rq.add_argument("--tenant-id", default="default")
    rq.add_argument("--as-of", dest="as_of_date", default=None, help="Queue as_of_date (YYYY-MM-DD). Default: today (UTC).")
    rq.add_argument("--limit", type=int, default=200)
    rq.add_argument("--timeframe", default="1d", help="Predicted series timeframe (default: 1d)")
    rq.add_argument("--forecast-days", type=int, default=30, help="Prediction window length (default: 30)")
    rq.add_argument("--ingress-days", type=int, default=30, help="Ingress lookback (default: 30)")
    rq.add_argument(
        "--mark-status",
        default="processed",
        choices=["processed", "skipped", "failed"],
        help="Queue status to write after a successful build (default: processed)",
    )
    rq.add_argument(
        "--freshness-hours",
        type=int,
        default=20,
        dest="freshness_hours",
        help="Skip tickers already predicted within this many hours (default: 20). Pass 0 to disable.",
    )
    rq.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would run without writing predicted series or updating queue rows",
    )
    return p


def _resolve_as_of_date(value: str | None) -> datetime:
    if value:
        return _parse_iso_date(value)
    # Default: today in UTC (midnight boundary is only for labeling as_of_date).
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _seed_consensus_from_queue_metadata(*, repo: AlphaRepository, ticker: str, metadata_json: str, tenant_id: str) -> bool:
    """
    Best-effort: if a ticker has no consensus_signals/predictions yet, seed a consensus signal
    from prediction_queue metadata (e.g. avg_score).

    This makes the daily batch pipeline usable even when upstream "signal materialization"
    hasn’t been wired up yet.
    """
    try:
        row = repo.conn.execute(
            "SELECT COUNT(*) as n FROM consensus_signals WHERE tenant_id=? AND ticker=?",
            (str(tenant_id), str(ticker)),
        ).fetchone()
        if row is not None and int(row["n"] or 0) > 0:
            return False
    except Exception:
        pass

    try:
        payload = json.loads(str(metadata_json or "{}"))
    except Exception:
        payload = {}

    avg_score = payload.get("avg_score")
    try:
        s = float(avg_score)
    except Exception:
        s = None

    if s is None:
        return False

    p_final = _clamp((s * 2.0) - 1.0, -1.0, 1.0)
    conf = _clamp(abs(p_final), 0.0, 1.0)

    repo.save_consensus_signal(
        {
            "ticker": str(ticker).upper(),
            "regime": "DISCOVERY",
            "sentiment_strategy_id": "seeded_queue",
            "quant_strategy_id": "seeded_queue",
            "sentiment_score": conf,
            "quant_score": conf,
            "ws": 0.5,
            "wq": 0.5,
            "agreement_bonus": 0.0,
            "p_final": p_final,
            "stability_score": 0.5,
        },
        tenant_id=str(tenant_id),
    )
    return True


def _merge_metadata(existing_json: str | None, extra: dict) -> str:
    base: dict = {}
    try:
        payload = json.loads(str(existing_json or "{}"))
        if isinstance(payload, dict):
            base = dict(payload)
    except Exception:
        base = {}
    base.update(dict(extra))
    return json.dumps(base, sort_keys=True)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "run-queue":
        tenant_id = str(args.tenant_id)
        as_of_dt = _resolve_as_of_date(args.as_of_date)
        as_of_date = as_of_dt.date().isoformat()

        repo = AlphaRepository(db_path=str(args.db))
        job_id: str | None = None
        try:
            job_id = repo.start_prediction_job(job_type="run_queue", as_of_date=as_of_date, tenant_id=tenant_id)
            rows = repo.list_prediction_queue(
                as_of_date=as_of_date,
                status="pending",
                limit=int(args.limit),
                tenant_id=tenant_id,
            )
            tickers = sorted({str(r.get("symbol") or "").strip().upper() for r in rows} - {""})

            freshness_hours = int(args.freshness_hours)
            if freshness_hours > 0 and tickers:
                cutoff = (datetime.now(timezone.utc) - timedelta(hours=freshness_hours)).isoformat()
                try:
                    recent_rows = repo.conn.execute(
                        "SELECT DISTINCT ticker FROM predictions WHERE tenant_id = ? AND timestamp >= ?",
                        (tenant_id, cutoff),
                    ).fetchall()
                    recently_predicted = {str(r["ticker"]).strip().upper() for r in recent_rows}
                    before = len(tickers)
                    tickers = [t for t in tickers if t not in recently_predicted]
                    skipped_fresh = before - len(tickers)
                    if skipped_fresh:
                        print(json.dumps({"freshness_filter": {"skipped": skipped_fresh, "window_hours": freshness_hours, "remaining": len(tickers)}}))
                except Exception:
                    pass

            if not tickers:
                repo.finish_prediction_job(
                    job_id=str(job_id),
                    status="success",
                    message="queued=0",
                    tenant_id=tenant_id,
                )
                print(
                    json.dumps(
                        {"status": "ok", "as_of_date": as_of_date, "tenant_id": tenant_id, "queued": 0, "built": 0},
                        indent=2,
                    )
                )
                return 0

            ingress_end = _isoz(datetime.now(timezone.utc))
            ingress_start = _isoz(datetime.now(timezone.utc) - timedelta(days=int(args.ingress_days)))

            pred_start_dt = datetime(as_of_dt.year, as_of_dt.month, as_of_dt.day, tzinfo=timezone.utc)
            pred_end_dt = pred_start_dt + timedelta(days=int(args.forecast_days))
            prediction_start = normalize_timestamp(pred_start_dt)
            prediction_end = normalize_timestamp(pred_end_dt)

            if bool(args.dry_run):
                repo.finish_prediction_job(
                    job_id=str(job_id),
                    status="success",
                    message=f"dry_run queued={len(tickers)}",
                    tenant_id=tenant_id,
                )
                print(
                    json.dumps(
                        {
                            "dry_run": True,
                            "as_of_date": as_of_date,
                            "tenant_id": tenant_id,
                            "tickers": tickers,
                            "ingress_start": ingress_start,
                            "ingress_end": ingress_end,
                            "prediction_start": prediction_start,
                            "prediction_end": prediction_end,
                            "timeframe": str(args.timeframe),
                        },
                        indent=2,
                    )
                )
                return 0

            run_id = repo.create_prediction_run(
                tenant_id=tenant_id,
                timeframe=str(args.timeframe),
                regime=None,
                ingress_start=str(ingress_start),
                ingress_end=str(ingress_end),
                prediction_start=str(prediction_start),
                prediction_end=str(prediction_end),
            )

            # Move queue items -> processing up front to avoid silent partial runs.
            started_at = _isoz(datetime.now(timezone.utc))
            base_meta = {
                "job_id": str(job_id),
                "run_id": str(run_id),
                "started_at": started_at,
                "timeframe": str(args.timeframe),
                "prediction_start": str(prediction_start),
                "prediction_end": str(prediction_end),
            }
            repo.set_prediction_queue_status_many(
                tenant_id=tenant_id,
                rows=[
                    {
                        "as_of_date": str(r["as_of_date"]),
                        "symbol": str(r["symbol"]),
                        "source": str(r.get("source") or "discovery"),
                        "status": "processing",
                        "metadata_json": _merge_metadata(
                            str(r.get("metadata_json") or "{}"),
                            {**base_meta, "state": "processing"},
                        ),
                    }
                    for r in rows
                ],
            )

            builder = PredictedSeriesBuilder(repository=repo)
            
            built = 0
            skipped = 0
            failed = 0
            updates: list[dict[str, object]] = []
            for r in rows:
                symbol = str(r.get("symbol") or "").strip().upper()
                source = str(r.get("source") or "discovery")
                
                # Use consensus for all, but discovery seeds provide the signal
                cfg = BuildConfig(
                    model="directional_drift",
                    signal_source="consensus",  # Must be "consensus" for PredictedSeriesBuilder
                    cap_daily_return=0.05,
                    vol_lookback=20,
                    skip_if_exists=True,
                    tenant_id=tenant_id,
                )
                try:
                    seeded = _seed_consensus_from_queue_metadata(
                        repo=repo,
                        ticker=symbol,
                        metadata_json=str(r.get("metadata_json") or "{}"),
                        tenant_id=tenant_id,
                    )
                    res = builder.build(run_id=str(run_id), ticker=symbol, config=cfg)
                    if res.skipped:
                        skipped += 1
                    else:
                        built += 1
                    updates.append(
                        {
                            "as_of_date": str(r["as_of_date"]),
                            "symbol": symbol,
                            "source": source,
                            "status": str(args.mark_status),
                            "metadata_json": _merge_metadata(
                                str(r.get("metadata_json") or "{}"),
                                {
                                    **base_meta,
                                    "state": "processed",
                                    "skipped": bool(res.skipped),
                                    "skip_reason": res.skip_reason,
                                    "points_written": int(res.points_written or 0),
                                    "model_used": str(res.model_used),
                                    "seeded_consensus": bool(seeded),
                                },
                            ),
                        }
                    )
                except Exception as e:
                    failed += 1
                    updates.append(
                        {
                            "as_of_date": str(r["as_of_date"]),
                            "symbol": symbol,
                            "source": source,
                            "status": "failed",
                            "metadata_json": _merge_metadata(
                                str(r.get("metadata_json") or "{}"),
                                {
                                    **base_meta,
                                    "state": "failed",
                                    "error": f"{type(e).__name__}: {e}",
                                },
                            ),
                        }
                    )

            repo.set_prediction_queue_status_many(tenant_id=tenant_id, rows=updates)

            repo.finish_prediction_job(
                job_id=str(job_id),
                status=("failed" if failed else "success"),
                message=f"queued={len(tickers)} built={built} skipped={skipped} failed={failed} run_id={run_id}",
                tenant_id=tenant_id,
            )

            print(
                json.dumps(
                    {
                        "status": "ok",
                        "as_of_date": as_of_date,
                        "tenant_id": tenant_id,
                        "run_id": str(run_id),
                        "queued": len(tickers),
                        "built": int(built),
                        "skipped": int(skipped),
                        "failed": int(failed),
                    },
                    indent=2,
                )
            )
            return 0 if failed == 0 else 3
        except Exception as e:
            if job_id:
                try:
                    repo.finish_prediction_job(
                        job_id=str(job_id),
                        status="failed",
                        message=f"{type(e).__name__}: {e}",
                        tenant_id=tenant_id,
                    )
                except Exception:
                    pass
            raise
        finally:
            repo.close()

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
