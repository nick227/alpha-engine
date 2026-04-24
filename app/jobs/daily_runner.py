from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone

from app.discovery.discovery_cli import main as discovery_main
from app.engine.prediction_cli import main as prediction_main


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Daily job runner (discovery -> prediction queue)",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m app.jobs.daily_runner --date 2026-04-12\n"
            "  python -m app.jobs.daily_runner --no-discovery\n"
        ),
    )
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--tenant-id", default="default")
    p.add_argument("--date", default=None, help="As-of date YYYY-MM-DD (default: latest available trading day in DB)")

    p.add_argument("--no-discovery", action="store_true", help="Skip discovery nightly")
    p.add_argument("--no-prediction", action="store_true", help="Skip prediction queue consumer")
    p.add_argument("--paper-trade", action="store_true", help="Also run paper trading daily (scripts/paper_trade_daily.py)")
    p.add_argument("--earnings-ingest", action="store_true", help="Also run earnings announcements ingester (scripts/ingest_earnings_announcements.py)")
    p.add_argument("--data-refresh", action="store_true", help="Also run data refresh (placeholder; no-op today)")

    p.add_argument("--forecast-days", type=int, default=30, help="Prediction forecast days (default: 30)")
    p.add_argument("--ingress-days", type=int, default=30, help="Ingress lookback days (default: 30)")
    p.add_argument("--limit", type=int, default=200, help="Prediction queue limit (default: 200)")
    return p


def _latest_price_bar_date(*, db_path: str, tenant_id: str, timeframe: str) -> str | None:
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            vix = conn.execute(
                "SELECT MAX(DATE(timestamp)) FROM price_bars WHERE tenant_id=? AND ticker='^VIX' AND timeframe=?",
                (str(tenant_id), str(timeframe)),
            ).fetchone()
            vix_max = str(vix[0]) if vix and vix[0] else None

            all_ = conn.execute(
                "SELECT MAX(DATE(timestamp)) FROM price_bars WHERE tenant_id=? AND timeframe=?",
                (str(tenant_id), str(timeframe)),
            ).fetchone()
            all_max = str(all_[0]) if all_ and all_[0] else None

            if vix_max and all_max:
                return max(vix_max, all_max)
            return vix_max or all_max
        finally:
            conn.close()
    except Exception:
        return None


def _latest_coherent_trading_day(
    *,
    db_path: str,
    tenant_id: str,
    timeframe: str,
    lookback_days: int = 21,
    min_coverage_ratio: float = 0.80,
) -> str | None:
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute(
                """
                SELECT DATE(timestamp) as d, COUNT(DISTINCT ticker) as n
                FROM price_bars
                WHERE tenant_id = ?
                  AND timeframe = ?
                  AND DATE(timestamp) >= date('now', '-' || ? || ' day')
                GROUP BY DATE(timestamp)
                ORDER BY d DESC
                """,
                (str(tenant_id), str(timeframe), int(lookback_days)),
            ).fetchall()
            if not rows:
                return None
            counts = [(str(r[0]), int(r[1] or 0)) for r in rows if r and r[0]]
            if not counts:
                return None
            max_n = max(n for _d, n in counts)
            if max_n <= 0:
                return None
            threshold = int(max_n * float(min_coverage_ratio))
            for d, n in counts:
                if n >= threshold:
                    return d
            return counts[0][0]
        finally:
            conn.close()
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    as_of_date = str(
        args.date
        or _latest_coherent_trading_day(db_path=str(args.db), tenant_id=str(args.tenant_id), timeframe="1d")
        or datetime.now(timezone.utc).date().isoformat()
    )
    db = str(args.db)
    tenant_id = str(args.tenant_id)

    steps: list[dict[str, object]] = []

    if not bool(args.no_discovery):
        rc = int(
            discovery_main(
                [
                    "nightly",
                    "--db",
                    db,
                    "--tenant-id",
                    tenant_id,
                    "--date",
                    as_of_date,
                ]
            )
            or 0
        )
        steps.append({"step": "discovery.nightly", "rc": rc})
        if rc != 0:
            print(json.dumps({"status": "failed", "at": "discovery.nightly", "rc": rc, "date": as_of_date}, indent=2))
            return rc

    if not bool(args.no_prediction):
        rc = int(
            prediction_main(
                [
                    "run-queue",
                    "--db",
                    db,
                    "--tenant-id",
                    tenant_id,
                    "--as-of",
                    as_of_date,
                    "--forecast-days",
                    str(int(args.forecast_days)),
                    "--ingress-days",
                    str(int(args.ingress_days)),
                    "--limit",
                    str(int(args.limit)),
                ]
            )
            or 0
        )
        steps.append({"step": "prediction.run-queue", "rc": rc})
        if rc != 0:
            print(json.dumps({"status": "failed", "at": "prediction.run-queue", "rc": rc, "date": as_of_date}, indent=2))
            return rc

    if bool(args.paper_trade):
        # Keep this as a subprocess so the scripts folder remains ad-hoc compatible.
        cmd = [sys.executable, "scripts/paper_trade_daily.py", "--date", as_of_date]
        proc = subprocess.run(cmd, check=False)
        rc = int(proc.returncode or 0)
        steps.append({"step": "paper_trade_daily", "rc": rc})
        if rc != 0:
            print(json.dumps({"status": "failed", "at": "paper_trade_daily", "rc": rc, "date": as_of_date}, indent=2))
            return rc

    if bool(args.earnings_ingest):
        cmd = [sys.executable, "scripts/ingest_earnings_announcements.py", "--date", as_of_date, "--db", db]
        proc = subprocess.run(cmd, check=False)
        rc = int(proc.returncode or 0)
        steps.append({"step": "earnings_ingest", "rc": rc})
        if rc != 0:
            print(json.dumps({"status": "failed", "at": "earnings_ingest", "rc": rc, "date": as_of_date}, indent=2))
            return rc

    if bool(args.data_refresh):
        steps.append({"step": "data_refresh", "rc": 0, "notes": "no-op (not implemented)"})

    print(json.dumps({"status": "ok", "date": as_of_date, "steps": steps}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
