from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.active_universe import get_active_universe_tickers
from app.internal_read_v1.app import app


def _parse_iso_utc(value: str) -> datetime:
    ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True,
            encoding="utf-8",
        ).strip()
    except Exception:
        return "unknown"


def _status(coverage_ratio: float, predictions_7d: int, ranking_rows: int, rec_unique: int) -> str:
    if coverage_ratio >= 0.9 and predictions_7d > 20 and ranking_rows >= 10 and rec_unique >= 5:
        return "PASS"
    if coverage_ratio >= 0.7 and predictions_7d > 0 and ranking_rows >= 5:
        return "WARNING"
    return "FAIL"


def generate_report(db_path: Path, output_path: Path) -> Path:
    os.environ["ALPHA_DB_PATH"] = str(db_path)
    os.environ["INTERNAL_READ_INSECURE"] = "1"
    os.environ.pop("INTERNAL_READ_KEY", None)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc)
    cutoff_7d = (now - timedelta(days=7)).isoformat()
    cutoff_bars = now - timedelta(days=7)

    universe = sorted(get_active_universe_tickers(tenant_id="default", sqlite_conn=conn))
    rows = conn.execute(
        """
        SELECT ticker, MAX(timestamp) AS ts
        FROM price_bars
        WHERE tenant_id = ? AND timeframe = '1d'
        GROUP BY ticker
        """,
        ("default",),
    ).fetchall()
    latest_by_ticker = {str(r["ticker"]).strip().upper(): r["ts"] for r in rows}

    fresh = 0
    missing: list[str] = []
    for t in universe:
        ts_raw = latest_by_ticker.get(t)
        if ts_raw is None:
            missing.append(t)
            continue
        if _parse_iso_utc(str(ts_raw)) >= cutoff_bars:
            fresh += 1
        else:
            missing.append(t)
    coverage_ratio = (fresh / len(universe)) if universe else 1.0

    pred_total = int(
        conn.execute("SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ?", ("default",)).fetchone()["n"]
    )
    pred_7d = int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ? AND timestamp >= ?",
            ("default", cutoff_7d),
        ).fetchone()["n"]
    )
    pred_distinct_7d = int(
        conn.execute(
            "SELECT COUNT(DISTINCT ticker) AS n FROM predictions WHERE tenant_id = ? AND timestamp >= ?",
            ("default", cutoff_7d),
        ).fetchone()["n"]
    )
    conn.close()

    with TestClient(app) as client:
        rank = client.get("/ranking/top").json()
        rec = client.get("/api/recommendations/latest").json()
        run = client.get("/api/predictions/runs/latest").json()

    ranking_rows = rank.get("rankings") or []
    rec_rows = rec.get("recommendations") or []
    rec_tickers = [str(r.get("ticker", "")).strip().upper() for r in rec_rows if r.get("ticker")]
    rec_unique = len(set(rec_tickers))
    mega = {"AAPL", "SPY", "QQQ"}
    mega_share = (sum(1 for t in rec_tickers if t in mega) / len(rec_tickers)) if rec_tickers else 0.0

    db_stat = db_path.stat()
    script_path = Path("scripts/windows/run_daily_pipeline.bat")
    script_mtime = (
        datetime.fromtimestamp(script_path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if script_path.exists()
        else "missing"
    )

    status = _status(coverage_ratio, pred_7d, len(ranking_rows), rec_unique)
    lines = [
        f"DATA HEALTH PROD: {status}",
        f"Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "runtime identity:",
        f"db_path: {db_path.resolve()}",
        f"db_modified_utc: {datetime.fromtimestamp(db_stat.st_mtime, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"db_size_bytes: {db_stat.st_size}",
        f"git_commit: {_git_commit()}",
        f"pipeline_script: {script_path.as_posix()}",
        f"pipeline_script_mtime_utc: {script_mtime}",
        "",
        "output KPIs:",
        f"coverage_pct: {round(coverage_ratio * 100.0, 1)}",
        f"predictions_total: {pred_total}",
        f"predictions_7d: {pred_7d}",
        f"predictions_distinct_7d: {pred_distinct_7d}",
        f"ranking_rows: {len(ranking_rows)}",
        f"ranking_provenance: {rank.get('rankingProvenance')}",
        f"recommendations_rows: {len(rec_rows)}",
        f"recommendations_unique_tickers: {rec_unique}",
        f"megacap_share_pct: {round(mega_share * 100.0, 1)}",
        "",
        "upstream run signals:",
        f"runStatus: {run.get('runStatus')}",
        f"runQuality: {run.get('runQuality')}",
        f"coverageRatio: {run.get('coverageRatio')}",
        "",
        f"top_missing_fresh_bars: {', '.join(missing[:20]) if missing else 'none'}",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def main() -> int:
    p = argparse.ArgumentParser(description="Generate production data health snapshot report")
    p.add_argument("--db", default="data/alpha.db")
    p.add_argument("--output", default="reports/data-health-prod.txt")
    args = p.parse_args()
    out = generate_report(Path(args.db), Path(args.output))
    print(json.dumps({"report": str(out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
