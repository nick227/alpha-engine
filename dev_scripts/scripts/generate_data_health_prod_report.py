"""
Generate a data health snapshot from the production DB (data/alpha.db).

Reads real warehouse state — not a test fixture. Writes reports/data-health-prod.txt.
Run standalone after the daily pipeline or on-demand to check real coverage.

Usage:
    python dev_scripts/scripts/generate_data_health_prod_report.py
    python dev_scripts/scripts/generate_data_health_prod_report.py --db data/alpha.db
    python dev_scripts/scripts/generate_data_health_prod_report.py --output reports/custom.txt
    python dev_scripts/scripts/generate_data_health_prod_report.py --no-api   # DB-only, no FastAPI needed
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_DB_ENV = (os.environ.get("ALPHA_DB_PATH") or "").strip()
DEFAULT_DB_PATH = Path(_DB_ENV) if _DB_ENV else (ROOT / "data" / "alpha.db")
DEFAULT_OUTPUT = ROOT / "reports" / "data-health-prod.txt"
TENANT_ID = "default"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_iso_utc(value: str) -> datetime:
    ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True, encoding="utf-8"
        ).strip()
    except Exception:
        return "unknown"


def _json_preview(name: str, payload: object, *, max_chars: int = 1400) -> list[str]:
    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if len(rendered) > max_chars:
        rendered = rendered[:max_chars].rstrip() + "\n... [truncated]"
    return [f"{name} preview:", "```json", rendered, "```"]


def _keys_csv(payload: dict) -> str:
    return ", ".join(sorted(str(k) for k in payload.keys()))


def _max_ts(conn: sqlite3.Connection, table: str, col: str, tenant_id: str) -> str:
    try:
        r = conn.execute(
            f"SELECT MAX({col}) AS ts FROM {table} WHERE tenant_id = ?", (tenant_id,)
        ).fetchone()
        return str(r["ts"]) if r and r["ts"] else "none"
    except sqlite3.OperationalError:
        return "n/a"


def _pipeline_sentinel(root: Path) -> list[str]:
    sentinel = root / "reports" / "pipeline-last-status.txt"
    if not sentinel.exists():
        return ["pipeline last run: unknown (reports/pipeline-last-status.txt not found — run pipeline once)"]
    return [f"pipeline last run: {sentinel.read_text(encoding='utf-8').strip()}"]


# ---------------------------------------------------------------------------
# Core status logic
# ---------------------------------------------------------------------------

def _status(bar_cov_ratio: float, pred_7d: int, ranking_db_count: int, ranking_prov: str) -> str:
    if bar_cov_ratio < 0.5 or ranking_db_count == 0:
        return "FAIL"
    from app.core.pipeline_gates import BAR_COVERAGE_SLA_RATIO
    if bar_cov_ratio < BAR_COVERAGE_SLA_RATIO or pred_7d == 0 or ranking_prov in ("seeded", "legacy_snapshot"):
        return "WARNING"
    return "PASS"


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------

def generate_report(db_path: Path, output_path: Path, *, no_api: bool = False, tenant_id: str = TENANT_ID) -> Path:
    os.environ["ALPHA_DB_PATH"] = str(db_path)
    os.environ["INTERNAL_READ_INSECURE"] = "1"
    os.environ.pop("INTERNAL_READ_KEY", None)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")

    now = datetime.now(timezone.utc)

    from app.core.active_universe import get_active_universe_tickers
    from app.core.pipeline_gates import (
        BAR_COVERAGE_SLA_RATIO,
        FRESH_BAR_MAX_AGE_DAYS,
        infer_ranking_provenance,
    )

    cutoff_bar = now - timedelta(days=FRESH_BAR_MAX_AGE_DAYS)
    cutoff_7d = (now - timedelta(days=7)).isoformat()
    cutoff_24h = (now - timedelta(hours=24)).isoformat()

    # --- Universe + bar coverage ---
    universe = sorted(get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn))
    bar_rows = conn.execute(
        "SELECT ticker, MAX(timestamp) AS ts FROM price_bars WHERE tenant_id = ? AND timeframe = '1d' GROUP BY ticker",
        (tenant_id,),
    ).fetchall()
    latest_by_ticker = {str(r["ticker"]).strip().upper(): r["ts"] for r in bar_rows}

    fresh_tickers: set[str] = set()
    stale_count = missing_count = 0
    for t in universe:
        ts_raw = latest_by_ticker.get(t)
        if ts_raw is None:
            missing_count += 1
        elif _parse_iso_utc(str(ts_raw)) >= cutoff_bar:
            fresh_tickers.add(t)
        else:
            stale_count += 1

    fresh_count = len(fresh_tickers)
    not_fresh = sorted(t for t in universe if t not in fresh_tickers)
    expected = len(universe)
    coverage_ratio = fresh_count / expected if expected else 1.0
    coverage_pct = round(coverage_ratio * 100.0, 1)
    sla_label = "PASS" if coverage_ratio >= BAR_COVERAGE_SLA_RATIO else "FAIL"

    # --- Stage timestamps ---
    max_bar_universe = "n/a"
    if universe:
        ph = ",".join("?" * len(universe))
        br = conn.execute(
            f"SELECT MAX(timestamp) AS ts FROM price_bars WHERE tenant_id = ? AND timeframe = '1d' AND ticker IN ({ph})",
            (tenant_id, *universe),
        ).fetchone()
        max_bar_universe = str(br["ts"]) if br and br["ts"] else "none"
    max_pred_ts = _max_ts(conn, "predictions", "timestamp", tenant_id)
    max_rank_ts = _max_ts(conn, "ranking_snapshots", "timestamp", tenant_id)
    max_consensus_ts = _max_ts(conn, "consensus_signals", "created_at", tenant_id)
    max_cq_seen = _max_ts(conn, "candidate_queue", "last_seen_at", tenant_id)

    # --- Candidate queue ---
    cq_by_status: dict[str, int] = {}
    try:
        for r in conn.execute(
            "SELECT status, COUNT(*) AS n FROM candidate_queue WHERE tenant_id = ? GROUP BY status",
            (tenant_id,),
        ).fetchall():
            cq_by_status[str(r["status"])] = int(r["n"])
    except sqlite3.OperationalError:
        pass
    cq_total = sum(cq_by_status.values())
    admitted_n = cq_by_status.get("admitted", 0)
    cq_status_line = ", ".join(f"{k}={v}" for k, v in sorted(cq_by_status.items())) or "none"

    # --- Predictions ---
    pred_total = pred_24h = pred_7d = pred_distinct_7d = 0
    try:
        pred_total = int(conn.execute("SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ?", (tenant_id,)).fetchone()["n"] or 0)
        pred_24h = int(conn.execute("SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ? AND timestamp >= ?", (tenant_id, cutoff_24h)).fetchone()["n"] or 0)
        pred_7d = int(conn.execute("SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ? AND timestamp >= ?", (tenant_id, cutoff_7d)).fetchone()["n"] or 0)
        pred_distinct_7d = int(conn.execute("SELECT COUNT(DISTINCT ticker) AS n FROM predictions WHERE tenant_id = ? AND timestamp >= ?", (tenant_id, cutoff_7d)).fetchone()["n"] or 0)
    except sqlite3.OperationalError:
        pass

    # --- Ranking snapshots ---
    latest_rank_ts_str = "none"
    ranking_db_count = 0
    ranking_tickers_sample: list[str] = []
    try:
        rmax = conn.execute("SELECT MAX(timestamp) AS ts FROM ranking_snapshots WHERE tenant_id = ?", (tenant_id,)).fetchone()
        if rmax and rmax["ts"]:
            latest_rank_ts_str = str(rmax["ts"])
            crow = conn.execute(
                "SELECT COUNT(DISTINCT ticker) AS n FROM ranking_snapshots WHERE tenant_id = ? AND timestamp = ?",
                (tenant_id, latest_rank_ts_str),
            ).fetchone()
            ranking_db_count = int(crow["n"] or 0)
            rs = conn.execute(
                "SELECT DISTINCT ticker FROM ranking_snapshots WHERE tenant_id = ? AND timestamp = ?",
                (tenant_id, latest_rank_ts_str),
            ).fetchall()
            ranking_tickers_sample = [str(r["ticker"]).strip().upper() for r in rs]
    except sqlite3.OperationalError:
        pass

    # --- House recommendations ---
    house_reco_n: int | None = None
    try:
        hr = conn.execute(
            "SELECT COUNT(*) AS n FROM house_recommendations WHERE tenant_id = ? AND mode = 'balanced'",
            (tenant_id,),
        ).fetchone()
        house_reco_n = int(hr["n"] or 0)
    except sqlite3.OperationalError:
        pass
    house_reco_display = house_reco_n if house_reco_n is not None else "n/a"

    ranking_prov = infer_ranking_provenance(conn, tenant_id=tenant_id, ranking_tickers=ranking_tickers_sample)

    # --- Bottleneck ---
    if fresh_count < expected:
        bottleneck = "ingest / market data coverage"
    elif admitted_n == 0 and cq_total > 0:
        bottleneck = "admission gate (candidates queued but none admitted)"
    elif pred_7d == 0:
        bottleneck = "discovery → queue → prediction_cli (no predictions in last 7d)"
    elif ranking_db_count == 0 and pred_total > 0:
        bottleneck = "ranking materialization (predictions exist but no ranking snapshot)"
    elif ranking_db_count == 0:
        bottleneck = "ranking_snapshots empty (run steps 6–7 after predictions exist)"
    elif house_reco_n == 0 and ranking_db_count > 0:
        bottleneck = "recommendation builder (rankings in DB but house_recommendations empty)"
    else:
        bottleneck = "none"

    # --- Prediction repeat pressure ---
    repeat_pressure_line = ""
    if pred_7d > 0 and pred_distinct_7d > 0:
        rp = round(100.0 * (1.0 - pred_distinct_7d / pred_7d), 2)
        repeat_pressure_line = f"  · prediction repeat pressure (7d): {rp}%"

    # --- API section (lazy import — safe to skip if httpx not available) ---
    api_lines: list[str] = []
    if not no_api:
        try:
            from starlette.testclient import TestClient
            from app.internal_read_v1.app import app

            def _safe(r) -> dict:
                try:
                    return r.json()
                except Exception:
                    return {"error": f"HTTP {r.status_code}"}

            with TestClient(app) as client:
                heartbeat = _safe(client.get("/api/system/heartbeat"))
                run = _safe(client.get("/api/predictions/runs/latest"))
                rec = _safe(client.get("/api/recommendations/latest"))
                best = _safe(client.get("/api/recommendations/best"))
                ranking = _safe(client.get("/ranking/top"))
                quote = _safe(client.get("/api/quote/AAPL"))

            rec_rows = rec.get("recommendations", []) if isinstance(rec, dict) else []
            ranking_rows = ranking.get("rankings", []) if isinstance(ranking, dict) else []
            rec_tickers = [str(r.get("ticker", "")).strip().upper() for r in rec_rows if r.get("ticker")]
            rec_unique = len(set(rec_tickers))
            mega = {"AAPL", "SPY", "QQQ"}
            mega_share = (sum(1 for t in rec_tickers if t in mega) / len(rec_tickers)) if rec_tickers else 0.0
            rec_conf = [float(r.get("confidence")) for r in rec_rows if r.get("confidence") is not None]
            rec_conf_avg = round(sum(rec_conf) / len(rec_conf), 1) if rec_conf else None
            rec_conf_min = round(min(rec_conf), 1) if rec_conf else None
            rec_conf_max = round(max(rec_conf), 1) if rec_conf else None
            sectors = [
                str((r.get("selectionDiagnostics") or {}).get("sector") or "").strip().lower()
                for r in rec_rows
            ]
            sector_unique = len({s for s in sectors if s})

            run_status = str(run.get("runStatus") or "UNKNOWN") if isinstance(run, dict) else "UNKNOWN"
            run_quality = float(run.get("runQuality") or 0.0) if isinstance(run, dict) else 0.0
            run_cov = float(run.get("coverageRatio") or 0.0) if isinstance(run, dict) else 0.0
            staleness_min = int(run.get("stalenessMinutes") or 0) if isinstance(run, dict) else 0

            heartbeat_loops = heartbeat.get("loops", []) if isinstance(heartbeat, dict) else []
            heartbeat_age: int | None = None
            if heartbeat_loops and isinstance(heartbeat_loops[0], dict):
                try:
                    heartbeat_age = max(0, int((now - _parse_iso_utc(str(heartbeat_loops[0].get("createdAt", "")))).total_seconds() / 60))
                except Exception:
                    pass

            quality_warnings: list[str] = []
            if run_status != "HEALTHY":
                quality_warnings.append(f"runStatus={run_status}")
            if run_quality < 0.8:
                quality_warnings.append(f"runQuality={run_quality:.2f}<0.80")
            if run_cov < 0.8:
                quality_warnings.append(f"coverageRatio={run_cov:.2%}<80%")

            api_lines = [
                "api responses (TestClient → production DB):",
                f"  heartbeat: {'fresh' if heartbeat_age is not None and heartbeat_age <= 180 else 'stale/missing'}"
                + (f" ({heartbeat_age}m ago)" if heartbeat_age is not None else ""),
                f"  latest run: {staleness_min}m ago  runStatus={run_status}  runQuality={run_quality:.2f}  coverageRatio={run_cov:.2%}",
                f"  recommendations/latest: {len(rec_rows)} rows, {rec_unique} unique tickers"
                + (f"  conf avg/min/max: {rec_conf_avg}/{rec_conf_min}/{rec_conf_max}%" if rec_conf_avg is not None else ""),
                f"  ranking/top: {len(ranking_rows)} rows  rankingProvenance: {ranking.get('rankingProvenance') if isinstance(ranking, dict) else 'n/a'}",
                f"  quote/AAPL: {'present' if isinstance(quote, dict) and quote.get('price') else 'missing'}",
                f"  megacap_share: {mega_share:.0%}  sector_diversity: {sector_unique} unique sectors",
                f"  quality warnings: {', '.join(quality_warnings) if quality_warnings else 'none'}",
                "",
                "response key inventory:",
                f"  /api/system/heartbeat: {_keys_csv(heartbeat) if isinstance(heartbeat, dict) else 'error'}",
                f"  /api/predictions/runs/latest: {_keys_csv(run) if isinstance(run, dict) else 'error'}",
                f"  /api/recommendations/latest: {_keys_csv(rec) if isinstance(rec, dict) else 'error'}",
                f"  /api/recommendations/best: {_keys_csv(best) if isinstance(best, dict) else 'error'}",
                f"  /ranking/top: {_keys_csv(ranking) if isinstance(ranking, dict) else 'error'}",
                f"  /api/quote/AAPL: {_keys_csv(quote) if isinstance(quote, dict) else 'error'}",
                "",
                "response previews:",
                *_json_preview("/api/system/heartbeat", heartbeat),
                *_json_preview("/api/predictions/runs/latest", run),
                *_json_preview(
                    "/api/recommendations/latest",
                    {"mode": rec.get("mode"), "recommendations": rec_rows[:3]} if isinstance(rec, dict) else rec,
                ),
                *_json_preview("/api/recommendations/best", best),
                *_json_preview(
                    "/ranking/top",
                    {"runStatus": ranking.get("runStatus"), "runQuality": ranking.get("runQuality"), "rankings": ranking_rows[:3]}
                    if isinstance(ranking, dict) else ranking,
                ),
                *_json_preview("/api/quote/AAPL", quote),
            ]
        except ImportError as exc:
            api_lines = [f"api section: unavailable ({exc}) — install httpx or run with --no-api"]
        except Exception as exc:
            api_lines = [f"api section: error — {exc}"]

    conn.close()

    # --- Runtime identity ---
    db_stat = db_path.stat()
    script_path = ROOT / "scripts" / "windows" / "run_daily_pipeline.bat"
    script_mtime = (
        datetime.fromtimestamp(script_path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        if script_path.exists() else "missing"
    )

    overall_status = _status(coverage_ratio, pred_7d, ranking_db_count, ranking_prov)

    lines: list[str] = [
        *_pipeline_sentinel(ROOT),
        "",
        "runtime identity:",
        f"  db_path: {db_path.resolve()}",
        f"  db_modified_utc: {datetime.fromtimestamp(db_stat.st_mtime, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"  db_size_bytes: {db_stat.st_size}",
        f"  git_commit: {_git_commit()}",
        f"  pipeline_script_mtime_utc: {script_mtime}",
        "",
        "reconciliation (market data):",
        f"Universe expected: {expected}",
        f"Fresh bars available ({FRESH_BAR_MAX_AGE_DAYS}d window): {fresh_count}",
        "bars freshness split:",
        f"  bars fresh: {fresh_count}",
        f"  bars stale: {stale_count}",
        f"  bars missing (no 1d rows): {missing_count}",
        f"Missing from fresh bar coverage: {expected - fresh_count}",
        f"Top missing (no fresh bar): {', '.join(not_fresh[:20]) if not_fresh else 'none'}",
        "",
        f"coverage SLA (target ≥{int(BAR_COVERAGE_SLA_RATIO * 100)}% fresh 1d bars on expected universe):",
        f"  ratio: {coverage_pct}%  status: {sla_label}",
        "",
        "stage freshness (latest warehouse timestamps):",
        f"  max 1d bar (universe tickers): {max_bar_universe}",
        f"  max prediction timestamp: {max_pred_ts}",
        f"  max ranking_snapshots batch: {max_rank_ts}",
        f"  max consensus_signals: {max_consensus_ts}",
        f"  max candidate_queue last_seen_at: {max_cq_seen}",
        "",
        "pipeline funnel (warehouse):",
        f"candidate_queue symbols: {cq_total}",
        f"  by status: {cq_status_line}",
        f"predictions rows (all time): {pred_total}",
        f"predictions rows (last 7d): {pred_7d}",
        f"predictions distinct tickers (last 7d): {pred_distinct_7d}",
        f"latest ranking snapshot ts: {latest_rank_ts_str}",
        f"ranking symbols (latest DB batch): {ranking_db_count}",
        f"house_recommendations rows (balanced, DB): {house_reco_display}",
        f"ranking provenance (heuristic): {ranking_prov}",
        "",
        "full funnel (incident sentence):",
        f"  {expected} universe → {fresh_count} eligible (fresh 1d) → {cq_total} queued → "
        f"{pred_7d} predicted (7d rows) → {ranking_db_count} ranked (DB) → "
        f"{house_reco_display} recommended (DB)",
        f"  likely bottleneck: {bottleneck}",
        "",
        "B+ output quality scorecard:",
        f"  · bar coverage: {coverage_pct}%  (target ≥{int(BAR_COVERAGE_SLA_RATIO * 100)}% fresh universe)",
        f"  · predictions: {pred_24h} rows (24h), {pred_7d} rows (7d), {pred_distinct_7d} distinct tickers (7d)",
        f"  · ranking breadth: {ranking_db_count} symbols (latest DB batch) — aim ≥10",
        f"  · ranking provenance: {ranking_prov}",
        f"  · house_recommendations: {house_reco_display} rows (balanced) — aim 5–15",
    ]
    if repeat_pressure_line:
        lines.append(repeat_pressure_line)

    if api_lines:
        lines += ["", *api_lines]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    header = [f"DATA HEALTH (PROD): {overall_status}", f"Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}", ""]
    output_path.write_text("\n".join(header + lines) + "\n", encoding="utf-8")
    return output_path


def main() -> int:
    p = argparse.ArgumentParser(description="Generate production data health snapshot report")
    p.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to alpha.db (default: data/alpha.db or ALPHA_DB_PATH)")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output path (default: reports/data-health-prod.txt)")
    p.add_argument("--tenant-id", default=TENANT_ID)
    p.add_argument("--no-api", action="store_true", help="Skip API section — faster, no FastAPI/httpx import needed")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: DB not found at {db_path.resolve()} — check ALPHA_DB_PATH or --db")
        return 1

    out = generate_report(Path(args.db), Path(args.output), no_api=args.no_api, tenant_id=args.tenant_id)
    print(json.dumps({"report": str(out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
