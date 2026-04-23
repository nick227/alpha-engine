"""Data Health lane checks for critical internal-read surfaces."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest
from starlette.testclient import TestClient

pytest.importorskip("httpx")

sys.path.append(str(Path(__file__).resolve().parent))

from internal_read_inventory.config import (
    BAR_COVERAGE_SLA_RATIO,
    DEFAULT_THRESHOLDS,
    FRESH_BAR_MAX_AGE_DAYS,
    MIN_RECOMMENDATION_UNIQUE_TICKERS_WARNING,
    SENTINEL_SYMBOLS,
)


def _bar(ts: str, close: float, *, high: float | None = None, low: float | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        timestamp=ts,
        open=close,
        high=high if high is not None else close,
        low=low if low is not None else close,
        close=close,
        volume=2_000_000.0,
    )


def _seed_data_health_baseline(db_path: Path, *, now: datetime) -> None:
    from app.db.repository import AlphaRepository

    repo = AlphaRepository(db_path=str(db_path))

    symbols = ("AAPL", "SPY", "QQQ")
    closes = {"AAPL": (175.0, 178.0), "SPY": (530.0, 535.0), "QQQ": (450.0, 455.0)}
    for sym in symbols:
        c1, c2 = closes[sym]
        bars = [
            _bar((now - timedelta(days=1)).isoformat(), c1, high=c1 * 1.03, low=c1 * 0.97),
            _bar(now.isoformat(), c2, high=c2 * 1.02, low=c2 * 0.98),
        ]
        repo.save_price_bars(sym, "1d", bars, tenant_id="default")

    for sym in symbols:
        repo.conn.execute(
            """
            INSERT OR REPLACE INTO candidate_queue
              (tenant_id, ticker, status, first_seen_at, last_seen_at, signal_count, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "default",
                sym,
                "admitted",
                (now - timedelta(days=3)).isoformat(),
                now.isoformat(),
                2,
                "{}",
            ),
        )
        repo.save_consensus_signal(
            {
                "ticker": sym,
                "regime": "NORMAL",
                "sentiment_strategy_id": "seed_sent",
                "quant_strategy_id": "seed_quant",
                "sentiment_score": 0.75,
                "quant_score": 0.72,
                "ws": 0.5,
                "wq": 0.5,
                "agreement_bonus": 0.05,
                "p_final": 0.78,
                "stability_score": 0.82,
            },
            tenant_id="default",
        )
        repo.conn.execute(
            """
            INSERT INTO ranking_snapshots
              (id, tenant_id, ticker, score, conviction, attribution_json, regime, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                "default",
                sym,
                0.65,
                0.7,
                "{}",
                "NORMAL",
                now.isoformat(),
            ),
        )

    repo.conn.execute(
        """
        CREATE TABLE IF NOT EXISTS loop_heartbeats (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          run_id TEXT,
          idempotency_key TEXT,
          loop_type TEXT NOT NULL,
          status TEXT NOT NULL,
          notes TEXT,
          created_at TEXT NOT NULL,
          timestamp TEXT
        )
        """
    )
    run_created_at = (now - timedelta(hours=3)).isoformat()
    repo.conn.execute(
        """
        INSERT INTO prediction_runs
          (id, tenant_id, ingress_start, ingress_end, prediction_start, prediction_end, timeframe, regime, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "run_health_1",
            "default",
            (now - timedelta(hours=3, minutes=20)).isoformat(),
            (now - timedelta(hours=3, minutes=10)).isoformat(),
            (now - timedelta(hours=3, minutes=9)).isoformat(),
            (now - timedelta(hours=3)).isoformat(),
            "1d",
            "risk_on",
            run_created_at,
        ),
    )
    repo.conn.execute(
        """
        INSERT INTO loop_heartbeats (id, tenant_id, run_id, idempotency_key, loop_type, status, notes, created_at, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "hb_health_1",
            "default",
            "run_health_1",
            "idem_health_1",
            "live",
            "ok",
            "fresh heartbeat",
            (now - timedelta(minutes=10)).isoformat(),
            (now - timedelta(minutes=10)).isoformat(),
        ),
    )

    repo.conn.commit()
    repo.conn.close()


@pytest.fixture
def data_health_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    db = tmp_path / "data_health.db"
    now = datetime.now(timezone.utc)
    monkeypatch.setenv("ALPHA_DB_PATH", str(db))
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    _seed_data_health_baseline(db, now=now)
    app = _load_app_or_skip()

    with TestClient(app) as client:
        yield client


def _iso_age_minutes(iso_ts: str) -> int:
    ts = datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds() / 60))


def _write_report(status: str, lines: list[str], *, filename: str = "data-health-latest.txt") -> None:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    generated_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    body = [f"DATA HEALTH: {status}", f"Generated: {generated_utc}", "", *lines]
    (reports_dir / filename).write_text("\n".join(body) + "\n", encoding="utf-8")


def _keys_csv(payload: dict) -> str:
    return ", ".join(sorted(str(k) for k in payload.keys()))


def _json_preview(name: str, payload: object, *, max_chars: int = 1400) -> list[str]:
    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if len(rendered) > max_chars:
        rendered = rendered[:max_chars].rstrip() + "\n... [truncated]"
    return [f"{name} preview:", "```json", rendered, "```"]


def _parse_iso_utc(value: str) -> datetime:
    ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _reconcile_upstream_funnel(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    run: dict,
    rec_rows_count: int,
    ranking_rows_count: int,
    rec_unique_tickers: int,
    ranking_tickers_sample: list[str],
    rec_tickers_sample: list[str],
    rec_top_concentration: float | None = None,
    rec_avg_confidence_pct: float | None = None,
    rec_sentinel_share_pct: float | None = None,
) -> list[str]:
    from app.core.active_universe import get_active_universe_tickers
    from app.core.pipeline_gates import infer_ranking_provenance

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=FRESH_BAR_MAX_AGE_DAYS)
    cutoff_pred_7d = (now - timedelta(days=7)).isoformat()
    cutoff_pred_24h = (now - timedelta(hours=24)).isoformat()

    universe = sorted(get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn))
    expected = len(universe)

    rows = conn.execute(
        """
        SELECT ticker, MAX(timestamp) AS ts
        FROM price_bars
        WHERE tenant_id = ? AND timeframe = '1d'
        GROUP BY ticker
        """,
        (tenant_id,),
    ).fetchall()
    latest_by_ticker = {str(r["ticker"]).strip().upper(): r["ts"] for r in rows}

    fresh_tickers: set[str] = set()
    stale_count = 0
    missing_bar_count = 0
    for t in universe:
        ts_raw = latest_by_ticker.get(t)
        if ts_raw is None:
            missing_bar_count += 1
            continue
        ts = _parse_iso_utc(str(ts_raw))
        if ts >= cutoff:
            fresh_tickers.add(t)
        else:
            stale_count += 1

    fresh_count = len(fresh_tickers)
    not_fresh = sorted([x for x in universe if x not in fresh_tickers])
    top_missing = ", ".join(not_fresh[:20]) if not_fresh else "none"

    cq_by_status: dict[str, int] = {}
    try:
        for r in conn.execute(
            """
            SELECT status, COUNT(*) AS n FROM candidate_queue
            WHERE tenant_id = ? GROUP BY status
            """,
            (tenant_id,),
        ).fetchall():
            cq_by_status[str(r["status"])] = int(r["n"])
    except sqlite3.OperationalError:
        pass
    cq_total = sum(cq_by_status.values())
    admitted_n = cq_by_status.get("admitted", 0)
    cq_status_line = ", ".join(f"{k}={v}" for k, v in sorted(cq_by_status.items())) or "none"

    pred_total = pred_24h = pred_7d = pred_distinct_7d = 0
    try:
        pt = conn.execute(
            "SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        pred_total = int(pt["n"]) if pt and pt["n"] is not None else 0
        p24 = conn.execute(
            """
            SELECT COUNT(*) AS n FROM predictions
            WHERE tenant_id = ? AND timestamp >= ?
            """,
            (tenant_id, cutoff_pred_24h),
        ).fetchone()
        pred_24h = int(p24["n"]) if p24 and p24["n"] is not None else 0
        p7 = conn.execute(
            """
            SELECT COUNT(*) AS n FROM predictions
            WHERE tenant_id = ? AND timestamp >= ?
            """,
            (tenant_id, cutoff_pred_7d),
        ).fetchone()
        pred_7d = int(p7["n"]) if p7 and p7["n"] is not None else 0
        pd = conn.execute(
            """
            SELECT COUNT(DISTINCT ticker) AS n FROM predictions
            WHERE tenant_id = ? AND timestamp >= ?
            """,
            (tenant_id, cutoff_pred_7d),
        ).fetchone()
        pred_distinct_7d = int(pd["n"]) if pd and pd["n"] is not None else 0
    except sqlite3.OperationalError:
        pass

    repeat_pressure_pct: float | None = None
    if pred_7d > 0:
        repeat_pressure_pct = round(
            100.0 * (1.0 - pred_distinct_7d / float(pred_7d)),
            2,
        )

    rmax = conn.execute(
        "SELECT MAX(timestamp) AS ts FROM ranking_snapshots WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchone()
    ranking_db_count = 0
    latest_rank_ts = "none"
    if rmax and rmax["ts"] is not None:
        latest_rank_ts = str(rmax["ts"])
        crow = conn.execute(
            """
            SELECT COUNT(DISTINCT ticker) AS n FROM ranking_snapshots
            WHERE tenant_id = ? AND timestamp = ?
            """,
            (tenant_id, latest_rank_ts),
        ).fetchone()
        ranking_db_count = int(crow["n"]) if crow and crow["n"] is not None else 0

    house_reco_n: int | None = None
    try:
        hr = conn.execute(
            """
            SELECT COUNT(*) AS n FROM house_recommendations
            WHERE tenant_id = ? AND mode = 'balanced'
            """,
            (tenant_id,),
        ).fetchone()
        house_reco_n = int(hr["n"]) if hr and hr["n"] is not None else 0
    except sqlite3.OperationalError:
        house_reco_n = None

    max_bar_universe = "n/a"
    if universe:
        ph = ",".join("?" * len(universe))
        br = conn.execute(
            f"""
            SELECT MAX(timestamp) AS ts FROM price_bars
            WHERE tenant_id = ? AND timeframe = '1d' AND ticker IN ({ph})
            """,
            (tenant_id, *universe),
        ).fetchone()
        if br and br["ts"]:
            max_bar_universe = str(br["ts"])

    max_pred_ts = "none"
    try:
        pr = conn.execute(
            "SELECT MAX(timestamp) AS ts FROM predictions WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        if pr and pr["ts"]:
            max_pred_ts = str(pr["ts"])
    except sqlite3.OperationalError:
        pass

    max_consensus_ts = "n/a"
    try:
        cs = conn.execute(
            "SELECT MAX(created_at) AS ts FROM consensus_signals WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        if cs and cs["ts"]:
            max_consensus_ts = str(cs["ts"])
    except sqlite3.OperationalError:
        pass

    max_cq_seen = "n/a"
    try:
        cqmx = conn.execute(
            "SELECT MAX(last_seen_at) AS ts FROM candidate_queue WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        if cqmx and cqmx["ts"]:
            max_cq_seen = str(cqmx["ts"])
    except sqlite3.OperationalError:
        pass

    bar_cov_ratio = (fresh_count / expected) if expected else 1.0
    bar_cov_pct = round(100.0 * bar_cov_ratio, 1)
    sla_pass = bar_cov_ratio >= BAR_COVERAGE_SLA_RATIO
    sla_label = "PASS" if sla_pass else "FAIL"
    ranking_prov = infer_ranking_provenance(
        conn, tenant_id=tenant_id, ranking_tickers=ranking_tickers_sample
    )

    api_expected = run.get("expectedUniverseCount")
    mismatch_note = ""
    if api_expected is not None and int(api_expected) != expected:
        mismatch_note = f" (API expectedUniverseCount={int(api_expected)})"

    if fresh_count < expected:
        bottleneck = "ingest / market data coverage"
    elif admitted_n == 0 and cq_total > 0:
        bottleneck = "admission gate (candidates in queue but none admitted)"
    elif pred_7d == 0:
        bottleneck = "discovery → queue → prediction_cli (no predictions in last 7d)"
    elif ranking_db_count == 0 and pred_total > 0:
        bottleneck = "ranking materialization (prediction_rank_sqlite → ranking_snapshots_from_predictions)"
    elif ranking_db_count == 0:
        bottleneck = "ranking_snapshots empty (run daily pipeline steps 6–7 after predictions exist)"
    elif rec_rows_count == 0 and ranking_db_count > 0:
        bottleneck = "recommendation builder (house_recommendations / rebuild)"
    elif ranking_rows_count == 0 and ranking_db_count > 0:
        bottleneck = "read API vs DB mismatch (rankings in DB but /ranking/top empty — check limits or filters)"
    elif rec_rows_count < ranking_db_count:
        bottleneck = "recommendation list shorter than ranking (limits/mode — may be OK)"
    else:
        bottleneck = "none (see quality signals if outputs still weak)"

    lines: list[str] = [
        "reconciliation (market data):",
        f"Universe expected: {expected}{mismatch_note}",
        f"Fresh bars available ({FRESH_BAR_MAX_AGE_DAYS}d window): {fresh_count}",
        "bars freshness split:",
        f"  bars fresh: {fresh_count}",
        f"  bars stale: {stale_count}",
        f"  bars missing (no 1d rows): {missing_bar_count}",
        f"Missing from fresh bar coverage: {expected - fresh_count}",
        f"Top missing (no fresh bar): {top_missing}",
        "",
        f"coverage SLA (target ≥{int(BAR_COVERAGE_SLA_RATIO * 100)}% fresh 1d bars on expected universe):",
        f"  ratio: {bar_cov_pct}%  status: {sla_label}",
        "",
        "stage freshness (latest warehouse timestamps):",
        f"  max 1d bar (universe tickers): {max_bar_universe}",
        f"  max prediction timestamp: {max_pred_ts}",
        f"  max ranking_snapshots batch: {latest_rank_ts}",
        f"  max consensus_signals: {max_consensus_ts}",
        f"  max candidate_queue last_seen_at: {max_cq_seen}",
        "",
        "pipeline funnel (warehouse):",
        f"candidate_queue symbols: {cq_total}",
        f"  by status: {cq_status_line}",
        f"predictions rows (all time): {pred_total}",
        f"predictions rows (last 7d): {pred_7d}",
        f"predictions distinct tickers (last 7d): {pred_distinct_7d}",
        f"latest ranking snapshot ts: {latest_rank_ts}",
        f"ranking symbols (latest DB batch): {ranking_db_count}",
        f"ranking/top response rows: {ranking_rows_count}",
        f"house_recommendations rows (balanced, DB): {house_reco_n if house_reco_n is not None else 'n/a'}",
        f"recommendations/latest (API rows): {rec_rows_count}",
        f"ranking provenance (heuristic): {ranking_prov}",
        "",
        "full funnel (incident sentence):",
        f"  {expected} universe → {fresh_count} eligible (fresh 1d) → {cq_total} queued → "
        f"{pred_7d} predicted (7d rows) → {ranking_db_count} ranked (DB) → {rec_rows_count} recommended (API)",
        f"  likely bottleneck: {bottleneck}",
    ]

    lines.append("")
    lines.append("output breadth (recommendations/latest):")
    lines.append(
        f"  unique tickers in API response: {rec_unique_tickers} "
        f"(warn if <{MIN_RECOMMENDATION_UNIQUE_TICKERS_WARNING} for thin breadth)"
    )

    lines.append("")
    lines.append("B+ output quality scorecard (what users experience):")
    lines.append(
        f"  · bar coverage: {bar_cov_pct}%  (target ≥{int(BAR_COVERAGE_SLA_RATIO * 100)}% fresh universe)"
    )
    lines.append(
        f"  · predictions: {pred_24h} rows (24h), {pred_7d} rows (7d), "
        f"{pred_distinct_7d} distinct tickers (7d)"
    )
    if repeat_pressure_pct is not None:
        lines.append(
            f"  · prediction repeat pressure (7d): {repeat_pressure_pct}% "
            "(high = same tickers dominate runs — widen discovery or refresh)"
        )
    lines.append(
        f"  · ranking breadth: {ranking_db_count} symbols (latest DB batch), "
        f"{ranking_rows_count} rows (/ranking/top — aim ≥10)"
    )
    lines.append(
        f"  · recommendations/latest: {rec_rows_count} rows, {rec_unique_tickers} unique tickers "
        "(aim 5–15 worthwhile names)"
    )
    if rec_top_concentration is not None:
        lines.append(
            f"  · top-ticker concentration (latest response): {rec_top_concentration:.0%} "
            "(lower is more diverse)"
        )
    if rec_avg_confidence_pct is not None:
        lines.append(f"  · mean recommendation confidence: {rec_avg_confidence_pct:.1f}%")
    if rec_sentinel_share_pct is not None:
        lines.append(
            f"  · sentinel mega-cap share (AAPL/SPY/QQQ rows): {rec_sentinel_share_pct:.0%} "
            "(lower usually feels less repetitive)"
        )

    lines.append("")
    lines.append("operational notes (two independent gates for B+ quality):")
    if pred_total == 0 and ranking_db_count > 0:
        lines.append(
            "  · Rankings in DB but predictions table has 0 rows: latest snapshot is not driven by the "
            "live prediction pipeline (seed, legacy batch, or materialized outside prediction_cli). "
            "Recommendations may mirror a shallow ranking surface only."
        )
    if fresh_count < expected:
        lines.append(
            "  · Gate 1 — market data: run the active-universe downloader until fresh 1d bars cover the "
            'universe (e.g. .venv python dev_scripts/scripts/download_prices_daily.py --days 40).'
        )
    cover_ratio = fresh_count / expected if expected else 1.0
    if pred_7d == 0 and expected > 0 and cover_ratio >= 0.7:
        lines.append(
            "  · Gate 2 — prediction pipeline: with sufficient bars, run full daily pipeline so discovery → "
            "prediction_cli → prediction_rank_sqlite → ranking_snapshots_from_predictions fills predictions "
            r"(e.g. scripts\windows\run_daily_pipeline.bat)."
        )
    if not sla_pass:
        lines.append(
            "  · Upstream: fresh 1d bar coverage below SLA — automation starvation / wrong job target / "
            "DB path drift / or provider throttling (check scheduler logs and ALPHA_DB_PATH)."
        )
    elif pred_total == 0:
        lines.append(
            "  · Diagnosis: API can respond 200 while the prediction layer is dormant — not a broken read API, "
            "but weak upstream generation."
        )
    if sla_pass and pred_total == 0 and ranking_db_count > 0:
        lines.append(
            "  · Policy hint (operational): do not promote rankings as trusted production output when "
            "bar SLA passes but predictions=0 — resolve prediction pipeline before relying on ranking depth."
        )
    if rec_unique_tickers > 0 and rec_unique_tickers < MIN_RECOMMENDATION_UNIQUE_TICKERS_WARNING:
        lines.append(
            f"  · Breadth: recommendation list has only {rec_unique_tickers} unique tickers — "
            "expect weak diversity until universe + pipeline feed more names."
        )
    uniq_rec = {t for t in rec_tickers_sample if t}
    if (
        rec_unique_tickers <= 3
        and uniq_rec
        and uniq_rec.issubset(set(SENTINEL_SYMBOLS))
    ):
        lines.append(
            "  · Repeated-name concentration: recommendations only show sentinel symbols "
            "(e.g. AAPL/SPY/QQQ) — valid but may feel stale; widen universe + pipeline throughput."
        )

    return lines


def _classify_quality(run: dict, rec_rows: list[dict]) -> tuple[str, str, list[str], dict[str, float | int]]:
    reasons: list[str] = []
    run_status = str(run.get("runStatus") or "UNKNOWN")
    run_quality = float(run.get("runQuality") or 0.0)
    coverage_ratio = float(run.get("coverageRatio") or 0.0)
    degraded_reasons = run.get("degradedReasons") or []
    degraded_count = len(degraded_reasons) if isinstance(degraded_reasons, list) else 0

    if run_status != "HEALTHY":
        reasons.append(f"runStatus={run_status}")
    if degraded_count > 0:
        reasons.append(f"degradedReasons={degraded_count}")
    if run_quality < 0.8:
        reasons.append(f"runQuality={run_quality:.2f}<0.80")
    if coverage_ratio < 0.8:
        reasons.append(f"coverageRatio={coverage_ratio:.2%}<80%")

    ticker_counts: dict[str, int] = {}
    for row in rec_rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        if ticker:
            ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
    total = len(rec_rows)
    max_count = max(ticker_counts.values()) if ticker_counts else 0
    top_concentration = (max_count / total) if total > 0 else 1.0
    if total > 0 and top_concentration > 0.7:
        reasons.append(f"recommendation_concentration={top_concentration:.0%}>70%")

    if reasons:
        status = "WARNING"
        confidence = "LOW" if run_status == "FAILED" or run_quality < 0.6 else "MEDIUM"
    else:
        status = "PASS"
        confidence = "HIGH"

    metrics: dict[str, float | int] = {
        "run_quality": run_quality,
        "coverage_ratio": coverage_ratio,
        "degraded_count": degraded_count,
        "unique_recommendation_tickers": len(ticker_counts),
        "top_recommendation_concentration": round(top_concentration, 4),
    }
    return status, confidence, reasons, metrics


def _load_app_or_skip():
    try:
        from app.internal_read_v1.app import app
    except ImportError as exc:
        pytest.skip(f"FastAPI app import unavailable in current environment: {exc}")
    return app


def _runtime_identity(db_path: str) -> list[str]:
    db_file = Path(db_path)
    db_exists = db_file.exists()
    db_mtime = "missing"
    db_size = "missing"
    if db_exists:
        st = db_file.stat()
        db_mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        db_size = str(st.st_size)

    commit = "unknown"
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True,
            encoding="utf-8",
        ).strip()
    except Exception:
        pass

    pipeline_script = Path("scripts/windows/run_daily_pipeline.bat")
    script_mtime = "missing"
    if pipeline_script.exists():
        st = pipeline_script.stat()
        script_mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return [
        "runtime identity (environment alignment):",
        f"db_path: {db_file.resolve()}",
        f"db_exists: {'yes' if db_exists else 'no'}",
        f"db_modified_utc: {db_mtime}",
        f"db_size_bytes: {db_size}",
        f"git_commit: {commit}",
        f"pipeline_script: {pipeline_script.as_posix()}",
        f"pipeline_script_mtime_utc: {script_mtime}",
    ]


def test_data_health_critical_surfaces_pass(data_health_client: TestClient) -> None:
    # Tier 0: root-cause / fail-early checks
    heartbeat_res = data_health_client.get("/api/system/heartbeat")
    run_res = data_health_client.get("/api/predictions/runs/latest")
    assert heartbeat_res.status_code == 200
    assert run_res.status_code == 200

    heartbeat = heartbeat_res.json()
    run = run_res.json()

    assert heartbeat["loops"], "heartbeat has no loop entries"
    heartbeat_age = _iso_age_minutes(heartbeat["loops"][0]["createdAt"])
    run_staleness = int(run["stalenessMinutes"])
    assert heartbeat_age <= DEFAULT_THRESHOLDS.max_heartbeat_age_minutes
    assert run_staleness <= (DEFAULT_THRESHOLDS.max_run_age_hours * 60)

    # Tier 1/2/3: freshness + presence + sentinel completeness on critical surfaces
    rec_latest_res = data_health_client.get("/api/recommendations/latest")
    rec_best_res = data_health_client.get("/api/recommendations/best")
    ranking_res = data_health_client.get("/ranking/top")
    quote_res = data_health_client.get("/api/quote/AAPL")
    assert rec_latest_res.status_code == 200
    assert rec_best_res.status_code == 200
    assert ranking_res.status_code == 200
    assert quote_res.status_code == 200

    rec_latest = rec_latest_res.json()
    rec_best = rec_best_res.json()
    ranking = ranking_res.json()
    quote = quote_res.json()

    rec_rows = rec_latest["recommendations"]
    ranking_rows = ranking["rankings"]
    assert len(rec_rows) >= 1
    assert len(ranking_rows) >= 1
    assert rec_best.get("ticker")
    assert quote.get("ticker") == "AAPL"
    assert quote.get("price") is not None

    rec_tickers = {str(r["ticker"]) for r in rec_rows}
    ranking_tickers = {str(r["ticker"]) for r in ranking_rows}
    assert all(sym in rec_tickers for sym in SENTINEL_SYMBOLS)
    assert all(sym in ranking_tickers for sym in SENTINEL_SYMBOLS)
    assert rec_best["ticker"] in SENTINEL_SYMBOLS

    rec_latest_consumer = {
        "mode": rec_latest.get("mode"),
        "selectionPreference": rec_latest.get("selectionPreference"),
        "tenant_id": rec_latest.get("tenant_id"),
        "recommendations": rec_rows[:3],
    }
    ranking_consumer = {
        "tenant_id": ranking.get("tenant_id"),
        "runStatus": ranking.get("runStatus"),
        "runQuality": ranking.get("runQuality"),
        "rankings": ranking_rows[:3],
    }

    health_status, confidence, warning_reasons, quality_metrics = _classify_quality(run, rec_rows)
    assert health_status == "WARNING"
    run_age_hours = round(run_staleness / 60.0, 1)

    db_path = os.environ.get("ALPHA_DB_PATH")
    assert db_path
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rec_unique = len({str(r["ticker"]).strip().upper() for r in rec_rows if r.get("ticker")})
        conf_nums = [
            float(r["confidence"]) for r in rec_rows if r.get("confidence") is not None
        ]
        rec_avg_conf = round(sum(conf_nums) / len(conf_nums), 1) if conf_nums else None
        fts = [
            str(r["ticker"]).strip().upper()
            for r in rec_rows
            if r.get("ticker")
        ]
        sent_set = set(SENTINEL_SYMBOLS)
        rec_sent_share = (
            round(sum(1 for t in fts if t in sent_set) / len(fts), 4) if fts else None
        )
        reconcile_lines = _reconcile_upstream_funnel(
            conn,
            tenant_id="default",
            run=run,
            rec_rows_count=len(rec_rows),
            ranking_rows_count=len(ranking_rows),
            rec_unique_tickers=rec_unique,
            ranking_tickers_sample=[
                str(r["ticker"]).strip().upper() for r in ranking_rows[:120]
            ],
            rec_tickers_sample=[str(r["ticker"]).strip().upper() for r in rec_rows[:120]],
            rec_top_concentration=float(quality_metrics["top_recommendation_concentration"]),
            rec_avg_confidence_pct=rec_avg_conf,
            rec_sentinel_share_pct=rec_sent_share,
        )
    finally:
        conn.close()

    lines = [
        "heartbeat: fresh",
        f"latest run: {run_age_hours}h ago",
        f"recommendations/latest: {len(rec_rows)} rows",
        f"ranking/top: {len(ranking_rows)} rows",
        "AAPL quote: present",
        "",
        *_runtime_identity(str(db_path)),
        "",
        "quality:",
        f"runStatus: {run.get('runStatus')}",
        f"coverageRatio: {float(quality_metrics['coverage_ratio']):.2%}",
        f"runQuality: {float(quality_metrics['run_quality']):.2f}",
        f"degradedReasons: {int(quality_metrics['degraded_count'])}",
        "",
        f"user-facing confidence: {confidence}",
        (
            "reason: degraded prediction run powering rankings"
            if confidence == "LOW"
            else "reason: run quality and coverage are acceptable"
        ),
        f"recommendation diversity: uniqueTickers={int(quality_metrics['unique_recommendation_tickers'])}, "
        f"topConcentration={float(quality_metrics['top_recommendation_concentration']):.0%}",
        f"quality warnings: {', '.join(warning_reasons) if warning_reasons else 'none'}",
        "",
        *reconcile_lines,
        "",
        "item counts:",
        f"heartbeat.loops: {len(heartbeat.get('loops') or [])}",
        "latest run.item: 1",
        f"recommendations/latest.items: {len(rec_rows)}",
        "recommendations/best.item: 1",
        f"ranking/top.items: {len(ranking_rows)}",
        "quote/AAPL.item: 1",
        "",
        "response keys:",
        f"/api/system/heartbeat keys: {_keys_csv(heartbeat)}",
        f"/api/predictions/runs/latest keys: {_keys_csv(run)}",
        f"/api/recommendations/latest keys: {_keys_csv(rec_latest)}",
        f"/api/recommendations/best keys: {_keys_csv(rec_best)}",
        f"/ranking/top keys: {_keys_csv(ranking)}",
        f"/api/quote/AAPL keys: {_keys_csv(quote)}",
        "",
        "consumer response simulation:",
        *_json_preview("/api/system/heartbeat", heartbeat),
        *_json_preview("/api/predictions/runs/latest", run),
        *_json_preview("/api/recommendations/latest", rec_latest_consumer),
        *_json_preview("/api/recommendations/best", rec_best),
        *_json_preview("/ranking/top", ranking_consumer),
        *_json_preview("/api/quote/AAPL", quote),
    ]
    _write_report(health_status, lines, filename="data-health-test.txt")


def test_data_health_fails_early_when_run_or_heartbeat_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = tmp_path / "data_health_stale.db"
    now = datetime.now(timezone.utc) - timedelta(days=3)
    monkeypatch.setenv("ALPHA_DB_PATH", str(db))
    monkeypatch.setenv("INTERNAL_READ_INSECURE", "1")
    monkeypatch.delenv("INTERNAL_READ_KEY", raising=False)
    _seed_data_health_baseline(db, now=now)
    app = _load_app_or_skip()

    with TestClient(app) as client:
        heartbeat = client.get("/api/system/heartbeat").json()
        run = client.get("/api/predictions/runs/latest").json()
        heartbeat_age = _iso_age_minutes(heartbeat["loops"][0]["createdAt"])
        run_staleness = int(run["stalenessMinutes"])
        stale = (
            heartbeat_age > DEFAULT_THRESHOLDS.max_heartbeat_age_minutes
            or run_staleness > (DEFAULT_THRESHOLDS.max_run_age_hours * 60)
        )
        assert stale, "expected stale root-cause condition did not trigger"

        _write_report(
            "FAIL",
            [
                "root_cause: pipeline_data_failure",
                f"heartbeat_age_minutes: {heartbeat_age}",
                f"run_staleness_minutes: {run_staleness}",
            ],
            filename="data-health-test-stale-scenario.txt",
        )

