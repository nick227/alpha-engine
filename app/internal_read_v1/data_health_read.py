"""Compact warehouse health for ops dashboards (prices, fundamentals, profiles, predictions)."""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.core.active_universe import get_active_universe_tickers
from app.core.pipeline_gates import BAR_COVERAGE_SLA_RATIO, fresh_bar_coverage

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _repo_root() -> Path:
    override = str(os.getenv("ALPHA_ENGINE_ROOT", "") or "").strip()
    return Path(override) if override else _REPO_ROOT


def _tri_ratio(ratio: float, *, sla: float, soft: float) -> str:
    if ratio >= sla:
        return "OK"
    if ratio >= soft:
        return "WARN"
    return "FAIL"


def _read_pipeline_sentinel(repo_root: Path) -> str | None:
    path = repo_root / "reports" / "pipeline-last-status.txt"
    if not path.is_file():
        return None
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip().splitlines()[0]
    except OSError:
        return None


def _last_run_display(sentinel_line: str | None) -> str | None:
    if not sentinel_line:
        return None
    m = re.search(r"finished_at=([^>]+)$", sentinel_line)
    raw = m.group(1).strip() if m else sentinel_line
    raw = raw.replace("_", " ").strip()
    if len(raw) > 48:
        return raw[:45] + "..."
    return raw or None


def _profiles_base(repo_root: Path) -> Path:
    raw = str(os.getenv("COMPANY_PROFILES_DIR", "") or "").strip()
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else (repo_root / p)
    return repo_root / "data" / "company_profiles"


def _fundamentals_coverage(
    conn: sqlite3.Connection, *, tenant_id: str, universe: list[str], max_age_days: int = 21
) -> tuple[int, int, float]:
    if not universe:
        return 0, 0, 1.0
    cutoff = (date.today() - timedelta(days=max_age_days)).isoformat()
    ph = ",".join("?" * len(universe))
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT ticker) AS n
            FROM fundamentals_snapshot
            WHERE tenant_id = ?
              AND as_of_date >= ?
              AND UPPER(TRIM(ticker)) IN ({ph})
            """,
            (tenant_id, cutoff, *[t.upper() for t in universe]),
        ).fetchone()
        n = int(row["n"] or 0) if row else 0
    except sqlite3.OperationalError:
        return 0, len(universe), 0.0
    exp = len(universe)
    return n, exp, round(n / float(exp), 4) if exp else 1.0


def _profiles_coverage(universe: list[str], base: Path) -> tuple[int, int, float]:
    if not universe:
        return 0, 0, 1.0
    ok = 0
    for t in universe:
        sym = str(t).strip().upper()
        if not sym:
            continue
        safe = "".join(c if (c.isalnum() or c in "-_.") else "_" for c in sym)
        if (base / f"{safe}.json").is_file():
            ok += 1
    exp = len(universe)
    return ok, exp, round(ok / float(exp), 4) if exp else 1.0


def _profile_ipo_coverage(universe: list[str], base: Path) -> tuple[int, int, float]:
    """Among existing profile files, fraction that include ipoDate (stats API)."""
    import json

    if not universe:
        return 0, 0, 1.0
    with_file = 0
    with_ipo = 0
    for t in universe:
        sym = str(t).strip().upper()
        if not sym:
            continue
        safe = "".join(c if (c.isalnum() or c in "-_.") else "_" for c in sym)
        p = base / f"{safe}.json"
        if not p.is_file():
            continue
        with_file += 1
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            v = data.get("ipoDate")
            if isinstance(v, str) and len(v.strip()) >= 8:
                with_ipo += 1
        except Exception:
            pass
    if with_file == 0:
        return 0, 0, 0.0
    return with_ipo, with_file, round(with_ipo / float(with_file), 4)


def build_data_health_compact(
    conn: sqlite3.Connection,
    *,
    tenant_id: str = "default",
) -> dict[str, Any]:
    repo_root = _repo_root()
    sentinel = _read_pipeline_sentinel(repo_root)
    last_run = _last_run_display(sentinel)

    universe = sorted(get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn))
    fresh_n, exp_bars, bar_ratio = fresh_bar_coverage(conn, tenant_id=tenant_id)
    prices = _tri_ratio(bar_ratio, sla=BAR_COVERAGE_SLA_RATIO, soft=max(0.75, BAR_COVERAGE_SLA_RATIO - 0.15))

    fn, fe, fund_ratio = _fundamentals_coverage(conn, tenant_id=tenant_id, universe=universe)
    fundamentals = _tri_ratio(fund_ratio, sla=0.85, soft=0.55)

    pdir = _profiles_base(repo_root)
    pn, pe, prof_ratio = _profiles_coverage(universe, pdir)
    profiles_files = _tri_ratio(prof_ratio, sla=0.85, soft=0.55)
    ipi, ipf, ipo_ratio = _profile_ipo_coverage(universe, pdir)
    profiles = profiles_files
    if profiles == "OK" and ipf > 0 and ipo_ratio < 0.7:
        profiles = "WARN"
    prof_detail = f"{pn}/{pe} json files"
    if ipf > 0:
        prof_detail += f", ipoDate {ipi}/{ipf} files"

    pred_7d = pred_total = 0
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        pred_total = int(
            conn.execute("SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ?", (tenant_id,)).fetchone()["n"]
            or 0
        )
        pred_7d = int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM predictions WHERE tenant_id = ? AND timestamp >= ?",
                (tenant_id, cutoff),
            ).fetchone()["n"]
            or 0
        )
    except sqlite3.OperationalError:
        pass
    if pred_7d > 0:
        predictions = "OK"
    elif pred_total > 0:
        predictions = "WARN"
    else:
        predictions = "FAIL"

    parts = (prices, fundamentals, profiles, predictions)
    if "FAIL" in parts:
        overall = "FAIL"
    elif "WARN" in parts:
        overall = "WARN"
    else:
        overall = "OK"

    summary = (
        f"prices={prices} fundamentals={fundamentals} profiles={profiles} "
        f"predictions={predictions} overall={overall}"
    )
    if last_run:
        summary += f" · last_run={last_run}"

    return {
        "tenant_id": tenant_id,
        "overall": overall,
        "summary": summary,
        "last_run": last_run,
        "pipeline_sentinel": sentinel,
        "prices": {"status": prices, "fresh": fresh_n, "expected": exp_bars, "ratio": bar_ratio},
        "fundamentals": {"status": fundamentals, "with_recent_row": fn, "expected": fe, "ratio": fund_ratio},
        "profiles": {"status": profiles, "detail": prof_detail},
        "predictions": {"status": predictions, "last_7d": pred_7d, "total": pred_total},
    }
