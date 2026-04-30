"""
Strategy coverage audit.

Cross-checks five layers of the pipeline to identify where strategies
drop out before reaching the API:

  Registry -> Candidates -> Predictions -> Rankings -> API Catalog

Usage:
    python scripts/audit_strategy_coverage.py
    python scripts/audit_strategy_coverage.py --db path/to/alpha.db --tenant-id default
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# -- helpers ------------------------------------------------------------------

def _conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _days_ago(ts: str | None) -> str:
    if not ts:
        return "never"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        days = (datetime.now(timezone.utc) - dt).days
        return f"{days}d ago" if days > 0 else "today"
    except Exception:
        return ts[:10]


def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    return col in cols


# -- layer 1: strategy registry (code) ----------------------------------------

def _registry_strategies() -> list[str]:
    try:
        from app.discovery.strategies.registry import STRATEGIES
        return sorted(STRATEGIES.keys())
    except Exception as e:
        print(f"  [warn] could not import registry: {e}")
        return []


# -- layer 2: candidate pipeline -----------------------------------------------

def _candidate_coverage(conn: sqlite3.Connection, tenant_id: str, lookback_days: int = 60) -> dict[str, dict]:
    out: dict[str, dict] = {}

    # discovery_candidates (raw scoring output before admission)
    try:
        for r in conn.execute(
            f"""
            SELECT strategy_type,
                   COUNT(*) AS n,
                   COUNT(DISTINCT symbol) AS tickers,
                   MAX(as_of_date) AS latest
            FROM discovery_candidates
            WHERE as_of_date >= date('now', '-{lookback_days} days')
            GROUP BY strategy_type
            """
        ).fetchall():
            key = r["strategy_type"]
            out.setdefault(key, {})["candidates"] = r["n"]
            out[key]["candidate_tickers"] = r["tickers"]
            out[key]["candidates_latest"] = r["latest"]
    except Exception as e:
        print(f"  [warn] discovery_candidates query failed: {e}")

    # candidate_queue (post-admission state)
    try:
        pf = "primary_strategy"
        if not _col_exists(conn, "candidate_queue", pf):
            pf = "strategy_type"
        for r in conn.execute(
            f"""
            SELECT {pf} AS strategy,
                   COUNT(*) AS n,
                   MAX(last_seen_at) AS latest
            FROM candidate_queue
            WHERE last_seen_at >= date('now', '-{lookback_days} days')
            GROUP BY {pf}
            """
        ).fetchall():
            key = r["strategy"]
            out.setdefault(key, {})["queue"] = r["n"]
            out[key]["queue_latest"] = r["latest"]
    except Exception as e:
        print(f"  [warn] candidate_queue query failed: {e}")

    return out


# -- layer 3: predictions ------------------------------------------------------

def _prediction_coverage(conn: sqlite3.Connection, tenant_id: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    try:
        for r in conn.execute(
            """
            SELECT strategy_id,
                   COUNT(*) AS rows,
                   COUNT(DISTINCT ticker) AS tickers,
                   MAX(timestamp) AS latest
            FROM predictions
            WHERE tenant_id = ?
            GROUP BY strategy_id
            ORDER BY latest DESC
            """,
            (tenant_id,),
        ).fetchall():
            out[r["strategy_id"]] = {
                "pred_rows": r["rows"],
                "pred_tickers": r["tickers"],
                "pred_latest": r["latest"],
            }
    except Exception as e:
        print(f"  [warn] predictions query failed: {e}")
    return out


# -- layer 4: rankings ---------------------------------------------------------

def _ranking_coverage(conn: sqlite3.Connection, tenant_id: str) -> dict:
    info: dict = {}
    try:
        row = conn.execute(
            "SELECT MAX(timestamp) AS latest, COUNT(DISTINCT ticker) AS tickers FROM ranking_snapshots WHERE tenant_id = ?",
            (tenant_id,),
        ).fetchone()
        info["latest"] = row["latest"]
        info["tickers"] = row["tickers"]
        info["has_strategy_id"] = _col_exists(conn, "ranking_snapshots", "strategy_id")

        # Check if ranking timestamps post-date the latest prediction
        latest_pred = conn.execute("SELECT MAX(timestamp) FROM predictions WHERE tenant_id = ?", (tenant_id,)).fetchone()[0]
        info["latest_prediction"] = latest_pred
    except Exception as e:
        print(f"  [warn] ranking_snapshots query failed: {e}")
    return info


# -- layer 5: api catalog ------------------------------------------------------

def _catalog_coverage(conn: sqlite3.Connection, tenant_id: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    try:
        for r in conn.execute(
            """
            SELECT id, name, status, active, is_champion, sample_size, activated_at
            FROM strategies
            WHERE tenant_id = ?
            ORDER BY is_champion DESC, status, id
            """,
            (tenant_id,),
        ).fetchall():
            out[r["id"]] = {
                "name": r["name"],
                "status": r["status"],
                "active": bool(r["active"]),
                "champion": bool(r["is_champion"]),
                "sample_size": r["sample_size"],
                "activated_at": r["activated_at"],
            }
    except Exception as e:
        print(f"  [warn] strategies query failed: {e}")
    return out


# -- normalise strategy IDs for cross-reference --------------------------------

def _base(sid: str) -> str:
    """Strip version/paper suffixes for fuzzy matching across layers."""
    return (
        sid.lower()
           .replace("_v1_paper", "")
           .replace("_v1", "")
           .replace("-v1", "")
           .replace("_default", "")
           .replace("-", "_")
    )


# -- report --------------------------------------------------------------------

def run_audit(db_path: str, tenant_id: str) -> None:
    conn = _conn(db_path)

    print(f"\n{'='*70}")
    print(f"  STRATEGY COVERAGE AUDIT")
    print(f"  db:     {db_path}")
    print(f"  tenant: {tenant_id}")
    print(f"  as-of:  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}\n")

    registry   = _registry_strategies()
    candidates = _candidate_coverage(conn, tenant_id)
    preds      = _prediction_coverage(conn, tenant_id)
    rankings   = _ranking_coverage(conn, tenant_id)
    catalog    = _catalog_coverage(conn, tenant_id)

    # Collect all known strategy base-names across every layer.
    all_bases: dict[str, list[str]] = {}  # base -> [original ids]
    for sid in registry:
        all_bases.setdefault(_base(sid), []).append(f"registry:{sid}")
    for sid in candidates:
        all_bases.setdefault(_base(sid), []).append(f"candidates:{sid}")
    for sid in preds:
        all_bases.setdefault(_base(sid), []).append(f"predictions:{sid}")
    for sid in catalog:
        all_bases.setdefault(_base(sid), []).append(f"catalog:{sid}")

    # -- Section 1: per-strategy completeness matrix ---------------------------
    print("LAYER COMPLETENESS MATRIX")
    print(f"  {'strategy':<36} {'registry':^8} {'candidates':^10} {'predictions':^12} {'catalog':^8}")
    print(f"  {'-'*36} {'-'*8} {'-'*10} {'-'*12} {'-'*8}")

    for base in sorted(all_bases):
        in_reg   = any(b == base for b in map(_base, registry))
        cand_info = candidates.get(base) or next((v for k, v in candidates.items() if _base(k) == base), None)
        pred_info = preds.get(base) or next((v for k, v in preds.items() if _base(k) == base), None)
        cat_info  = catalog.get(base) or next((v for k, v in catalog.items() if _base(k) == base), None)

        reg_mark   = "Y" if in_reg else "--"
        cand_mark  = f"Y {cand_info['candidates']}c" if cand_info and cand_info.get("candidates") else "--"
        pred_mark  = f"Y {pred_info['pred_rows']}r" if pred_info else "--"
        cat_mark   = "Y" if cat_info else "--"

        print(f"  {base:<36} {reg_mark:^8} {cand_mark:^10} {pred_mark:^12} {cat_mark:^8}")

    # -- Section 2: discovery strategies drop-out ------------------------------
    print(f"\n{'-'*70}")
    print("DISCOVERY PIPELINE DROP-OUT  (registry -> candidates -> predictions)")
    print()

    for strat in registry:
        base = _base(strat)
        cand_info = candidates.get(strat) or next((v for k, v in candidates.items() if _base(k) == base), None)
        pred_key  = next((k for k in preds if _base(k) == base), None)
        pred_info = preds.get(pred_key) if pred_key else None

        cand_n      = cand_info.get("candidates", 0) if cand_info else 0
        cand_latest = cand_info.get("candidates_latest") if cand_info else None
        queue_n     = cand_info.get("queue", 0) if cand_info else 0
        pred_n      = pred_info["pred_rows"] if pred_info else 0
        pred_latest = pred_info["pred_latest"] if pred_info else None

        status = "OK" if pred_info else ("STALLED -- never reaches predictions" if cand_n else "STALLED -- no candidates generated")

        print(f"  {strat}")
        print(f"    candidates (60d): {cand_n:>5}   latest: {_days_ago(cand_latest)}")
        print(f"    queue:            {queue_n:>5}")
        print(f"    predictions:      {pred_n:>5}   latest: {_days_ago(pred_latest)}")
        print(f"    status:           {status}")
        print()

    # -- Section 3: prediction strategies not in discovery registry ------------
    print(f"{'-'*70}")
    print("PREDICTION STRATEGIES NOT IN DISCOVERY REGISTRY")
    print()
    orphan_preds = [k for k in preds if not any(_base(k) == _base(r) for r in registry)]
    if orphan_preds:
        for sid in orphan_preds:
            p = preds[sid]
            print(f"  {sid}")
            print(f"    rows: {p['pred_rows']}   tickers: {p['pred_tickers']}   latest: {_days_ago(p['pred_latest'])}")
    else:
        print("  (none)")
    print()

    # -- Section 4: rankings layer ---------------------------------------------
    print(f"{'-'*70}")
    print("RANKINGS LAYER")
    print()
    print(f"  Latest snapshot:         {_days_ago(rankings.get('latest'))}  ({rankings.get('latest', '--')[:19]})")
    print(f"  Tickers in snapshot:     {rankings.get('tickers', 0)}")
    print(f"  Strategy attribution:    {'YES -- strategy_id column present' if rankings.get('has_strategy_id') else 'NO  -- cannot trace which strategies contributed'}")
    latest_pred_ts = rankings.get("latest_prediction")
    snap_ts        = rankings.get("latest")
    if latest_pred_ts and snap_ts:
        pred_dt = datetime.fromisoformat(latest_pred_ts.replace("Z", "+00:00"))
        snap_dt = datetime.fromisoformat(snap_ts.replace("Z", "+00:00"))
        lag     = (snap_dt - pred_dt).days
        print(f"  Snapshot vs pred lag:    {lag:+d}d  (snapshot {'after' if lag >= 0 else 'BEFORE'} latest prediction)")
    print()

    # -- Section 5: catalog reconciliation ------------------------------------
    print(f"{'-'*70}")
    print("API CATALOG RECONCILIATION  (strategies table vs reality)")
    print()
    print(f"  {'catalog_id':<36} {'in_predictions':^14} {'champion':^9} {'sample':>7}")
    print(f"  {'-'*36} {'-'*14} {'-'*9} {'-'*7}")
    for cid, info in sorted(catalog.items()):
        in_preds = any(_base(cid) == _base(k) for k in preds)
        print(
            f"  {cid:<36} {'YES' if in_preds else 'NO -- never ran':^14} "
            f"{'*' if info['champion'] else ' ':^9} {info['sample_size']:>7}"
        )
    print()
    catalog_orphans = [cid for cid in catalog if not any(_base(cid) == _base(k) for k in preds)]
    reg_not_in_catalog = [r for r in registry if not any(_base(r) == _base(k) for k in catalog)]
    print(f"  Catalog entries with zero predictions: {len(catalog_orphans)}")
    print(f"  Registry strategies absent from catalog: {', '.join(reg_not_in_catalog) or 'none'}")
    print()

    # -- Section 6: summary ---------------------------------------------------
    print(f"{'-'*70}")
    print("SUMMARY")
    print()
    stalled = [s for s in registry if not any(_base(s) == _base(k) for k in preds)]
    active  = [s for s in registry if any(_base(s) == _base(k) for k in preds)]
    print(f"  Registry strategies:          {len(registry)}")
    print(f"  Reaching predictions (any):   {len(active)}  -> {', '.join(active)}")
    print(f"  Stalled before predictions:   {len(stalled)}  -> {', '.join(stalled)}")
    print(f"  Orphan prediction strategies: {len(orphan_preds)}  -> {', '.join(orphan_preds)}")
    print(f"  Catalog entries / match rate: {len(catalog)} / {len(catalog) - len(catalog_orphans)} matched")
    print(f"{'='*70}\n")

    conn.close()


# -- entry point ---------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit strategy coverage across all pipeline layers.")
    parser.add_argument("--db", default=os.environ.get("ALPHA_DB_PATH", "data/alpha.db"))
    parser.add_argument("--tenant-id", default="default")
    args = parser.parse_args()
    run_audit(args.db, args.tenant_id)
