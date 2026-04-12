from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _isoz(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        # Accept "Z" and "+00:00" forms.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except (TypeError, ValueError):
        return int(default)


def _json_get(snapshot_json: str | None, key: str) -> Any:
    if not snapshot_json:
        return None
    try:
        obj = json.loads(snapshot_json)
        if isinstance(obj, dict):
            return obj.get(key)
    except Exception:
        return None
    return None


def _print_table(rows: list[dict[str, Any]], headers: list[str], limit: int | None = None) -> None:
    if not rows:
        print("(no rows)")
        return
    shown = rows if limit is None else rows[: max(0, int(limit))]

    widths: dict[str, int] = {h: len(h) for h in headers}
    for r in shown:
        for h in headers:
            widths[h] = max(widths[h], len(str(r.get(h, ""))))

    line = " | ".join(h.ljust(widths[h]) for h in headers)
    sep = "-+-".join("-" * widths[h] for h in headers)
    print(line)
    print(sep)
    for r in shown:
        print(" | ".join(str(r.get(h, "")).ljust(widths[h]) for h in headers))


def main() -> int:
    ap = argparse.ArgumentParser(description="Report strategy performance grouped by regime and trend.")
    ap.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")
    ap.add_argument("--tenant", default="default", help="tenant_id (default: default)")
    ap.add_argument("--days", type=int, default=30, help="lookback window in days (default: 30)")
    ap.add_argument("--limit", type=int, default=50, help="max rows to display (default: 50)")
    ap.add_argument("--horizon", default="", help="filter horizon (e.g. 1d, 7d, 30d)")
    ap.add_argument("--regime", default="", help="filter regime (e.g. HIGH, NORMAL, bull_market)")
    ap.add_argument("--trend", default="", help="filter trend (e.g. WEAK, NORMAL, STRONG)")
    ap.add_argument("--strategy-contains", default="", help="case-insensitive substring filter on strategy_id")
    ap.add_argument("--only-families", default="", help="comma-separated: volatility,cross_asset,breakout,momentum,mean_reversion,ml,sentiment,consensus,other")
    ap.add_argument("--family-summary", action="store_true", help="print additional family summary table")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"db not found: {db_path}")

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=int(args.days))
    since_s = _isoz(since)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Join predictions to outcomes (left join so we can see missing outcomes too).
        rows = conn.execute(
            """
            SELECT
              p.strategy_id,
              COALESCE(p.horizon, '') AS horizon,
              COALESCE(p.regime, p.feature_snapshot_json) AS regime_hint,
              COALESCE(p.trend_strength, '') AS trend_strength,
              p.feature_snapshot_json,
              o.return_pct,
              o.direction_correct
            FROM predictions p
            LEFT JOIN prediction_outcomes o
              ON o.tenant_id = p.tenant_id AND o.prediction_id = p.id
            WHERE p.tenant_id = ?
              AND p.timestamp >= ?
            """,
            (str(args.tenant), since_s),
        ).fetchall()

        horizon_filter = str(args.horizon).strip().lower() if args.horizon else ""
        regime_filter = str(args.regime).strip()
        trend_filter = str(args.trend).strip().upper() if args.trend else ""
        strat_contains = str(args.strategy_contains).strip().lower() if args.strategy_contains else ""
        fam_allow = {x.strip().lower() for x in str(args.only_families).split(",") if x.strip()} if args.only_families else set()

        agg: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        fam_agg: dict[tuple[str, str, str], dict[str, Any]] = {}
        for r in rows:
            strategy_id = str(r["strategy_id"])
            if strat_contains and strat_contains not in strategy_id.lower():
                continue
            horizon = str(r["horizon"] or "")
            if horizon_filter and str(horizon).strip().lower() != horizon_filter:
                continue

            regime = r["regime_hint"]
            # If `regime` column is null, try feature_snapshot.regime.
            if regime is None or (isinstance(regime, str) and regime.startswith("{")):
                snap_regime = _json_get(r["feature_snapshot_json"], "regime")
                regime = snap_regime if snap_regime is not None else "UNKNOWN"
            regime_s = str(regime or "UNKNOWN")
            if regime_filter and str(regime_s) != regime_filter:
                continue

            trend = str(r["trend_strength"] or _json_get(r["feature_snapshot_json"], "trend_strength") or "UNKNOWN")
            if trend_filter and str(trend).upper() != trend_filter:
                continue

            setup = _json_get(r["feature_snapshot_json"], "setup")
            family = _json_get(r["feature_snapshot_json"], "family")
            # Derive family from snapshot first; fall back to strategy_id patterns.
            fam = str(family or "").strip().lower()
            if not fam:
                sid = strategy_id.lower()
                if "consensus" in sid:
                    fam = "consensus"
                elif "text_mra" in sid or "sentiment" in sid:
                    fam = "sentiment"
                elif "ml_" in sid or "ml_factor" in sid:
                    fam = "ml"
                elif "breakout" in sid:
                    fam = "breakout"
                elif "relative_strength" in sid or "cross_asset" in sid:
                    fam = "cross_asset"
                elif "vol_" in sid or "volatility" in sid:
                    fam = "volatility"
                elif "rsi_reversion" in sid or "bollinger_reversion" in sid or "mean_reversion" in sid:
                    fam = "mean_reversion"
                elif "momentum" in sid or "vwap_reclaim" in sid or "ma_cross" in sid:
                    fam = "momentum"
                else:
                    fam = "other"
            if fam_allow and fam not in fam_allow:
                continue

            key = (strategy_id, horizon, regime_s, trend)
            a = agg.get(key)
            if a is None:
                a = {
                    "strategy_id": strategy_id,
                    "horizon": horizon,
                    "regime": regime_s,
                    "trend": trend,
                    "n": 0,
                    "n_scored": 0,
                    "win_rate": 0.0,
                    "avg_return": 0.0,
                }
                agg[key] = a

            a["n"] += 1
            if r["return_pct"] is None:
                continue
            a["n_scored"] += 1
            a.setdefault("_wins", 0)
            a.setdefault("_ret_sum", 0.0)
            if bool(r["direction_correct"]):
                a["_wins"] += 1
            a["_ret_sum"] += _safe_float(r["return_pct"], 0.0)

            fk = (fam, horizon, regime_s)
            fa = fam_agg.get(fk)
            if fa is None:
                fa = {"family": fam, "horizon": horizon, "regime": regime_s, "n_scored": 0, "_wins": 0, "_ret_sum": 0.0}
                fam_agg[fk] = fa
            fa["n_scored"] += 1
            if bool(r["direction_correct"]):
                fa["_wins"] += 1
            fa["_ret_sum"] += _safe_float(r["return_pct"], 0.0)

        out_rows: list[dict[str, Any]] = []
        for a in agg.values():
            n_scored = _safe_int(a.get("n_scored"), 0)
            wins = _safe_int(a.get("_wins"), 0)
            ret_sum = _safe_float(a.get("_ret_sum"), 0.0)
            win_rate = (wins / n_scored) if n_scored else 0.0
            avg_return = (ret_sum / n_scored) if n_scored else 0.0
            out_rows.append(
                {
                    "strategy_id": a["strategy_id"],
                    "horizon": a["horizon"],
                    "regime": a["regime"],
                    "trend": a["trend"],
                    "n": a["n"],
                    "n_scored": n_scored,
                    "win_rate": f"{win_rate*100:5.1f}%",
                    "avg_return": f"{avg_return:+.4f}",
                }
            )

        # Sort: most scored first, then best avg_return.
        out_rows.sort(key=lambda x: (_safe_int(x["n_scored"]), float(x["avg_return"])), reverse=True)

        print(f"DB: {db_path} tenant={args.tenant} since={since_s}")
        _print_table(
            out_rows,
            headers=["strategy_id", "horizon", "regime", "trend", "n", "n_scored", "win_rate", "avg_return"],
            limit=int(args.limit),
        )

        if bool(args.family_summary):
            fam_rows: list[dict[str, Any]] = []
            for fa in fam_agg.values():
                n_scored = _safe_int(fa.get("n_scored"), 0)
                wins = _safe_int(fa.get("_wins"), 0)
                ret_sum = _safe_float(fa.get("_ret_sum"), 0.0)
                win_rate = (wins / n_scored) if n_scored else 0.0
                avg_return = (ret_sum / n_scored) if n_scored else 0.0
                fam_rows.append(
                    {
                        "family": fa["family"],
                        "horizon": fa["horizon"],
                        "regime": fa["regime"],
                        "n_scored": n_scored,
                        "win_rate": f"{win_rate*100:5.1f}%",
                        "avg_return": f"{avg_return:+.4f}",
                    }
                )
            fam_rows.sort(key=lambda x: (_safe_int(x["n_scored"]), float(x["avg_return"])), reverse=True)
            print("")
            _print_table(
                fam_rows,
                headers=["family", "horizon", "regime", "n_scored", "win_rate", "avg_return"],
                limit=int(args.limit),
            )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
