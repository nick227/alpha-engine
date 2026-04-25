"""
Discovery Integration Module

Queues discovery candidates into prediction_queue using threshold rules across
registered strategies (see app.engine.threshold_queue). PROMOTED_STRATEGIES
supplies per-strategy price bands and horizons where we have validated IC.
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta
from typing import Any

from app.db.repository import AlphaRepository
from app.discovery.runner import run_discovery
from app.discovery.strategies.registry import STRATEGIES, THRESHOLDS
from app.engine.threshold_queue import build_threshold_queue_rows


# Strategies promoted directly to prediction queue (bypassing multi-strategy gate).
# Only strategies with confirmed IC on filtered universe belong here.
PROMOTED_STRATEGIES: dict[str, dict[str, Any]] = {
    "silent_compounder": {
        # Equity vol band (~2% daily) + positive 63d drift → bullish continuation
        # Confirmed (2026-04-14): 58.2% at 5d, 64.3% at 20d, avg +1.08% on $20+ equity universe
        # Stop-loss at -15%: CORT lost -58% without it; capped losses improve Sharpe materially
        "direction": "UP",
        "horizon_days": 20,      # 20d is the confirmed best horizon (64.3% win, n=6035)
        "max_candidates": 15,
        "min_close": 20.0,       # $20+ is the quality sweet spot (58% vs 49% for $10-$20)
        "max_loss_pct": 0.15,    # exit if position down 15% — wired into compute_candidate_outcomes
        "priority_base": 20,
    },
    "balance_sheet_survivor": {
        # Distress (negative 63d) + volatility stabilization → mean-reversion bounce
        # Sweet spot is $10-$20 (56.9% at 5d, avg +2.16%). $20+ inverts to 46% — do not use.
        "direction": "UP",
        "horizon_days": 5,
        "max_candidates": 10,
        "min_close": 10.0,
        "max_close": 20.0,       # cap at $20 — above that the thesis breaks down
        "max_loss_pct": 0.15,
        "priority_base": 15,
    },
}

QUEUE_DIVERSITY_ENABLED = str(os.getenv("QUEUE_DIVERSITY_ENABLED", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
QUEUE_MIN_ROWS_PER_STRATEGY = int(os.getenv("QUEUE_MIN_ROWS_PER_STRATEGY", "2"))
QUEUE_TOPUP_MAX_TOTAL = int(os.getenv("QUEUE_TOPUP_MAX_TOTAL", "10"))
QUEUE_TOPUP_ACTIVE_ONLY = str(os.getenv("QUEUE_TOPUP_ACTIVE_ONLY", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_PER_STRATEGY_CAP = int(os.getenv("ALPHA_PER_STRATEGY_CAP", "22"))
DEFAULT_MIN_CONFIDENCE = float(os.getenv("ALPHA_MIN_DISCOVERY_CONFIDENCE", "0.42"))


def _inactive_strategies() -> set[str]:
    raw = str(os.getenv("ALPHA_INACTIVE_STRATEGIES", ""))
    return {s.strip() for s in raw.split(",") if s.strip()}


def _active_strategy_list() -> list[str]:
    names = list(STRATEGIES.keys())
    if not QUEUE_TOPUP_ACTIVE_ONLY:
        return names
    inactive = _inactive_strategies()
    return [s for s in names if s not in inactive]


def _strategy_candidate_top_rows(
    *,
    disc_summary: dict[str, Any],
    strategy_name: str,
    as_of_str: str,
    exclude_symbols: set[str],
    symbol_claims: dict[str, set[str]],
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    per_strategy_cap: int = DEFAULT_PER_STRATEGY_CAP,
) -> list[tuple[float, dict[str, Any]]]:
    """
    Build eligible queue candidates for one strategy from discovery summary.
    Returns list[(score, queue_row)] ordered by score desc.
    """
    out: list[tuple[float, dict[str, Any]]] = []
    strategies_block = disc_summary.get("strategies") or {}
    st_summary = strategies_block.get(strategy_name)
    if not isinstance(st_summary, dict):
        return out
    tops = st_summary.get("top") or []
    if not isinstance(tops, list):
        return out

    gate = max(float(THRESHOLDS.get(strategy_name, 0.5)), float(min_confidence))
    cfg = PROMOTED_STRATEGIES.get(strategy_name) if isinstance(PROMOTED_STRATEGIES.get(strategy_name), dict) else {}
    min_close = float(cfg.get("min_close", 5.0))
    max_close = float(cfg.get("max_close", 1e9))
    direction = str(cfg.get("direction", "UP"))
    horizon_days = int(cfg.get("horizon_days", 15))
    priority_base = int(cfg.get("priority_base", 12))

    for c in tops[: int(per_strategy_cap)]:
        if not isinstance(c, dict):
            continue
        raw = float(c.get("score") or 0.0)
        if raw < gate:
            continue
        sym = str(c.get("symbol") or "").strip().upper()
        if not sym or sym in exclude_symbols:
            continue
        meta = c.get("metadata") or {}
        if not isinstance(meta, dict):
            meta = {}
        close = float(meta.get("close") or 0.0)
        if close < min_close or close >= max_close:
            continue
        claiming = sorted(symbol_claims.get(sym) or {strategy_name})
        metadata = {
            "strategy": strategy_name,
            "primary_strategy": strategy_name,
            "claiming_strategies": claiming,
            "claim_count": int(len(claiming)),
            "direction": direction,
            "avg_score": raw,
            "horizon_days": horizon_days,
            "close": close,
            "raw_score": raw,
            "source_pipeline": "diversity_topup",
            "as_of_date": as_of_str,
            "queue_path": "diversity_topup",
        }
        priority = priority_base + int((1.0 - raw) * 10)
        out.append(
            (
                raw,
                {
                    "symbol": sym,
                    "source": f"discovery_{strategy_name}",
                    "priority": priority,
                    "status": "pending",
                    "metadata_json": json.dumps(metadata, sort_keys=True),
                },
            )
        )
    out.sort(key=lambda t: -float(t[0]))
    return out


def _build_symbol_claims(disc_summary: dict[str, Any], *, min_confidence: float) -> dict[str, set[str]]:
    claims: dict[str, set[str]] = {}
    strategies_block = disc_summary.get("strategies") or {}
    if not isinstance(strategies_block, dict):
        return claims
    for strategy_name, st_summary in strategies_block.items():
        if not isinstance(st_summary, dict):
            continue
        tops = st_summary.get("top") or []
        if not isinstance(tops, list):
            continue
        gate = max(float(THRESHOLDS.get(str(strategy_name), 0.5)), float(min_confidence))
        for c in tops:
            if not isinstance(c, dict):
                continue
            raw = float(c.get("score") or 0.0)
            if raw < gate:
                continue
            sym = str(c.get("symbol") or "").strip().upper()
            if not sym:
                continue
            claims.setdefault(sym, set()).add(str(strategy_name))
    return claims


def merge_strategy_threshold_queue(
    *,
    repo: AlphaRepository,
    disc_summary: dict[str, Any],
    as_of_date: str,
    tenant_id: str = "default",
    min_rows_per_strategy: int = QUEUE_MIN_ROWS_PER_STRATEGY,
    max_total: int = QUEUE_TOPUP_MAX_TOTAL,
    per_strategy_cap: int = DEFAULT_PER_STRATEGY_CAP,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
) -> dict[str, Any]:
    """
    Diversity top-up after watchlist + supplement writes.
    - Never overwrites/removes existing queue rows.
    - Adds rows for underrepresented active strategies, up to max_total.
    - Skips symbols already present in queue.
    """
    if not QUEUE_DIVERSITY_ENABLED or int(max_total) <= 0 or int(min_rows_per_strategy) <= 0:
        return {"enabled": False, "added": 0, "by_strategy": {}}

    existing_rows = repo.conn.execute(
        """
        SELECT symbol, source, metadata_json
        FROM prediction_queue
        WHERE tenant_id = ? AND as_of_date = ?
        """,
        (str(tenant_id), str(as_of_date)),
    ).fetchall()
    existing_symbols = {str(r["symbol"]).strip().upper() for r in existing_rows if r["symbol"]}
    current_counts: dict[str, int] = {}
    for r in existing_rows:
        strategy = None
        try:
            md = json.loads(str(r["metadata_json"] or "{}"))
            if isinstance(md, dict):
                strategy = md.get("strategy")
        except Exception:
            strategy = None
        if strategy:
            key = str(strategy)
            current_counts[key] = current_counts.get(key, 0) + 1

    active_strategies = _active_strategy_list()
    selected_rows: list[dict[str, Any]] = []
    added_by_strategy: dict[str, int] = {}
    slots_left = int(max_total)
    symbol_claims = _build_symbol_claims(disc_summary, min_confidence=float(min_confidence))

    for strategy_name in active_strategies:
        if slots_left <= 0:
            break
        have = int(current_counts.get(strategy_name, 0))
        needed = max(0, int(min_rows_per_strategy) - have)
        if needed <= 0:
            continue

        candidates = _strategy_candidate_top_rows(
            disc_summary=disc_summary,
            strategy_name=strategy_name,
            as_of_str=str(as_of_date),
            exclude_symbols=existing_symbols,
            symbol_claims=symbol_claims,
            min_confidence=float(min_confidence),
            per_strategy_cap=int(per_strategy_cap),
        )
        if not candidates:
            continue

        take = min(int(needed), int(slots_left), len(candidates))
        if take <= 0:
            continue

        for _score, row in candidates[:take]:
            sym = str(row["symbol"]).upper()
            if sym in existing_symbols:
                continue
            existing_symbols.add(sym)
            selected_rows.append(row)
            added_by_strategy[strategy_name] = added_by_strategy.get(strategy_name, 0) + 1
            slots_left -= 1
            if slots_left <= 0:
                break

    if selected_rows:
        repo.upsert_prediction_queue(
            as_of_date=str(as_of_date),
            rows_in=selected_rows,
            tenant_id=str(tenant_id),
        )

    return {
        "enabled": True,
        "added": len(selected_rows),
        "by_strategy": added_by_strategy,
        "min_rows_per_strategy": int(min_rows_per_strategy),
        "max_total": int(max_total),
        "active_strategies": active_strategies,
    }


def _seed_consensus_for_queue_rows(
    repo: AlphaRepository,
    queue_rows: list[dict[str, Any]],
    *,
    tenant_id: str,
) -> int:
    seeds_created = 0
    for row in queue_rows:
        symbol = str(row["symbol"])
        meta = json.loads(str(row["metadata_json"]))
        avg_score = float(meta.get("avg_score") or 0.7)
        p_final = max(-1.0, min(1.0, (avg_score * 2.0) - 1.0))
        conf = abs(p_final)
        strategy_name = str(meta.get("strategy") or "discovery")

        try:
            existing = repo.conn.execute(
                "SELECT COUNT(*) as n FROM consensus_signals WHERE tenant_id=? AND ticker=?",
                (tenant_id, symbol),
            ).fetchone()
            if existing is None or int(existing["n"] or 0) == 0:
                repo.save_consensus_signal(
                    {
                        "ticker": symbol,
                        "regime": "DISCOVERY",
                        "sentiment_strategy_id": f"{strategy_name}_v1",
                        "quant_strategy_id": f"{strategy_name}_v1",
                        "sentiment_score": conf,
                        "quant_score": conf,
                        "ws": 0.5,
                        "wq": 0.5,
                        "agreement_bonus": 0.0,
                        "p_final": p_final,
                        "stability_score": conf,
                    },
                    tenant_id=tenant_id,
                )
                seeds_created += 1
        except Exception as e:
            print(f"[WARN] consensus seed failed for {symbol}: {e}")
    return seeds_created


def supplement_prediction_queue_from_discovery(
    *,
    repo: AlphaRepository,
    disc_summary: dict[str, Any],
    as_of_date: str,
    tenant_id: str = "default",
    target_signals: int = 120,
    min_confidence: float = 0.42,
    per_strategy_cap: int = 22,
) -> dict[str, Any]:
    """
    After watchlist rows are queued, add threshold-based discovery rows without
    duplicating symbols already present for this as-of date.
    """
    ex = repo.conn.execute(
        """
        SELECT DISTINCT UPPER(TRIM(symbol)) AS s
        FROM prediction_queue
        WHERE tenant_id = ? AND as_of_date = ?
        """,
        (tenant_id, as_of_date),
    ).fetchall()
    exclude_symbols = {str(r["s"]) for r in ex if r and r["s"]}

    queue_rows, by_strategy = build_threshold_queue_rows(
        disc_summary=disc_summary,
        as_of_str=as_of_date,
        target_signals=target_signals,
        per_strategy_cap=per_strategy_cap,
        min_confidence=min_confidence,
        promoted_overrides=PROMOTED_STRATEGIES,
        exclude_symbols=exclude_symbols,
        source_pipeline="threshold_supplement",
    )
    if not queue_rows:
        return {"added": 0, "by_strategy": {}, "consensus_seeded": 0}

    repo.conn.execute("PRAGMA journal_mode=WAL;")
    repo.upsert_prediction_queue(
        as_of_date=as_of_date,
        rows_in=queue_rows,
        tenant_id=tenant_id,
    )
    seeded = _seed_consensus_for_queue_rows(repo, queue_rows, tenant_id=tenant_id)
    return {"added": len(queue_rows), "by_strategy": by_strategy, "consensus_seeded": seeded}


def queue_discovery_predictions(
    *,
    repo: AlphaRepository,
    as_of: date,
    tenant_id: str = "default",
    min_adv: float = 2_000_000,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Run discovery and enqueue candidates using global score/confidence gates
    (threshold_queue) plus per-strategy overrides in PROMOTED_STRATEGIES.
    """
    as_of_str = as_of.isoformat()

    result = run_discovery(
        db_path=repo.db_path,
        as_of=as_of,
        min_avg_dollar_volume_20d=min_adv,
        use_feature_snapshot=True,
        top_n=50,
    )

    queue_rows, by_strategy_counts = build_threshold_queue_rows(
        disc_summary=result,
        as_of_str=as_of_str,
        promoted_overrides=PROMOTED_STRATEGIES,
        exclude_symbols=set(),
        source_pipeline="nightly_discovery",
    )

    by_strategy: dict[str, int] = {}
    for row in queue_rows:
        src = str(row["source"])
        by_strategy[src] = by_strategy.get(src, 0) + 1

    if dry_run:
        return {
            "as_of_date": as_of_str,
            "dry_run": True,
            "total_queued": len(queue_rows),
            "by_strategy": by_strategy,
            "by_strategy_scoring": by_strategy_counts,
            "feature_rows": result.get("feature_rows", 0),
            "candidates": queue_rows,
        }

    seeds_created = 0
    if queue_rows:
        repo.conn.execute("PRAGMA journal_mode=WAL;")
        repo.upsert_prediction_queue(
            as_of_date=as_of_str,
            rows_in=queue_rows,
            tenant_id=tenant_id,
        )
        seeds_created = _seed_consensus_for_queue_rows(repo, queue_rows, tenant_id=tenant_id)

    return {
        "as_of_date": as_of_str,
        "total_queued": len(queue_rows),
        "by_strategy": by_strategy,
        "by_strategy_scoring": by_strategy_counts,
        "consensus_seeded": seeds_created,
        "feature_rows": result.get("feature_rows", 0),
    }


def batch_queue_discovery(
    *,
    repo: AlphaRepository,
    start_date: date,
    end_date: date,
    tenant_id: str = "default",
    min_adv: float = 2_000_000,
) -> dict[str, Any]:
    """
    Queue discovery predictions for a range of dates.
    Useful for backfilling paper trade history.
    """
    total = 0
    current = start_date
    daily_summaries: list[dict[str, Any]] = []

    while current <= end_date:
        try:
            summary = queue_discovery_predictions(
                repo=repo,
                as_of=current,
                tenant_id=tenant_id,
                min_adv=min_adv,
            )
            total += int(summary.get("total_queued") or 0)
            daily_summaries.append(summary)
        except Exception as e:
            print(f"[WARN] {current}: {e}")

        current += timedelta(days=1)

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_queued": total,
        "days_processed": len(daily_summaries),
    }
