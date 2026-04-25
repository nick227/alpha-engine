from __future__ import annotations

import json
from typing import Any

from app.db.repository import AlphaRepository


def _safe_float(v: Any) -> float | None:
    try:
        return float(v)
    except Exception:
        return None


def _trading_dates(conn: Any, *, tenant_id: str, symbol: str, as_of_date: str, lookahead: int = 40) -> list[tuple[str, float]]:
    rows = conn.execute(
        """
        SELECT substr(timestamp, 1, 10) AS d, open, close
        FROM price_bars
        WHERE tenant_id = ? AND timeframe = '1d' AND ticker = ? AND substr(timestamp, 1, 10) > ?
        ORDER BY timestamp ASC
        LIMIT ?
        """,
        (str(tenant_id), str(symbol), str(as_of_date), int(lookahead)),
    ).fetchall()
    out: list[tuple[str, float]] = []
    for r in rows:
        entry_px = _safe_float(r["open"])
        close_px = _safe_float(r["close"])
        if entry_px is None and close_px is None:
            continue
        out.append((str(r["d"]), entry_px if entry_px is not None else float(close_px)))
    return out


def build_and_store_trade_intents(
    *,
    repo: AlphaRepository,
    run_id: str,
    class_key: str,
    experiment_key: str,
    as_of_date: str,
    tenant_id: str = "default",
    selected_symbols: list[str],
    score_map: dict[str, dict[str, Any]],
    horizons: tuple[int, ...] = (5, 20),
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for sym in sorted({str(s).strip().upper() for s in selected_symbols if str(s).strip()}):
        dates = _trading_dates(
            repo.conn,
            tenant_id=str(tenant_id),
            symbol=sym,
            as_of_date=str(as_of_date),
            lookahead=max(30, max(horizons) + 5),
        )
        if not dates:
            for hz in horizons:
                rows.append(
                    {
                        "run_id": str(run_id),
                        "class_key": str(class_key),
                        "experiment_key": str(experiment_key),
                        "as_of_date": str(as_of_date),
                        "symbol": sym,
                        "horizon_days": int(hz),
                        "entry_date": None,
                        "entry_price": None,
                        "exit_date": None,
                        "exit_price": None,
                        "intent_status": "missing_market_data",
                        "score_json": json.dumps(score_map.get(sym, {}), sort_keys=True),
                        "metadata_json": json.dumps({"reason": "no_forward_price_bars"}, sort_keys=True),
                    }
                )
            continue

        entry_date, entry_price = dates[0]
        for hz in horizons:
            idx = min(len(dates) - 1, int(hz))
            exit_date, exit_price = dates[idx]
            rows.append(
                {
                    "run_id": str(run_id),
                    "class_key": str(class_key),
                    "experiment_key": str(experiment_key),
                    "as_of_date": str(as_of_date),
                    "symbol": sym,
                    "horizon_days": int(hz),
                    "entry_date": str(entry_date),
                    "entry_price": float(entry_price),
                    "exit_date": str(exit_date),
                    "exit_price": float(exit_price),
                    "intent_status": "planned",
                    "score_json": json.dumps(score_map.get(sym, {}), sort_keys=True),
                    "metadata_json": json.dumps(
                        {
                            "entry_rule": "next_trading_day_open",
                            "exit_rule": f"{int(hz)}_trading_days_horizon_close",
                        },
                        sort_keys=True,
                    ),
                }
            )

    n = repo.upsert_trade_intents(rows=rows, tenant_id=str(tenant_id))
    return {
        "intents_written": int(n),
        "symbols": len({str(r.get("symbol")) for r in rows}),
        "horizons": [int(x) for x in horizons],
    }
