"""
Refresh consensus signals for active universe using legitimate warehouse price history and candidate signals.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.active_universe import get_active_universe_tickers
from app.db.repository import AlphaRepository


def _clamp(val: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(val)))


def refresh_consensus_for_universe(
    db_path: str = "data/alpha.db",
    tenant_id: str = "default",
) -> dict[str, int]:
    repo = AlphaRepository(db_path=db_path)
    now_iso = datetime.now(timezone.utc).isoformat()
    synced = 0

    try:
        universe = get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=repo.conn)
        for sym in universe:
            # Query recent 1d bars
            bars = repo.conn.execute(
                """
                SELECT timestamp, close FROM price_bars
                WHERE tenant_id = ? AND ticker = ? AND timeframe = '1d'
                ORDER BY timestamp DESC LIMIT 60
                """,
                (tenant_id, sym),
            ).fetchall()

            if not bars or len(bars) < 5:
                continue

            closes = [float(b["close"]) for b in reversed(bars) if b["close"] is not None]
            if len(closes) < 5:
                continue

            latest_close = closes[-1]
            sma_20 = sum(closes[-20:]) / len(closes[-20:])
            ret_20 = (latest_close - closes[0]) / closes[0] if closes[0] > 0 else 0.0

            # Daily log returns for realized volatility
            log_returns = []
            for i in range(1, len(closes[-20:])):
                if closes[-20 + i - 1] > 0 and closes[-20 + i] > 0:
                    log_returns.append(math.log(closes[-20 + i] / closes[-20 + i - 1]))

            vol = 0.20
            if len(log_returns) > 2:
                mean_r = sum(log_returns) / len(log_returns)
                var_r = sum((r - mean_r) ** 2 for r in log_returns) / (len(log_returns) - 1)
                vol = math.sqrt(var_r) * math.sqrt(252)

            regime = "NORMAL"
            if vol > 0.35:
                regime = "HIGH_VOLATILITY"
            elif vol < 0.15:
                regime = "LOW_VOLATILITY"

            # Quant score based on trend vs 20d SMA and 20d return
            trend_ratio = (latest_close - sma_20) / sma_20 if sma_20 > 0 else 0.0
            quant_score = _clamp(trend_ratio * 5.0 + ret_20 * 2.0)

            # Sentiment / candidate score lookup
            cq = repo.conn.execute(
                "SELECT metadata_json FROM candidate_queue WHERE tenant_id = ? AND ticker = ?",
                (tenant_id, sym),
            ).fetchone()
            sent_score = 0.0
            if cq and cq["metadata_json"]:
                try:
                    cq_meta = json.loads(cq["metadata_json"])
                    raw_s = cq_meta.get("avg_score")
                    if raw_s is not None:
                        sent_score = _clamp((float(raw_s) * 2.0) - 1.0)
                except Exception:
                    pass
            if sent_score == 0.0:
                sent_score = quant_score * 0.9

            p_final = round((0.5 * quant_score) + (0.5 * sent_score), 4)
            stability = round(max(0.1, min(0.99, 1.0 - (vol / 2.0))), 4)

            repo.save_consensus_signal(
                {
                    "ticker": sym,
                    "regime": regime,
                    "sentiment_strategy_id": "market_sentiment_v1",
                    "quant_strategy_id": "technical_quant_v1",
                    "sentiment_score": round(abs(sent_score), 4),
                    "quant_score": round(abs(quant_score), 4),
                    "ws": 0.5,
                    "wq": 0.5,
                    "agreement_bonus": 0.05 if (quant_score > 0 and sent_score > 0) else 0.0,
                    "p_final": p_final,
                    "stability_score": stability,
                },
                tenant_id=tenant_id,
            )
            synced += 1

        repo.conn.commit()
    finally:
        repo.close()

    return {"synced": synced, "universe_total": len(universe)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh consensus signals for active universe")
    parser.add_argument("--db", default="data/alpha.db")
    parser.add_argument("--tenant-id", default="default")
    args = parser.parse_args()

    res = refresh_consensus_for_universe(db_path=args.db, tenant_id=args.tenant_id)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
