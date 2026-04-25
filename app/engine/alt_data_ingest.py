from __future__ import annotations

import json
import math
import os

from app.db.repository import AlphaRepository

META_RANKER_ALT_DATA_MODE = str(os.getenv("META_RANKER_ALT_DATA_MODE", "off")).strip().lower()


def _safe_float(v: object, fallback: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(fallback)


def _price_rows(repo: AlphaRepository, *, tenant_id: str, symbol: str, as_of_date: str, lookback: int = 25) -> list[tuple[float, float]]:
    rows = repo.conn.execute(
        """
        SELECT close, volume
        FROM price_bars
        WHERE tenant_id = ? AND timeframe = '1d' AND ticker = ? AND substr(timestamp, 1, 10) <= ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (str(tenant_id), str(symbol), str(as_of_date), int(lookback)),
    ).fetchall()
    return [(_safe_float(r["close"]), _safe_float(r["volume"])) for r in rows if r["close"] is not None and r["volume"] is not None]


def ingest_alt_data_snapshot(
    *,
    repo: AlphaRepository,
    as_of_date: str,
    tenant_id: str,
    symbols: list[str],
    source: str = "proxy_free",
) -> dict[str, object]:
    mode = META_RANKER_ALT_DATA_MODE
    if mode in {"off", "none", "baseline"}:
        return {"enabled": False, "mode": mode, "written": 0, "coverage": 0.0}

    uniq = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
    if not uniq:
        return {"enabled": True, "mode": mode, "written": 0, "coverage": 0.0}

    rows: list[dict[str, object]] = []
    for symbol in uniq:
        series = _price_rows(repo, tenant_id=str(tenant_id), symbol=symbol, as_of_date=str(as_of_date), lookback=25)
        if len(series) < 3:
            continue
        closes = [x[0] for x in series]
        vols = [x[1] for x in series]
        r1 = ((closes[0] / closes[1]) - 1.0) if closes[1] > 0 else 0.0
        avg_vol = (sum(vols) / len(vols)) if vols else 0.0
        vol_std = math.sqrt(sum((v - avg_vol) ** 2 for v in vols) / max(1, len(vols))) if vols else 0.0
        vol_z = ((vols[0] - avg_vol) / vol_std) if vol_std > 1e-12 else 0.0

        features: dict[str, float] = {}
        # Cheap proxy features usable for challenger A/B without paid feeds.
        if mode in {"news", "all", "news+search"}:
            features["news_sentiment_1d"] = max(-1.0, min(1.0, r1 * 20.0))
            features["news_volume_1d"] = max(0.0, vol_z)
            features["sentiment_delta_3d"] = max(-1.0, min(1.0, (r1 * 20.0) - (closes[2] / closes[1] - 1.0) * 20.0))
        if mode in {"search", "all", "news+search"}:
            features["search_interest_z"] = vol_z
            features["search_momentum_7d"] = ((closes[0] / closes[min(7, len(closes) - 1)]) - 1.0) if len(closes) > 7 and closes[min(7, len(closes) - 1)] > 0 else r1
        if mode in {"sec", "all"}:
            features["filing_8k_recent"] = 1.0 if abs(r1) >= 0.05 else 0.0
            features["filing_density_30d"] = max(0.0, min(1.0, abs(r1) * 5.0))

        if not features:
            continue
        quality = 0.5 + (0.5 if len(series) >= 20 else 0.0)
        rows.append(
            {
                "symbol": symbol,
                "source": str(source),
                "feature_json": json.dumps(features, sort_keys=True),
                "quality_score": float(quality),
            }
        )

    written = repo.upsert_alt_data_daily(as_of_date=str(as_of_date), rows=rows, tenant_id=str(tenant_id))
    coverage = (written / len(uniq)) if uniq else 0.0
    return {
        "enabled": True,
        "mode": mode,
        "source": source,
        "written": int(written),
        "requested_symbols": len(uniq),
        "coverage": float(coverage),
    }
