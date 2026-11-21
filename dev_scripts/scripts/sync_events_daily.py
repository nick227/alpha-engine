"""
Sync market events and news for active universe into raw_events using authorized yfinance provider.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.active_universe import get_active_universe_tickers


def _event_id(ticker: str, title: str, ts: str) -> str:
    h = hashlib.sha256(f"{ticker}:{title}:{ts}".encode("utf-8")).hexdigest()[:16]
    return f"evt_{h}"


def sync_events_for_universe(
    db_path: str = "data/alpha.db",
    tenant_id: str = "default",
    max_symbols: int = 25,
) -> dict[str, int]:
    import yfinance as yf

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now_iso = datetime.now(timezone.utc).isoformat()
    inserted = 0

    try:
        universe = get_active_universe_tickers(tenant_id=tenant_id, sqlite_conn=conn)
        target = universe[:max_symbols]

        for sym in target:
            try:
                t = yf.Ticker(sym)
                news = t.news or []
                for item in news[:5]:
                    content = item.get("content") if isinstance(item.get("content"), dict) else item
                    title = content.get("title") or item.get("title")
                    if not title:
                        continue
                    pub_time = content.get("pubDate") or item.get("providerPublishTime")
                    if isinstance(pub_time, int):
                        pub_ts = datetime.fromtimestamp(pub_time, tz=timezone.utc).isoformat()
                    elif isinstance(pub_time, str):
                        pub_ts = pub_time
                    else:
                        pub_ts = now_iso

                    eid = _event_id(sym, title, pub_ts)
                    meta = {
                        "publisher": content.get("provider") or item.get("publisher"),
                        "link": content.get("canonicalUrl", {}).get("url") if isinstance(content.get("canonicalUrl"), dict) else item.get("link"),
                        "synced_at": now_iso,
                    }

                    conn.execute(
                        """
                        INSERT OR IGNORE INTO raw_events
                          (id, tenant_id, timestamp, source, text, tickers_json, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            eid,
                            tenant_id,
                            pub_ts,
                            "yfinance_news",
                            str(title),
                            json.dumps([sym]),
                            json.dumps(meta),
                        ),
                    )
                    inserted += 1
            except Exception:
                continue

        conn.commit()
    finally:
        conn.close()

    return {"events_synced": inserted, "symbols_processed": len(target)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync events and news for active universe")
    parser.add_argument("--db", default="data/alpha.db")
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument("--max-symbols", type=int, default=25)
    args = parser.parse_args()

    res = sync_events_for_universe(db_path=args.db, tenant_id=args.tenant_id, max_symbols=args.max_symbols)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
