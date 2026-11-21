"""
Generate comprehensive symbol-level coverage report for all 153 consumer universe symbols.
"""
import json
import sqlite3
import yaml
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

DB_PATH = "/home/administrator/web/alpha-trading-platform/alpha-engine/data/alpha.db"
CONFIG_PATH = "/home/administrator/web/alpha-trading-platform/alpha-engine/config/consumer_universe_150.yaml"
OUTPUT_REPORT = "/home/administrator/web/alpha-trading-platform/alpha-engine/reports/consumer_universe_coverage_manifest.json"

NYSE_HOLIDAYS_2026_WINDOW = {
    date(2026, 5, 25), # Memorial Day
    date(2026, 6, 19), # Juneteenth
    date(2026, 7, 3),  # Independence Day (Observed)
    date(2026, 9, 7),  # Labor Day
}

def parse_date(ts):
    s = str(ts).strip()[:10]
    return date.fromisoformat(s)

def run():
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    symbols = []
    for s_list in data.values():
        if isinstance(s_list, list):
            for s in s_list:
                clean = str(s).strip().upper().replace(".", "-")
                if clean not in symbols:
                    symbols.append(clean)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Calculate expected trading sessions in backfill window: 2026-04-15 to 2026-11-20
    start_dt = date(2026, 4, 15)
    end_dt = date(2026, 11, 20)
    expected_sessions = []
    curr = start_dt
    while curr <= end_dt:
        if curr.weekday() < 5 and curr not in NYSE_HOLIDAYS_2026_WINDOW:
            expected_sessions.append(curr)
        curr += timedelta(days=1)

    expected_count = len(expected_sessions)
    print(f"Total canonical symbols: {len(symbols)}")
    print(f"Backfill window: {start_dt} to {end_dt}")
    print(f"Expected NYSE trading sessions in window: {expected_count}")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": DB_PATH,
        "tenant_id": "default",
        "provider": "yfinance",
        "price_adjustment_convention": "auto_adjust=True (Split & Dividend Adjusted)",
        "backfill_window": {
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "expected_sessions": expected_count,
            "holidays_excluded": [d.isoformat() for d in sorted(NYSE_HOLIDAYS_2026_WINDOW)]
        },
        "symbols_total": len(symbols),
        "symbols_fully_covered": 0,
        "symbols_incomplete": 0,
        "symbol_details": {}
    }

    fully_covered = 0
    incomplete = 0

    for sym in symbols:
        cur.execute("""
            SELECT timestamp
            FROM price_bars
            WHERE tenant_id = 'default' AND ticker = ? AND timeframe = '1d'
            ORDER BY timestamp ASC
        """, (sym,))
        ts_rows = [r[0] for r in cur.fetchall()]
        total_bars = len(ts_rows)
        if total_bars == 0:
            report["symbol_details"][sym] = {
                "status": "MISSING",
                "total_bars": 0,
                "unexplained_gaps": [d.isoformat() for d in expected_sessions]
            }
            incomplete += 1
            continue

        min_d = parse_date(ts_rows[0])
        max_d = parse_date(ts_rows[-1])
        dates_present = {parse_date(ts) for ts in ts_rows}

        window_bars_present = sum(1 for d in expected_sessions if d in dates_present)
        unexplained = [d.isoformat() for d in expected_sessions if d not in dates_present]

        status = "COMPLETE" if len(unexplained) == 0 and max_d >= end_dt else "INCOMPLETE"
        if status == "COMPLETE":
            fully_covered += 1
        else:
            incomplete += 1

        report["symbol_details"][sym] = {
            "status": status,
            "earliest_date": min_d.isoformat(),
            "latest_date": max_d.isoformat(),
            "total_lifetime_bars": total_bars,
            "expected_sessions_in_window": expected_count,
            "actual_sessions_in_window": window_bars_present,
            "legitimate_holiday_exclusions": len(NYSE_HOLIDAYS_2026_WINDOW),
            "unexplained_gaps_count": len(unexplained),
            "unexplained_gaps": unexplained,
            "provenance": "yfinance_auto_adjust"
        }

    report["symbols_fully_covered"] = fully_covered
    report["symbols_incomplete"] = incomplete

    conn.close()

    Path(OUTPUT_REPORT).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_REPORT, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Coverage audit complete: {fully_covered}/{len(symbols)} symbols 100% COMPLETE. Incomplete: {incomplete}.")
    print(f"Report saved to: {OUTPUT_REPORT}")

if __name__ == "__main__":
    run()
