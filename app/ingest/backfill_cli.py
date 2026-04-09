import asyncio
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
from app.ingest.backfill_runner import BackfillRunner
from app.ingest.event_store import EventStore
from app.core.time_utils import normalize_timestamp, to_utc_datetime
from app.ingest.validator import validate_sources_yaml
from app.core.target_stocks import (
    add_target_stock,
    get_target_stocks_registry,
    get_target_stocks,
    load_target_stock_specs,
    remove_target_stock,
    set_target_stock_enabled,
)

load_dotenv()

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Alpha Engine Backfill CLI",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m app.ingest.backfill_cli run --days 30\n"
            "  python -m app.ingest.backfill_cli backfill-range --start 2024-02-20 --end 2024-03-20\n"
            "  python -m app.ingest.backfill_cli list-target-stocks --asof 2024-03-01\n"
            "\n"
            "Tip: Prefer the interactive launcher:\n"
            "  python start.py\n"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Command: run (default 90 days)
    run_parser = subparsers.add_parser("run", help="Run full backfill", description="Run full backfill")
    run_parser.add_argument("--days", type=int, default=90, help="Number of days to backfill")
    run_parser.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")

    # Command: backfill-range
    range_parser = subparsers.add_parser("backfill-range", help="Backfill a specific range", description="Backfill a specific range")
    range_parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    range_parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    range_parser.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")
    range_parser.add_argument("--batch-size-days", type=int, default=1, help="Slice size in days (default: 1)")
    range_parser.add_argument(
        "--replay",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Replay after fetch (use --no-replay to skip)",
    )
    range_parser.add_argument(
        "--force-replay",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Force replay for the full requested window (ignores replayed_min/max markers)",
    )
    range_parser.add_argument(
        "--check-only",
        action="store_true",
        help="No-network coverage check: print missing/complete/partial windows and exit",
    )
    range_parser.add_argument(
        "--force-refetch-source",
        default=None,
        help="Ignore ingest_runs/slice markers for this source_id within the requested window",
    )
    range_parser.add_argument(
        "--skip-completed",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip fetching slices already marked complete per source (use --no-skip-completed to refetch)",
    )
    range_parser.add_argument(
        "--fail-fast",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Abort on suspicious zero-insert behavior (use --no-fail-fast to disable)",
    )
    range_parser.add_argument(
        "--max-zero-insert-slices",
        type=int,
        default=2,
        help="Consecutive zero-insert slices (with fetched+unique>0) before aborting",
    )

    # Command: list-target-stocks
    lts = subparsers.add_parser(
        "list-target-stocks",
        help="List the canonical Target Stocks universe",
        description="List the canonical Target Stocks universe",
    )
    lts.add_argument("--asof", default=None, help="Optional as-of timestamp/date (YYYY-MM-DD or ISO)")

    # Command: add-target-stock
    ats = subparsers.add_parser("add-target-stock", help="Add or update a Target Stock", description="Add or update a Target Stock")
    ats.add_argument("symbol", help="Ticker symbol (e.g. NVDA)")
    ats.add_argument("--group", default=None, help="Optional group label")
    ats.add_argument("--active-from", default=None, help="Optional active_from (YYYY-MM-DD or ISO)")
    ats.add_argument("--disabled", action="store_true", help="Add as disabled")

    # Command: remove-target-stock
    rts = subparsers.add_parser("remove-target-stock", help="Remove a Target Stock", description="Remove a Target Stock")
    rts.add_argument("symbol", help="Ticker symbol to remove")

    # Command: enable-target-stock
    ets = subparsers.add_parser("enable-target-stock", help="Enable a Target Stock", description="Enable a Target Stock")
    ets.add_argument("symbol", help="Ticker symbol to enable")

    # Command: disable-target-stock
    dts = subparsers.add_parser("disable-target-stock", help="Disable a Target Stock", description="Disable a Target Stock")
    dts.add_argument("symbol", help="Ticker symbol to disable")

    # Command: ingest-runs
    ir = subparsers.add_parser(
        "ingest-runs",
        help="Report ingest window idempotency coverage",
        description="Summarize ingest_runs (window-level idempotency markers)",
    )
    ir.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")
    ir.add_argument("--start", default=None, help="Filter windows with end_ts > start (YYYY-MM-DD or ISO)")
    ir.add_argument("--end", default=None, help="Filter windows with start_ts < end (YYYY-MM-DD or ISO)")
    ir.add_argument("--source", default=None, help="Filter by source_id")
    ir.add_argument("--top", type=int, default=8, help="Top N reasons to show per category")
    ir.add_argument("--show-spec-hash", action="store_true", help="Include spec_hash in summary output (debugging)")

    # Command: ingest-runs-detail
    ird = subparsers.add_parser(
        "ingest-runs-detail",
        help="Detailed ingest window view for one source",
        description="Show ingest_runs + ingest_run_stats per window for a source over a date range",
    )
    ird.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")
    ird.add_argument("--source", required=True, help="source_id to show")
    ird.add_argument("--start", default=None, help="Filter windows with end_ts > start (YYYY-MM-DD or ISO)")
    ird.add_argument("--end", default=None, help="Filter windows with start_ts < end (YYYY-MM-DD or ISO)")

    # Command: ingest-runs-cleanup
    irc = subparsers.add_parser(
        "ingest-runs-cleanup",
        help="Cleanup stale running ingest windows",
        description="Mark stale ingest_runs rows (status=running) as failed based on provider-specific TTLs",
    )
    irc.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")
    irc.add_argument("--dry-run", action="store_true", help="Print what would change without updating DB")

    # Command: ingest-health
    ih = subparsers.add_parser(
        "ingest-health",
        help="Ingestion health KPI summary",
        description="Coverage %, freshness, drift warnings, and latency per source over a date range",
    )
    ih.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")
    ih.add_argument("--start", required=True, help="Start date/time (YYYY-MM-DD or ISO)")
    ih.add_argument("--end", required=True, help="End date/time (YYYY-MM-DD or ISO)")
    ih.add_argument("--batch-size-days", type=int, default=1, help="Slice size in days (default: 1)")

    return parser

def _parse_dt_or_none(s: str | None) -> datetime | None:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    try:
        return to_utc_datetime(s)
    except Exception:
        return None

def _print_ingest_runs_report(
    *,
    db_path: str,
    start: str | None,
    end: str | None,
    source: str | None,
    top: int,
    show_spec_hash: bool,
) -> None:
    # Ensure tables exist.
    store = EventStore(db_path=db_path)

    import sqlite3

    start_dt = _parse_dt_or_none(start)
    end_dt = _parse_dt_or_none(end)
    start_ts = normalize_timestamp(start_dt) if start_dt else None
    end_ts = normalize_timestamp(end_dt) if end_dt else None

    where = []
    params: list[str] = []
    if source:
        where.append("source_id = ?")
        params.append(str(source))
    # Overlap semantics: include windows that overlap [start, end)
    if start_ts:
        where.append("end_ts > ?")
        params.append(str(start_ts))
    if end_ts:
        where.append("start_ts < ?")
        params.append(str(end_ts))

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        summary = conn.execute(
            f"""
            SELECT
              source_id,
              COUNT(*) as windows,
              SUM(CASE WHEN status = 'complete' THEN 1 ELSE 0 END) as complete_windows,
              SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running_windows,
              SUM(CASE WHEN status LIKE 'failed%' THEN 1 ELSE 0 END) as failed_windows,
              SUM(empty_count) as empty_windows,
              SUM(retry_count) as retries_total,
              SUM(fetched_count) as fetched_rows,
              SUM(emitted_count) as emitted_rows,
              SUM(CASE WHEN status = 'complete' AND fetched_count = 0 AND last_error IS NOT NULL THEN 1 ELSE 0 END) as skipped_windows
            FROM ingest_runs
            {where_sql}
            GROUP BY source_id
            ORDER BY windows DESC, source_id ASC
            """
            ,
            tuple(params),
        ).fetchall()

        if not summary:
            print("No ingest_runs rows found for the requested filters.")
            return

        print("Ingest Runs Summary")
        print("-------------------")
        if start_ts or end_ts:
            print(f"window_filter: start={start_ts or '(none)'} end={end_ts or '(none)'}")
        if source:
            print(f"source_filter: {source}")
        print(f"db: {db_path}")
        print()

        for r in summary:
            print(
                f"{r['source_id']}: windows={int(r['windows'] or 0)} "
                f"complete={int(r['complete_windows'] or 0)} running={int(r['running_windows'] or 0)} failed={int(r['failed_windows'] or 0)} "
                f"empty={int(r['empty_windows'] or 0)} retries={int(r['retries_total'] or 0)} skipped={int(r['skipped_windows'] or 0)} "
                f"fetched_rows={int(r['fetched_rows'] or 0)} emitted_rows={int(r['emitted_rows'] or 0)}"
            )
            if show_spec_hash:
                hashes = conn.execute(
                    """
                    SELECT DISTINCT spec_hash
                    FROM ingest_runs
                    WHERE source_id = ?
                    ORDER BY spec_hash ASC
                    """,
                    (str(r["source_id"]),),
                ).fetchall()
                uniq = [str(h[0]) for h in hashes if h and h[0]]
                if uniq:
                    print(f"  spec_hashes: {', '.join(uniq[:6])}{' ...' if len(uniq) > 6 else ''}")

        print()
        top_n = max(1, int(top))

        # Skip reasons (ok=1 and fetched_count=0 with last_error set)
        skip_rows = conn.execute(
            f"""
            SELECT source_id, COALESCE(last_error, 'unknown') as reason, COUNT(*) as c
            FROM ingest_runs
            {where_sql}
              {"AND" if where_sql else "WHERE"} status = 'complete' AND fetched_count = 0 AND last_error IS NOT NULL
            GROUP BY source_id, reason
            ORDER BY c DESC, source_id ASC, reason ASC
            """,
            tuple(params),
        ).fetchall()
        if skip_rows:
            print("Skip Reasons (top)")
            print("------------------")
            shown = 0
            for row in skip_rows:
                print(f"{row['source_id']}: {row['reason']} = {int(row['c'] or 0)}")
                shown += 1
                if shown >= top_n:
                    break
            print()

        # Fail reasons (ok=0)
        fail_rows = conn.execute(
            f"""
            SELECT source_id, COALESCE(last_error, 'unknown') as reason, COUNT(*) as c
            FROM ingest_runs
            {where_sql}
              {"AND" if where_sql else "WHERE"} status LIKE 'failed%'
            GROUP BY source_id, reason
            ORDER BY c DESC, source_id ASC, reason ASC
            """,
            tuple(params),
        ).fetchall()
        if fail_rows:
            print("Failure Reasons (top)")
            print("---------------------")
            shown = 0
            for row in fail_rows:
                print(f"{row['source_id']}: {row['reason']} = {int(row['c'] or 0)}")
                shown += 1
                if shown >= top_n:
                    break
            print()

        # Request cache hit stats + dropped-row reasons from ingest_run_stats (best-effort).
        stats_rows = conn.execute(
            f"""
            SELECT
              source_id,
              SUM(request_cache_hit) as cache_hits,
              COUNT(*) as stats_windows,
              SUM(CASE WHEN warnings_json LIKE '%provider_schema_changed%' THEN 1 ELSE 0 END) as schema_drift_windows,
              SUM(dropped_empty_text) as dropped_empty_text,
              SUM(dropped_bad_timestamp) as dropped_bad_timestamp,
              SUM(dropped_invalid_shape) as dropped_invalid_shape,
              SUM(dropped_out_of_bounds) as dropped_out_of_bounds,
              SUM(dropped_duplicate) as dropped_duplicate
            FROM ingest_run_stats
            {where_sql}
            GROUP BY source_id
            ORDER BY stats_windows DESC, source_id ASC
            """,
            tuple(params),
        ).fetchall()
        if stats_rows:
            print("Ingest Stats (cache + drops)")
            print("----------------------------")
            for r in stats_rows:
                hits = int(r["cache_hits"] or 0)
                windows = int(r["stats_windows"] or 0)
                pct = (100.0 * hits / windows) if windows > 0 else 0.0
                drift = int(r["schema_drift_windows"] or 0)
                print(
                    f"{r['source_id']}: request_cache_hits={hits}/{windows} ({pct:.1f}%) schema_drift_windows={drift} "
                    f"drops(empty_text={int(r['dropped_empty_text'] or 0)}, bad_timestamp={int(r['dropped_bad_timestamp'] or 0)}, "
                    f"invalid_shape={int(r['dropped_invalid_shape'] or 0)}, out_of_bounds={int(r['dropped_out_of_bounds'] or 0)}, "
                    f"duplicate={int(r['dropped_duplicate'] or 0)})"
                )
            print()

def _print_ingest_runs_detail(
    *,
    db_path: str,
    source: str,
    start: str | None,
    end: str | None,
) -> None:
    EventStore(db_path=db_path)  # ensure schema
    import sqlite3

    start_dt = _parse_dt_or_none(start)
    end_dt = _parse_dt_or_none(end)
    start_ts = normalize_timestamp(start_dt) if start_dt else None
    end_ts = normalize_timestamp(end_dt) if end_dt else None

    where = ["r.source_id = ?"]
    params: list[str] = [str(source)]
    if start_ts:
        where.append("r.end_ts > ?")
        params.append(str(start_ts))
    if end_ts:
        where.append("r.start_ts < ?")
        params.append(str(end_ts))
    where_sql = "WHERE " + " AND ".join(where)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT
              r.source_id, r.start_ts, r.end_ts, r.spec_hash, r.provider, r.status, r.ok, r.retry_count,
              r.fetched_count, r.emitted_count, r.empty_count, r.oldest_event_ts, r.newest_event_ts,
              r.last_error, r.started_at, r.completed_at, r.updated_at,
              s.request_cache_hit, s.response_fingerprint, s.fetch_time_s, s.total_time_s, s.warnings_json,
              s.raw_rows_count, s.normalized_count, s.valid_count, s.bounded_count,
              s.dropped_empty_text, s.dropped_bad_timestamp, s.dropped_invalid_shape, s.dropped_out_of_bounds, s.dropped_duplicate
            FROM ingest_runs r
            LEFT JOIN ingest_run_stats s
              ON s.source_id = r.source_id AND s.start_ts = r.start_ts AND s.end_ts = r.end_ts AND s.spec_hash = r.spec_hash
            {where_sql}
            ORDER BY r.start_ts ASC
            """,
            tuple(params),
        ).fetchall()

        if not rows:
            print("No ingest_runs rows found for the requested filters.")
            return

        print(f"Ingest Runs Detail: {source}")
        print("---------------------------")
        if start_ts or end_ts:
            print(f"window_filter: start={start_ts or '(none)'} end={end_ts or '(none)'}")
        print(f"db: {db_path}")
        print()

        for r in rows:
            print(
                f"{r['start_ts']}..{r['end_ts']} status={r['status']} ok={int(r['ok'] or 0)} "
                f"retry={int(r['retry_count'] or 0)} fetched={int(r['fetched_count'] or 0)} emitted={int(r['emitted_count'] or 0)} "
                f"empty={int(r['empty_count'] or 0)} provider={r['provider']} spec_hash={r['spec_hash']}"
            )
            if r["last_error"]:
                print(f"  last_error: {r['last_error']}")
            print(
                f"  started_at={r['started_at']} completed_at={r['completed_at'] or '(none)'} updated_at={r['updated_at']}"
            )
            if r["oldest_event_ts"] or r["newest_event_ts"]:
                print(f"  event_ts: oldest={r['oldest_event_ts'] or '(none)'} newest={r['newest_event_ts'] or '(none)'}")
            if r["response_fingerprint"] or r["request_cache_hit"] is not None:
                print(
                    f"  request_cache_hit={int(r['request_cache_hit'] or 0)} response_fingerprint={r['response_fingerprint'] or '(none)'}"
                )
            if r["fetch_time_s"] is not None or r["total_time_s"] is not None:
                print(f"  timing: fetch_s={r['fetch_time_s'] if r['fetch_time_s'] is not None else '(none)'} total_s={r['total_time_s'] if r['total_time_s'] is not None else '(none)'}")
            if r["warnings_json"] and r["warnings_json"] != "[]":
                print(f"  warnings: {r['warnings_json']}")
            if r["raw_rows_count"] is not None:
                print(
                    f"  rows(raw={int(r['raw_rows_count'] or 0)}, normalized={int(r['normalized_count'] or 0)}, valid={int(r['valid_count'] or 0)}, bounded={int(r['bounded_count'] or 0)}) "
                    f"drops(empty_text={int(r['dropped_empty_text'] or 0)}, bad_timestamp={int(r['dropped_bad_timestamp'] or 0)}, "
                    f"invalid_shape={int(r['dropped_invalid_shape'] or 0)}, out_of_bounds={int(r['dropped_out_of_bounds'] or 0)}, duplicate={int(r['dropped_duplicate'] or 0)})"
                )
            print()

def _cleanup_stale_running(*, db_path: str, dry_run: bool) -> None:
    EventStore(db_path=db_path)  # ensure schema
    import sqlite3
    from datetime import datetime, timezone, timedelta

    # TTL config per provider (same env pattern as backfill_runner).
    def ttl_for(provider: str) -> int:
        base = int(float((__import__("os").getenv("INGEST_RUNNING_TTL_S", "1800") or "1800")))
        key = f"INGEST_RUNNING_TTL_S_{str(provider).strip().upper()}"
        try:
            override = __import__("os").getenv(key)
            if override is None or str(override).strip() == "":
                return base
            return int(float(str(override).strip()))
        except Exception:
            return base

    now = datetime.now(timezone.utc)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        running = conn.execute(
            """
            SELECT source_id, start_ts, end_ts, spec_hash, provider, updated_at
            FROM ingest_runs
            WHERE status = 'running'
            """,
        ).fetchall()

        stale: list[sqlite3.Row] = []
        for r in running:
            provider = str(r["provider"] or "")
            updated_at = str(r["updated_at"] or "")
            try:
                updated_dt = to_utc_datetime(updated_at)
            except Exception:
                updated_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            ttl = ttl_for(provider)
            if updated_dt < (now - timedelta(seconds=ttl)):
                stale.append(r)

        if not stale:
            print("No stale running windows found.")
            return

        print(f"Stale running windows: {len(stale)}")
        for r in stale[:50]:
            print(f"{r['provider']} {r['source_id']} {r['start_ts']}..{r['end_ts']} spec_hash={r['spec_hash']} updated_at={r['updated_at']}")
        if len(stale) > 50:
            print(f"... ({len(stale) - 50} more)")

        if dry_run:
            print("Dry-run: no DB updates applied.")
            return

        now_iso = now.isoformat()
        for r in stale:
            conn.execute(
                """
                UPDATE ingest_runs
                SET status = 'failed', ok = 0, last_error = 'stale_running_expired', completed_at = ?, updated_at = ?
                WHERE source_id = ? AND start_ts = ? AND end_ts = ? AND spec_hash = ? AND status = 'running'
                """,
                (
                    now_iso,
                    now_iso,
                    str(r["source_id"]),
                    str(r["start_ts"]),
                    str(r["end_ts"]),
                    str(r["spec_hash"]),
                ),
            )
        print(f"Updated {len(stale)} windows to failed.")


def _percent(n: int, d: int) -> float:
    return (100.0 * float(n) / float(d)) if d > 0 else 0.0


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    idx = int((len(xs) - 1) * 0.95)
    return float(xs[idx])


def _print_ingest_health(*, db_path: str, start: str, end: str, batch_size_days: int) -> None:
    EventStore(db_path=db_path)  # ensure schema
    import sqlite3
    from datetime import datetime, timezone, timedelta

    start_dt = _parse_dt_or_none(start)
    end_dt = _parse_dt_or_none(end)
    if start_dt is None or end_dt is None:
        print("ingest-health requires --start and --end (YYYY-MM-DD or ISO)")
        return

    start_dt = to_utc_datetime(start_dt).replace(microsecond=0)
    end_dt = to_utc_datetime(end_dt).replace(microsecond=0)
    if start_dt >= end_dt:
        print("Invalid range: start must be < end")
        return

    step = max(1, int(batch_size_days))
    expected_windows = 0
    cur = start_dt
    while cur < end_dt:
        cur = min(cur + timedelta(days=step), end_dt)
        expected_windows += 1

    store = EventStore(db_path=db_path)
    specs = [s for s in validate_sources_yaml() if getattr(s, "enabled", True)]
    if not specs:
        print("No enabled sources in config/sources.yaml")
        return

    spec_hashes = {str(s.id): _spec_hash_for_cli(store, s) for s in specs}

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        print("Ingestion Health")
        print("----------------")
        print(f"db: {db_path}")
        print(f"range: {normalize_timestamp(start_dt)} -> {normalize_timestamp(end_dt)} (step_days={step})")
        print()

        for s in sorted(specs, key=lambda x: str(x.id)):
            sid = str(s.id)
            spec_hash = spec_hashes[sid]
            backfilled_until = store.get_backfilled_until(source_id=sid, spec_hash=spec_hash)

            # Coverage: count windows fully contained in range with matching spec_hash.
            complete = conn.execute(
                """
                SELECT COUNT(*) as c
                FROM ingest_runs
                WHERE source_id = ? AND spec_hash = ?
                  AND start_ts >= ? AND end_ts <= ?
                  AND status = 'complete'
                """,
                (sid, spec_hash, normalize_timestamp(start_dt), normalize_timestamp(end_dt)),
            ).fetchone()["c"]

            running = conn.execute(
                """
                SELECT COUNT(*) as c
                FROM ingest_runs
                WHERE source_id = ? AND spec_hash = ?
                  AND start_ts >= ? AND end_ts <= ?
                  AND status = 'running'
                """,
                (sid, spec_hash, normalize_timestamp(start_dt), normalize_timestamp(end_dt)),
            ).fetchone()["c"]

            failed = conn.execute(
                """
                SELECT COUNT(*) as c
                FROM ingest_runs
                WHERE source_id = ? AND spec_hash = ?
                  AND start_ts >= ? AND end_ts <= ?
                  AND status LIKE 'failed%'
                """,
                (sid, spec_hash, normalize_timestamp(start_dt), normalize_timestamp(end_dt)),
            ).fetchone()["c"]

            cov = _percent(int(complete), int(expected_windows))

            last_ok = conn.execute(
                """
                SELECT MAX(completed_at) as t
                FROM ingest_runs
                WHERE source_id = ? AND spec_hash = ? AND status = 'complete' AND ok = 1
                """,
                (sid, spec_hash),
            ).fetchone()["t"]
            freshness = ""
            if last_ok:
                try:
                    dt = to_utc_datetime(str(last_ok))
                    age_s = (datetime.now(timezone.utc) - dt).total_seconds()
                    if age_s < 120:
                        freshness = f"last ok {int(age_s)}s ago"
                    elif age_s < 7200:
                        freshness = f"last ok {int(age_s/60)}m ago"
                    else:
                        freshness = f"last ok {int(age_s/3600)}h ago"
                except Exception:
                    freshness = "last ok (unknown)"
            else:
                freshness = "last ok (none)"

            # Schema drift warnings + latency p95 for this range.
            stats = conn.execute(
                """
                SELECT
                  SUM(CASE WHEN warnings_json LIKE '%provider_schema_changed%' THEN 1 ELSE 0 END) as drift,
                  AVG(fetch_time_s) as avg_fetch_s,
                  AVG(total_time_s) as avg_total_s
                FROM ingest_run_stats
                WHERE source_id = ? AND spec_hash = ? AND start_ts >= ? AND end_ts <= ?
                """,
                (sid, spec_hash, normalize_timestamp(start_dt), normalize_timestamp(end_dt)),
            ).fetchone()
            drift = int(stats["drift"] or 0)

            fetch_times = conn.execute(
                """
                SELECT fetch_time_s
                FROM ingest_run_stats
                WHERE source_id = ? AND spec_hash = ? AND start_ts >= ? AND end_ts <= ? AND fetch_time_s IS NOT NULL
                """,
                (sid, spec_hash, normalize_timestamp(start_dt), normalize_timestamp(end_dt)),
            ).fetchall()
            p95_fetch = _p95([float(r["fetch_time_s"]) for r in fetch_times if r["fetch_time_s"] is not None])

            warn_txt = " schema_drift" if drift > 0 else ""
            p95_txt = f" p95_fetch={p95_fetch:.2f}s" if p95_fetch is not None else ""
            horizon_txt = f" backfilled_until={backfilled_until}" if backfilled_until else ""
            print(f"{sid:20s} {cov:5.1f}% coverage  running={int(running)} failed={int(failed)}  {freshness}{warn_txt}{p95_txt}{horizon_txt}")

def _spec_hash_for_cli(store: EventStore, spec) -> str:
    try:
        payload = spec.model_dump()
    except Exception:
        payload = dict(getattr(spec, "__dict__", {}) or {})
        payload.setdefault("id", str(getattr(spec, "id", "unknown")))
    for k in ("enabled", "poll", "backfill_days", "priority"):
        payload.pop(k, None)
    return store.stable_spec_hash(payload)

def _print_backfill_coverage_check(
    *,
    db_path: str,
    start_dt: datetime,
    end_dt: datetime,
    batch_size_days: int,
) -> None:
    store = EventStore(db_path=db_path)
    specs = [s for s in validate_sources_yaml() if getattr(s, "enabled", True)]

    start_dt = to_utc_datetime(start_dt).replace(microsecond=0)
    end_dt = to_utc_datetime(end_dt).replace(microsecond=0)
    if start_dt >= end_dt:
        print("Invalid range: start must be < end")
        return

    # Precompute spec hashes.
    spec_hashes = {str(s.id): _spec_hash_for_cli(store, s) for s in specs}

    # Counts: source_id -> {never_fetched, complete, running, failed}
    counts: dict[str, dict[str, int]] = {str(s.id): {"never_fetched": 0, "complete": 0, "running": 0, "failed": 0} for s in specs}
    totals = {"never_fetched": 0, "complete": 0, "running": 0, "failed": 0}

    cur = start_dt
    step = max(1, int(batch_size_days))
    from datetime import timedelta
    while cur < end_dt:
        nxt = min(cur + timedelta(days=step), end_dt)

        start_ts = normalize_timestamp(cur)
        end_ts = normalize_timestamp(nxt)

        for s in specs:
            sid = str(s.id)
            spec_hash = spec_hashes[sid]
            status = store.get_ingest_window_status(source_id=sid, start_ts=start_ts, end_ts=end_ts, spec_hash=spec_hash)
            if status == "complete":
                counts[sid]["complete"] += 1
                totals["complete"] += 1
            elif status == "running":
                counts[sid]["running"] += 1
                totals["running"] += 1
            elif status == "failed":
                counts[sid]["failed"] += 1
                totals["failed"] += 1
            else:
                counts[sid]["never_fetched"] += 1
                totals["never_fetched"] += 1

        cur = nxt

    print("Backfill Coverage Check (no network)")
    print("-----------------------------------")
    print(f"db: {db_path}")
    print(f"range: {normalize_timestamp(start_dt)} -> {normalize_timestamp(end_dt)} (step_days={step})")
    print(
        f"totals: never_fetched={totals['never_fetched']} complete={totals['complete']} running={totals['running']} failed={totals['failed']}"
    )
    print()

    for sid in sorted(counts.keys()):
        c = counts[sid]
        print(f"{sid}: never_fetched={c['never_fetched']} complete={c['complete']} running={c['running']} failed={c['failed']}")

async def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "run":
        runner = BackfillRunner(db_path=str(args.db))
        await runner.run_backfill(days=args.days)
        return 0
    elif args.command == "backfill-range":
        start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        if bool(args.check_only):
            _print_backfill_coverage_check(
                db_path=str(args.db),
                start_dt=start_dt,
                end_dt=end_dt,
                batch_size_days=int(args.batch_size_days),
            )
            return 0
        runner = BackfillRunner(db_path=str(args.db))
        await runner.backfill_range(
            start_time=start_dt,
            end_time=end_dt,
            batch_size_days=int(args.batch_size_days),
            replay=args.replay,
            force_replay=bool(args.force_replay),
            skip_completed=args.skip_completed,
            fail_fast=args.fail_fast,
            max_zero_insert_slices=args.max_zero_insert_slices,
            force_refetch_source=(str(args.force_refetch_source) if args.force_refetch_source else None),
        )
        return 0
    elif args.command == "list-target-stocks":
        asof = args.asof
        reg = get_target_stocks_registry()
        specs = load_target_stock_specs()
        active = set(get_target_stocks(asof=asof))
        print("Target Stocks")
        print("-------------")
        for r in sorted(specs, key=lambda s: s.symbol):
            flags = ["enabled" if r.enabled else "disabled"]
            if r.symbol in active:
                flags.append("active")
            elif r.enabled and asof:
                flags.append("inactive")
            if r.group:
                flags.append(f"group={r.group}")
            if r.active_from:
                flags.append(f"active_from={r.active_from.isoformat()}")
            print(f"{r.symbol}  ({', '.join(flags)})")
        print()
        print(f"target_universe_version: {reg.target_universe_version}")
        return 0
    elif args.command == "add-target-stock":
        v = add_target_stock(
            args.symbol,
            group=args.group,
            active_from=args.active_from,
            enabled=(not bool(args.disabled)),
        )
        print(f"Updated Target Stocks. target_universe_version={v}")
        return 0
    elif args.command == "remove-target-stock":
        v = remove_target_stock(args.symbol)
        print(f"Updated Target Stocks. target_universe_version={v}")
        return 0
    elif args.command == "enable-target-stock":
        v = set_target_stock_enabled(args.symbol, enabled=True)
        print(f"Updated Target Stocks. target_universe_version={v}")
        return 0
    elif args.command == "disable-target-stock":
        v = set_target_stock_enabled(args.symbol, enabled=False)
        print(f"Updated Target Stocks. target_universe_version={v}")
        return 0
    elif args.command == "ingest-runs":
        _print_ingest_runs_report(
            db_path=str(args.db),
            start=args.start,
            end=args.end,
            source=args.source,
            top=int(args.top),
            show_spec_hash=bool(args.show_spec_hash),
        )
        return 0
    elif args.command == "ingest-runs-detail":
        _print_ingest_runs_detail(
            db_path=str(args.db),
            source=str(args.source),
            start=args.start,
            end=args.end,
        )
        return 0
    elif args.command == "ingest-runs-cleanup":
        _cleanup_stale_running(db_path=str(args.db), dry_run=bool(args.dry_run))
        return 0
    elif args.command == "ingest-health":
        _print_ingest_health(
            db_path=str(args.db),
            start=str(args.start),
            end=str(args.end),
            batch_size_days=int(args.batch_size_days),
        )
        return 0
    return 1

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
