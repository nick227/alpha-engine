import asyncio
import argparse
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
from app.ingest.backfill_runner import BackfillRunner
from app.core.target_stocks import (
    add_target_stock,
    get_target_stocks_registry,
    get_target_stocks,
    load_target_stock_specs,
    remove_target_stock,
    set_target_stock_enabled,
)

load_dotenv()

async def main():
    parser = argparse.ArgumentParser(description="Alpha Engine Backfill CLI")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Command: run (default 90 days)
    run_parser = subparsers.add_parser("run", help="Run full backfill")
    run_parser.add_argument("--days", type=int, default=90, help="Number of days to backfill")

    # Command: backfill-range
    range_parser = subparsers.add_parser("backfill-range", help="Backfill a specific range")
    range_parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    range_parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    range_parser.add_argument(
        "--replay",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Replay after fetch (use --no-replay to skip)",
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
    lts = subparsers.add_parser("list-target-stocks", help="List the canonical Target Stocks universe")
    lts.add_argument("--asof", default=None, help="Optional as-of timestamp/date (YYYY-MM-DD or ISO)")

    # Command: add-target-stock
    ats = subparsers.add_parser("add-target-stock", help="Add or update a Target Stock")
    ats.add_argument("symbol", help="Ticker symbol (e.g. NVDA)")
    ats.add_argument("--group", default=None, help="Optional group label")
    ats.add_argument("--active-from", default=None, help="Optional active_from (YYYY-MM-DD or ISO)")
    ats.add_argument("--disabled", action="store_true", help="Add as disabled")

    # Command: remove-target-stock
    rts = subparsers.add_parser("remove-target-stock", help="Remove a Target Stock")
    rts.add_argument("symbol", help="Ticker symbol to remove")

    # Command: enable-target-stock
    ets = subparsers.add_parser("enable-target-stock", help="Enable a Target Stock")
    ets.add_argument("symbol", help="Ticker symbol to enable")

    # Command: disable-target-stock
    dts = subparsers.add_parser("disable-target-stock", help="Disable a Target Stock")
    dts.add_argument("symbol", help="Ticker symbol to disable")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "run":
        runner = BackfillRunner()
        await runner.run_backfill(days=args.days)
    elif args.command == "backfill-range":
        runner = BackfillRunner()
        start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        await runner.backfill_range(
            start_time=start_dt,
            end_time=end_dt,
            replay=args.replay,
            fail_fast=args.fail_fast,
            max_zero_insert_slices=args.max_zero_insert_slices,
        )
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
    elif args.command == "add-target-stock":
        v = add_target_stock(
            args.symbol,
            group=args.group,
            active_from=args.active_from,
            enabled=(not bool(args.disabled)),
        )
        print(f"Updated Target Stocks. target_universe_version={v}")
    elif args.command == "remove-target-stock":
        v = remove_target_stock(args.symbol)
        print(f"Updated Target Stocks. target_universe_version={v}")
    elif args.command == "enable-target-stock":
        v = set_target_stock_enabled(args.symbol, enabled=True)
        print(f"Updated Target Stocks. target_universe_version={v}")
    elif args.command == "disable-target-stock":
        v = set_target_stock_enabled(args.symbol, enabled=False)
        print(f"Updated Target Stocks. target_universe_version={v}")

if __name__ == "__main__":
    asyncio.run(main())
