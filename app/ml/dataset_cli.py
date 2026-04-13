from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from app.ml.dataset import build_dataset


def _parse_date(s: str) -> date:
    return date.fromisoformat(str(s).strip())


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="ML dataset utilities (build ml_learning_rows)",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m app.ml.dataset_cli build --symbols AAPL --horizon 7d --start 2024-01-01 --end 2026-01-01 --tenant-id backfill\n"
            "  python -m app.ml.dataset_cli build --symbols AAPL,MSFT --horizon 7d --start 2025-01-01 --end 2026-01-01 --min-coverage 0.8\n"
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("build", help="Build ml_learning_rows for symbols/date range")
    b.add_argument("--symbols", required=True, help="Comma-separated symbols (e.g. AAPL,MSFT)")
    b.add_argument("--horizon", default="7d", help="Horizon string (default: 7d)")
    b.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    b.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    b.add_argument("--db", default="data/alpha.db", help="SQLite DB path (default: data/alpha.db)")
    b.add_argument("--tenant-id", default="backfill", help="Tenant id (default: backfill)")
    b.add_argument("--min-coverage", type=float, default=0.8, help="Min feature coverage ratio (default: 0.8)")
    b.add_argument("--dumps-root", default="data/raw_dumps", help="Dump root (default: data/raw_dumps)")
    b.add_argument("--factors-path", default="config/factors.yaml", help="Factor config (default: config/factors.yaml)")
    b.add_argument("--split", default="train", help="Split label (default: train)")
    b.add_argument("--force", action="store_true", help="Delete and rebuild rows for the requested scope")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "build":
        symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
        horizon = str(args.horizon).strip()
        start = _parse_date(args.start)
        end = _parse_date(args.end)

        inserted = build_dataset(
            symbols=symbols,
            date_range=(start, end),
            horizons=[horizon],
            db_path=Path(str(args.db)),
            dumps_root=Path(str(args.dumps_root)),
            min_feature_coverage=float(args.min_coverage),
            tenant_id=str(args.tenant_id),
            split=str(args.split),
            factors_path=str(args.factors_path),
            force_rebuild=bool(args.force),
        )
        print(f"Inserted rows: {inserted}")
        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
