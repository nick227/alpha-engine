"""
CLI entry point for safe backfill testing.

Usage:
    python -m app.testing --phase dry_run
    python -m app.testing --phase trace_test
    python -m app.testing --phase config
"""

from app.testing.safe_backfill import main

if __name__ == '__main__':
    main()
