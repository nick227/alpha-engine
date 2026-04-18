#!/usr/bin/env bash
# Start the internal read API (FastAPI) for trading-platform — local Linux/macOS and Railway.
# Daily ingestion/backfill stays on your scheduler; this process only serves read HTTP.
#
# Prereq: .env at repo root (ALPHA_DB_PATH, INTERNAL_READ_KEY, etc.) — loaded by Python.
# Railway: set INTERNAL_READ_HOST=0.0.0.0; PORT is provided automatically.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ -x "$ROOT/.venv/bin/python" ]]; then
  exec "$ROOT/.venv/bin/python" -m app.internal_read_v1
fi
exec python3 -m app.internal_read_v1
