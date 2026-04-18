# Start the internal read API (FastAPI) for trading-platform — local Windows.
# Daily ingestion/backfill stays on your scheduler; this process only serves read HTTP.
#
# Prereq: .env at repo root (ALPHA_DB_PATH, INTERNAL_READ_KEY, etc.) — loaded by Python.
# Usage: .\scripts\start_internal_read_api.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$Py = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Py)) {
    Write-Error "Python venv not found at $Py — create .venv and pip install -r requirements.txt"
}

& $Py -m app.internal_read_v1
