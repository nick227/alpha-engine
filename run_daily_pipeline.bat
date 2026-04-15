@echo off
setlocal

cd /d C:\wamp64\www\alpha-engine-poc
set PYTHONPATH=.
set LOG=logs\daily_pipeline.log
set LOCK=pipeline.lock

if not exist logs mkdir logs

:: ----------------------------------------------------------------
:: Run lock — prevent overlap if scheduler fires while still running
:: ----------------------------------------------------------------
if exist %LOCK% (
    echo [%DATE% %TIME%] Pipeline already running ^(lock exists^). Aborting. >> %LOG%
    exit /b 99
)
echo lock > %LOCK%

:: ----------------------------------------------------------------
:: Header
:: ----------------------------------------------------------------
echo. >> %LOG%
echo ============================================================ >> %LOG%
echo [%DATE% %TIME%] Daily pipeline started >> %LOG%
echo ============================================================ >> %LOG%

:: ----------------------------------------------------------------
:: STEP 1 — Download prices
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 1 START: Downloading prices >> %LOG%
python dev_scripts\scripts\download_prices_daily.py >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 1 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 1 END: Download complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 2 — Run discovery + queue predictions
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 2 START: Discovery pipeline >> %LOG%
python dev_scripts\scripts\nightly_discovery_pipeline.py >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 2 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 2 END: Discovery complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 3 — Create today's predictions from queue
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 3 START: Creating predictions >> %LOG%
python run_paper_trading.py >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 3 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 3 END: Predictions created >> %LOG%

:: ----------------------------------------------------------------
:: STEP 4 — Replay: score predictions whose horizon has expired
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 4 START: Replaying expired predictions >> %LOG%
python run_paper_trading.py --replay >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 4 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 4 END: Replay complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 5 — Backfill any outcomes still NULL now that prices exist
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 5 START: Backfilling outcomes >> %LOG%
python dev_scripts\scripts\auto_backfill_outcomes.py >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 5 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 5 END: Backfill complete >> %LOG%

:: ----------------------------------------------------------------
:: Success
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] Pipeline finished OK >> %LOG%
echo ============================================================ >> %LOG%
del %LOCK%
exit /b 0

:abort
echo [%DATE% %TIME%] Pipeline ABORTED >> %LOG%
echo ============================================================ >> %LOG%
del %LOCK%
exit /b 1
