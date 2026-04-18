@echo off
setlocal

REM Force working directory - critical for SYSTEM account execution
set "ROOT=%~dp0..\.."
cd /d "%ROOT%"
set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
set PYTHONPATH=.
set LOG=logs\daily_pipeline_%DATE:~-4,4%-%DATE:~-10,2%-%DATE:~-7,2%.log
set LOCK=pipeline.lock

for /f "delims=" %%I in ('"%PYTHON%" -c "from datetime import date; print(date.today().isoformat())"') do set "ASOF=%%I"

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
echo [%DATE% %TIME%] PYTHON=%PYTHON% >> %LOG%
%PYTHON% -m pip list >> %LOG%
echo ============================================================ >> %LOG%

:: ----------------------------------------------------------------
:: STEP 1 — Download prices
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 1 START: Downloading prices >> %LOG%
%PYTHON% dev_scripts\scripts\download_prices_daily.py >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 1 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 1 END: Download complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 2 — Discovery CLI nightly: candidates, watchlist, queue, outcomes, stats + threshold supplement
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 2 START: discovery_cli nightly >> %LOG%
%PYTHON% -m app.discovery.discovery_cli nightly --db data\alpha.db --tenant-id default >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 2 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 2 END: Discovery nightly complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 3 — Global rank score + trim pending queue (competition across strategies)
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 3 START: queue_rank_trim >> %LOG%
%PYTHON% -m app.engine.queue_rank_trim --as-of %ASOF% --db data\alpha.db --tenant-id default >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 3 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 3 END: Queue ranked and trimmed >> %LOG%

:: ----------------------------------------------------------------
:: STEP 4 — Build predicted series from queue (engine path)
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 4 START: prediction_cli run-queue >> %LOG%
%PYTHON% -m app.engine.prediction_cli run-queue --as-of %ASOF% --db data\alpha.db --tenant-id default --limit 400 --forecast-days 30 --ingress-days 30 >> %LOG% 2>&1
if %ERRORLEVEL% equ 0 goto step4_ok
if %ERRORLEVEL% equ 3 goto step4_ok
echo [%DATE% %TIME%] STEP 4 FAILED >> %LOG%
goto :abort
:step4_ok
echo [%DATE% %TIME%] STEP 4 END: Predicted series built >> %LOG%

:: ----------------------------------------------------------------
:: STEP 5 — Materialize predictions rows for replay / reporting (queue is processed after step 4)
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 5 START: Materialize discovery predictions >> %LOG%
%PYTHON% run_paper_trading.py --materialize-discovery-predictions --materialize-date %ASOF% >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 5 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 5 END: Predictions materialized >> %LOG%

:: ----------------------------------------------------------------
:: STEP 6 — Rank predictions (rank_score + optional global top-N / per-strategy cap)
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 6 START: prediction_rank_sqlite >> %LOG%
%PYTHON% -m app.engine.prediction_rank_sqlite --as-of %ASOF% --db data\alpha.db --tenant-id default >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 6 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 6 END: Predictions ranked >> %LOG%

:: ----------------------------------------------------------------
:: STEP 7 — Replay: score predictions whose horizon has expired
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 7 START: Replaying expired predictions >> %LOG%
%PYTHON% run_paper_trading.py --replay >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 7 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 7 END: Replay complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 8 — Backfill any outcomes still NULL now that prices exist
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 8 START: Backfilling outcomes >> %LOG%
%PYTHON% dev_scripts\scripts\auto_backfill_outcomes.py >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 8 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 8 END: Backfill complete >> %LOG%

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
