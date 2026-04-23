@echo off
setlocal

REM Force working directory - critical for SYSTEM account execution
set "ROOT=%~dp0..\.."
cd /d "%ROOT%"
set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
set PYTHONPATH=.
set LOG=logs\daily_pipeline_%DATE:~-4,4%-%DATE:~-10,2%-%DATE:~-7,2%.log
set LOCK=pipeline.lock
set "RANKING_LOOKBACK_DAYS=%ALPHA_RANKING_SNAPSHOT_LOOKBACK_DAYS%"
if "%RANKING_LOOKBACK_DAYS%"=="" set "RANKING_LOOKBACK_DAYS=7"
set "RANKING_MAX_TICKERS=%ALPHA_RANKING_SNAPSHOT_MAX_TICKERS%"
if "%RANKING_MAX_TICKERS%"=="" set "RANKING_MAX_TICKERS=50"

for /f "delims=" %%I in ('%PYTHON% -c "from datetime import date; print(date.today().isoformat())"') do set "ASOF=%%I"

if not exist logs mkdir logs
if not exist reports mkdir reports

:: ----------------------------------------------------------------
:: DB path sanity check — fail fast before acquiring lock
:: ----------------------------------------------------------------
set "DB_FILE=data\alpha.db"
if not "%ALPHA_DB_PATH%"=="" set "DB_FILE=%ALPHA_DB_PATH%"
if not exist "%DB_FILE%" (
    echo [%DATE% %TIME%] ERROR: DB not found at %DB_FILE% ^(check ALPHA_DB_PATH or run DB init^) >> %LOG%
    echo FAILED step=0_db_check at=%DATE%_%TIME% > reports\pipeline-last-status.txt
    exit /b 2
)
echo [%DATE% %TIME%] DB: %DB_FILE% >> %LOG%

:: ----------------------------------------------------------------
:: Run lock — prevent overlap if scheduler fires while still running
:: ----------------------------------------------------------------
set "FAILED_STEP=unknown"
if exist %LOCK% (
    set /p LOCK_TS=<%LOCK%
    echo [%DATE% %TIME%] Lock exists ^(created: %LOCK_TS%^) ^(if pipeline is not running, delete %LOCK% manually^) >> %LOG%
    echo FAILED step=lock_collision at=%DATE%_%TIME% lock_created=%LOCK_TS% > reports\pipeline-last-status.txt
    exit /b 99
)
echo %DATE%_%TIME% > %LOCK%

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
    set FAILED_STEP=1_download_prices
    echo [%DATE% %TIME%] STEP 1 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 1 END: Download complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 2 — Discovery CLI nightly: candidates, watchlist, queue, outcomes, stats + threshold supplement
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 2 START: discovery_cli nightly >> %LOG%
%PYTHON% -m app.discovery.discovery_cli nightly --db data\alpha.db --tenant-id default --admission-max 40 --admission-per-lens 6 --admission-max-overrule-swaps 5 >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    set FAILED_STEP=2_discovery_nightly
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
    set FAILED_STEP=3_queue_rank_trim
    echo [%DATE% %TIME%] STEP 3 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 3 END: Queue ranked and trimmed >> %LOG%

:: ----------------------------------------------------------------
:: STEP 4 — Build predicted series from queue (engine path)
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 4 START: prediction_cli run-queue >> %LOG%
%PYTHON% -m app.engine.prediction_cli run-queue --as-of %ASOF% --db data\alpha.db --tenant-id default --limit 400 --forecast-days 30 --ingress-days 30 --freshness-hours 20 >> %LOG% 2>&1
if %ERRORLEVEL% equ 0 goto step4_ok
if %ERRORLEVEL% equ 3 goto step4_ok
set FAILED_STEP=4_prediction_cli
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
    set FAILED_STEP=5_materialize_predictions
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
    set FAILED_STEP=6_prediction_rank_sqlite
    echo [%DATE% %TIME%] STEP 6 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 6 END: Predictions ranked >> %LOG%

:: ----------------------------------------------------------------
:: STEP 7 — Persist ranking_snapshots from ranked predictions (movers / top-N read API)
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 7 START: ranking_snapshots from predictions >> %LOG%
%PYTHON% -m app.engine.ranking_snapshots_from_predictions --as-of %ASOF% --db data\alpha.db --tenant-id default --lookback-days %RANKING_LOOKBACK_DAYS% --max-tickers %RANKING_MAX_TICKERS% >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    set FAILED_STEP=7_ranking_snapshots
    echo [%DATE% %TIME%] STEP 7 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 7 END: Ranking snapshot written >> %LOG%

:: ----------------------------------------------------------------
:: STEP 8 — Replay: score predictions whose horizon has expired
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 8 START: Replaying expired predictions >> %LOG%
%PYTHON% run_paper_trading.py --replay >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    set FAILED_STEP=8_replay
    echo [%DATE% %TIME%] STEP 8 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 8 END: Replay complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 9 — Backfill any outcomes still NULL now that prices exist
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 9 START: Backfilling outcomes >> %LOG%
%PYTHON% dev_scripts\scripts\auto_backfill_outcomes.py >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    set FAILED_STEP=9_backfill_outcomes
    echo [%DATE% %TIME%] STEP 9 FAILED >> %LOG%
    goto :abort
)
echo [%DATE% %TIME%] STEP 9 END: Backfill complete >> %LOG%

:: ----------------------------------------------------------------
:: STEP 10 — Real vs sim learning snapshot (log line; does not fail pipeline)
:: ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 10 START: Learning feedback report >> %LOG%
%PYTHON% -m app.analytics.learning_feedback_report --db data\alpha.db --tenant-id default >> %LOG% 2>&1
echo [%DATE% %TIME%] STEP 10 END: Learning feedback report >> %LOG%

:: ----------------------------------------------------------------
:: Success
:: ----------------------------------------------------------------
REM ----------------------------------------------------------------
REM STEP 11 Production data-health snapshot (ops truth; non-blocking)
REM ----------------------------------------------------------------
echo [%DATE% %TIME%] STEP 11 START: data-health prod snapshot >> %LOG%
%PYTHON% dev_scripts\scripts\generate_data_health_prod_report.py --db data\alpha.db --output reports\data-health-prod.txt >> %LOG% 2>&1
if %ERRORLEVEL% neq 0 (
    echo [%DATE% %TIME%] STEP 11 WARNING: data-health prod snapshot failed ^(pipeline continues^) >> %LOG%
) else (
    echo [%DATE% %TIME%] STEP 11 END: data-health prod snapshot written >> %LOG%
)

echo [%DATE% %TIME%] Pipeline finished OK >> %LOG%
echo ============================================================ >> %LOG%
echo OK finished_at=%DATE%_%TIME% > reports\pipeline-last-status.txt
del %LOCK%
exit /b 0

:abort
echo [%DATE% %TIME%] Pipeline ABORTED >> %LOG%
echo ============================================================ >> %LOG%
echo FAILED step=%FAILED_STEP% at=%DATE%_%TIME% > reports\pipeline-last-status.txt
del %LOCK%
exit /b 1
