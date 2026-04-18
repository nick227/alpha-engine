@echo off
setlocal EnableExtensions

REM Periodic discovery milestone (deep soak). Separate from daily pipeline; uses milestone.lock.
set "ROOT=%~dp0..\.."
cd /d "%ROOT%"
set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
set PYTHONPATH=.
set LOCK=milestone.lock
set "PERIODIC_LOG=logs\periodic_runs.log"

if not exist logs mkdir logs

for /f "delims=" %%I in ('"%PYTHON%" -c "from datetime import date; print(date.today().isoformat())"') do set "RUN_DATE=%%I"
set "RUNLOG=logs\discovery_milestone_%RUN_DATE%.log"

if "%ALPHA_DB_PATH%"=="" (set "DB=data\alpha.db") else (set "DB=%ALPHA_DB_PATH%")

if exist %LOCK% (
    echo [%DATE% %TIME%] SKIP: %LOCK% exists >> "%PERIODIC_LOG%"
    exit /b 99
)
echo lock > %LOCK%

echo. >> "%RUNLOG%"
echo ============================================================ >> "%RUNLOG%"
echo [%DATE% %TIME%] discovery_milestone START db=%DB% >> "%RUNLOG%"
echo %RUN_DATE% discovery_milestone START >> "%PERIODIC_LOG%"

"%PYTHON%" dev_scripts\scripts\run_discovery_milestone.py --db "%DB%" --tenant-id default >> "%RUNLOG%" 2>&1
set ERR=%ERRORLEVEL%

echo %RUN_DATE% discovery_milestone END exit=%ERR% >> "%PERIODIC_LOG%"
echo [%DATE% %TIME%] discovery_milestone END exit=%ERR% >> "%RUNLOG%"
echo ============================================================ >> "%RUNLOG%"

del %LOCK%
exit /b %ERR%
