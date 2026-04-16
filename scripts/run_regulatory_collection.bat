@echo off
echo Starting SEC Regulatory Data Collection
echo.

REM Check if API key is set
if "%SEC_API_KEY%"=="" (
    echo ERROR: SEC_API_KEY environment variable not set
    echo Set it with: set SEC_API_KEY=your_api_key
    echo Get key from: https://sec-api.io/
    pause
    exit /b 1
)

echo API Key found: %SEC_API_KEY:~0,10%...
echo.

REM Run collection with optional days parameter
set DAYS_BACK=7
if not "%1"=="" set DAYS_BACK=%1

echo Collecting SEC data for last %DAYS_BACK% days...
python scripts\collect_regulatory_data.py %DAYS_BACK%

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ SEC data collection completed successfully
) else (
    echo.
    echo ❌ SEC data collection failed
    echo Check logs for details
)

echo.
pause
