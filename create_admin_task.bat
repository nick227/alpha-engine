@echo off
echo Creating AlphaEngine Daily Pipeline Task...
echo.
echo This batch file will create the scheduled task with admin rights.
echo If prompted, click "Yes" to allow administrator access.
echo.

REM Create the task with proper permissions
schtasks /create /tn "AlphaEngine - Daily Pipeline" /tr "C:\wamp64\www\alpha-engine-poc\run_daily_pipeline.bat" /sc daily /st 10:00 /ru SYSTEM /rl HIGHEST /f

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Task created successfully!
    echo.
    echo Verifying task details:
    schtasks /query /tn "AlphaEngine - Daily Pipeline" /fo LIST | findstr /C:"TaskName:" /C:"Status:" /C:"Next Run Time:"
) else (
    echo.
    echo Error creating task. Please run this as Administrator.
    echo Right-click this file and select "Run as administrator".
)

echo.
pause
