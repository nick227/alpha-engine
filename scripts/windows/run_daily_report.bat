@echo off
echo Generating daily trading report...
setlocal
set "ROOT=%~dp0..\.."
cd /d "%ROOT%"
python scripts\generate_daily_report.py
echo Daily report generation complete.
pause
