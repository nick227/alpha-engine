@echo off
echo Generating daily trading report...
cd /d "C:\wamp64\www\alpha-engine-poc"
python scripts\generate_daily_report.py
echo Daily report generation complete.
pause
