@echo off
setlocal
set "ROOT=%~dp0..\.."
cd /d "%ROOT%"

echo ========================================
echo Trading Report Generation - %date% %time%
echo ========================================

echo Setting up organized logging structure...
python scripts\setup_organized_logging.py

echo Generating daily trading report with health checks...
python scripts\generate_daily_report.py

echo Generating 30-day trading report...
powershell -NoProfile -Command "$root = (Resolve-Path '%ROOT%').Path; $end = (Get-Date).ToString('yyyy-MM-dd'); $start = (Get-Date).AddDays(-30).ToString('yyyy-MM-dd'); & \"$root\\.venv\\Scripts\\python.exe\" \"$root\\run_paper_trading.py\" --report-only --start-date $start --end-date $end"

echo Running log rotation...
python scripts\log_rotation.py

echo Generating task summary report...
python scripts\task_summary_report.py

echo ========================================
echo Trading reports complete.
echo Reports saved to: reports\daily\
echo Logs organized in: logs\
echo Task summary: reports\task_summary_YYYY-MM-DD.txt
echo ========================================
