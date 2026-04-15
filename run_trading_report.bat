@echo off
cd C:\wamp64\www\alpha-engine-poc

echo ========================================
echo Trading Report Generation - %date% %time%
echo ========================================

echo Setting up organized logging structure...
python scripts\setup_organized_logging.py

echo Generating daily trading report with health checks...
python scripts\generate_daily_report.py

echo Generating 30-day trading report...
python run_paper_trading.py --report-only --days 30

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
