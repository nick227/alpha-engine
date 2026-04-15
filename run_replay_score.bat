@echo off
cd /d C:\wamp64\www\alpha-engine-poc
call .venv\Scripts\activate
set PYTHONPATH=.

echo ========================================
echo Replay Score Task - %date% %time%
echo ========================================

echo Setting up organized logging...
python scripts\setup_organized_logging.py

echo Running replay score calculation...
python run_paper_trading.py --replay

echo ========================================
echo Replay score task complete.
echo Logs saved to: logs\daily\ and logs\system\
echo ========================================
