@echo off
cd /d C:\wamp64\www\alpha-engine-poc
call .venv\Scripts\activate
set PYTHONPATH=.

echo ========================================
echo Discovery Pipeline Task - %date% %time%
echo ========================================

echo Setting up organized logging...
python scripts\setup_organized_logging.py

echo Running nightly discovery pipeline...
python scripts\log_discovery_pipeline.py

echo ========================================
echo Discovery pipeline task complete.
echo Logs saved to: logs\daily\ and logs\system\
echo ========================================
