@echo off
cd /d C:\wamp64\www\alpha-engine-poc
call .venv\Scripts\activate

echo ========================================
echo Price Download Task - %date% %time%
echo ========================================

echo Setting up organized logging...
python scripts\setup_organized_logging.py

echo Downloading daily price data...
python scripts\log_price_download.py

echo ========================================
echo Price download task complete.
echo Logs saved to: logs\daily\ and logs\system\
echo ========================================
