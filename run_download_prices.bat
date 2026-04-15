@echo off
cd /d C:\wamp64\www\alpha-engine-poc
call .venv\Scripts\activate
python dev_scripts\scripts\download_prices_daily.py >> logs\prices.log 2>&1
