@echo off
cd C:\wamp64\www\alpha-engine-poc
python run_paper_trading.py --days 1 >> logs\morning.log 2>&1
