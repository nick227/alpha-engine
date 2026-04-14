@echo off
cd C:\wamp64\www\alpha-engine-poc
python run_paper_trading.py --report-only --days 30 >> logs\report.log 2>&1
