@echo off
cd /d C:\wamp64\www\alpha-engine-poc
call .venv\Scripts\activate
set PYTHONPATH=.
python run_paper_trading.py --replay >> logs\replay_score.log 2>&1
