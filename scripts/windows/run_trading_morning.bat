@echo off
setlocal
set "ROOT=%~dp0..\.."
cd /d "%ROOT%"
python run_paper_trading.py --days 1 >> logs\morning.log 2>&1
