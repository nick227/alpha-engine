@echo off
setlocal
set "ROOT=%~dp0..\.."
cd /d "%ROOT%"
python scripts\replay_once.py >> logs\replay.log 2>&1
