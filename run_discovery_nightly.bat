@echo off
cd /d C:\wamp64\www\alpha-engine-poc
set PYTHONPATH=.
python dev_scripts\scripts\nightly_discovery_pipeline.py --run-predictions >> logs\discovery_nightly.log 2>&1
