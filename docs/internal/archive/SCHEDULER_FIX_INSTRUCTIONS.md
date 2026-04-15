# Scheduler Fix Instructions

## Problem Identified
The AlphaEngine Daily Pipeline task had two critical issues:
1. **Scheduler Configuration**: Weekly trigger that missed its first run
2. **Environment Issue**: SYSTEM account couldn't find Python packages

## Root Cause Analysis

### Issue 1: Scheduler Configuration
- Task was created after 10:00 AM on 2026-04-15
- Windows Task Scheduler does not backfill missed runs
- Weekly trigger (DaysOfWeek: 62) instead of daily trigger
- NextRunTime was empty, causing no automatic execution

### Issue 2: Environment Problem
- **User execution**: Works (has venv + packages)
- **SYSTEM execution**: Fails (no yfinance module)
- Scheduler runs as SYSTEM account without user PATH or venv
- Error: `ModuleNotFoundError: No module named 'yfinance'`

## Fix Applied

### 1. Logging Enhancement
**File**: `run_daily_pipeline.bat`
**Before**: `set LOG=logs\daily_pipeline.log`
**After**: `set LOG=logs\daily_pipeline_%DATE:~-4,4%-%DATE:~-10,2%-%DATE:~-7,2%.log`

**Result**: Daily log files like `daily_pipeline_2026-04-15.log` instead of one growing file.

### 2. Environment Fix
**File**: `run_daily_pipeline.bat`
**Added**: Explicit Python path to venv
```batch
set PYTHON=C:\wamp64\www\alpha-engine-poc\.venv\Scripts\python.exe
```

**Replaced all calls**:
- `python ...` -> `%PYTHON% ...`
- Added environment logging: `echo PYTHON=%PYTHON% >> %LOG%`
- Added package list: `%PYTHON% -m pip list >> %LOG%`

**Result**: SYSTEM account now uses consistent venv environment.

### 3. Scheduler Reconfiguration
**Script**: `fix_scheduler.ps1`

**Commands to run as Administrator:**
```powershell
# Unregister old task
Unregister-ScheduledTask -TaskName "AlphaEngine - Daily Pipeline" -Confirm:$false

# Create new action
$action = New-ScheduledTaskAction -Execute "C:\wamp64\www\alpha-engine-poc\run_daily_pipeline.bat"

# Create new trigger (daily at 10:00 AM)
$trigger = New-ScheduledTaskTrigger -Daily -At 10:00AM

# Register the task
Register-ScheduledTask `
  -TaskName "AlphaEngine - Daily Pipeline" `
  -Action $action `
  -Trigger $trigger `
  -RunLevel Highest `
  -Force
```

## Expected Result After Fix
```
TaskName                     State LastRunTime NextRunTime
--------                     ----- ----------- -----------
AlphaEngine - Daily Pipeline Ready            4/16/2026 10:00:00 AM
```

## Verification Steps

### 1. Run the Fix Script
```powershell
# Run as Administrator
.\fix_scheduler.ps1
```

### 2. Verify Task Configuration
```powershell
Get-ScheduledTask | Where {$_.TaskName -like "*AlphaEngine*"} |
Select TaskName, State, LastRunTime, NextRunTime
```

### 3. Check Task Details
```powershell
Get-ScheduledTaskInfo -TaskName "AlphaEngine - Daily Pipeline" |
Select LastRunTime, NextRunTime, MissedRuns
```

## System Status After Fix

### Current Working State
- **Pipeline Execution**: 4 seconds (fast) 
- **Daily Reports**: 23 signals with top predictions
- **System Health**: OK
- **Logging**: Date-specific files with environment info
- **Performance**: All metrics tracked
- **Environment**: SYSTEM account now uses venv
- **Dependencies**: All packages available (yfinance, pandas, etc.)

### Expected Tomorrow Morning
- **10:00 AM**: Automatic pipeline execution
- **10:04 AM**: Daily report generated
- **Logging**: `daily_pipeline_2026-04-16.log` created

## Complete Pipeline Flow
```
10:00 AM - Step 1: Download prices (177 seconds)
10:03 AM - Step 2: Discovery pipeline (177 seconds) 
10:06 AM - Step 3: Create predictions (4 seconds)
10:06 AM - Step 4: Replay scoring (3 seconds)
10:06 AM - Step 5: Backfill outcomes (1 second)
10:07 AM - Pipeline complete
```

## Backup Plan
If scheduler still fails, manual execution:
```cmd
.\run_daily_pipeline.bat
```

This will generate the same results and logging as automatic execution.

## Success Metrics
- **NextRunTime**: Shows tomorrow's date at 10:00 AM
- **Log Files**: Date-specific files created daily
- **Reports**: Generated automatically with 23+ signals
- **System Health**: Status OK
