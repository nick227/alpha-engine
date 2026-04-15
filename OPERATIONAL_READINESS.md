# Operational Readiness Guide

## What's Solid (Foundation)

### 1. SYSTEM + venv Fix
- **Eliminated**: Biggest failure class (missing packages)
- **Result**: Consistent environment regardless of execution context
- **Debug**: Environment logging shows exact package versions

### 2. Single-Day Pipeline
- **Eliminated**: Hang/overlap risk from multi-day loops
- **Result**: Predictable 4-second execution for Step 3
- **Benefit**: No scheduler conflicts or resource contention

### 3. Step Sequencing
```
10:00 AM - Step 1: Download prices (177 seconds)
10:03 AM - Step 2: Discovery pipeline (177 seconds) 
10:06 AM - Step 3: Create predictions (4 seconds)
10:06 AM - Step 4: Replay scoring (3 seconds)
10:07 AM - Step 5: Backfill outcomes (1 second)
```
- **Correct**: Prices before discovery before predictions
- **Efficient**: No redundant data fetching
- **Idempotent**: Skips existing predictions

### 4. Idempotency Protection
- **Feature**: "Already have 23 predictions - skipping"
- **Benefit**: Prevents duplication on re-runs
- **Safety**: Lock file prevents concurrent execution

### 5. Logging System
- **Per-day**: `daily_pipeline_YYYY-MM-DD.log`
- **Environment**: Python path + package list logged
- **Debuggable**: Complete execution trace

## Subtle Things to Watch

### 1. Step Timing Analysis
**Current Observations:**
- Step 1: ~177 seconds (price download)
- Step 2: ~177 seconds (discovery pipeline)
- **Total**: ~6 minutes (acceptable)

**What to Verify:**
- Are both steps really taking the same time?
- Is Step 1 (price download) consistently ~3 minutes?
- Is Step 2 (discovery) consistently ~3 minutes?

**Acceptable Range:**
- Total runtime: 5-10 minutes
- Individual steps: 1-5 minutes each

### 2. Data Quality Noise
**Current Issue:**
```
$AFTY: possibly delisted; no price data found
$ABTX: possibly delisted; no timezone found
$AGTC: possibly delisted; no timezone found
```

**Impact:**
- Wastes download time
- Pollutes discovery universe
- May affect strategy signal quality

**Future Optimization:**
- Curate ticker universe (NASDAQ/S&P 500)
- Auto-prune delisted symbols
- Filter by minimum liquidity/volume

### 3. Replay Lag (Expected)
**Current State:**
- Today: 0 predictions scored
- Reason: Horizons (5d/20d) haven't matured

**Timeline:**
- Day 1-4: 0 scored (normal)
- Day 5: First 5-day horizons score
- Day 20: First 20-day horizons score

**Success Metric:**
- By Day 5: Some predictions scoring
- By Day 20: Full scoring pipeline active

### 4. Scheduler Overlap Protection
**Current Safeguard:**
```batch
if exist %LOCK% (
    echo [%DATE% %TIME%] Pipeline already running - Aborting
    exit /b 99
)
```

**Edge Case:**
- Pipeline now runs in ~7 minutes
- Scheduler fires daily at 10:00 AM
- Overlap risk: Nearly impossible

## Success Indicators for Tomorrow

### Required Evidence of Success

#### 1. Log File Exists
```cmd
dir logs\daily_pipeline_2026-04-16.log
```

#### 2. Key Log Entries
```
[Wed 04/16/2026 10:00:XX] Daily pipeline started
[Wed 04/16/2026 10:00:XX] PYTHON=C:\wamp64\www\alpha-engine-poc\.venv\Scripts\python.exe

[Wed 04/16/2026 10:03:XX] STEP 1 END: Download complete
[Wed 04/16/2026 10:06:XX] STEP 2 END: Discovery complete

[Queue] total queued: > 0
[Wed 04/16/2026 10:06:XX] STEP 3 END: Predictions created

[Wed 04/16/2026 10:07:XX] Pipeline finished OK
```

#### 3. Daily Report Generated
```cmd
dir reports\daily\2026-04-16_report.txt
```

#### 4. Report Content
- **Signals today**: > 0
- **Top signals**: Multiple entries with confidence scores
- **System health**: OK

### Failure Indicators

#### Red Flags
- No log file created
- Log shows "STEP X FAILED"
- Log ends with "Pipeline ABORTED"
- Daily report shows 0 signals

#### Debugging Steps
1. Check log file for error messages
2. Verify environment section shows Python path
3. Check for "ModuleNotFoundError" (shouldn't happen)
4. Verify database connectivity

## High-Value Improvements Implemented

### Working Directory Guarantee
```batch
REM Force working directory - critical for SYSTEM account execution
cd /d C:\wamp64\www\alpha-engine-poc
```

**Why This Matters:**
- SYSTEM account may have different default paths
- Eliminates "file not found" errors
- Ensures relative paths work correctly

## Operational Monitoring

### Daily Checklist (10:05 AM)
1. **Log File**: `daily_pipeline_YYYY-MM-DD.log` exists
2. **Success**: Ends with "Pipeline finished OK"
3. **Signals**: Daily report shows > 0 signals
4. **Timing**: Total runtime < 10 minutes

### Weekly Review
1. **Consistency**: Pipeline runs daily at 10:00 AM
2. **Performance**: No increasing runtime trends
3. **Signal Quality**: Maintain 20-50 signals per day
4. **Error Rate**: Zero failed runs

### Monthly Optimization
1. **Ticker Universe**: Remove delisted symbols
2. **Strategy Performance**: Review win rates
3. **Resource Usage**: Monitor memory/disk trends
4. **Schedule Adjustments**: Optimize timing if needed

## System Architecture Summary

### Execution Context
- **Account**: SYSTEM (highest privileges)
- **Environment**: `.venv\Scripts\python.exe`
- **Working Directory**: `C:\wamp64\www\alpha-engine-poc`
- **Database**: `data\alpha.db`

### Pipeline Flow
```
10:00:00 - Start pipeline
10:00:01 - Download prices (yfinance)
10:03:00 - Discovery pipeline (strategies)
10:06:00 - Create predictions (from queue)
10:06:04 - Replay scoring (expired predictions)
10:06:07 - Backfill outcomes (NULL values)
10:06:08 - Generate daily report
10:06:09 - Pipeline finished OK
```

### Success Metrics
- **Runtime**: < 10 minutes
- **Signals**: 20-50 per day
- **Errors**: 0 per week
- **Availability**: > 99% uptime

---

## Status: OPERATIONAL READY

Your AlphaEngine Daily Pipeline is configured for production automation with comprehensive monitoring and fail-safes.

**Next automated run: April 16, 2026 at 10:00 AM**
