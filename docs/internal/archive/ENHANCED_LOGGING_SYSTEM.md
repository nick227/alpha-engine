# Enhanced Logging & Reporting System

## Overview
Comprehensive logging and reporting system for all scheduled tasks with organized file structure, performance monitoring, and automated reporting.

## Enhanced Batch Files

### 1. `run_download_prices.bat`
- **Purpose**: Daily price data download (6:00 AM)
- **Logging**: Enhanced with structured logging and metrics
- **Output**: `logs/system/price_download.log`

### 2. `run_discovery_nightly.bat`
- **Purpose**: Discovery pipeline + predictions (8:00 AM)
- **Logging**: Enhanced with performance metrics extraction
- **Output**: `logs/system/discovery_pipeline.log`

### 3. `run_replay_score.bat`
- **Purpose**: Replay score calculation (8:30 AM)
- **Logging**: Enhanced with trading metrics extraction
- **Output**: `logs/system/replay_score.log`

### 4. `run_trading_report.bat`
- **Purpose**: Daily trading report generation (9:00 AM)
- **Logging**: Complete reporting pipeline
- **Output**: Multiple reports and logs

## Logging Structure

```
logs/
|-- daily/                     # Daily logs by date
|   |-- 2026-04-15.log        # All daily activity
|-- weekly/                    # Weekly summaries
|-- system/                    # System and task logs
|   |-- price_download.log    # Price download task
|   |-- discovery_pipeline.log # Discovery pipeline task
|   |-- replay_score.log      # Replay score task
|   |-- daily_report.log      # Daily report generation
|   |-- performance.json      # Performance metrics
|   |-- reporting.log        # Reporting system
|-- trading/                   # Trading activity logs
|-- errors/                    # Error logs
`-- archive/                   # Compressed old logs
```

## Reports Structure

```
reports/
|-- daily/                     # Daily reports
|   |-- 2026-04-15_report.txt # Human-readable report
|   |-- 2026-04-15_data.json  # Machine-readable data
|-- weekly/                    # Weekly summaries
|-- summaries/                 # Monthly analysis
|-- task_summary_2026-04-15.txt # Task status summary
|-- system_monitor.txt         # System health monitor
|-- complete_monitor_2026-04-15.txt # Complete dashboard
```

## Key Scripts

### Enhanced Logging Utilities
- `scripts/enhanced_logging.py` - Structured logging framework
- `scripts/setup_organized_logging.py` - Directory structure setup
- `scripts/log_rotation.py` - Automatic log cleanup

### Task Wrappers
- `scripts/log_price_download.py` - Price download with logging
- `scripts/log_discovery_pipeline.py` - Discovery pipeline with logging
- `scripts/log_replay_score.py` - Replay score with logging

### Reporting & Monitoring
- `scripts/generate_daily_report.py` - Enhanced daily reports
- `scripts/system_monitor.py` - System health dashboard
- `scripts/task_summary_report.py` - Task status summary
- `scripts/complete_system_monitor.py` - Complete system dashboard
- `scripts/performance_tracker.py` - Performance metrics collection

## Report Features

### Daily Trading Report
- System health checks
- Strategy performance leaderboard
- Top predictions and consensus analysis
- Activity level monitoring
- Recent outcomes and trends
- Performance metrics
- Historical comparisons
- Data quality indicators

### Task Summary Report
- Status of all 4 scheduled tasks
- Execution metrics
- Recent activity timeline
- Quick actions for failed tasks

### Complete System Monitor
- Executive summary
- Detailed system health
- Task status overview
- Trading performance trends
- System performance metrics
- Recommendations and actions

## Performance Metrics

### System Metrics
- CPU usage (current and average)
- Memory usage (current and average)
- Disk space usage
- Database size

### Trading Metrics
- Win rates and returns
- Signal generation counts
- Strategy performance
- Consensus signals

### Task Metrics
- Execution time
- Success/failure rates
- Error counts
- Output volumes

## Log Rotation & Archival

### Automatic Cleanup
- Daily logs: Keep 30 days
- Reports: Keep 90 days
- Error logs: Keep 7 days
- Performance data: Keep 7 days

### Compression
- Old logs compressed to `.gz` format
- Moved to `logs/archive/` directory
- Disk space monitoring

## Monitoring Dashboard Features

### Health Status Indicators
- **OK** - All systems healthy
- **WARN** - Minor issues detected
- **CRIT** - Critical problems

### Task Status Icons
- **OK** - Task completed successfully
- **FAIL** - Task failed
- **SKIP** - Task not run
- **NOLOG** - No log file found

### Performance Alerts
- High CPU usage (>80%)
- High memory usage (>85%)
- Low disk space (<10%)
- Large database (>1GB)

## Quick Reference

### Run Individual Tasks
```bash
run_download_prices.bat      # Price download
run_discovery_nightly.bat   # Discovery pipeline
run_replay_score.bat        # Replay score
run_trading_report.bat      # Complete reporting
```

### Generate Reports
```bash
python scripts/generate_daily_report.py     # Daily report
python scripts/system_monitor.py           # System health
python scripts/task_summary_report.py      # Task summary
python scripts/complete_system_monitor.py  # Complete dashboard
```

### Check Performance
```bash
python scripts/performance_tracker.py      # Performance metrics
python scripts/log_rotation.py             # Log cleanup
```

### View Logs
```bash
dir logs\daily\          # Daily logs
dir logs\system\         # Task logs
dir reports\daily\       # Daily reports
```

## Benefits for Paper Trading

1. **Reliable Monitoring**: Catch issues early with health checks
2. **Comprehensive Logging**: Every task execution is logged
3. **Performance Tracking**: System and trading performance metrics
4. **Historical Analysis**: Compare performance over time
5. **Automated Maintenance**: Log rotation prevents disk issues
6. **Professional Reports**: Consistent, comprehensive summaries
7. **Quick Troubleshooting**: Task status and error tracking
8. **Trend Analysis**: Performance trends and patterns

This enhanced system ensures you have complete visibility into your paper trading operations with reliable, organized logging and comprehensive reporting.
