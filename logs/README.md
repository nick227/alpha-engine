# Logs Directory Structure

## Daily Logs (`logs/daily/`)
- `YYYY-MM-DD.log` - Daily system activity
- Format: `[TIMESTAMP] [LEVEL] MESSAGE`

## Weekly Logs (`logs/weekly/`)  
- `YYYY-WXX.log` - Weekly summaries
- Generated every Sunday

## System Logs (`logs/system/`)
- `pipeline.log` - Pipeline execution
- `database.log` - Database operations
- `performance.log` - System performance

## Trading Logs (`logs/trading/`)
- `positions.log` - Position changes
- `orders.log` - Order execution
- `outcomes.log` - Prediction outcomes

## Error Logs (`logs/errors/`)
- `YYYY-MM-DD_errors.log` - Daily error collection
- `critical.log` - Critical system errors

## Reports (`reports/`)
- Daily reports in `reports/daily/`
- Weekly summaries in `reports/weekly/`
- Monthly analysis in `reports/summaries/`
