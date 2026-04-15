#!/usr/bin/env python3
"""
Setup Organized Logging Structure
Creates proper directory structure for reliable system monitoring
"""

import os
from pathlib import Path
from datetime import datetime

def setup_logging_structure():
    """Create organized directory structure for logs and reports"""
    
    base_path = Path(__file__).parent.parent
    logs_path = base_path / "logs"
    reports_path = base_path / "reports"
    
    # Create main directories
    directories = [
        logs_path / "daily",
        logs_path / "weekly", 
        logs_path / "system",
        logs_path / "trading",
        logs_path / "errors",
        reports_path / "daily",
        reports_path / "weekly", 
        reports_path / "summaries"
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"Created: {directory}")
    
    # Create .gitkeep files to preserve empty directories
    for directory in directories:
        (directory / ".gitkeep").touch()
    
    print("\nLogging structure created successfully!")
    
    # Create a README for the logs directory
    readme_content = """# Logs Directory Structure

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
"""
    
    (logs_path / "README.md").write_text(readme_content)
    print(f"Created: {logs_path / 'README.md'}")

if __name__ == "__main__":
    setup_logging_structure()
