#!/usr/bin/env python3
"""
Log Rotation and Archival System
Maintains clean, organized logs with automatic archival
"""

import os
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

class LogRotator:
    def __init__(self, base_path: Path = None):
        self.base_path = base_path or Path(__file__).parent.parent
        self.logs_path = self.base_path / "logs"
        self.archive_path = self.base_path / "logs" / "archive"
        self.archive_path.mkdir(exist_ok=True)
        
    def rotate_daily_logs(self, days_to_keep: int = 30):
        """Rotate daily logs, keeping recent ones and archiving old ones"""
        daily_logs_path = self.logs_path / "daily"
        if not daily_logs_path.exists():
            return
            
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        rotated_count = 0
        
        for log_file in daily_logs_path.glob("*.log"):
            try:
                # Extract date from filename (format: YYYY-MM-DD.log)
                file_date_str = log_file.stem
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                
                if file_date < cutoff_date:
                    # Compress and archive
                    archive_file = self.archive_path / f"{file_date_str}.log.gz"
                    with open(log_file, 'rb') as f_in:
                        with gzip.open(archive_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    # Remove original
                    log_file.unlink()
                    rotated_count += 1
                    
            except ValueError:
                # Skip files that don't match expected format
                continue
                
        print(f"Rotated {rotated_count} daily logs (keeping {days_to_keep} days)")
        
    def rotate_reports(self, days_to_keep: int = 90):
        """Rotate daily reports, keeping recent ones and archiving old ones"""
        reports_path = self.base_path / "reports" / "daily"
        if not reports_path.exists():
            return
            
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        archive_reports_path = self.archive_path / "reports"
        archive_reports_path.mkdir(exist_ok=True)
        
        rotated_count = 0
        
        for report_file in reports_path.glob("*_report.txt"):
            try:
                # Extract date from filename (format: YYYY-MM-DD_report.txt)
                file_date_str = report_file.stem.split('_')[0]
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                
                if file_date < cutoff_date:
                    # Move to archive
                    archive_file = archive_reports_path / report_file.name
                    shutil.move(str(report_file), str(archive_file))
                    rotated_count += 1
                    
            except (ValueError, IndexError):
                continue
                
        print(f"Archived {rotated_count} daily reports (keeping {days_to_keep} days)")
        
    def clean_error_logs(self, days_to_keep: int = 7):
        """Clean old error logs"""
        error_logs_path = self.logs_path / "errors"
        if not error_logs_path.exists():
            return
            
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cleaned_count = 0
        
        for log_file in error_logs_path.glob("*_errors.log"):
            try:
                # Extract date from filename
                file_date_str = log_file.stem.split('_')[0]
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                
                if file_date < cutoff_date:
                    log_file.unlink()
                    cleaned_count += 1
                    
            except (ValueError, IndexError):
                continue
                
        print(f"Cleaned {cleaned_count} error logs (keeping {days_to_keep} days)")
        
    def get_disk_usage(self) -> dict:
        """Get disk usage statistics"""
        total_size = 0
        file_count = 0
        
        for item in self.logs_path.rglob("*"):
            if item.is_file():
                total_size += item.stat().st_size
                file_count += 1
                
        return {
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'file_count': file_count,
            'archive_size_mb': round(sum(f.stat().st_size for f in self.archive_path.rglob("*") if f.is_file()) / (1024 * 1024), 2)
        }
        
    def run_rotation(self):
        """Run complete log rotation process"""
        print("Starting log rotation...")
        
        self.rotate_daily_logs(days_to_keep=30)
        self.rotate_reports(days_to_keep=90)
        self.clean_error_logs(days_to_keep=7)
        
        stats = self.get_disk_usage()
        print(f"Log directory stats: {stats['file_count']} files, {stats['total_size_mb']}MB total")
        print(f"Archive size: {stats['archive_size_mb']}MB")
        print("Log rotation complete.")

if __name__ == "__main__":
    rotator = LogRotator()
    rotator.run_rotation()
