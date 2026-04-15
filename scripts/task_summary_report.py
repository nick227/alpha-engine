#!/usr/bin/env python3
"""
Task Summary Report Generator
Aggregates and reports on all scheduled tasks
"""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

class TaskSummaryReporter:
    def __init__(self, base_path: Path = None):
        self.base_path = base_path or Path(__file__).parent.parent
        self.logs_path = self.base_path / "logs"
        self.reports_path = self.base_path / "reports"
        
    def get_task_status_from_logs(self, task_name: str, date: datetime.date) -> Dict:
        """Extract task status from system logs"""
        
        task_log = self.logs_path / "system" / f"{task_name}.log"
        if not task_log.exists():
            return {'status': 'no_log', 'message': 'No log file found'}
        
        try:
            with open(task_log, 'r', encoding='utf-8') as f:
                log_content = f.read()
            
            # Look for today's task execution
            date_str = date.strftime('%Y-%m-%d')
            today_entries = []
            
            for line in log_content.split('\n'):
                if date_str in line and f"TASK {task_name.upper()}" in line:
                    today_entries.append(line)
            
            if not today_entries:
                return {'status': 'not_run', 'message': 'No execution found today'}
            
            # Parse the most recent execution
            status = 'unknown'
            execution_time = ''
            error_message = ''
            
            for entry in reversed(today_entries):
                if 'SUCCESS' in entry:
                    status = 'success'
                    break
                elif 'FAILED' in entry:
                    status = 'failed'
                    break
            
            # Extract metrics from the log
            metrics = self.extract_metrics_from_log(log_content, date_str)
            
            return {
                'status': status,
                'message': f'Task {status}',
                'metrics': metrics,
                'last_execution': today_entries[-1] if today_entries else ''
            }
            
        except Exception as e:
            return {'status': 'error', 'message': f'Error reading log: {str(e)}'}
    
    def extract_metrics_from_log(self, log_content: str, date_str: str) -> Dict:
        """Extract performance metrics from log content"""
        metrics = {}
        
        lines = log_content.split('\n')
        for line in lines:
            if date_str in line and 'METRIC:' in line:
                # Parse metric line: "METRIC: metric_name = value"
                metric_match = re.search(r'METRIC:\s+(\w+)\s*=\s*(.+)', line)
                if metric_match:
                    metric_name = metric_match.group(1)
                    metric_value = metric_match.group(2).strip()
                    metrics[metric_name] = metric_value
        
        return metrics
    
    def get_all_tasks_status(self, date: datetime.date = None) -> Dict:
        """Get status for all scheduled tasks"""
        
        if date is None:
            date = datetime.now().date()
        
        tasks = {
            'price_download': self.get_task_status_from_logs('price_download', date),
            'discovery_pipeline': self.get_task_status_from_logs('discovery_pipeline', date),
            'replay_score': self.get_task_status_from_logs('replay_score', date),
            'trading_report': self.get_task_status_from_logs('daily_report', date)
        }
        
        return {
            'date': str(date),
            'tasks': tasks,
            'summary': self.generate_summary(tasks)
        }
    
    def generate_summary(self, tasks: Dict) -> Dict:
        """Generate overall summary of task status"""
        
        total_tasks = len(tasks)
        successful_tasks = len([t for t in tasks.values() if t['status'] == 'success'])
        failed_tasks = len([t for t in tasks.values() if t['status'] == 'failed'])
        not_run_tasks = len([t for t in tasks.values() if t['status'] == 'not_run'])
        
        if successful_tasks == total_tasks:
            overall_status = 'all_success'
            status_message = 'All tasks completed successfully'
        elif failed_tasks > 0:
            overall_status = 'some_failed'
            status_message = f'{failed_tasks} task(s) failed'
        elif not_run_tasks == total_tasks:
            overall_status = 'none_run'
            status_message = 'No tasks executed today'
        else:
            overall_status = 'partial'
            status_message = f'{successful_tasks}/{total_tasks} tasks completed'
        
        return {
            'overall_status': overall_status,
            'status_message': status_message,
            'total_tasks': total_tasks,
            'successful_tasks': successful_tasks,
            'failed_tasks': failed_tasks,
            'not_run_tasks': not_run_tasks
        }
    
    def generate_task_report(self) -> str:
        """Generate comprehensive task summary report"""
        
        today = datetime.now().date()
        status_data = self.get_all_tasks_status(today)
        
        # Build report
        lines = []
        lines.append("=" * 70)
        lines.append("SCHEDULED TASKS SUMMARY REPORT")
        lines.append("=" * 70)
        lines.append(f"Date: {today}")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # Overall Summary
        summary = status_data['summary']
        lines.append("OVERALL STATUS")
        lines.append("-" * 40)
        lines.append(f"Status: {summary['status_message'].upper()}")
        lines.append(f"Tasks: {summary['successful_tasks']}/{summary['total_tasks']} successful")
        if summary['failed_tasks'] > 0:
            lines.append(f"Failed: {summary['failed_tasks']} tasks")
        if summary['not_run_tasks'] > 0:
            lines.append(f"Not run: {summary['not_run_tasks']} tasks")
        lines.append("")
        
        # Individual Task Status
        lines.append("INDIVIDUAL TASK STATUS")
        lines.append("-" * 40)
        
        task_descriptions = {
            'price_download': 'Price Data Download',
            'discovery_pipeline': 'Discovery Pipeline + Predictions',
            'replay_score': 'Replay Score Calculation',
            'trading_report': 'Daily Trading Report'
        }
        
        for task_name, task_data in status_data['tasks'].items():
            task_desc = task_descriptions.get(task_name, task_name.replace('_', ' ').title())
            status_icon = self.get_status_icon(task_data['status'])
            
            lines.append(f"{status_icon} {task_desc}")
            lines.append(f"   Status: {task_data['status'].upper()}")
            lines.append(f"   Message: {task_data['message']}")
            
            # Add metrics if available
            if task_data.get('metrics'):
                lines.append("   Metrics:")
                for metric_name, metric_value in task_data['metrics'].items():
                    lines.append(f"     - {metric_name}: {metric_value}")
            
            lines.append("")
        
        # Recent Activity (last 3 days)
        lines.append("RECENT ACTIVITY (Last 3 Days)")
        lines.append("-" * 40)
        
        for days_ago in range(3):
            check_date = today - timedelta(days=days_ago)
            day_status = self.get_all_tasks_status(check_date)
            day_summary = day_status['summary']
            
            lines.append(f"{check_date.strftime('%A')} ({check_date}): {day_summary['status_message']}")
        
        lines.append("")
        
        # Quick Actions
        lines.append("QUICK ACTIONS")
        lines.append("-" * 40)
        lines.append("1. Run price download: run_download_prices.bat")
        lines.append("2. Run discovery pipeline: run_discovery_nightly.bat")
        lines.append("3. Run replay score: run_replay_score.bat")
        lines.append("4. Generate reports: run_trading_report.bat")
        lines.append("5. View task logs: dir logs\\system\\")
        lines.append("")
        
        lines.append("=" * 70)
        
        return "\n".join(lines)
    
    def get_status_icon(self, status: str) -> str:
        """Get status icon for display"""
        icons = {
            'success': 'OK',
            'failed': 'FAIL',
            'not_run': 'SKIP',
            'no_log': 'NOLOG',
            'error': 'ERROR',
            'unknown': '???'
        }
        return icons.get(status, '???')

def main():
    """Generate and save task summary report"""
    
    reporter = TaskSummaryReporter()
    report = reporter.generate_task_report()
    
    # Print to console
    print(report)
    
    # Save to reports directory
    reports_path = Path(__file__).parent.parent / "reports"
    reports_path.mkdir(exist_ok=True)
    
    today = datetime.now().strftime('%Y-%m-%d')
    report_file = reports_path / f"task_summary_{today}.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\nTask summary report saved to: {report_file}")

if __name__ == "__main__":
    main()
