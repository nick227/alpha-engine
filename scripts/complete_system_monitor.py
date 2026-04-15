#!/usr/bin/env python3
"""
Complete System Monitor
Unified dashboard for all system components and scheduled tasks
"""

import sys
import datetime as dt
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from scripts.system_monitor import SystemMonitor
from scripts.task_summary_report import TaskSummaryReporter
from scripts.enhanced_logging import setup_task_logging

class CompleteSystemMonitor:
    def __init__(self):
        self.base_path = Path(__file__).parent.parent
        self.logger = setup_task_logging("system_monitor")
        
    def generate_complete_report(self) -> str:
        """Generate comprehensive system monitoring report"""
        
        self.logger.start_task("Complete system monitoring and reporting")
        
        try:
            # Gather all system data
            self.logger.log_step("Data Collection", "Gathering system health and performance data")
            
            # 1. System Health and Performance
            system_monitor = SystemMonitor()
            system_trends = system_monitor.get_system_trends()
            system_health = system_monitor.get_health_summary()
            system_performance = system_monitor.get_performance_summary()
            
            # 2. Task Status
            self.logger.log_step("Task Status", "Checking scheduled tasks status")
            task_reporter = TaskSummaryReporter()
            task_status = task_reporter.get_all_tasks_status()
            
            # 3. Build comprehensive report
            self.logger.log_step("Report Generation", "Building comprehensive monitoring report")
            
            lines = []
            lines.append("=" * 80)
            lines.append("COMPLETE SYSTEM MONITORING DASHBOARD")
            lines.append("=" * 80)
            lines.append(f"Generated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append("")
            
            # Executive Summary
            lines.append("EXECUTIVE SUMMARY")
            lines.append("-" * 50)
            
            # System health status
            if system_health.get('available'):
                health_icon = "OK" if system_health['status'] == 'HEALTHY' else "WARN" if system_health['status'] == 'WARNING' else "CRIT"
                lines.append(f"System Health: {health_icon} - {system_health['status']}")
            else:
                lines.append("System Health: UNKNOWN - Monitoring unavailable")
            
            # Task status summary
            task_summary = task_status['summary']
            if task_summary['successful_tasks'] == task_summary['total_tasks']:
                task_icon = "OK"
            elif task_summary['failed_tasks'] > 0:
                task_icon = "FAIL"
            else:
                task_icon = "PARTIAL"
            
            lines.append(f"Scheduled Tasks: {task_icon} - {task_summary['status_message']}")
            
            # Performance summary
            if system_performance.get('available'):
                perf_icon = "OK" if system_performance['current_cpu_percent'] < 80 and system_performance['current_memory_percent'] < 85 else "WARN"
                lines.append(f"Performance: {perf_icon} - CPU: {system_performance['current_cpu_percent']}%, Memory: {system_performance['current_memory_percent']}%")
            else:
                lines.append("Performance: UNKNOWN - Metrics unavailable")
            
            lines.append("")
            
            # Detailed System Health
            lines.append("DETAILED SYSTEM HEALTH")
            lines.append("-" * 50)
            if system_health.get('available'):
                lines.append(f"Status: {system_health['status']} ({system_health['status_color']})")
                lines.append(f"Database: {system_health['database_status']}")
                lines.append(f"Prediction Activity: {system_health['prediction_activity']}")
                lines.append(f"Last Check: {system_health['last_check']}")
                
                if system_health['health_issues']:
                    lines.append("Issues:")
                    for issue in system_health['health_issues']:
                        lines.append(f"  - {issue}")
            else:
                lines.append("Health monitoring unavailable")
            lines.append("")
            
            # Scheduled Tasks Status
            lines.append("SCHEDULED TASKS STATUS")
            lines.append("-" * 50)
            
            task_descriptions = {
                'price_download': 'Price Data Download (6:00 AM)',
                'discovery_pipeline': 'Discovery Pipeline + Predictions (8:00 AM)',
                'replay_score': 'Replay Score Calculation (8:30 AM)',
                'trading_report': 'Daily Trading Report (9:00 AM)'
            }
            
            for task_name, task_data in task_status['tasks'].items():
                task_desc = task_descriptions.get(task_name, task_name.replace('_', ' ').title())
                status_icon = self.get_status_icon(task_data['status'])
                
                lines.append(f"{status_icon} {task_desc}")
                lines.append(f"   Status: {task_data['status'].upper()}")
                lines.append(f"   Message: {task_data['message']}")
                
                # Add key metrics if available
                if task_data.get('metrics'):
                    key_metrics = ['execution_time_seconds', 'detected_win_rate_percent', 'total_return']
                    for metric in key_metrics:
                        if metric in task_data['metrics']:
                            lines.append(f"   {metric}: {task_data['metrics'][metric]}")
                
                lines.append("")
            
            # Trading Performance Trends
            lines.append("TRADING PERFORMANCE TRENDS")
            lines.append("-" * 50)
            if system_trends.get('available'):
                lines.append(f"Recent Win Rate: {system_trends['recent_win_rate']}% (avg: {system_trends['avg_win_rate']}%)")
                lines.append(f"Recent Signals: {system_trends['recent_signals']} (avg: {system_trends['avg_signals']}/day)")
                lines.append(f"Win Rate Trend: {system_trends['win_rate_trend'].upper()}")
                lines.append(f"Signal Trend: {system_trends['signal_trend'].upper()}")
            else:
                lines.append("Trend analysis unavailable - insufficient data")
            lines.append("")
            
            # System Performance Metrics
            lines.append("SYSTEM PERFORMANCE METRICS")
            lines.append("-" * 50)
            if system_performance.get('available'):
                lines.append(f"CPU Usage: {system_performance['current_cpu_percent']}% (avg: {system_performance['avg_cpu_percent']}%)")
                lines.append(f"Memory Usage: {system_performance['current_memory_percent']}% (avg: {system_performance['avg_memory_percent']}%)")
                lines.append(f"Disk Usage: {system_performance['disk_usage_percent']}%")
                lines.append(f"Database Size: {system_performance['database_size_mb']}MB")
                
                # Performance alerts
                from scripts.performance_tracker import PerformanceTracker
                perf_tracker = PerformanceTracker()
                alerts = perf_tracker.check_performance_alerts()
                if alerts:
                    lines.append("Performance Alerts:")
                    for alert in alerts:
                        lines.append(f"  - {alert}")
            else:
                lines.append("Performance monitoring unavailable")
            lines.append("")
            
            # Recent Activity Timeline
            lines.append("RECENT ACTIVITY TIMELINE")
            lines.append("-" * 50)
            
            # Get last 3 days of activity
            for days_ago in range(3):
                check_date = dt.datetime.now().date() - dt.timedelta(days=days_ago)
                day_status = task_reporter.get_all_tasks_status(check_date)
                day_summary = day_status['summary']
                
                lines.append(f"{check_date.strftime('%A')} ({check_date}): {day_summary['status_message']}")
            lines.append("")
            
            # Quick Actions and Recommendations
            lines.append("QUICK ACTIONS & RECOMMENDATIONS")
            lines.append("-" * 50)
            
            # Task-specific actions
            if task_summary['failed_tasks'] > 0:
                lines.append("URGENT - Failed Tasks Detected:")
                lines.append("1. Check individual task logs: dir logs\\system\\")
                lines.append("2. Run failed tasks manually for debugging")
                lines.append("3. Check system resources and dependencies")
                lines.append("")
            
            if system_health.get('status') == 'WARNING':
                lines.append("System Health Warnings:")
                lines.append("1. Review system health issues above")
                lines.append("2. Check database connectivity")
                lines.append("3. Verify prediction pipeline is running")
                lines.append("")
            
            # Standard actions
            lines.append("Standard Actions:")
            lines.append("1. Run price download: run_download_prices.bat")
            lines.append("2. Run discovery pipeline: run_discovery_nightly.bat")
            lines.append("3. Run replay score: run_replay_score.bat")
            lines.append("4. Generate complete report: run_trading_report.bat")
            lines.append("5. View detailed logs: dir logs\\daily\\")
            lines.append("6. Check system performance: python scripts/performance_tracker.py")
            lines.append("")
            
            # File Locations
            lines.append("IMPORTANT FILE LOCATIONS")
            lines.append("-" * 50)
            lines.append("Daily Reports: reports\\daily\\YYYY-MM-DD_report.txt")
            lines.append("Task Summary: reports\\task_summary_YYYY-MM-DD.txt")
            lines.append("System Monitor: reports\\system_monitor.txt")
            lines.append("Daily Logs: logs\\daily\\YYYY-MM-DD.log")
            lines.append("Task Logs: logs\\system\\[task_name].log")
            lines.append("Performance Data: logs\\system\\performance.json")
            lines.append("")
            
            lines.append("=" * 80)
            
            self.logger.log_metric("report_sections", str(len([l for l in lines if l.startswith('-')])))
            self.logger.end_task(True, "Complete system monitoring report generated")
            
            return "\n".join(lines)
            
        except Exception as e:
            self.logger.log_error("Failed to generate complete system report", e)
            self.logger.end_task(False, f"Failed: {str(e)}")
            raise
    
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
    """Generate complete system monitoring report"""
    
    monitor = CompleteSystemMonitor()
    report = monitor.generate_complete_report()
    
    # Print to console
    print(report)
    
    # Save to reports directory
    reports_path = Path(__file__).parent.parent / "reports"
    reports_path.mkdir(exist_ok=True)
    
    today = dt.datetime.now().strftime('%Y-%m-%d')
    report_file = reports_path / f"complete_monitor_{today}.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\nComplete system monitor report saved to: {report_file}")

if __name__ == "__main__":
    main()
