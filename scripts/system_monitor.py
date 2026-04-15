#!/usr/bin/env python3
"""
System Monitor Dashboard
Comprehensive system health and performance monitoring
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

class SystemMonitor:
    def __init__(self, base_path: Path = None):
        self.base_path = base_path or Path(__file__).parent.parent
        self.reports_path = self.base_path / "reports" / "daily"
        self.logs_path = self.base_path / "logs"
        
    def get_recent_reports(self, days: int = 7) -> List[Dict]:
        """Get recent daily reports for analysis"""
        reports = []
        
        for i in range(days):
            date = datetime.now().date() - timedelta(days=i)
            json_file = self.reports_path / f"{date}_data.json"
            
            if json_file.exists():
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    reports.append(data)
                    
        return reports
    
    def get_system_trends(self) -> Dict:
        """Analyze system trends over time"""
        reports = self.get_recent_reports(7)
        
        if not reports:
            return {'available': False}
        
        # Extract trend data
        win_rates = [r.get('outcomes', {}).get('win_rate', 0) for r in reports]
        signal_counts = [r.get('activity', {}).get('signals_today', 0) for r in reports]
        returns = [r.get('outcomes', {}).get('avg_return', 0) for r in reports]
        
        # Calculate trends
        recent_win_rate = win_rates[0] if win_rates else 0
        avg_win_rate = sum(wr for wr in win_rates if wr > 0) / len([wr for wr in win_rates if wr > 0]) if any(win_rates) else 0
        
        recent_signals = signal_counts[0] if signal_counts else 0
        avg_signals = sum(signal_counts) / len(signal_counts)
        
        return {
            'available': True,
            'days_analyzed': len(reports),
            'recent_win_rate': recent_win_rate,
            'avg_win_rate': round(avg_win_rate, 1),
            'recent_signals': recent_signals,
            'avg_signals': round(avg_signals, 1),
            'win_rate_trend': 'improving' if len(win_rates) >= 2 and win_rates[0] > win_rates[1] else 'declining' if len(win_rates) >= 2 and win_rates[0] < win_rates[1] else 'stable',
            'signal_trend': 'increasing' if len(signal_counts) >= 2 and signal_counts[0] > signal_counts[1] else 'decreasing' if len(signal_counts) >= 2 and signal_counts[0] < signal_counts[1] else 'stable'
        }
    
    def get_health_summary(self) -> Dict:
        """Get overall system health summary"""
        reports = self.get_recent_reports(3)  # Last 3 days
        
        if not reports:
            return {'available': False, 'status': 'No data available'}
        
        # Check health status
        health_issues = []
        latest_report = reports[0]
        
        health = latest_report.get('health', {})
        if not health.get('database_connected', True):
            health_issues.append('Database connection failed')
        if not health.get('recent_predictions', True):
            health_issues.append('No recent predictions')
        if not health.get('disk_space_ok', True):
            health_issues.append('Low disk space')
        
        # Determine overall status
        if not health_issues:
            status = 'HEALTHY'
            status_color = 'GREEN'
        elif len(health_issues) <= 2:
            status = 'WARNING'
            status_color = 'YELLOW'
        else:
            status = 'CRITICAL'
            status_color = 'RED'
        
        return {
            'available': True,
            'status': status,
            'status_color': status_color,
            'health_issues': health_issues,
            'last_check': latest_report.get('date', 'Unknown'),
            'database_status': 'Connected' if health.get('database_connected', True) else 'Disconnected',
            'prediction_activity': 'Active' if health.get('recent_predictions', False) else 'Inactive'
        }
    
    def get_performance_summary(self) -> Dict:
        """Get performance metrics summary"""
        try:
            from scripts.performance_tracker import PerformanceTracker
            tracker = PerformanceTracker()
            return tracker.get_performance_summary()
        except:
            return {'available': False}
    
    def generate_monitoring_report(self) -> str:
        """Generate comprehensive monitoring report"""
        
        # Gather all data
        trends = self.get_system_trends()
        health = self.get_health_summary()
        performance = self.get_performance_summary()
        
        # Build report
        lines = []
        lines.append("=" * 70)
        lines.append("SYSTEM MONITORING DASHBOARD")
        lines.append("=" * 70)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # System Health
        lines.append("SYSTEM HEALTH")
        lines.append("-" * 40)
        if health.get('available'):
            lines.append(f"Status: {health['status']} ({health['status_color']})")
            lines.append(f"Database: {health['database_status']}")
            lines.append(f"Prediction Activity: {health['prediction_activity']}")
            lines.append(f"Last Check: {health['last_check']}")
            
            if health['health_issues']:
                lines.append("Issues:")
                for issue in health['health_issues']:
                    lines.append(f"  - {issue}")
        else:
            lines.append("Health monitoring unavailable")
        lines.append("")
        
        # Trading Trends
        lines.append("TRADING TRENDS (7 Days)")
        lines.append("-" * 40)
        if trends.get('available'):
            lines.append(f"Recent Win Rate: {trends['recent_win_rate']}% (avg: {trends['avg_win_rate']}%)")
            lines.append(f"Recent Signals: {trends['recent_signals']} (avg: {trends['avg_signals']}/day)")
            lines.append(f"Win Rate Trend: {trends['win_rate_trend'].upper()}")
            lines.append(f"Signal Trend: {trends['signal_trend'].upper()}")
        else:
            lines.append("Trend analysis unavailable")
        lines.append("")
        
        # System Performance
        lines.append("SYSTEM PERFORMANCE")
        lines.append("-" * 40)
        if performance.get('available'):
            lines.append(f"CPU: {performance['current_cpu_percent']}% (avg: {performance['avg_cpu_percent']}%)")
            lines.append(f"Memory: {performance['current_memory_percent']}% (avg: {performance['avg_memory_percent']}%)")
            lines.append(f"Disk Usage: {performance['disk_usage_percent']}%")
            lines.append(f"Database Size: {performance['database_size_mb']}MB")
        else:
            lines.append("Performance monitoring unavailable")
        lines.append("")
        
        # Quick Actions
        lines.append("QUICK ACTIONS")
        lines.append("-" * 40)
        lines.append("1. Run daily report: python scripts/generate_daily_report.py")
        lines.append("2. Check performance: python scripts/performance_tracker.py")
        lines.append("3. Rotate logs: python scripts/log_rotation.py")
        lines.append("4. View reports: dir reports\\daily\\")
        lines.append("")
        
        lines.append("=" * 70)
        
        return "\n".join(lines)

def main():
    """Run system monitoring dashboard"""
    monitor = SystemMonitor()
    report = monitor.generate_monitoring_report()
    print(report)
    
    # Save monitoring report
    reports_path = Path(__file__).parent.parent / "reports"
    reports_path.mkdir(exist_ok=True)
    
    monitor_file = reports_path / "system_monitor.txt"
    with open(monitor_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\nMonitoring report saved to: {monitor_file}")

if __name__ == "__main__":
    main()
