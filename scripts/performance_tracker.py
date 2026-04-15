#!/usr/bin/env python3
"""
Performance Metrics Tracker
Tracks system performance over time for reliable monitoring
"""

import json
import time
import psutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict

class PerformanceTracker:
    def __init__(self, base_path: Path = None):
        self.base_path = base_path or Path(__file__).parent.parent
        self.metrics_path = self.base_path / "logs" / "system" / "performance.json"
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        
    def get_system_metrics(self) -> Dict:
        """Collect current system performance metrics"""
        try:
            # CPU and Memory
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage(self.base_path)
            
            # Database performance (basic)
            db_size = self.base_path / "cache" / "alpha.db"
            db_size_mb = db_size.stat().st_size / (1024 * 1024) if db_size.exists() else 0
            
            return {
                'timestamp': datetime.now().isoformat(),
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used_gb': round(memory.used / (1024**3), 2),
                'disk_free_gb': round(disk.free / (1024**3), 2),
                'disk_usage_percent': round((disk.used / disk.total) * 100, 1),
                'database_size_mb': round(db_size_mb, 2)
            }
        except Exception as e:
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def get_trading_metrics(self) -> Dict:
        """Collect trading-specific performance metrics"""
        try:
            from app.db.repository import AlphaRepository
            
            db = AlphaRepository()
            today = datetime.now().date()
            today_start = datetime.combine(today, datetime.min.time())
            
            # Today's activity
            today_predictions = db.conn.execute(
                "SELECT COUNT(*) as count FROM predictions WHERE timestamp >= ?",
                (today_start.isoformat(),)
            ).fetchone()['count']
            
            today_outcomes = db.conn.execute(
                "SELECT COUNT(*) as count FROM prediction_outcomes WHERE evaluated_at >= ?",
                (today_start.isoformat(),)
            ).fetchone()['count']
            
            # Recent performance
            week_ago = today - timedelta(days=7)
            week_start = datetime.combine(week_ago, datetime.min.time())
            
            recent_outcomes = db.conn.execute(
                """
                SELECT 
                    COUNT(*) as count,
                    AVG(return_pct) as avg_return,
                    SUM(CASE WHEN direction_correct = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
                FROM prediction_outcomes 
                WHERE evaluated_at >= ?
                """,
                (week_start.isoformat(),)
            ).fetchone()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'today_predictions': today_predictions,
                'today_outcomes': today_outcomes,
                'week_total_predictions': recent_outcomes['count'],
                'week_avg_return': round(recent_outcomes['avg_return'] or 0, 3),
                'week_win_rate': round(recent_outcomes['win_rate'] or 0, 1)
            }
            
        except Exception as e:
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def save_metrics(self, metrics: Dict):
        """Save metrics to JSON file"""
        try:
            # Load existing metrics
            if self.metrics_path.exists():
                with open(self.metrics_path, 'r') as f:
                    data = json.load(f)
            else:
                data = {'metrics': []}
            
            # Add new metrics
            data['metrics'].append(metrics)
            
            # Keep only last 7 days of metrics
            cutoff = datetime.now() - timedelta(days=7)
            data['metrics'] = [
                m for m in data['metrics'] 
                if datetime.fromisoformat(m['timestamp']) > cutoff
            ]
            
            # Save updated metrics
            with open(self.metrics_path, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            print(f"Failed to save metrics: {e}")
    
    def get_performance_summary(self) -> Dict:
        """Get performance summary for reports"""
        try:
            if not self.metrics_path.exists():
                return {'available': False}
            
            with open(self.metrics_path, 'r') as f:
                data = json.load(f)
            
            metrics = data.get('metrics', [])
            if not metrics:
                return {'available': False}
            
            # Calculate averages
            avg_cpu = sum(m.get('cpu_percent', 0) for m in metrics) / len(metrics)
            avg_memory = sum(m.get('memory_percent', 0) for m in metrics) / len(metrics)
            
            # Get latest values
            latest = metrics[-1]
            
            return {
                'available': True,
                'samples': len(metrics),
                'avg_cpu_percent': round(avg_cpu, 1),
                'avg_memory_percent': round(avg_memory, 1),
                'current_cpu_percent': latest.get('cpu_percent', 0),
                'current_memory_percent': latest.get('memory_percent', 0),
                'disk_usage_percent': latest.get('disk_usage_percent', 0),
                'database_size_mb': latest.get('database_size_mb', 0)
            }
            
        except Exception as e:
            return {'available': False, 'error': str(e)}
    
    def check_performance_alerts(self) -> List[str]:
        """Check for performance issues that need attention"""
        alerts = []
        summary = self.get_performance_summary()
        
        if not summary.get('available'):
            return ["Performance monitoring unavailable"]
        
        # CPU alerts
        if summary['current_cpu_percent'] > 80:
            alerts.append(f"High CPU usage: {summary['current_cpu_percent']}%")
        
        # Memory alerts
        if summary['current_memory_percent'] > 85:
            alerts.append(f"High memory usage: {summary['current_memory_percent']}%")
        
        # Disk alerts
        if summary['disk_usage_percent'] > 90:
            alerts.append(f"Low disk space: {summary['disk_usage_percent']}% used")
        
        # Database size alerts
        if summary['database_size_mb'] > 1000:  # 1GB
            alerts.append(f"Large database: {summary['database_size_mb']}MB")
        
        return alerts
    
    def track_and_save(self):
        """Collect and save all performance metrics"""
        system_metrics = self.get_system_metrics()
        trading_metrics = self.get_trading_metrics()
        
        # Combine metrics
        combined_metrics = {**system_metrics, **trading_metrics}
        self.save_metrics(combined_metrics)
        
        return combined_metrics

if __name__ == "__main__":
    tracker = PerformanceTracker()
    metrics = tracker.track_and_save()
    
    print("Performance Metrics Collected:")
    print(f"CPU: {metrics.get('cpu_percent', 'N/A')}%")
    print(f"Memory: {metrics.get('memory_percent', 'N/A')}%")
    print(f"Disk Free: {metrics.get('disk_free_gb', 'N/A')}GB")
    print(f"Today's Predictions: {metrics.get('today_predictions', 'N/A')}")
    print(f"Week Win Rate: {metrics.get('week_win_rate', 'N/A')}%")
    
    alerts = tracker.check_performance_alerts()
    if alerts:
        print("\nPerformance Alerts:")
        for alert in alerts:
            print(f"  - {alert}")
    else:
        print("\nNo performance issues detected.")
