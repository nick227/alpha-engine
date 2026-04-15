#!/usr/bin/env python3
"""
Daily Trading System Report Generator

Replaces Streamlit dashboard with concise text-based insights:
- Strategy leaderboard
- Top predictions  
- Consensus analysis
- Activity level
- Recent outcomes
- Auto-generated observations
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import defaultdict, Counter
import statistics

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from app.db.repository import AlphaRepository

# Setup logging
def setup_logging():
    """Setup organized logging for the reporting system"""
    base_path = Path(__file__).parent.parent
    logs_path = base_path / "logs"
    
    # Ensure directories exist
    logs_path.mkdir(exist_ok=True)
    (logs_path / "daily").mkdir(exist_ok=True)
    (logs_path / "system").mkdir(exist_ok=True)
    
    # Setup daily log file
    today = datetime.now().strftime("%Y-%m-%d")
    daily_log = logs_path / "daily" / f"{today}.log"
    system_log = logs_path / "system" / "reporting.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(daily_log),
            logging.FileHandler(system_log),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


class DailyReporter:
    def __init__(self, logger=None):
        self.db = AlphaRepository()
        self.today = datetime.now().date()
        self.week_ago = self.today - timedelta(days=7)
        self.logger = logger or setup_logging()
        self.base_path = Path(__file__).parent.parent
        
    def get_system_health(self) -> Dict:
        """Check system health metrics"""
        health_status = {
            'database_connected': True,
            'recent_predictions': False,
            'recent_outcomes': False,
            'disk_space_ok': True,
            'memory_usage_ok': True,
            'error_rate_normal': True
        }
        
        try:
            # Check recent predictions
            today_start = datetime.combine(self.today, datetime.min.time())
            recent_preds = self.db.conn.execute(
                "SELECT COUNT(*) as count FROM predictions WHERE timestamp >= ?",
                (today_start.isoformat(),)
            ).fetchone()
            health_status['recent_predictions'] = recent_preds['count'] > 0
            
            # Check recent outcomes
            recent_outcomes = self.db.conn.execute(
                "SELECT COUNT(*) as count FROM prediction_outcomes WHERE evaluated_at >= ?",
                (today_start.isoformat(),)
            ).fetchone()
            health_status['recent_outcomes'] = recent_outcomes['count'] > 0
            
            # Check disk space
            import shutil
            total, used, free = shutil.disk_usage(self.base_path)
            health_status['disk_space_ok'] = free > (1024 * 1024 * 1024)  # 1GB free
            
            # Log health status
            self.logger.info(f"System health check: {health_status}")
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            health_status['database_connected'] = False
            
        return health_status
        
    def save_historical_data(self, report_data: Dict):
        """Save report data for historical tracking"""
        try:
            reports_path = self.base_path / "reports" / "daily"
            reports_path.mkdir(parents=True, exist_ok=True)
            
            # Save JSON data for analysis
            json_file = reports_path / f"{self.today}_data.json"
            with open(json_file, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)
                
            self.logger.info(f"Historical data saved to {json_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save historical data: {e}")
            
    def get_historical_comparison(self, days_back: int = 7) -> Dict:
        """Compare current data with historical averages"""
        try:
            reports_path = self.base_path / "reports" / "daily"
            historical_data = []
            
            for i in range(1, min(days_back, 7) + 1):
                past_date = self.today - timedelta(days=i)
                json_file = reports_path / f"{past_date}_data.json"
                
                if json_file.exists():
                    with open(json_file, 'r') as f:
                        data = json.load(f)
                        historical_data.append(data)
            
            if not historical_data:
                return {'comparison_available': False}
                
            # Calculate historical averages
            avg_win_rate = statistics.mean([d.get('outcomes', {}).get('win_rate', 0) for d in historical_data])
            avg_signals = statistics.mean([d.get('activity', {}).get('signals_today', 0) for d in historical_data])
            avg_return = statistics.mean([d.get('outcomes', {}).get('avg_return', 0) for d in historical_data])
            
            return {
                'comparison_available': True,
                'historical_avg_win_rate': round(avg_win_rate, 1),
                'historical_avg_signals': round(avg_signals, 1),
                'historical_avg_return': round(avg_return, 2),
                'days_compared': len(historical_data)
            }
            
        except Exception as e:
            self.logger.error(f"Historical comparison failed: {e}")
            return {'comparison_available': False}
        
    def get_strategy_leaderboard(self, days: int = 7) -> List[Dict]:
        """Get strategy performance ranking for last N days"""
        
        week_ago_dt = datetime.combine(self.week_ago, datetime.min.time())
        
        # Get strategies with recent predictions and outcomes
        query = """
        SELECT 
            s.name,
            s.id,
            COUNT(p.id) as prediction_count,
            COUNT(po.id) as outcome_count,
            SUM(CASE WHEN po.direction_correct = 1 THEN 1 ELSE 0 END) as wins,
            AVG(po.return_pct) as avg_return
        FROM strategies s
        LEFT JOIN predictions p ON s.id = p.strategy_id 
            AND p.timestamp >= ?
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE s.active = 1 AND s.status = 'ACTIVE'
        GROUP BY s.id, s.name
        HAVING prediction_count > 0
        ORDER BY 
            CASE 
                WHEN outcome_count > 0 THEN (SUM(CASE WHEN po.direction_correct = 1 THEN 1 ELSE 0 END) * 100.0 / outcome_count)
                ELSE 0 
            END DESC,
            avg_return DESC
        LIMIT 10
        """
        
        rows = self.db.conn.execute(query, (week_ago_dt.isoformat(),)).fetchall()
        
        leaderboard = []
        for row in rows:
            if row['outcome_count'] > 0:
                win_rate = (row['wins'] / row['outcome_count']) * 100
            else:
                win_rate = 0
                
            avg_return = row['avg_return'] or 0
            
            leaderboard.append({
                'name': row['name'],
                'win_rate': round(win_rate, 1),
                'avg_return': round(avg_return, 2),
                'predictions': row['prediction_count']
            })
        
        return leaderboard
        
    def get_top_predictions_today(self, limit: int = 10) -> List[Dict]:
        """Get today's top predictions by confidence"""
        
        today_start = datetime.combine(self.today, datetime.min.time())
        today_end = datetime.combine(self.today, datetime.max.time())
        
        query = """
        SELECT 
            p.ticker,
            p.confidence,
            p.prediction as direction,
            p.horizon,
            s.name as strategy
        FROM predictions p
        JOIN strategies s ON p.strategy_id = s.id
        WHERE p.timestamp >= ? AND p.timestamp <= ?
        ORDER BY p.confidence DESC
        LIMIT ?
        """
        
        rows = self.db.conn.execute(query, (today_start.isoformat(), today_end.isoformat(), limit)).fetchall()
        
        top_predictions = []
        for row in rows:
            top_predictions.append({
                'ticker': row['ticker'],
                'confidence': round(row['confidence'], 3),
                'strategy': row['strategy'],
                'direction': row['direction'],
                'horizon': row['horizon']
            })
            
        return top_predictions
        
    def get_consensus_signals(self) -> List[Dict]:
        """Get tickers with multi-strategy agreement"""
        
        today_start = datetime.combine(self.today, datetime.min.time())
        
        # Get today's predictions grouped by ticker
        query = """
        SELECT 
            p.ticker,
            COUNT(DISTINCT p.strategy_id) as strategy_count,
            AVG(p.confidence) as avg_confidence,
            p.prediction,
            COUNT(*) as total_predictions,
            GROUP_CONCAT(DISTINCT s.name) as strategies
        FROM predictions p
        JOIN strategies s ON p.strategy_id = s.id
        WHERE p.timestamp >= ?
        GROUP BY p.ticker, p.prediction
        HAVING strategy_count >= 2
        ORDER BY strategy_count DESC, avg_confidence DESC
        LIMIT 10
        """
        
        rows = self.db.conn.execute(query, (today_start.isoformat(),)).fetchall()
        
        consensus_signals = []
        for row in rows:
            strategies = row['strategies'].split(',') if row['strategies'] else []
            
            consensus_signals.append({
                'ticker': row['ticker'],
                'strategy_count': row['strategy_count'],
                'strategies': strategies,
                'avg_confidence': round(row['avg_confidence'], 3),
                'dominant_direction': row['prediction'],
                'agreement_strength': round(100.0, 1)  # Since we grouped by prediction, this is 100%
            })
        
        return consensus_signals
        
    def get_activity_metrics(self) -> Dict:
        """Get system activity level and comparison"""
        
        today_start = datetime.combine(self.today, datetime.min.time())
        week_ago_start = datetime.combine(self.week_ago, datetime.min.time())
        
        # Today's predictions
        today_query = "SELECT COUNT(*) as count FROM predictions WHERE timestamp >= ?"
        today_result = self.db.conn.execute(today_query, (today_start.isoformat(),)).fetchone()
        today_count = today_result['count']
        
        # Last 7 days average
        week_query = """
        SELECT 
            DATE(timestamp) as day,
            COUNT(*) as count
        FROM predictions 
        WHERE timestamp >= ? AND timestamp < ?
        GROUP BY DATE(timestamp)
        """
        week_rows = self.db.conn.execute(week_query, (week_ago_start.isoformat(), today_start.isoformat())).fetchall()
        
        if week_rows:
            avg_daily = statistics.mean([row['count'] for row in week_rows])
        else:
            avg_daily = 0
            
        # Determine activity level
        if avg_daily > 0:
            if today_count > avg_daily * 1.2:
                activity_level = "High"
            elif today_count < avg_daily * 0.8:
                activity_level = "Low"
            else:
                activity_level = "Normal"
            percent_change = round(((today_count - avg_daily) / avg_daily * 100), 1)
        else:
            activity_level = "Normal"
            percent_change = 0
            
        return {
            'signals_today': today_count,
            'avg_last_week': round(avg_daily, 1),
            'activity_level': activity_level,
            'percent_change': percent_change
        }
        
    def get_recent_outcomes(self, days: int = 5) -> Dict:
        """Get recent prediction outcomes"""
        
        start_date = datetime.combine(self.today - timedelta(days=days), datetime.min.time())
        
        query = """
        SELECT 
            COUNT(*) as total_predictions,
            SUM(CASE WHEN direction_correct = 1 THEN 1 ELSE 0 END) as wins,
            AVG(return_pct) as avg_return,
            MIN(return_pct) as worst_loss,
            MAX(return_pct) as best_return
        FROM prediction_outcomes 
        WHERE evaluated_at >= ?
        """
        
        result = self.db.conn.execute(query, (start_date.isoformat(),)).fetchone()
        
        if result['total_predictions'] > 0:
            win_rate = (result['wins'] / result['total_predictions']) * 100
            return {
                'win_rate': round(win_rate, 1),
                'avg_return': round(result['avg_return'] or 0, 2),
                'worst_loss': round(result['worst_loss'] or 0, 2),
                'best_return': round(result['best_return'] or 0, 2),
                'total_predictions': result['total_predictions']
            }
        else:
            return {
                'win_rate': 0,
                'avg_return': 0,
                'worst_loss': 0,
                'best_return': 0,
                'total_predictions': 0
            }
        
    def generate_interesting_observations(self, leaderboard: List[Dict], 
                                             consensus: List[Dict], 
                                             activity: Dict) -> List[str]:
        """Generate auto-generated insights"""
        
        observations = []
        
        # Strategy dominance
        if leaderboard and len(leaderboard) >= 2:
            top_strategy = leaderboard[0]
            second_strategy = leaderboard[1]
            
            if top_strategy['win_rate'] > second_strategy['win_rate'] + 10:
                observations.append(f"{top_strategy['name']} dominating with {top_strategy['win_rate']}% win rate")
                
        # High consensus activity
        high_consensus = [c for c in consensus if c['strategy_count'] >= 3]
        if len(high_consensus) >= 3:
            observations.append(f"High consensus environment: {len(high_consensus)} tickers with 3+ strategies aligned")
            
        # Activity level insights
        if activity['percent_change'] > 30:
            observations.append(f"Unusually high activity: {activity['signals_today']} signals ({activity['percent_change']}% above average)")
        elif activity['percent_change'] < -30:
            observations.append(f"Quiet market: {activity['signals_today']} signals ({activity['percent_change']}% below average)")
            
        # Strategy diversity
        if leaderboard:
            active_strategies = len([s for s in leaderboard if s['predictions'] >= 5])
            if active_strategies >= 5:
                observations.append(f"Diverse strategy environment: {active_strategies} strategies with meaningful activity")
                
        return observations[:3]  # Top 3 observations
        
    def generate_daily_report(self) -> str:
        """Generate complete daily report with system health and historical tracking"""
        
        self.logger.info("Starting daily report generation")
        
        # Gather all data
        try:
            leaderboard = self.get_strategy_leaderboard()
            top_predictions = self.get_top_predictions_today()
            consensus = self.get_consensus_signals()
            activity = self.get_activity_metrics()
            outcomes = self.get_recent_outcomes()
            observations = self.generate_interesting_observations(leaderboard, consensus, activity)
            health = self.get_system_health()
            historical = self.get_historical_comparison()
            
            # Store data for historical tracking
            report_data = {
                'date': str(self.today),
                'leaderboard': leaderboard,
                'top_predictions': top_predictions,
                'consensus': consensus,
                'activity': activity,
                'outcomes': outcomes,
                'observations': observations,
                'health': health,
                'historical_comparison': historical
            }
            
            self.save_historical_data(report_data)
            
            self.logger.info(f"Report data gathered: {len(leaderboard)} strategies, {activity['signals_today']} signals")
            
        except Exception as e:
            self.logger.error(f"Failed to gather report data: {e}")
            raise
        
        # Build report
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append(f"DAILY TRADING REPORT - {self.today.strftime('%Y-%m-%d')}")
        report_lines.append("=" * 60)
        report_lines.append("")
        
        # 0. System Health (NEW)
        report_lines.append("SYSTEM HEALTH")
        report_lines.append("-" * 40)
        health_status = "OK" if all(health.values()) else "ISSUES DETECTED"
        report_lines.append(f"Status: {health_status}")
        
        if not health['database_connected']:
            report_lines.append("  - Database connection: FAILED")
        if not health['recent_predictions']:
            report_lines.append("  - Recent predictions: NONE")
        if not health['recent_outcomes']:
            report_lines.append("  - Recent outcomes: NONE")
        if not health['disk_space_ok']:
            report_lines.append("  - Disk space: LOW")
        report_lines.append("")
        
        # 1. Strategy Leaderboard
        report_lines.append("STRATEGY LEADERBOARD (Last 7 Days)")
        report_lines.append("-" * 40)
        if leaderboard:
            for i, strategy in enumerate(leaderboard[:5], 1):
                report_lines.append(f"{i}. {strategy['name']} => {strategy['win_rate']}% win ({strategy['predictions']} preds)")
        else:
            report_lines.append("No recent strategy activity")
        report_lines.append("")
        
        # 2. Top Predictions
        report_lines.append("TOP SIGNALS (Today)")
        report_lines.append("-" * 40)
        if top_predictions:
            for pred in top_predictions[:5]:
                report_lines.append(f"{pred['ticker']} => {pred['confidence']} => {pred['strategy']} ({pred['direction']})")
        else:
            report_lines.append("No predictions today")
        report_lines.append("")
        
        # 3. Consensus Analysis
        report_lines.append("MULTI-STRATEGY AGREEMENT")
        report_lines.append("-" * 40)
        if consensus:
            for signal in consensus[:3]:
                report_lines.append(f"{signal['ticker']} => {signal['strategy_count']} strategies aligned ({signal['agreement_strength']}% agreement)")
        else:
            report_lines.append("No multi-strategy consensus today")
        report_lines.append("")
        
        # 4. Activity Level
        report_lines.append("ACTIVITY LEVEL")
        report_lines.append("-" * 40)
        report_lines.append(f"Signals today: {activity['signals_today']}")
        report_lines.append(f"Avg last week: {activity['avg_last_week']}")
        report_lines.append(f"Market: {activity['activity_level']} ({activity['percent_change']}% vs avg)")
        
        # Historical comparison (NEW)
        if historical.get('comparison_available'):
            report_lines.append(f"Historical avg: {historical['historical_avg_signals']} signals")
        report_lines.append("")
        
        # 5. Recent Outcomes
        report_lines.append("RECENT OUTCOMES (Last 5 Days)")
        report_lines.append("-" * 40)
        report_lines.append(f"Win rate: {outcomes['win_rate']}%")
        report_lines.append(f"Avg return: {outcomes['avg_return']:+.2f}%")
        report_lines.append(f"Best: {outcomes['best_return']:+.2f}% | Worst: {outcomes['worst_loss']:+.2f}%")
        
        # Historical comparison for outcomes (NEW)
        if historical.get('comparison_available'):
            report_lines.append(f"Historical win rate: {historical['historical_avg_win_rate']}%")
            report_lines.append(f"Historical avg return: {historical['historical_avg_return']:+.2f}%")
        report_lines.append("")
        
        # 6. Interesting Observations
        report_lines.append("INTERESTING OBSERVATIONS")
        report_lines.append("-" * 40)
        if observations:
            for obs in observations:
                report_lines.append(f"  {obs}")
        else:
            report_lines.append("No notable patterns today")
        report_lines.append("")
        
        # 7. System Performance (NEW)
        try:
            from scripts.performance_tracker import PerformanceTracker
            perf_tracker = PerformanceTracker()
            perf_tracker.track_and_save()
            perf_summary = perf_tracker.get_performance_summary()
            perf_alerts = perf_tracker.check_performance_alerts()
            
            report_lines.append("SYSTEM PERFORMANCE")
            report_lines.append("-" * 40)
            if perf_summary.get('available'):
                report_lines.append(f"CPU: {perf_summary['current_cpu_percent']}% (avg: {perf_summary['avg_cpu_percent']}%)")
                report_lines.append(f"Memory: {perf_summary['current_memory_percent']}% (avg: {perf_summary['avg_memory_percent']}%)")
                report_lines.append(f"Disk usage: {perf_summary['disk_usage_percent']}%")
                report_lines.append(f"Database size: {perf_summary['database_size_mb']}MB")
                
                if perf_alerts:
                    report_lines.append("Performance alerts:")
                    for alert in perf_alerts:
                        report_lines.append(f"  - {alert}")
            else:
                report_lines.append("Performance monitoring unavailable")
            report_lines.append("")
            
        except Exception as e:
            self.logger.warning(f"Could not load performance metrics: {e}")
            report_lines.append("SYSTEM PERFORMANCE")
            report_lines.append("-" * 40)
            report_lines.append("Performance monitoring unavailable")
            report_lines.append("")
        
        # 8. Data Quality (NEW)
        report_lines.append("DATA QUALITY")
        report_lines.append("-" * 40)
        report_lines.append(f"Total strategies tracked: {len(leaderboard)}")
        report_lines.append(f"Days with historical data: {historical.get('days_compared', 0)}")
        report_lines.append(f"Report generated: {datetime.now().strftime('%H:%M:%S')}")
        report_lines.append("")
        
        report_lines.append("=" * 60)
        
        self.logger.info("Daily report generated successfully")
        return "\n".join(report_lines)
        
    def generate_weekly_report(self) -> str:
        """Generate weekly summary report"""
        
        # Get broader data for weekly view
        leaderboard = self.get_strategy_leaderboard(days=7)
        outcomes = self.get_recent_outcomes(days=7)
        
        # Get weekly prediction trends
        week_ago = datetime.combine(self.week_ago, datetime.min.time())
        week_query = """
        SELECT 
            DATE(timestamp) as day,
            COUNT(*) as prediction_count,
            GROUP_CONCAT(DISTINCT s.name) as strategies
        FROM predictions p
        JOIN strategies s ON p.strategy_id = s.id
        WHERE timestamp >= ?
        GROUP BY DATE(timestamp)
        ORDER BY day
        """
        week_rows = self.db.conn.execute(week_query, (week_ago.isoformat(),)).fetchall()
        
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append(f"WEEKLY SUMMARY - {self.week_ago.strftime('%Y-%m-%d')} to {self.today.strftime('%Y-%m-%d')}")
        report_lines.append("=" * 60)
        report_lines.append("")
        
        # Weekly performance
        report_lines.append("WEEKLY PERFORMANCE")
        report_lines.append("-" * 40)
        total_signals = sum(row['prediction_count'] for row in week_rows)
        report_lines.append(f"Total signals: {total_signals}")
        report_lines.append(f"Win rate: {outcomes['win_rate']}%")
        report_lines.append(f"Average return: {outcomes['avg_return']:+.2f}%")
        report_lines.append("")
        
        # Top strategies for the week
        report_lines.append("TOP STRATEGIES THIS WEEK")
        report_lines.append("-" * 40)
        if leaderboard:
            for i, strategy in enumerate(leaderboard[:3], 1):
                report_lines.append(f"{i}. {strategy['name']} => {strategy['win_rate']}% win ({strategy['predictions']} preds)")
        report_lines.append("")
        
        # Daily activity trend
        report_lines.append("DAILY ACTIVITY TREND")
        report_lines.append("-" * 40)
        for row in week_rows:
            day = datetime.fromisoformat(row['day']).date()
            day_name = day.strftime('%A')
            count = row['prediction_count']
            report_lines.append(f"{day_name}: {count} signals")
        report_lines.append("")
        
        report_lines.append("=" * 60)


def main():
    """Main execution function with organized logging"""
    logger = setup_logging()
    logger.info("Starting daily report generation process")
    
    try:
        reporter = DailyReporter(logger)
        
        # Generate daily report
        daily_report = reporter.generate_daily_report()
        
        # Save daily report to organized structure
        reports_path = Path(__file__).parent.parent / "reports" / "daily"
        reports_path.mkdir(parents=True, exist_ok=True)
        
        daily_path = reports_path / f"{reporter.today}_report.txt"
        with open(daily_path, 'w', encoding='utf-8') as f:
            f.write(daily_report)
        logger.info(f"Daily report saved to {daily_path}")
        
        # Also save to root for backward compatibility
        root_daily_path = Path(__file__).parent.parent / "daily_report.txt"
        with open(root_daily_path, 'w', encoding='utf-8') as f:
            f.write(daily_report)
        
        # Generate weekly report (only on Mondays or if requested)
        if reporter.today.weekday() == 0:  # Monday
            weekly_report = reporter.generate_weekly_report()
            weekly_path = Path(__file__).parent.parent / "reports" / "weekly" / f"{reporter.today}_weekly.txt"
            weekly_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(weekly_path, 'w', encoding='utf-8') as f:
                f.write(weekly_report)
            logger.info(f"Weekly report saved to {weekly_path}")
            
            # Also save to root for backward compatibility
            root_weekly_path = Path(__file__).parent.parent / "weekly_report.txt"
            with open(root_weekly_path, 'w', encoding='utf-8') as f:
                f.write(weekly_report)
        
        # Print to console for immediate viewing
        print("\n" + daily_report)
        print(f"\nReports saved to: {reports_path}")
        logger.info("Daily report generation completed successfully")
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        print(f"Error generating report: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
