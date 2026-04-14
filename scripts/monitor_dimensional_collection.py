"""
Monitor Dimensional Data Collection Progress.

Tracks axis key diversity and sample accumulation for statistical significance.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any


class DimensionalCollectionMonitor:
    """Monitor dimensional data collection progress and statistical significance."""
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
    
    def get_collection_status(self) -> Dict[str, Any]:
        """Get current data collection status."""
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Total predictions
            cursor = conn.execute("SELECT COUNT(*) FROM dimensional_predictions")
            total_predictions = cursor.fetchone()[0]
            
            # Axis key diversity
            cursor = conn.execute("""
                SELECT axis_key, COUNT(*) as count,
                       MIN(created_at) as first_seen,
                       MAX(created_at) as last_seen
                FROM dimensional_predictions 
                GROUP BY axis_key
                ORDER BY count DESC
            """)
            
            axis_stats = cursor.fetchall()
            
            # Sector distribution
            cursor = conn.execute("""
                SELECT sector_tag, COUNT(*) as count
                FROM dimensional_predictions 
                GROUP BY sector_tag
                ORDER BY count DESC
            """)
            
            sector_stats = cursor.fetchall()
            
            # Environment distribution
            cursor = conn.execute("""
                SELECT environment_tag, COUNT(*) as count
                FROM dimensional_predictions 
                GROUP BY environment_tag
                ORDER BY count DESC
            """)
            
            env_stats = cursor.fetchall()
            
            # Model distribution
            cursor = conn.execute("""
                SELECT model_tag, COUNT(*) as count
                FROM dimensional_predictions 
                GROUP BY model_tag
                ORDER BY count DESC
            """)
            
            model_stats = cursor.fetchall()
            
            return {
                "total_predictions": total_predictions,
                "unique_axis_keys": len(axis_stats),
                "axis_stats": axis_stats,
                "sector_stats": sector_stats,
                "environment_stats": env_stats,
                "model_stats": model_stats,
                "collection_date": datetime.now().isoformat()
            }
            
        finally:
            conn.close()
    
    def get_statistical_readiness(self, min_samples: int = 50) -> Dict[str, Any]:
        """Check which axis keys have statistical significance."""
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Get axis keys with sample counts
            cursor = conn.execute("""
                SELECT axis_key, COUNT(*) as count
                FROM dimensional_predictions 
                GROUP BY axis_key
            """)
            
            axis_counts = cursor.fetchall()
            
            # Categorize by readiness
            ready_axes = []
            building_axes = []
            insufficient_axes = []
            
            for axis_key, count in axis_counts:
                if count >= min_samples:
                    ready_axes.append((axis_key, count))
                elif count >= 10:
                    building_axes.append((axis_key, count))
                else:
                    insufficient_axes.append((axis_key, count))
            
            return {
                "min_samples_threshold": min_samples,
                "ready_for_analysis": ready_axes,
                "building_significance": building_axes,
                "insufficient_samples": insufficient_axes,
                "total_axes": len(axis_counts),
                "ready_percentage": len(ready_axes) / len(axis_counts) if axis_counts else 0
            }
            
        finally:
            conn.close()
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for axes with outcomes."""
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Only axes with actual returns (outcomes)
            cursor = conn.execute("""
                SELECT axis_key, 
                       COUNT(*) as sample_count,
                       AVG(actual_return) as avg_return,
                       SUM(CASE WHEN actual_return > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as win_rate,
                       AVG(prediction_error) as avg_error
                FROM dimensional_predictions 
                WHERE actual_return IS NOT NULL
                GROUP BY axis_key
                HAVING COUNT(*) >= 10
                ORDER BY win_rate DESC
            """)
            
            performance_stats = cursor.fetchall()
            
            return {
                "axes_with_outcomes": len(performance_stats),
                "performance_stats": performance_stats,
                "analysis_date": datetime.now().isoformat()
            }
            
        finally:
            conn.close()
    
    def print_collection_report(self):
        """Print comprehensive collection report."""
        
        print("DIMENSIONAL DATA COLLECTION MONITOR")
        print("=" * 60)
        
        # Collection status
        status = self.get_collection_status()
        
        print(f"COLLECTION STATUS:")
        print(f"  Total Predictions: {status['total_predictions']}")
        print(f"  Unique Axis Keys: {status['unique_axis_keys']}")
        print(f"  Collection Date: {status['collection_date']}")
        
        # Top axis keys
        print(f"\nTOP AXIS KEYS:")
        for axis_key, count, first_seen, last_seen in status['axis_stats'][:10]:
            print(f"  {axis_key}: {count} samples")
        
        # Sector distribution
        print(f"\nSECTOR DISTRIBUTION:")
        for sector, count in status['sector_stats']:
            print(f"  {sector}: {count} samples")
        
        # Environment distribution
        print(f"\nENVIRONMENT DISTRIBUTION:")
        for env, count in status['environment_stats']:
            print(f"  {env}: {count} samples")
        
        # Model distribution
        print(f"\nMODEL DISTRIBUTION:")
        for model, count in status['model_stats']:
            print(f"  {model}: {count} samples")
        
        # Statistical readiness
        readiness = self.get_statistical_readiness()
        
        print(f"\nSTATISTICAL READINESS (min_samples={readiness['min_samples_threshold']}):")
        print(f"  Ready for Analysis: {len(readiness['ready_for_analysis'])} axes")
        print(f"  Building Significance: {len(readiness['building_significance'])} axes")
        print(f"  Insufficient Samples: {len(readiness['insufficient_samples'])} axes")
        print(f"  Readiness Percentage: {readiness['ready_percentage']:.1%}")
        
        # Performance summary
        performance = self.get_performance_summary()
        
        if performance['axes_with_outcomes'] > 0:
            print(f"\nPERFORMANCE SUMMARY:")
            print(f"  Axes with Outcomes: {performance['axes_with_outcomes']}")
            print(f"  Top Performers:")
            for axis_key, count, avg_return, win_rate, avg_error in performance['performance_stats'][:5]:
                print(f"    {axis_key}: {win_rate:.1%} win rate, {avg_return:.4f} avg return")
        else:
            print(f"\nPERFORMANCE SUMMARY:")
            print(f"  No outcomes recorded yet - need actual return data")
        
        # Recommendations
        print(f"\nRECOMMENDATIONS:")
        
        if readiness['ready_percentage'] < 0.5:
            print(f"  - Continue data collection - need more samples per axis")
        
        if performance['axes_with_outcomes'] == 0:
            print(f"  - Need to start recording actual returns (outcomes)")
        
        if len(status['sector_stats']) < 4:
            print(f"  - Increase sector diversity for better coverage")
        
        if readiness['ready_percentage'] >= 0.5 and performance['axes_with_outcomes'] > 0:
            print(f"  - Ready to enable selective activation gating")
        
        print(f"\nNEXT MILESTONE:")
        if readiness['ready_percentage'] < 1.0:
            remaining = readiness['total_axes'] - len(readiness['ready_for_analysis'])
            print(f"  Need {readiness['min_samples_threshold']} samples for {remaining} more axes")
        else:
            print(f"  All axes have statistical significance - ready for activation")


def main():
    """Run dimensional collection monitoring."""
    
    monitor = DimensionalCollectionMonitor()
    monitor.print_collection_report()


if __name__ == "__main__":
    main()
