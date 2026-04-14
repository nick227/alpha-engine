#!/usr/bin/env python3
"""
Automatic outcome backfill script.
Run this daily to fill actual outcomes for mature predictions.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.ml.outcome_backfill import OutcomeBackfill
from app.ml.dimensional_tagger import get_dimensional_tagger


def main():
    """Run automatic outcome backfill and check system status."""
    print("🔄 AUTOMATIC OUTCOME BACKFILL")
    print("=" * 50)
    
    # Initialize systems
    backfill = OutcomeBackfill()
    tagger = get_dimensional_tagger()
    
    # 1. Show current status
    print("\n📊 CURRENT SYSTEM STATUS")
    print("-" * 30)
    
    stats = backfill.get_outcome_statistics()
    print(f"Total predictions: {stats['total_predictions']}")
    print(f"Predictions with outcomes: {stats['predictions_with_outcomes']}")
    print(f"Outcome coverage: {stats['outcome_coverage']:.1%}")
    
    if stats['predictions_with_outcomes'] > 0:
        print(f"Win rate: {stats['win_rate']:.1%}")
        print(f"Average return: {stats['avg_actual_return']:.3f}")
    
    # Show maturity status
    import sqlite3
    conn = sqlite3.connect('data/alpha.db')
    cursor = conn.execute('SELECT COUNT(*) FROM dimensional_predictions WHERE matured = TRUE')
    matured_total = cursor.fetchone()[0]
    
    cursor = conn.execute('SELECT COUNT(*) FROM dimensional_predictions WHERE matured = TRUE AND actual_return IS NULL')
    matured_needing_outcomes = cursor.fetchone()[0]
    
    print(f"Matured predictions: {matured_total}")
    print(f"Matured needing outcomes: {matured_needing_outcomes}")
    conn.close()
    
    # 2. Check self-correcting readiness
    print("\n🎯 SELF-CORRECTING READINESS")
    print("-" * 30)
    
    status = tagger.get_real_outcome_status()
    print(f"Can self-correct: {status['can_self_correct']}")
    print(f"Axes with outcomes: {status['axes_with_outcomes']}/{status['total_axes']}")
    print(f"Outcomes needed: {status['min_outcomes_per_axis']} per axis")
    
    if status['can_self_correct']:
        print("✅ System ready for self-correction!")
        print("🚀 Consider enabling self-correcting in DimensionalTagger")
    else:
        needed = status['min_outcomes_per_axis'] * status['min_axes_with_outcomes']
        current = status['predictions_with_outcomes']
        print(f"⏳ Need {needed - current} more outcomes to enable self-correction")
    
    # 3. Run backfill
    print("\n🔄 RUNNING BACKFILL")
    print("-" * 30)
    
    result = backfill.backfill_outcomes()
    
    # 4. Show results
    print("\n📈 BACKFILL RESULTS")
    print("-" * 30)
    
    # Show daily maturity monitoring
    if 'newly_matured' in result:
        print(f"Matured today: {result['newly_matured']}")
        print(f"Backfilled: {result['updated']}")
        print(f"Missed: {result['missed']}")
        
        # Critical: missed should be 0 forever
        if result['missed'] == 0:
            print("✅ PERFECT: No missed predictions today")
        else:
            print(f"⚠️  WARNING: {result['missed']} predictions missed!")
    else:
        print(f"Processed: {result['processed']}")
        print(f"Updated: {result['updated']}")
        print(f"Failed: {result['failed']}")
    
    if result['updated'] > 0:
        print(f"\n✅ Successfully filled {result['updated']} outcomes!")
        
        # Show updated stats
        updated_stats = backfill.get_outcome_statistics()
        print(f"New outcome coverage: {updated_stats['outcome_coverage']:.1%}")
        if updated_stats['predictions_with_outcomes'] > 0:
            print(f"New win rate: {updated_stats['win_rate']:.1%}")
    
    print("\n🏁 BACKFILL COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    main()
