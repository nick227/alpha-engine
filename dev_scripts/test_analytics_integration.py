#!/usr/bin/env python3
"""
Test Analytics Integration

Verifies end-to-end analytics pipeline:
backfill → replay → analytics.run() → real consensus → champions
"""

import asyncio
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.core.repository import Repository
from app.engine.analytics_runner import AnalyticsRunner
from app.ingest.backfill_runner import BackfillRunner, BACKFILL_TENANT_ID


async def test_analytics_integration():
    """Test complete analytics integration."""
    print("🧪 Testing Analytics Integration")
    print("=" * 50)
    
    # Initialize components
    db_path = "data/test_analytics.db"
    backfill_runner = BackfillRunner(db_path=db_path)
    repo = Repository(db_path)
    analytics_runner = AnalyticsRunner(repo)
    
    try:
        # 1. Run small backfill (1 day) to generate data
        print("\n📊 Step 1: Running backfill (1 day)...")
        start_time = datetime.now(timezone.utc) - timedelta(days=1)
        end_time = datetime.now(timezone.utc)
        
        await backfill_runner.backfill_range(
            start_time=start_time,
            end_time=end_time,
            batch_size_days=1,
            replay=True,
            skip_completed=False,
            fail_fast=False,
        )
        
        print("✅ Backfill complete")
        
        # 2. Run analytics pipeline
        print("\n🧠 Step 2: Running analytics pipeline...")
        analytics_results = analytics_runner.run(tenant_id=BACKFILL_TENANT_ID)
        
        print(f"✅ Analytics complete: {analytics_results}")
        
        # 3. Verify results
        print("\n🔍 Step 3: Verifying results...")
        
        # Check strategy performance
        performances = repo.get_prediction_outcomes(tenant_id=BACKFILL_TENANT_ID)
        print(f"📈 Prediction outcomes: {len(performances)}")
        
        signals = repo.get_signals(tenant_id=BACKFILL_TENANT_ID)
        print(f"📡 Signals: {len(signals)}")
        
        # Check consensus signals
        consensus_rows = repo.conn.execute(
            "SELECT COUNT(*) as count FROM consensus_signals WHERE tenant_id = ?",
            (BACKFILL_TENANT_ID,)
        ).fetchone()
        print(f"🤝 Consensus signals: {consensus_rows['count']}")
        
        # Check promotion events
        promotion_rows = repo.conn.execute(
            "SELECT COUNT(*) as count FROM promotion_events WHERE tenant_id = ?",
            (BACKFILL_TENANT_ID,)
        ).fetchone()
        print(f"🏆 Promotion events: {promotion_rows['count']}")
        
        # 4. Validate pipeline flow
        print("\n✅ Pipeline Validation:")
        validations = []
        
        if analytics_results["outcomes"] > 0:
            validations.append("✅ prediction_outcomes → strategy_performance")
        else:
            validations.append("❌ No outcomes found")
            
        if analytics_results["performance"] > 0:
            validations.append("✅ strategy_performance computed")
        else:
            validations.append("❌ No performance metrics")
            
        if analytics_results["weights"] > 0:
            validations.append("✅ strategy_weights computed")
        else:
            validations.append("❌ No weights computed")
            
        if analytics_results["consensus"] > 0:
            validations.append("✅ real consensus_signals generated")
        else:
            validations.append("❌ No consensus signals")
            
        if analytics_results["promotions"] > 0:
            validations.append("✅ promotion_events created")
        else:
            validations.append("❌ No promotion events")
        
        for validation in validations:
            print(f"  {validation}")
        
        # 5. Overall result
        all_valid = all("✅" in v for v in validations)
        if all_valid:
            print("\n🎉 FULL ANALYTICS INTEGRATION SUCCESS!")
            print("System is now decision-capable with:")
            print("  - Real strategy performance metrics")
            print("  - Adaptive strategy weights")
            print("  - Regime-aware consensus signals")
            print("  - Champion selection and promotion")
            print("  - Complete audit trail")
        else:
            print("\n⚠️  Partial integration - some components need attention")
        
        return all_valid
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if repo:
            repo.close()


if __name__ == "__main__":
    success = asyncio.run(test_analytics_integration())
    sys.exit(0 if success else 1)
