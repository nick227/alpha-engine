#!/usr/bin/env python3
"""
Test Horizon Compliance

Verifies B1, B2, B3 requirements:
B1: Strategy performance grouped by (strategy_id, ticker, horizon)
B2: Weight normalization per (ticker, horizon) - sum(weights) = 1
B3: Champion selection per (ticker, horizon) not global
"""

import sys
from pathlib import Path

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.core.repository import Repository
from app.engine.analytics_runner import AnalyticsRunner
from app.ingest.backfill_runner import BACKFILL_TENANT_ID


def test_horizon_compliance():
    """Test B1, B2, B3 horizon compliance requirements."""
    print("🧪 Testing Horizon Compliance (B1, B2, B3)")
    print("=" * 50)
    
    # Initialize components
    db_path = "data/test_horizon.db"
    repo = Repository(db_path)
    analytics_runner = AnalyticsRunner(repo)
    
    try:
        print("\n📊 B1: Strategy Performance Horizon Scope")
        print("-" * 40)
        
        # Check strategy_performance table grouping
        perf_rows = repo.conn.execute("""
            SELECT strategy_id, ticker, horizon, COUNT(*) as count
            FROM strategy_performance 
            WHERE tenant_id = ?
            GROUP BY strategy_id, ticker, horizon
            ORDER BY strategy_id, ticker, horizon
        """, (BACKFILL_TENANT_ID,)).fetchall()
        
        print(f"✅ Found {len(perf_rows)} (strategy_id, ticker, horizon) groups:")
        for row in perf_rows[:5]:  # Show first 5
            print(f"   {row['strategy_id']} | {row['ticker']} | {row['horizon']} | {row['count']} records")
        
        if len(perf_rows) > 5:
            print(f"   ... and {len(perf_rows) - 5} more")
        
        # Verify no cross-horizon mixing
        cross_horizon_check = repo.conn.execute("""
            SELECT strategy_id, ticker, COUNT(DISTINCT horizon) as horizon_count
            FROM strategy_performance 
            WHERE tenant_id = ?
            GROUP BY strategy_id, ticker
            HAVING horizon_count > 1
        """, (BACKFILL_TENANT_ID,)).fetchall()
        
        if cross_horizon_check:
            print(f"❌ B1 FAIL: Found {len(cross_horizon_check)} strategies mixing horizons")
            for row in cross_horizon_check:
                print(f"   {row['strategy_id']} | {row['ticker']} has {row['horizon_count']} horizons")
        else:
            print("✅ B1 PASS: No horizon mixing in strategy performance")
        
        print("\n⚖️  B2: Weight Normalization Per Ticker")
        print("-" * 40)
        
        # Check weight normalization per (ticker, horizon)
        weight_groups = repo.conn.execute("""
            SELECT ticker, horizon, strategy_id, weight,
                   SUM(weight) OVER (PARTITION BY ticker, horizon) as group_sum,
                   COUNT(*) OVER (PARTITION BY ticker, horizon) as group_count
            FROM strategy_weights 
            WHERE tenant_id = ?
            ORDER BY ticker, horizon, weight DESC
        """, (BACKFILL_TENANT_ID,)).fetchall()
        
        # Group by (ticker, horizon) and check normalization
        ticker_horizon_groups = {}
        for row in weight_groups:
            key = (row['ticker'], row['horizon'])
            if key not in ticker_horizon_groups:
                ticker_horizon_groups[key] = []
            ticker_horizon_groups[key].append(row)
        
        print(f"✅ Found {len(ticker_horizon_groups)} (ticker, horizon) weight groups:")
        
        normalization_failures = 0
        for (ticker, horizon), group in ticker_horizon_groups.items():
            total_weight = sum(row['weight'] for row in group)
            is_normalized = abs(total_weight - 1.0) < 0.001  # Allow floating point tolerance
            
            if is_normalized:
                print(f"   ✅ {ticker} | {horizon}: Σweights = {total_weight:.6f}")
            else:
                print(f"   ❌ {ticker} | {horizon}: Σweights = {total_weight:.6f} (should be 1.0)")
                normalization_failures += 1
        
        if normalization_failures == 0:
            print("✅ B2 PASS: All weights normalized per (ticker, horizon)")
        else:
            print(f"❌ B2 FAIL: {normalization_failures} groups not normalized")
        
        print("\n🏆 B3: Champion Selection Scope")
        print("-" * 40)
        
        # Check champion selection per (ticker, horizon)
        champion_rows = repo.conn.execute("""
            SELECT strategy_id, ticker, horizon, rank, score
            FROM promotion_events 
            WHERE tenant_id = ?
            ORDER BY ticker, horizon, rank
        """, (BACKFILL_TENANT_ID,)).fetchall()
        
        # Group champions by (ticker, horizon)
        champion_groups = {}
        for row in champion_rows:
            key = (row['ticker'], row['horizon'])
            if key not in champion_groups:
                champion_groups[key] = []
            champion_groups[key].append(row)
        
        print(f"✅ Found {len(champion_groups)} (ticker, horizon) champion groups:")
        
        scope_failures = 0
        for (ticker, horizon), champions in champion_groups.items():
            if len(champions) == 1 and champions[0]['rank'] == 1:
                print(f"   ✅ {ticker} | {horizon}: 1 champion (rank {champions[0]['rank']})")
            else:
                print(f"   ❌ {ticker} | {horizon}: {len(champions)} champions (should be 1)")
                scope_failures += 1
        
        if scope_failures == 0:
            print("✅ B3 PASS: Champions selected per (ticker, horizon)")
        else:
            print(f"❌ B3 FAIL: {scope_failures} groups have incorrect champion scope")
        
        # Overall result
        print("\n🎯 HORIZON COMPLIANCE SUMMARY")
        print("=" * 50)
        
        b1_pass = len(cross_horizon_check) == 0
        b2_pass = normalization_failures == 0
        b3_pass = scope_failures == 0
        
        print(f"B1 (Performance Scope): {'✅ PASS' if b1_pass else '❌ FAIL'}")
        print(f"B2 (Weight Normalization): {'✅ PASS' if b2_pass else '❌ FAIL'}")
        print(f"B3 (Champion Scope): {'✅ PASS' if b3_pass else '❌ FAIL'}")
        
        all_pass = b1_pass and b2_pass and b3_pass
        if all_pass:
            print("\n🎉 ALL HORIZON COMPLIANCE REQUIREMENTS SATISFIED!")
            print("System correctly implements:")
            print("  - Horizon-scoped strategy performance")
            print("  - Ticker-horizon weight normalization")
            print("  - Per-ticker champion selection")
        else:
            print("\n⚠️  Some horizon compliance requirements need attention")
        
        return all_pass
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if repo:
            repo.close()


if __name__ == "__main__":
    success = test_horizon_compliance()
    sys.exit(0 if success else 1)
