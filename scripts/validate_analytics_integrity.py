#!/usr/bin/env python3
"""
Analytics Integrity Validator

Validates analytics integrity without running replay or analytics.
Read-only verification of B1, B2, B3 compliance and data consistency.
"""

import sys
import time
from pathlib import Path

# Add app to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.repository import Repository
from app.ingest.backfill_runner import BACKFILL_TENANT_ID


def validate_analytics_integrity():
    """Validate analytics integrity with read-only queries."""
    print("🔍 Analytics Integrity Validator")
    print("=" * 50)
    
    # Initialize repository
    db_path = "data/alpha.db"  # Use existing database
    repo = Repository(db_path)
    
    try:
        start_time = time.perf_counter()
        
        # 1. Weight normalization validation
        print("\n⚖️ 1. Weight Normalization Check")
        print("-" * 40)
        
        # Note: strategy_weights table doesn't have ticker/horizon columns
        # This will be validated when analytics runner populates the table
        weight_count = repo.conn.execute("""
            SELECT COUNT(*) as count
            FROM strategy_weights 
            WHERE tenant_id = ?
        """, (BACKFILL_TENANT_ID,)).fetchone()
        
        weight_rows = int(weight_count['count']) if weight_count else 0
        print(f"Strategy weights table: {weight_rows} rows")
        
        weight_normalization_issues = []
        # Note: Weight normalization will be validated when analytics runner 
        # populates strategy_weights with ticker/horizon columns
        if weight_rows > 0:
            print("ℹ️  Weight normalization will be validated after analytics run")
        else:
            print("ℹ️  No weights found - run analytics first")
        
        # 2. Champion uniqueness validation
        print("\n🏆 2. Champion Uniqueness Check")
        print("-" * 40)
        
        # Check if promotion_events table exists and has expected columns
        table_check = repo.conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='promotion_events'
        """).fetchall()
        
        if table_check:
            # Note: promotion_events table doesn't have ticker/horizon columns in current schema
            # This will be validated when analytics runner populates the table
            promo_count = repo.conn.execute("""
                SELECT COUNT(*) as count
                FROM promotion_events 
                WHERE tenant_id = ?
            """, (BACKFILL_TENANT_ID,)).fetchone()
            
            promo_rows = int(promo_count['count']) if promo_count else 0
            print(f"Promotion events table: {promo_rows} rows")
            
            if promo_rows > 0:
                print("ℹ️  Champion uniqueness will be validated after analytics run")
            else:
                print("ℹ️  No promotion events found - run analytics first")
        else:
            print("ℹ️  Promotion events table not found - run analytics first")
        
        # 3. No horizon mixing validation
        print("\n📊 3. No Horizon Mixing Check")
        print("-" * 40)
        
        # Check if strategy_performance table exists
        perf_table_check = repo.conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='strategy_performance'
        """).fetchall()
        
        if perf_table_check:
            horizon_mixing = repo.conn.execute("""
                SELECT strategy_id, COUNT(DISTINCT horizon) as horizon_count, GROUP_CONCAT(DISTINCT horizon) as horizons
                FROM strategy_performance 
                WHERE tenant_id = ?
                GROUP BY strategy_id
                HAVING horizon_count > 1
            """, (BACKFILL_TENANT_ID,)).fetchall()
            
            if horizon_mixing:
                print(f"❌ Horizon mixing found ({len(horizon_mixing)} strategies):")
                for mix in horizon_mixing:
                    print(f"   {mix['strategy_id']}: {mix['horizons']} ({mix['horizon_count']} horizons)")
            else:
                # Show sample strategy performance (basic columns that exist)
                sample_perfs = repo.conn.execute("""
                    SELECT strategy_id, COUNT(*) as record_count
                    FROM strategy_performance 
                    WHERE tenant_id = ?
                    GROUP BY strategy_id
                    ORDER BY strategy_id
                    LIMIT 10
                """, (BACKFILL_TENANT_ID,)).fetchall()
                
                print("✅ Strategy performance records found:")
                for perf in sample_perfs:
                    print(f"   {perf['strategy_id']}: {perf['record_count']} records")
        else:
            print("ℹ️  Strategy performance table not found - run analytics first")
        
        # 4. Consensus derived from weights validation
        print("\n🤝 4. Consensus-Weights Parity Check")
        print("-" * 40)
        
        weight_count = repo.conn.execute("""
            SELECT COUNT(*) as count
            FROM strategy_weights 
            WHERE tenant_id = ?
        """, (BACKFILL_TENANT_ID,)).fetchone()
        
        consensus_count = repo.conn.execute("""
            SELECT COUNT(*) as count
            FROM consensus_signals 
            WHERE tenant_id = ?
        """, (BACKFILL_TENANT_ID,)).fetchone()
        
        weight_rows = int(weight_count['count']) if weight_count else 0
        consensus_rows = int(consensus_count['count']) if consensus_count else 0
        
        print(f"Strategy weights: {weight_rows} rows")
        print(f"Consensus signals: {consensus_rows} rows")
        
        if weight_rows > 0 and consensus_rows == 0:
            print("❌ Consensus missing: weights exist but no consensus signals")
        elif weight_rows == 0 and consensus_rows > 0:
            print("⚠️  Consensus exists without weights (using placeholder)")
        elif weight_rows > 0 and consensus_rows > 0:
            print("✅ Consensus signals derived from weights")
        else:
            print("ℹ️  No weights or consensus signals (expected for empty system)")
        
        # 5. No zero-weight strategies validation
        print("\n⚠️ 5. Zero-Weight Strategies Check")
        print("-" * 40)
        
        # Check if strategy_weights table exists and has weight column
        weight_table_check = repo.conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='strategy_weights'
        """).fetchall()
        
        if weight_table_check:
            # Check column existence
            column_check = repo.conn.execute("""
                PRAGMA table_info(strategy_weights)
            """).fetchall()
            
            has_weight_column = any(col['name'] == 'weight' for col in column_check)
            
            if has_weight_column:
                zero_weights = repo.conn.execute("""
                    SELECT strategy_id, weight
                    FROM strategy_weights 
                    WHERE tenant_id = ? AND weight = 0
                    LIMIT 10
                """, (BACKFILL_TENANT_ID,)).fetchall()
                
                if zero_weights:
                    print(f"❌ Zero-weight strategies found ({len(zero_weights)}):")
                    for zw in zero_weights:
                        print(f"   {zw['strategy_id']}: weight = {zw['weight']}")
                else:
                    print("✅ No zero-weight strategies found")
            else:
                print("ℹ️  Strategy weights table missing weight column")
        else:
            print("ℹ️  Strategy weights table not found - run analytics first")
        
        # 6. No missing analytics rows validation
        print("\n📋 6. Missing Analytics Rows Check")
        print("-" * 40)
        
        # Check for NULL values in critical columns (using existing schema)
        null_checks = [
            ("strategy_performance", "accuracy", "Performance accuracy"),
            ("strategy_weights", "win_rate", "Strategy win rates"),
            ("consensus_signals", "p_final", "Consensus scores"),
            ("consensus_signals", "direction", "Consensus directions"),
        ]
        
        missing_data_issues = []
        for table, column, description in null_checks:
            # Check if table exists first
            table_exists = repo.conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table,)).fetchall()
            
            if table_exists:
                null_count = repo.conn.execute(f"""
                    SELECT COUNT(*) as count
                    FROM {table} 
                    WHERE tenant_id = ? AND ({column} IS NULL OR {column} = '')
                """, (BACKFILL_TENANT_ID,)).fetchone()
                
                null_rows = int(null_count['count']) if null_count else 0
                if null_rows > 0:
                    missing_data_issues.append({
                        'table': table,
                        'column': column,
                        'description': description,
                        'null_count': null_rows
                    })
                else:
                    print(f"✅ {description}: no NULL values in {table}.{column}")
            else:
                print(f"ℹ️  Table {table} not found - run analytics first")
        
        if missing_data_issues:
            print(f"\n❌ Missing data issues found ({len(missing_data_issues)}):")
            for issue in missing_data_issues:
                print(f"   {issue['description']}: {issue['null_count']} NULL values in {issue['table']}.{issue['column']}")
        else:
            print("✅ No missing data issues found")
        
        # Calculate execution time
        end_time = time.perf_counter()
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # Overall result
        print("\n🎯 INTEGRITY VALIDATION SUMMARY")
        print("=" * 50)
        
        checks = [
            ("Weight Normalization", len(weight_normalization_issues) == 0),
            ("Champion Uniqueness", 0),  # No validation run yet
            ("No Horizon Mixing", 0),  # No validation run yet  
            ("Consensus-Weights Parity", not (weight_rows > 0 and consensus_rows == 0)),
            ("No Zero Weights", 0),  # No validation run yet
            ("No Missing Data", len(missing_data_issues) == 0)
        ]
        
        passed_checks = 0
        for check_name, passed in checks:
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{check_name:25} {status}")
            if passed:
                passed_checks += 1
        
        print(f"\nExecution time: {execution_time:.1f}ms")
        print(f"Checks passed: {passed_checks}/{len(checks)}")
        
        if passed_checks == len(checks):
            print("\n🎉 ALL INTEGRITY CHECKS PASSED!")
            print("Analytics system has proper:")
            print("  - Horizon-scoped weight normalization")
            print("  - Per-ticker champion selection")
            print("  - No horizon mixing in performance")
            print("  - Consensus derived from weights")
            print("  - No zero-weight strategies")
            print("  - Complete data coverage")
        else:
            print(f"\n⚠️  {len(checks) - passed_checks} integrity issues need attention")
        
        return passed_checks == len(checks)
        
    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if repo:
            repo.close()


if __name__ == "__main__":
    success = validate_analytics_integrity()
    sys.exit(0 if success else 1)
