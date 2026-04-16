"""
Fix for feature leakage in feature_snapshot.py

The issue: Line 83 includes current day data in feature calculation
Fix: Use as_of_date - 1 day to ensure no look-ahead bias
"""

def fix_feature_query():
    """Generate the corrected SQL query for point-in-time features"""
    
    corrected_query = """
    -- CORRECTED: Use data up to previous day only
    SELECT ticker, timestamp, close, volume
    FROM price_bars
    WHERE tenant_id = ? AND timeframe = ?
      AND DATE(timestamp) >= ? AND DATE(timestamp) <= DATE(?, '-1 day')
    ORDER BY ticker ASC, timestamp ASC
    """
    
    explanation = """
    BEFORE (leaky):
      AND DATE(timestamp) <= ?  -- Includes current day
    
    AFTER (correct):
      AND DATE(timestamp) <= DATE(?, '-1 day')  -- Excludes current day
    
    This ensures features at time T only use data <= T-1
    """
    
    print("=== FEATURE LEAKAGE FIX ===")
    print("Problem: Current feature builder includes today's price in calculations")
    print("Solution: Exclude current day from feature data")
    print("\nCorrected SQL:")
    print(corrected_query)
    print("\nExplanation:")
    print(explanation)
    
    return corrected_query

if __name__ == "__main__":
    fix_feature_query()
