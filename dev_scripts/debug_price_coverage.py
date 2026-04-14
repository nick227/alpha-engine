import sqlite3
from datetime import datetime, timezone, timedelta

def debug_price_coverage():
    """Debug price data coverage for discovery symbols."""
    conn = sqlite3.connect("data/alpha.db")
    conn.row_factory = sqlite3.Row
    
    target_date = datetime(2026, 3, 25, tzinfo=timezone.utc)
    expiry_date = target_date + timedelta(days=5)
    
    print(f"Checking price coverage for {expiry_date.date()}")
    
    # Get discovery candidates
    candidates = conn.execute("""
        SELECT symbol, strategy_type, score FROM discovery_candidates
        WHERE as_of_date = ?
        ORDER BY score DESC
        LIMIT 20
    """, (target_date.date(),)).fetchall()
    
    print(f"\nTop 20 discovery candidates:")
    for c in candidates:
        print(f"  {c['symbol']}: {c['strategy_type']} (score: {c['score']:.3f})")
        
        # Check price data
        price_check = conn.execute("""
            SELECT COUNT(*) FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) = ?
        """, (c['symbol'], expiry_date.date())).fetchone()[0]
        
        # Check price data within 3 days
        price_check_plus = conn.execute("""
            SELECT COUNT(*) FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ? AND DATE(timestamp) BETWEEN ? AND ?
        """, (c['symbol'], expiry_date.date(), (expiry_date + timedelta(days=3)).date())).fetchone()[0]
        
        print(f"    Price on {expiry_date.date()}: {price_check} bars")
        print(f"    Price within 3 days: {price_check_plus} bars")
        
        # Check if symbol exists in price_bars at all
        total_bars = conn.execute("""
            SELECT COUNT(*) FROM price_bars 
            WHERE tenant_id = 'ml_train' AND ticker = ?
        """, (c['symbol'],)).fetchone()[0]
        
        print(f"    Total price bars: {total_bars}")
        print()
    
    # Check what symbols DO have price data on expiry date
    print(f"\nSymbols with price data on {expiry_date.date()}:")
    price_symbols = conn.execute("""
        SELECT DISTINCT ticker FROM price_bars 
        WHERE tenant_id = 'ml_train' AND DATE(timestamp) = ?
        LIMIT 20
    """, (expiry_date.date(),)).fetchall()
    
    for s in price_symbols:
        print(f"  {s['ticker']}")
    
    # Check price data date range
    print(f"\nPrice data date range:")
    date_range = conn.execute("""
        SELECT 
            MIN(DATE(timestamp)) as min_date,
            MAX(DATE(timestamp)) as max_date,
            COUNT(DISTINCT DATE(timestamp)) as unique_dates
        FROM price_bars 
        WHERE tenant_id = 'ml_train'
    """).fetchone()
    
    print(f"  Range: {date_range['min_date']} to {date_range['max_date']}")
    print(f"  Unique dates: {date_range['unique_dates']}")
    
    conn.close()

if __name__ == "__main__":
    debug_price_coverage()
