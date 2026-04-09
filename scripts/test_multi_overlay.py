import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.ui.middle.dashboard_service import DashboardService

def test_multi_overlay():
    service = DashboardService(db_path="data/alpha.db")
    
    # DEBUG: Check tenants
    tenants = service.store.list_tenants()
    print(f"DEBUG: Available tenants: {tenants}")
    target_tenant = "backfill" if "backfill" in tenants else "default"
    print(f"DEBUG: Using tenant: {target_tenant}")

    # 1. Fetch available runs/tickers
    tickers = service.store.list_tickers(tenant_id=target_tenant)
    if not tickers:
        print("No tickers found for overlay test.")
        return
    
    # Get a valid run_id
    runs = service.store.list_prediction_runs(tenant_id="backfill", limit=1)
    if not runs:
        print("No prediction runs found.")
        return
    run_id = runs[0].id
    
    ticker = tickers[0]
    # Fetch available strategies for this run/ticker
    strat_rows = service.store.conn.execute(
        "SELECT DISTINCT strategy_id FROM predicted_series_points WHERE run_id = ? AND ticker = ?",
        (run_id, ticker)
    ).fetchall()
    strategies = [str(r["strategy_id"]) for r in strat_rows]
    
    if not strategies:
        print(f"No strategies found for run_id={run_id} ticker={ticker}")
        return
    
    print(f"DEBUG: Found strategies: {strategies}")
    
    print(f"--- Overlay Test: {ticker} (run: {run_id[:8]}) ---")
    overlay = service.get_multi_strategy_overlay(
        run_id=run_id,
        ticker=ticker,
        strategy_ids=strategies,
        tenant_id="backfill"
    )
    
    # Correct the field names from the orchestrator's output
    print(f"Found {len(overlay['actual'])} actual price points.")
    print(f"Found {len(overlay['strategies'])} strategy forecast series.")
    
    for s_data in overlay['strategies']:
        sid = s_data["strategy_id"]
        points = s_data["predicted"]
        print(f"  Strategy: {sid} | Forecast Points: {len(points)}")
        if points:
            print(f"    Sample: {points[0]['x']} -> {points[0]['y']:.4f}")

if __name__ == "__main__":
    test_multi_overlay()
