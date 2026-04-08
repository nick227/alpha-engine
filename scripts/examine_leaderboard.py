import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.ui.middle.engine_read_store import EngineReadStore

def examine_leaderboard():
    store = EngineReadStore("data/alpha.db")
    
    print("--- Leaderboard Analysis: Canonical Alpha Strategy ---")
    
    # rank_strategies_by_efficiency now re-sorts by alpha_strategy internally
    rankings = store.rank_strategies_by_efficiency(limit=10)
    
    print(f"{'Strategy':<20} | {'Avg Eff':<10} | {'Alpha Strat':<12} | {'Win Rate':<8} | {'Stability':<10}")
    print("-" * 75)
    for r in rankings:
        # Check raw attributes
        print(f"DEBUG: {r.strategy_id} r.alpha_strategy={r.alpha_strategy}")
        print(f"{r.strategy_id:<20} | {r.avg_efficiency_rating:<10.4f} | {r.alpha_strategy:<12.4f} | {r.win_rate:<8.1%} | {r.stability:<10.2f}")

if __name__ == "__main__":
    examine_leaderboard()
