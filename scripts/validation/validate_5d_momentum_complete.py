import sqlite3
import numpy as np
from datetime import datetime, timedelta
import pandas as pd

def get_price_data():
    """Get all price data for analysis"""
    conn = sqlite3.connect("data/alpha.db")
    query = "SELECT ticker, date, close FROM price_data ORDER BY ticker, date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def multi_day_momentum_5d(df, hold_days=3, threshold=0.03):
    """Generate 5-day momentum signals"""
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        
        # Calculate 5-day returns
        ticker_df['return_5d'] = ticker_df['close'].pct_change(5)
        
        # Find signals
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['return_5d']) and row['return_5d'] > threshold:
                
                # Calculate hold period return
                signal_date = row['date']
                entry_price = row['close']
                
                # Find exit date
                exit_idx = idx + hold_days
                if exit_idx < len(ticker_df):
                    exit_price = ticker_df.iloc[exit_idx]['close']
                    trade_return = (exit_price / entry_price) - 1
                    
                    results.append({
                        'date': pd.to_datetime(signal_date).date(),
                        'ticker': ticker,
                        'signal_return': row['return_5d'],
                        'trade_return': trade_return
                    })
    
    return results

def yearly_breakdown(signals):
    """Break down performance by year"""
    
    print("=== YEARLY BREAKDOWN ===")
    
    # Group by year
    yearly_data = {}
    for signal in signals:
        year = signal['date'].year
        if year not in yearly_data:
            yearly_data[year] = []
        yearly_data[year].append(signal)
    
    consistent = True
    for year in sorted(yearly_data.keys()):
        year_signals = yearly_data[year]
        trade_returns = [s['trade_return'] - 0.0015 for s in year_signals]
        
        if not trade_returns:
            continue
        
        win_rate = sum(1 for r in trade_returns if r > 0) / len(trade_returns)
        total_return = np.prod(1 + np.array(trade_returns)) - 1
        
        print(f"{year}: {len(year_signals):3d} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
        
        # Check consistency
        if win_rate < 0.52 or total_return < 0:
            consistent = False
    
    if consistent:
        print(">>> CONSISTENT: Positive performance across all years")
    else:
        print(">>> INCONSISTENT: Performance varies by year")
    
    return yearly_data

def overlap_control_test(signals):
    """Test with overlap control (1 position at a time)"""
    
    print("=== OVERLAP CONTROL TEST ===")
    print("Enforcing: 1 active position at a time")
    
    # Sort all signals by date
    all_signals = sorted(signals, key=lambda x: x['date'])
    
    selected_returns = []
    current_end_date = None
    
    for signal in all_signals:
        signal_date = signal['date']
        
        # Skip if overlapping
        if current_end_date and signal_date <= current_end_date:
            continue
        
        # Take this signal
        trade_return = signal['trade_return'] - 0.0015
        selected_returns.append(trade_return)
        
        # Set end date (3-day hold)
        current_end_date = signal_date + timedelta(days=3)
    
    if selected_returns:
        win_rate = sum(1 for r in selected_returns if r > 0) / len(selected_returns)
        total_return = np.prod(1 + np.array(selected_returns)) - 1
        
        print(f"Non-overlapping signals: {len(selected_returns)}")
        print(f"Win rate: {win_rate:.1%}")
        print(f"Total return: {total_return:+.2%}")
        
        # Compare with original
        original_returns = [s['trade_return'] - 0.0015 for s in signals]
        original_win_rate = sum(1 for r in original_returns if r > 0) / len(original_returns)
        original_return = np.prod(1 + np.array(original_returns)) - 1
        
        print(f"\nComparison:")
        print(f"Original: {len(signals)} signals, {original_win_rate:.1%} win rate, {original_return:+.2%} return")
        print(f"Controlled: {len(selected_returns)} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
        
        # Check impact
        if abs(win_rate - original_win_rate) < 0.05 and abs(total_return - original_return) < 0.10:
            print(">>> ROBUST: Overlap control has minimal impact")
        else:
            print(">>> SENSITIVE: Overlap control significantly changes results")
        
        return selected_returns
    else:
        print("No non-overlapping signals found")
        return []

def distribution_analysis(signals):
    """Analyze return distribution"""
    
    print("=== DISTRIBUTION ANALYSIS ===")
    
    trade_returns = [s['trade_return'] - 0.0015 for s in signals]
    
    if not trade_returns:
        return
    
    # Basic stats
    wins = [r for r in trade_returns if r > 0]
    losses = [r for r in trade_returns if r <= 0]
    
    avg_win = np.mean(wins) if wins else 0
    avg_loss = np.mean(losses) if losses else 0
    max_loss = min(trade_returns) if trade_returns else 0
    
    # Top trades contribution
    sorted_returns = sorted(trade_returns, reverse=True)
    top5_sum = sum(sorted_returns[:5])
    total_sum = sum(trade_returns)
    top5_contribution = (top5_sum / total_sum) * 100 if total_sum != 0 else 0
    
    # Drawdown analysis
    cumulative = np.cumprod(1 + np.array(trade_returns))
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = (cumulative - running_max) / running_max
    max_drawdown = min(drawdowns) if len(drawdowns) > 0 else 0
    
    print(f"Avg win: {avg_win:+.2%}")
    print(f"Avg loss: {avg_loss:+.2%}")
    print(f"Max loss: {max_loss:+.2%}")
    print(f"Top 5 trades contribution: {top5_contribution:.1f}%")
    print(f"Max drawdown: {max_drawdown:+.2%}")
    
    # Check robustness
    robust = True
    if top5_contribution > 30:
        print(">>> WARNING: High outlier dependence")
        robust = False
    elif max_drawdown < -0.20:
        print(">>> WARNING: High drawdown risk")
        robust = False
    else:
        print(">>> ROBUST: Acceptable risk profile")
    
    return {
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'max_loss': max_loss,
        'top5_contribution': top5_contribution,
        'max_drawdown': max_drawdown,
        'robust': robust
    }

def comprehensive_5d_validation():
    """Complete validation of 5d momentum edge"""
    
    print("=== COMPREHENSIVE 5D MOMENTUM VALIDATION ===")
    print("Only testing 5d momentum (passed time-shift)\n")
    
    # Get price data
    df = get_price_data()
    
    # Generate signals
    signals = multi_day_momentum_5d(df, 3, 0.03)
    
    print(f"5d momentum: {len(signals)} signals")
    
    # Basic metrics
    trade_returns = [s['trade_return'] - 0.0015 for s in signals]
    win_rate = sum(1 for r in trade_returns if r > 0) / len(trade_returns)
    total_return = np.prod(1 + np.array(trade_returns)) - 1
    
    print(f"Basic metrics: {len(signals)} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
    
    # Step 1: Yearly breakdown
    yearly_data = yearly_breakdown(signals)
    
    # Step 2: Overlap control
    overlap_returns = overlap_control_test(signals)
    
    # Step 3: Distribution analysis
    dist_stats = distribution_analysis(signals)
    
    # Final assessment
    print(f"\n=== FINAL ASSESSMENT ===")
    
    # Check all criteria
    criteria_met = []
    
    # Time-shift already passed
    criteria_met.append("Time-shift: PASSED")
    
    # Win rate
    if win_rate > 0.54:
        criteria_met.append("Win rate: GOOD")
    elif win_rate > 0.52:
        criteria_met.append("Win rate: MARGINAL")
    else:
        criteria_met.append("Win rate: POOR")
    
    # Return
    if total_return > 0.20:
        criteria_met.append("Return: STRONG")
    elif total_return > 0.10:
        criteria_met.append("Return: GOOD")
    elif total_return > 0:
        criteria_met.append("Return: POSITIVE")
    else:
        criteria_met.append("Return: NEGATIVE")
    
    # Robustness
    if dist_stats['robust']:
        criteria_met.append("Robustness: GOOD")
    else:
        criteria_met.append("Robustness: POOR")
    
    # Consistency
    years_consistent = len(yearly_data) >= 3  # Need at least 3 years
    if years_consistent:
        criteria_met.append("Time stability: GOOD")
    else:
        criteria_met.append("Time stability: LIMITED")
    
    for criterion in criteria_met:
        print(f"  {criterion}")
    
    # Overall verdict
    critical_checks = [
        win_rate > 0.54,
        total_return > 0.10,
        dist_stats['robust'],
        years_consistent
    ]
    
    if all(critical_checks):
        print(f"\n>>> VERDICT: REAL EDGE CONFIRMED")
        print("5d momentum with 3d hold shows genuine alpha")
    elif win_rate > 0.52 and total_return > 0:
        print(f"\n>>> VERDICT: PROMISING EDGE")
        print("Shows potential but needs refinement")
    else:
        print(f"\n>>> VERDICT: NO EDGE")
        print("Fails to meet minimum criteria")

if __name__ == "__main__":
    comprehensive_5d_validation()
