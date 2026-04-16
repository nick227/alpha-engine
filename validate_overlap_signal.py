import sqlite3
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import random

def get_price_data():
    """Get all price data for analysis"""
    conn = sqlite3.connect("data/alpha.db")
    query = "SELECT ticker, date, close FROM price_data ORDER BY ticker, date"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def multi_day_momentum_5d_fixed(df, hold_days=3, threshold=0.03):
    """Fixed 5-day momentum signal generation"""
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        ticker_df = ticker_df.reset_index(drop=True)
        
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
                        'trade_return': trade_return,
                        'signal_strength': row['return_5d']  # For deterministic sorting
                    })
    
    return results

def deterministic_overlap_control(signals):
    """Deterministic overlap control - no hindsight bias"""
    
    print("=== DETERMINISTIC OVERLAP CONTROL ===")
    
    # Sort by date, then by signal strength (deterministic)
    sorted_signals = sorted(signals, key=lambda x: (x['date'], -x['signal_strength']))
    
    selected_signals = []
    current_end_date = None
    
    for signal in sorted_signals:
        signal_date = signal['date']
        
        # Skip if overlapping
        if current_end_date and signal_date <= current_end_date:
            continue
        
        # Take this signal
        selected_signals.append(signal)
        
        # Set end date (3-day hold)
        current_end_date = signal_date + timedelta(days=3)
    
    print(f"Selected {len(selected_signals)} signals from {len(signals)} total")
    
    return selected_signals

def random_baseline_test(signals, num_trades=292, num_simulations=100):
    """Random baseline test - pick random trades"""
    
    print(f"=== RANDOM BASELINE TEST ===")
    print(f"Testing {num_simulations} random selections of {num_trades} trades\n")
    
    # Apply friction to all returns
    all_returns = [s['trade_return'] - 0.0015 for s in signals]
    
    random_results = []
    
    for i in range(num_simulations):
        # Random selection
        selected_returns = random.sample(all_returns, min(num_trades, len(all_returns)))
        
        # Calculate portfolio return
        portfolio_return = np.prod(1 + np.array(selected_returns)) - 1
        win_rate = sum(1 for r in selected_returns if r > 0) / len(selected_returns)
        
        random_results.append({
            'simulation': i + 1,
            'return': portfolio_return,
            'win_rate': win_rate
        })
    
    # Calculate statistics
    returns = [r['return'] for r in random_results]
    win_rates = [r['win_rate'] for r in random_results]
    
    avg_return = np.mean(returns)
    std_return = np.std(returns)
    avg_win_rate = np.mean(win_rates)
    
    print(f"Random baseline results:")
    print(f"  Average return: {avg_return:+.2%}")
    print(f"  Std deviation: {std_return:.2%}")
    print(f"  Average win rate: {avg_win_rate:.1%}")
    
    # Calculate percentiles
    p5 = np.percentile(returns, 5)
    p95 = np.percentile(returns, 95)
    
    print(f"  5th percentile: {p5:+.2%}")
    print(f"  95th percentile: {p95:+.2%}")
    
    return {
        'avg_return': avg_return,
        'std_return': std_return,
        'avg_win_rate': avg_win_rate,
        'p5': p5,
        'p95': p95,
        'results': random_results
    }

def time_shift_test_overlap(signals, shift_days=2):
    """Time-shift test on overlap-controlled signals"""
    
    print(f"=== TIME-SHIFT TEST (overlap-controlled, shift {shift_days} days) ===")
    
    # Get deterministic overlap-controlled signals
    selected_signals = deterministic_overlap_control(signals)
    
    # Group by date
    signals_by_date = {}
    for signal in selected_signals:
        date = signal['date']
        if date not in signals_by_date:
            signals_by_date[date] = []
        signals_by_date[date].append(signal)
    
    # Normal and shifted returns
    normal_returns = []
    shifted_returns = []
    
    for date in sorted(signals_by_date.keys()):
        day_signals = signals_by_date[date]
        
        # Normal test
        normal_trade_returns = []
        for signal in day_signals:
            trade_return = signal['trade_return'] - 0.0015
            normal_trade_returns.append(trade_return)
        
        if normal_trade_returns:
            normal_returns.append(np.mean(normal_trade_returns))
        
        # Time-shift test
        shifted_date = date + timedelta(days=shift_days)
        shifted_trade_returns = []
        
        for signal in day_signals:
            # Find shifted signal
            shifted_signals = [s for s in selected_signals if s['date'] == shifted_date and s['ticker'] == signal['ticker']]
            if shifted_signals:
                shifted_trade_returns.append(shifted_signals[0]['trade_return'] - 0.0015)
        
        if shifted_trade_returns:
            shifted_returns.append(np.mean(shifted_trade_returns))
    
    # Calculate portfolio returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    print(f"Normal portfolio return: {normal_portfolio:+.2%}")
    print(f"Time-shift portfolio return: {shifted_portfolio:+.2%}")
    print(f"Time-shift alpha collapse: {(normal_portfolio - shifted_portfolio):+.2%}")
    
    passes = abs(shifted_portfolio) <= 0.01
    print(f"TIME-SHIFT TEST: {'PASSED' if passes else 'FAILED'}")
    
    return normal_portfolio, shifted_portfolio, passes

def outlier_robustness_test(signals):
    """Test robustness by removing top outliers"""
    
    print("=== OUTLIER ROBUSTNESS TEST (overlap-controlled) ===")
    
    # Get deterministic overlap-controlled signals
    selected_signals = deterministic_overlap_control(signals)
    
    # Original returns
    original_returns = [s['trade_return'] - 0.0015 for s in selected_signals]
    original_win_rate = sum(1 for r in original_returns if r > 0) / len(original_returns)
    original_return = np.prod(1 + np.array(original_returns)) - 1
    
    print(f"Original: {len(selected_signals)} signals, {original_win_rate:.1%} win rate, {original_return:+.2%} return")
    
    # Remove top 5 winners
    sorted_returns = sorted(original_returns, reverse=True)
    top5_threshold = sorted_returns[4] if len(sorted_returns) >= 5 else sorted_returns[-1]
    
    filtered_returns = [r for r in original_returns if r <= top5_threshold]
    
    if filtered_returns:
        filtered_win_rate = sum(1 for r in filtered_returns if r > 0) / len(filtered_returns)
        filtered_return = np.prod(1 + np.array(filtered_returns)) - 1
        
        print(f"Without top 5: {len(filtered_returns)} signals, {filtered_win_rate:.1%} win rate, {filtered_return:+.2%} return")
        
        # Check robustness
        robust = filtered_win_rate > 0.52 and filtered_return > 0
        print(f"Robustness: {'ROBUST' if robust else 'FRAGILE'}")
        
        return {
            'original': {'signals': len(selected_signals), 'win_rate': original_win_rate, 'return': original_return},
            'filtered': {'signals': len(filtered_returns), 'win_rate': filtered_win_rate, 'return': filtered_return},
            'robust': robust
        }
    else:
        print("No returns after removing outliers")
        return None

def comprehensive_overlap_validation():
    """Comprehensive validation of overlap-controlled signal"""
    
    print("=== COMPREHENSIVE OVERLAP-CONTROLLED VALIDATION ===")
    
    # Get data and signals
    df = get_price_data()
    signals = multi_day_momentum_5d_fixed(df, 3, 0.03)
    
    print(f"Total signals: {len(signals)}")
    
    # Get overlap-controlled signals
    selected_signals = deterministic_overlap_control(signals)
    
    # Basic metrics
    trade_returns = [s['trade_return'] - 0.0015 for s in selected_signals]
    win_rate = sum(1 for r in trade_returns if r > 0) / len(trade_returns)
    total_return = np.prod(1 + np.array(trade_returns)) - 1
    
    print(f"Overlap-controlled: {len(selected_signals)} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
    
    # Test 1: Random baseline
    random_results = random_baseline_test(signals, len(selected_signals))
    
    # Test 2: Time-shift
    normal, shifted, time_shift_passes = time_shift_test_overlap(signals)
    
    # Test 3: Outlier robustness
    outlier_results = outlier_robustness_test(signals)
    
    # Final assessment
    print(f"\n=== FINAL ASSESSMENT ===")
    
    criteria_met = []
    
    # Random baseline comparison
    if total_return > random_results['p95']:  # Better than 95% of random
        criteria_met.append("Random baseline: EXCEEDS")
    elif total_return > random_results['avg_return']:
        criteria_met.append("Random baseline: ABOVE AVG")
    else:
        criteria_met.append("Random baseline: BELOW AVG")
    
    # Time-shift test
    if time_shift_passes:
        criteria_met.append("Time-shift: PASSED")
    else:
        criteria_met.append("Time-shift: FAILED")
    
    # Outlier robustness
    if outlier_results and outlier_results['robust']:
        criteria_met.append("Outlier robustness: GOOD")
    elif outlier_results and outlier_results['filtered']['return'] > 0:
        criteria_met.append("Outlier robustness: MARGINAL")
    else:
        criteria_met.append("Outlier robustness: POOR")
    
    # Win rate
    if win_rate > 0.55:
        criteria_met.append("Win rate: GOOD")
    elif win_rate > 0.52:
        criteria_met.append("Win rate: MARGINAL")
    else:
        criteria_met.append("Win rate: POOR")
    
    for criterion in criteria_met:
        print(f"  {criterion}")
    
    # Overall verdict
    critical_checks = [
        total_return > random_results['p95'],
        time_shift_passes,
        outlier_results and outlier_results['robust'] if outlier_results else False,
        win_rate > 0.52
    ]
    
    if all(critical_checks):
        print(f"\n>>> VERDICT: REAL EDGE CONFIRMED")
        print("Overlap-controlled signal passes all validation tests")
    elif sum(critical_checks) >= 3:
        print(f"\n>>> VERDICT: PROMISING EDGE")
        print("Signal shows potential but needs refinement")
    elif total_return > random_results['avg_return'] and time_shift_passes:
        print(f"\n>>> VERDICT: WEAK EDGE")
        print("Signal beats random but needs significant improvement")
    else:
        print(f"\n>>> VERDICT: NO EDGE")
        print("Signal fails validation tests")
    
    return {
        'signals': len(selected_signals),
        'win_rate': win_rate,
        'return': total_return,
        'random_results': random_results,
        'time_shift': {'normal': normal, 'shifted': shifted, 'passes': time_shift_passes},
        'outlier_results': outlier_results,
        'criteria': criteria_met
    }

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    comprehensive_overlap_validation()
