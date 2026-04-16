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
                        'signal_strength': row['return_5d']
                    })
    
    return results

def deterministic_overlap_control(signals):
    """Frozen deterministic overlap control rule"""
    
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
    
    return selected_signals

def walk_forward_validation(df, train_years=2, test_years=1):
    """Walk-forward validation with rolling windows"""
    
    print("=== WALK-FORWARD VALIDATION ===")
    print(f"Training window: {train_years} years")
    print(f"Testing window: {test_years} years")
    print(f"Rolling window analysis\n")
    
    # Convert dates
    df['date'] = pd.to_datetime(df['date'])
    
    # Get date range
    min_date = df['date'].min()
    max_date = df['date'].max()
    
    # Create rolling windows
    windows = []
    current_date = min_date
    
    while current_date + timedelta(days=train_years*365 + test_years*365) <= max_date:
        train_start = current_date
        train_end = current_date + timedelta(days=train_years*365)
        test_start = train_end
        test_end = test_start + timedelta(days=test_years*365)
        
        windows.append({
            'train_start': train_start,
            'train_end': train_end,
            'test_start': test_start,
            'test_end': test_end
        })
        
        # Move to next window (6-month rolling)
        current_date = current_date + timedelta(days=180)
    
    print(f"Created {len(windows)} rolling windows")
    
    # Run walk-forward test
    window_results = []
    
    for i, window in enumerate(windows):
        print(f"\n--- Window {i+1} ---")
        print(f"Train: {window['train_start'].strftime('%Y-%m-%d')} to {window['train_end'].strftime('%Y-%m-%d')}")
        print(f"Test:  {window['test_start'].strftime('%Y-%m-%d')} to {window['test_end'].strftime('%Y-%m-%d')}")
        
        # Split data
        train_data = df[(df['date'] >= window['train_start']) & (df['date'] <= window['train_end'])]
        test_data = df[(df['date'] >= window['test_start']) & (df['date'] <= window['test_end'])]
        
        # Generate signals on test data (using frozen rule)
        test_signals = multi_day_momentum_5d_fixed(test_data, 3, 0.03)
        
        if len(test_signals) < 10:
            print(f"Insufficient signals: {len(test_signals)}")
            continue
        
        # Apply overlap control
        selected_signals = deterministic_overlap_control(test_signals)
        
        if len(selected_signals) < 5:
            print(f"Insufficient selected signals: {len(selected_signals)}")
            continue
        
        # Calculate performance
        trade_returns = [s['trade_return'] - 0.0015 for s in selected_signals]
        win_rate = sum(1 for r in trade_returns if r > 0) / len(trade_returns)
        total_return = np.prod(1 + np.array(trade_returns)) - 1
        
        window_results.append({
            'window': i + 1,
            'train_start': window['train_start'],
            'train_end': window['train_end'],
            'test_start': window['test_start'],
            'test_end': window['test_end'],
            'signals': len(selected_signals),
            'win_rate': win_rate,
            'return': total_return
        })
        
        print(f"Signals: {len(selected_signals)}, Win rate: {win_rate:.1%}, Return: {total_return:+.2%}")
    
    return window_results

def yearly_regime_breakdown(df):
    """Yearly and regime breakdown analysis"""
    
    print("\n=== YEARLY AND REGIME BREAKDOWN ===")
    
    # Generate signals
    signals = multi_day_momentum_5d_fixed(df, 3, 0.03)
    
    # Apply overlap control
    selected_signals = deterministic_overlap_control(signals)
    
    # Convert to DataFrame
    signals_df = pd.DataFrame(selected_signals)
    signals_df['date'] = pd.to_datetime(signals_df['date'])
    
    # Add year column
    signals_df['year'] = signals_df['date'].dt.year
    
    # Calculate market regime (simplified)
    # Use equal-weighted market return
    market_data = df.groupby('date')['close'].mean().reset_index()
    market_data['date'] = pd.to_datetime(market_data['date'])
    market_data = market_data.sort_values('date')
    market_data['market_return'] = market_data['close'].pct_change()
    market_data['cumulative_return'] = (1 + market_data['market_return']).cumprod()
    
    # Classify regimes
    regime_data = []
    for date in market_data['date']:
        if pd.notna(date):
            # Get 60-day rolling return
            date_mask = market_data['date'] <= date
            recent_data = market_data[date_mask].tail(60)
            
            if len(recent_data) >= 30:
                recent_return = recent_data['cumulative_return'].iloc[-1] / recent_data['cumulative_return'].iloc[0] - 1
                annualized_return = (1 + recent_return) ** (365/len(recent_data)) - 1
                
                if annualized_return > 0.15:
                    regime = 'bull'
                elif annualized_return < -0.10:
                    regime = 'bear'
                else:
                    regime = 'neutral'
            else:
                regime = 'unknown'
            
            regime_data.append({'date': date, 'regime': regime})
    
    regime_df = pd.DataFrame(regime_data)
    
    # Merge signals with regimes
    signals_with_regime = signals_df.merge(regime_df, on='date', how='left')
    
    # Yearly breakdown
    print("Yearly Performance:")
    yearly_results = {}
    
    for year in sorted(signals_with_regime['year'].unique()):
        year_data = signals_with_regime[signals_with_regime['year'] == year]
        
        if len(year_data) < 5:
            continue
        
        trade_returns = year_data['trade_return'] - 0.0015
        win_rate = (trade_returns > 0).mean()
        total_return = np.prod(1 + trade_returns) - 1
        
        yearly_results[year] = {
            'signals': len(year_data),
            'win_rate': win_rate,
            'return': total_return
        }
        
        print(f"  {year}: {len(year_data):3d} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
    
    # Regime breakdown
    print("\nRegime Performance:")
    regime_results = {}
    
    for regime in ['bull', 'bear', 'neutral']:
        regime_data = signals_with_regime[signals_with_regime['regime'] == regime]
        
        if len(regime_data) < 5:
            continue
        
        trade_returns = regime_data['trade_return'] - 0.0015
        win_rate = (trade_returns > 0).mean()
        total_return = np.prod(1 + trade_returns) - 1
        
        regime_results[regime] = {
            'signals': len(regime_data),
            'win_rate': win_rate,
            'return': total_return
        }
        
        print(f"  {regime.upper():7s}: {len(regime_data):3d} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
    
    return yearly_results, regime_results

def friction_slippage_stress_test(signals):
    """Stress test with realistic friction and slippage"""
    
    print("\n=== FRICTION AND SLIPPAGE STRESS TEST ===")
    
    # Test different friction levels
    friction_levels = [0.0015, 0.003, 0.005, 0.0075, 0.01]  # 0.15% to 1.0%
    
    results = {}
    
    for friction in friction_levels:
        # Apply overlap control
        selected_signals = deterministic_overlap_control(signals)
        
        # Calculate returns with friction
        trade_returns = [s['trade_return'] - friction for s in selected_signals]
        
        if len(trade_returns) < 10:
            continue
        
        win_rate = sum(1 for r in trade_returns if r > 0) / len(trade_returns)
        total_return = np.prod(1 + np.array(trade_returns)) - 1
        
        results[friction] = {
            'signals': len(selected_signals),
            'win_rate': win_rate,
            'return': total_return
        }
        
        print(f"  {friction*100:.1f}% friction: {len(selected_signals):3d} signals, {win_rate:.1%} win rate, {total_return:+.2%} return")
    
    # Find break-even point
    break_even_friction = None
    for friction, result in results.items():
        if result['return'] <= 0:
            break_even_friction = friction
            break
    
    if break_even_friction:
        print(f"\nBreak-even friction: {break_even_friction*100:.1f}%")
    else:
        print(f"\nRemains positive even at 1.0% friction")
    
    return results

def comprehensive_out_of_sample_validation():
    """Comprehensive out-of-sample validation"""
    
    print("=== COMPREHENSIVE OUT-OF-SAMPLE VALIDATION ===")
    print("Testing overlap-controlled 5d momentum with strict validation\n")
    
    # Get data
    df = get_price_data()
    
    # Generate signals
    signals = multi_day_momentum_5d_fixed(df, 3, 0.03)
    
    print(f"Total signals: {len(signals)}")
    
    # Test 1: Walk-forward validation
    walk_forward_results = walk_forward_validation(df)
    
    # Test 2: Yearly and regime breakdown
    yearly_results, regime_results = yearly_regime_breakdown(df)
    
    # Test 3: Friction stress test
    friction_results = friction_slippage_stress_test(signals)
    
    # Final assessment
    print(f"\n=== FINAL OUT-OF-SAMPLE ASSESSMENT ===")
    
    criteria_met = []
    
    # Walk-forward consistency
    if walk_forward_results:
        positive_windows = sum(1 for w in walk_forward_results if w['return'] > 0)
        consistency = positive_windows / len(walk_forward_results)
        
        if consistency >= 0.7:
            criteria_met.append("Walk-forward: CONSISTENT")
        elif consistency >= 0.5:
            criteria_met.append("Walk-forward: MIXED")
        else:
            criteria_met.append("Walk-forward: INCONSISTENT")
        
        print(f"Walk-forward: {positive_windows}/{len(walk_forward_results)} windows positive ({consistency:.1%})")
    else:
        criteria_met.append("Walk-forward: INSUFFICIENT DATA")
    
    # Yearly stability
    if yearly_results:
        positive_years = sum(1 for y in yearly_results.values() if y['return'] > 0)
        year_consistency = positive_years / len(yearly_results)
        
        if year_consistency >= 0.6:
            criteria_met.append("Yearly: STABLE")
        elif year_consistency >= 0.4:
            criteria_met.append("Yearly: MIXED")
        else:
            criteria_met.append("Yearly: UNSTABLE")
        
        print(f"Yearly: {positive_years}/{len(yearly_results)} years positive ({year_consistency:.1%})")
    
    # Regime performance
    if regime_results.get('bull', {}).get('return', 0) > 0:
        criteria_met.append("Regime: BULL POSITIVE")
    else:
        criteria_met.append("Regime: BULL NEGATIVE")
    
    # Friction tolerance
    if friction_results:
        # Check if positive at 0.5% friction
        if friction_results.get(0.005, {}).get('return', 0) > 0:
            criteria_met.append("Friction: TOLERANT")
        else:
            criteria_met.append("Friction: SENSITIVE")
    
    for criterion in criteria_met:
        print(f"  {criterion}")
    
    # Overall verdict
    print(f"\n=== DEPLOYMENT READINESS ===")
    
    if walk_forward_results and yearly_results:
        walk_forward_consistency = sum(1 for w in walk_forward_results if w['return'] > 0) / len(walk_forward_results)
        yearly_consistency = sum(1 for y in yearly_results.values() if y['return'] > 0) / len(yearly_results)
        friction_tolerance = friction_results.get(0.005, {}).get('return', 0) > 0
        
        if walk_forward_consistency >= 0.7 and yearly_consistency >= 0.6 and friction_tolerance:
            print(">>> READY FOR DEPLOYMENT REFINEMENT")
            print("Signal shows consistent out-of-sample performance")
        elif walk_forward_consistency >= 0.5 and yearly_consistency >= 0.4:
            print(">>> PROMISING - NEEDS REFINEMENT")
            print("Signal shows potential but inconsistent performance")
        else:
            print(">>> NOT READY FOR DEPLOYMENT")
            print("Signal fails out-of-sample validation")
    else:
        print(">>> INSUFFICIENT DATA FOR VALIDATION")
    
    return {
        'walk_forward': walk_forward_results,
        'yearly': yearly_results,
        'regime': regime_results,
        'friction': friction_results,
        'criteria': criteria_met
    }

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    comprehensive_out_of_sample_validation()
