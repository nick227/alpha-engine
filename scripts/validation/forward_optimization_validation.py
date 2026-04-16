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
    """Fixed 5-day momentum signal generation (FROZEN RULE)"""
    
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
                        'signal_strength': row['return_5d'],
                        'entry_price': entry_price,
                        'exit_price': exit_price
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

def realistic_slippage_model(trade_return, ticker, trade_size, market_volatility):
    """Realistic slippage model (same as optimization)"""
    
    # Base slippage components
    base_spread = 0.0008  # 0.08% base spread
    impact_factor = 0.00015  # 0.015% per $1M traded
    gap_risk = 0.0002  # 0.02% gap risk
    
    # Adjust for volatility
    volatility_multiplier = 1 + (market_volatility - 0.2)
    size_multiplier = 1 + (trade_size / 1000000)
    
    # Calculate total slippage
    total_slippage = (base_spread + impact_factor * trade_size / 1000000 + gap_risk) * volatility_multiplier * size_multiplier
    
    # Apply slippage to return
    adjusted_return = trade_return - total_slippage
    
    return adjusted_return, total_slippage

def calculate_volatility_sizing(trade, base_size=0.1):
    """Calculate volatility-adjusted position size"""
    
    # Use signal strength as volatility proxy
    signal_vol = trade['signal_strength']
    
    # Higher momentum = higher volatility = smaller position
    vol_adjustment = 1 / (1 + signal_vol * 10)
    position_size = base_size * vol_adjustment
    
    return position_size

def apply_stop_loss(trade_return, stop_level=0.02):
    """Apply stop-loss rule"""
    
    if trade_return < -stop_level:
        return -stop_level
    else:
        return trade_return

def forward_validation_comparison(df, start_date, end_date):
    """Forward validation comparing base vs optimized"""
    
    print(f"=== FORWARD VALIDATION: {start_date} to {end_date} ===")
    
    # Filter to forward period
    forward_data = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    
    if len(forward_data) == 0:
        print("No data for forward period")
        return None
    
    # Generate signals (frozen rule)
    signals = multi_day_momentum_5d_fixed(forward_data, 3, 0.03)
    
    if len(signals) < 10:
        print(f"Insufficient signals: {len(signals)}")
        return None
    
    # Apply overlap control (frozen rule)
    selected_signals = deterministic_overlap_control(signals)
    
    print(f"Forward period signals: {len(selected_signals)}")
    
    # Prepare trade details
    trade_details = []
    
    for signal in selected_signals:
        # Estimate market volatility
        ticker_data = forward_data[forward_data['ticker'] == signal['ticker']]
        if len(ticker_data) > 20:
            daily_returns = ticker_data['close'].pct_change().dropna()
            market_volatility = daily_returns.std() * np.sqrt(252)
        else:
            market_volatility = 0.25
        
        # Apply realistic slippage
        adjusted_return, slippage = realistic_slippage_model(
            signal['trade_return'], 
            signal['ticker'], 
            1000000 / len(selected_signals), 
            market_volatility
        )
        
        trade_details.append({
            'date': signal['date'],
            'ticker': signal['ticker'],
            'raw_return': signal['trade_return'],
            'adjusted_return': adjusted_return,
            'slippage': slippage,
            'signal_strength': signal['signal_strength']
        })
    
    # Test 1: Base signal (fixed sizing, no stop)
    print(f"\n--- BASE SIGNAL ---")
    base_returns = []
    
    for trade in trade_details:
        # Fixed 10% sizing
        base_sized_return = trade['adjusted_return'] * 0.1
        base_returns.append(base_sized_return)
    
    base_portfolio_return = sum(base_returns)
    base_win_rate = sum(1 for r in base_returns if r > 0) / len(base_returns)
    
    print(f"Base portfolio return: {base_portfolio_return:+.2%}")
    print(f"Base win rate: {base_win_rate:.1%}")
    
    # Test 2: Volatility sizing (no stop)
    print(f"\n--- VOLATILITY SIZING ---")
    vol_returns = []
    
    for trade in trade_details:
        # Volatility-adjusted sizing
        position_size = calculate_volatility_sizing(trade, 0.1)
        vol_sized_return = trade['adjusted_return'] * position_size
        vol_returns.append(vol_sized_return)
    
    vol_portfolio_return = sum(vol_returns)
    vol_win_rate = sum(1 for r in vol_returns if r > 0) / len(vol_returns)
    
    print(f"Volatility portfolio return: {vol_portfolio_return:+.2%}")
    print(f"Volatility win rate: {vol_win_rate:.1%}")
    
    # Test 3: Base signal + 2% stop
    print(f"\n--- BASE + 2% STOP ---")
    base_stop_returns = []
    
    for trade in trade_details:
        # Apply stop-loss
        stopped_return = apply_stop_loss(trade['adjusted_return'], 0.02)
        base_stop_sized_return = stopped_return * 0.1
        base_stop_returns.append(base_stop_sized_return)
    
    base_stop_portfolio_return = sum(base_stop_returns)
    base_stop_win_rate = sum(1 for r in base_stop_returns if r > 0) / len(base_stop_returns)
    
    print(f"Base + stop portfolio return: {base_stop_portfolio_return:+.2%}")
    print(f"Base + stop win rate: {base_stop_win_rate:.1%}")
    
    # Test 4: Volatility sizing + 2% stop (OPTIMIZED)
    print(f"\n--- OPTIMIZED (VOL SIZING + 2% STOP) ---")
    optimized_returns = []
    stop_frequency = 0
    
    for trade in trade_details:
        # Apply stop-loss
        stopped_return = apply_stop_loss(trade['adjusted_return'], 0.02)
        
        # Check if stop was triggered
        if stopped_return == -0.02:
            stop_frequency += 1
        
        # Volatility-adjusted sizing
        position_size = calculate_volatility_sizing(trade, 0.1)
        optimized_sized_return = stopped_return * position_size
        optimized_returns.append(optimized_sized_return)
    
    optimized_portfolio_return = sum(optimized_returns)
    optimized_win_rate = sum(1 for r in optimized_returns if r > 0) / len(optimized_returns)
    stop_freq_pct = stop_frequency / len(optimized_returns)
    
    print(f"Optimized portfolio return: {optimized_portfolio_return:+.2%}")
    print(f"Optimized win rate: {optimized_win_rate:.1%}")
    print(f"Stop frequency: {stop_freq_pct:.1%}")
    
    # Drawdown analysis
    def calculate_drawdown(returns):
        if not returns:
            return 0
        
        cumulative = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = cumulative - running_max
        max_drawdown = min(drawdowns) if len(drawdowns) > 0 else 0
        
        return max_drawdown
    
    base_drawdown = calculate_drawdown(base_returns)
    vol_drawdown = calculate_drawdown(vol_returns)
    base_stop_drawdown = calculate_drawdown(base_stop_returns)
    optimized_drawdown = calculate_drawdown(optimized_returns)
    
    print(f"\n--- DRAWDOWN COMPARISON ---")
    print(f"Base max drawdown: {base_drawdown:+.2%}")
    print(f"Volatility max drawdown: {vol_drawdown:+.2%}")
    print(f"Base + stop max drawdown: {base_stop_drawdown:+.2%}")
    print(f"Optimized max drawdown: {optimized_drawdown:+.2%}")
    
    # Comparison results
    results = {
        'base': {
            'return': base_portfolio_return,
            'win_rate': base_win_rate,
            'drawdown': base_drawdown,
            'returns': base_returns
        },
        'volatility': {
            'return': vol_portfolio_return,
            'win_rate': vol_win_rate,
            'drawdown': vol_drawdown,
            'returns': vol_returns
        },
        'base_stop': {
            'return': base_stop_portfolio_return,
            'win_rate': base_stop_win_rate,
            'drawdown': base_stop_drawdown,
            'returns': base_stop_returns
        },
        'optimized': {
            'return': optimized_portfolio_return,
            'win_rate': optimized_win_rate,
            'drawdown': optimized_drawdown,
            'returns': optimized_returns,
            'stop_frequency': stop_freq_pct
        }
    }
    
    return results

def forward_validation_pipeline():
    """Forward validation pipeline for optimization testing"""
    
    print("=== FORWARD OPTIMIZATION VALIDATION ===")
    print("Testing optimized spec vs base on fresh unseen period\n")
    
    # Get data
    df = get_price_data()
    df['date'] = pd.to_datetime(df['date'])
    
    # Use fresh forward period (last 3 months not used in optimization)
    max_date = df['date'].max()
    forward_start = max_date - timedelta(days=90)  # Last 3 months
    forward_end = max_date
    
    print(f"Forward validation period: {forward_start.strftime('%Y-%m-%d')} to {forward_end.strftime('%Y-%m-%d')}")
    print(f"Duration: {(forward_end - forward_start).days} days")
    print(f"This period was NOT used in optimization\n")
    
    # Run forward validation
    results = forward_validation_comparison(df, forward_start, forward_end)
    
    if not results:
        print("Forward validation failed")
        return None
    
    # Analysis
    print(f"\n=== FORWARD VALIDATION ANALYSIS ===")
    
    # Performance comparison
    print(f"PERFORMANCE COMPARISON:")
    print(f"  Base:          {results['base']['return']:+.2%} return, {results['base']['win_rate']:.1%} win rate")
    print(f"  Volatility:      {results['volatility']['return']:+.2%} return, {results['volatility']['win_rate']:.1%} win rate")
    print(f"  Base + Stop:   {results['base_stop']['return']:+.2%} return, {results['base_stop']['win_rate']:.1%} win rate")
    print(f"  Optimized:      {results['optimized']['return']:+.2%} return, {results['optimized']['win_rate']:.1%} win rate")
    
    # Drawdown comparison
    print(f"\nDRAWDOWN COMPARISON:")
    print(f"  Base:          {results['base']['drawdown']:+.2%}")
    print(f"  Volatility:      {results['volatility']['drawdown']:+.2%}")
    print(f"  Base + Stop:   {results['base_stop']['drawdown']:+.2%}")
    print(f"  Optimized:      {results['optimized']['drawdown']:+.2%}")
    
    # Optimization effectiveness
    print(f"\nOPTIMIZATION EFFECTIVENESS:")
    
    # Does optimized beat base?
    optimized_beats_base = results['optimized']['return'] > results['base']['return']
    print(f"  Optimized beats base: {'YES' if optimized_beats_base else 'NO'}")
    
    # Does optimized reduce drawdown?
    reduces_drawdown = abs(results['optimized']['drawdown']) < abs(results['base']['drawdown'])
    print(f"  Reduces drawdown: {'YES' if reduces_drawdown else 'NO'}")
    
    # Does optimized maintain EV?
    maintains_ev = results['optimized']['return'] > 0
    print(f"  Maintains positive EV: {'YES' if maintains_ev else 'NO'}")
    
    # Overall assessment
    success_criteria = [optimized_beats_base, reduces_drawdown, maintains_ev]
    success_rate = sum(success_criteria) / len(success_criteria)
    
    print(f"\nSUCCESS RATE: {success_rate:.1%} ({sum(success_criteria)}/{len(success_criteria)} criteria)")
    
    if success_rate >= 0.67:  # 2/3 criteria
        print(">>> OPTIMIZATION SUCCESSFUL")
        print("Optimized version outperforms base in forward validation")
    elif success_rate >= 0.33:  # 1/3 criteria
        print(">>> OPTIMIZATION PARTIAL")
        print("Optimized version shows some improvement")
    else:
        print(">>> OPTIMIZATION FAILED")
        print("Optimized version underperforms base")
    
    return {
        'results': results,
        'success_rate': success_rate,
        'criteria_met': success_criteria
    }

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    forward_validation_pipeline()
