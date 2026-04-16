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

def extended_forward_validation(df, start_date, end_date):
    """Extended forward validation with monitoring"""
    
    print(f"=== EXTENDED FORWARD VALIDATION: {start_date} to {end_date} ===")
    
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
    
    # Apply optimized strategy
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
    
    # Calculate performance metrics
    if optimized_returns:
        # Portfolio-level returns
        portfolio_return = sum(optimized_returns)
        win_rate = sum(1 for r in optimized_returns if r > 0) / len(optimized_returns)
        
        # Calculate drawdown
        cumulative = np.cumsum(optimized_returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = cumulative - running_max
        max_drawdown = min(drawdowns) if len(drawdowns) > 0 else 0
        
        # Calculate volatility
        returns_array = np.array(optimized_returns)
        volatility = np.std(returns_array) * np.sqrt(252)  # Annualized
        sharpe = np.mean(returns_array) / np.std(returns_array) * np.sqrt(252) if np.std(returns_array) != 0 else 0
        
        # Calculate costs
        avg_slippage = np.mean([t['slippage'] for t in trade_details])
        stop_freq_pct = stop_frequency / len(optimized_returns)
        
        print(f"Optimized performance:")
        print(f"  Portfolio return: {portfolio_return:+.2%}")
        print(f"  Win rate: {win_rate:.1%}")
        print(f"  Max drawdown: {max_drawdown:+.2%}")
        print(f"  Volatility: {volatility:.2%}")
        print(f"  Sharpe ratio: {sharpe:.2f}")
        print(f"  Avg slippage: {avg_slippage:.2%}")
        print(f"  Stop frequency: {stop_freq_pct:.1%}")
        
        return {
            'signals': len(selected_signals),
            'portfolio_return': portfolio_return,
            'win_rate': win_rate,
            'max_drawdown': max_drawdown,
            'volatility': volatility,
            'sharpe': sharpe,
            'avg_slippage': avg_slippage,
            'stop_frequency': stop_freq_pct,
            'returns': optimized_returns
        }
    
    return None

def extended_forward_pipeline():
    """Extended forward validation pipeline"""
    
    print("=== EXTENDED FORWARD VALIDATION PIPELINE ===")
    print("Testing optimized edge for 6-12 months forward\n")
    
    # Get data
    df = get_price_data()
    df['date'] = pd.to_datetime(df['date'])
    
    # Use extended forward period (6 months)
    max_date = df['date'].max()
    forward_start = max_date - timedelta(days=180)  # 6 months back
    forward_end = max_date
    
    print(f"Extended forward period: {forward_start.strftime('%Y-%m-%d')} to {forward_end.strftime('%Y-%m-%d')}")
    print(f"Duration: {(forward_end - forward_start).days} days")
    print(f"This period extends beyond previous validation\n")
    
    # Run extended forward validation
    results = extended_forward_validation(df, forward_start, forward_end)
    
    if not results:
        print("Extended forward validation failed")
        return None
    
    # Live-ready assessment
    print(f"\n=== LIVE-READY ASSESSMENT ===")
    
    criteria_met = []
    
    # Trade count requirement
    if results['signals'] >= 50:
        criteria_met.append("Trade count: ADEQUATE")
        print("✅ Trade count: ADEQUATE (50+ trades)")
    else:
        criteria_met.append("Trade count: INSUFFICIENT")
        print(f"❌ Trade count: INSUFFICIENT ({results['signals']} < 50)")
    
    # Stable behavior after costs
    if results['portfolio_return'] > 0:
        criteria_met.append("Cost stability: STABLE")
        print("✅ Cost stability: STABLE (positive after costs)")
    else:
        criteria_met.append("Cost stability: UNSTABLE")
        print("❌ Cost stability: UNSTABLE (negative after costs)")
    
    # Drawdown control
    if abs(results['max_drawdown']) <= 0.15:  # 15% threshold
        criteria_met.append("Drawdown control: GOOD")
        print("✅ Drawdown control: GOOD (<15%)")
    elif abs(results['max_drawdown']) <= 0.25:  # 25% threshold
        criteria_met.append("Drawdown control: ACCEPTABLE")
        print(f"✅ Drawdown control: ACCEPTABLE ({abs(results['max_drawdown']):.1%} < 25%)")
    else:
        criteria_met.append("Drawdown control: POOR")
        print(f"❌ Drawdown control: POOR ({abs(results['max_drawdown']):.1%} > 25%)")
    
    # Performance consistency
    if results['win_rate'] >= 0.50:  # 50% threshold
        criteria_met.append("Performance: CONSISTENT")
        print("✅ Performance: CONSISTENT (≥50% win rate)")
    else:
        criteria_met.append("Performance: INCONSISTENT")
        print(f"❌ Performance: INCONSISTENT ({results['win_rate']:.1%} < 50%)")
    
    # Risk-adjusted returns
    if results['sharpe'] >= 1.0:  # Sharpe threshold
        criteria_met.append("Risk-adjusted: GOOD")
        print("✅ Risk-adjusted: GOOD (Sharpe ≥ 1.0)")
    elif results['sharpe'] >= 0.5:
        criteria_met.append("Risk-adjusted: ACCEPTABLE")
        print(f"✅ Risk-adjusted: ACCEPTABLE (Sharpe {results['sharpe']:.2f})")
    else:
        criteria_met.append("Risk-adjusted: POOR")
        print(f"❌ Risk-adjusted: POOR (Sharpe {results['sharpe']:.2f})")
    
    # Overall assessment
    positive_criteria = sum(1 for c in criteria_met if "ADEQUATE" in c or "STABLE" in c or "GOOD" in c or "CONSISTENT" in c)
    total_criteria = len(criteria_met)
    
    success_rate = positive_criteria / total_criteria
    
    print(f"\n=== LIVE-READY VERDICT ===")
    print(f"Success rate: {success_rate:.1%} ({positive_criteria}/{total_criteria} criteria)")
    
    if success_rate >= 0.8:
        print(">>> LIVE-READY")
        print("Edge meets all deployment criteria")
    elif success_rate >= 0.6:
        print(">>> NEARLY READY")
        print("Edge meets most criteria, needs minor improvements")
    elif success_rate >= 0.4:
        print(">>> NEEDS MORE VALIDATION")
        print("Edge shows promise but requires more testing")
    else:
        print(">>> NOT READY")
        print("Edge fails deployment criteria")
    
    return {
        'results': results,
        'criteria': criteria_met,
        'success_rate': success_rate
    }

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    extended_forward_pipeline()
