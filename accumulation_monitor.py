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

def calculate_market_regime(df, window_days=60):
    """Calculate market regime classification"""
    
    # Use equal-weighted market index
    market_data = df.groupby('date')['close'].mean().reset_index()
    market_data = market_data.sort_values('date')
    market_data['date'] = pd.to_datetime(market_data['date'])
    
    # Calculate returns
    market_data['market_return'] = market_data['close'].pct_change()
    
    # Calculate rolling returns for regime classification
    market_data['rolling_return'] = market_data['market_return'].rolling(window=window_days).mean()
    
    # Classify regimes
    regimes = []
    for idx, row in market_data.iterrows():
        if pd.notna(row['rolling_return']):
            # Annualize the rolling return
            annualized_return = (1 + row['rolling_return']) ** (365/window_days) - 1
            
            if annualized_return > 0.15:
                regime = 'bull'
            elif annualized_return < -0.10:
                regime = 'bear'
            else:
                regime = 'neutral'
        else:
            regime = 'unknown'
        
        regimes.append({
            'date': row['date'],
            'regime': regime,
            'annualized_return': annualized_return if pd.notna(row['rolling_return']) else 0
        })
    
    return pd.DataFrame(regimes)

def accumulation_monitor():
    """Monitor accumulation toward deployment gates"""
    
    print("=== ACCUMULATION MONITOR ===")
    print("Tracking progress toward deployment gates\n")
    
    # Get data
    df = get_price_data()
    df['date'] = pd.to_datetime(df['date'])
    
    # Calculate market regimes
    regimes_df = calculate_market_regime(df)
    
    # Use maximum available period for accumulation
    max_date = df['date'].max()
    accumulation_start = max_date - timedelta(days=365)  # 1 year back
    accumulation_end = max_date
    
    print(f"Accumulation period: {accumulation_start.strftime('%Y-%m-%d')} to {accumulation_end.strftime('%Y-%m-%d')}")
    print(f"Duration: {(accumulation_end - accumulation_start).days} days")
    
    # Generate signals (frozen rule)
    signals = multi_day_momentum_5d_fixed(df, 3, 0.03)
    
    # Filter to accumulation period
    accumulation_signals = [s for s in signals if accumulation_start <= pd.to_datetime(s['date']) <= accumulation_end]
    
    # Apply overlap control (frozen rule)
    selected_signals = deterministic_overlap_control(accumulation_signals)
    
    print(f"Accumulation signals: {len(selected_signals)}")
    
    # Prepare trade details
    trade_details = []
    
    for signal in selected_signals:
        # Estimate market volatility
        ticker_data = df[(df['ticker'] == signal['ticker']) & (df['date'] <= pd.to_datetime(signal['date']))]
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
        
        # Calculate volatility and Sharpe
        returns_array = np.array(optimized_returns)
        volatility = np.std(returns_array) * np.sqrt(252)  # Annualized
        sharpe = np.mean(returns_array) / np.std(returns_array) * np.sqrt(252) if np.std(returns_array) != 0 else 0
        
        # Calculate costs
        avg_slippage = np.mean([t['slippage'] for t in trade_details])
        stop_freq_pct = stop_frequency / len(optimized_returns)
        
        # Regime analysis
        trade_df = pd.DataFrame(trade_details)
        trade_df['date'] = pd.to_datetime(trade_df['date'])
        
        # Merge with regimes
        regime_analysis = trade_df.merge(regimes_df, on='date', how='left')
        
        regime_performance = {}
        for regime in ['bull', 'bear', 'neutral']:
            regime_trades = regime_analysis[regime_analysis['regime'] == regime]
            
            if len(regime_trades) > 0:
                regime_returns = [optimized_returns[i] for i, trade in enumerate(trade_details) 
                                if trade['date'] in regime_trades['date'].values]
                
                if regime_returns:
                    regime_return = sum(regime_returns)
                    regime_win_rate = sum(1 for r in regime_returns if r > 0) / len(regime_returns)
                    
                    regime_performance[regime] = {
                        'trades': len(regime_trades),
                        'return': regime_return,
                        'win_rate': regime_win_rate
                    }
        
        print(f"\n=== ACCUMULATION METRICS ===")
        print(f"Portfolio return: {portfolio_return:+.2%}")
        print(f"Win rate: {win_rate:.1%}")
        print(f"Max drawdown: {max_drawdown:+.2%}")
        print(f"Volatility: {volatility:.2%}")
        print(f"Sharpe ratio: {sharpe:.2f}")
        print(f"Avg slippage: {avg_slippage:.2%}")
        print(f"Stop frequency: {stop_freq_pct:.1%}")
        
        print(f"\n=== REGIME ANALYSIS ===")
        for regime, perf in regime_performance.items():
            print(f"{regime.upper()}: {perf['trades']} trades, {perf['return']:+.2%} return, {perf['win_rate']:.1%} win rate")
        
        # Gate progress tracking
        print(f"\n=== GATE PROGRESS TRACKING ===")
        
        # Current status
        current_trades = len(selected_signals)
        current_days = (accumulation_end - accumulation_start).days
        
        # Gate requirements
        pilot_trades_needed = 50
        live_trades_needed = 100
        live_days_needed = 365
        
        # Progress calculation
        pilot_progress = min(100, (current_trades / pilot_trades_needed) * 100)
        live_progress = min(100, (current_trades / live_trades_needed) * 100)
        days_progress = min(100, (current_days / live_days_needed) * 100)
        
        print(f"SMALL-CAPITAL PILOT GATE:")
        print(f"  Trades: {current_trades}/{pilot_trades_needed} ({pilot_progress:.0f}%)")
        print(f"  Status: {'CLEARED' if current_trades >= pilot_trades_needed else 'IN PROGRESS'}")
        
        print(f"\nLIVE-READY GATE:")
        print(f"  Trades: {current_trades}/{live_trades_needed} ({live_progress:.0f}%)")
        print(f"  Days: {current_days}/{live_days_needed} ({days_progress:.0f}%)")
        print(f"  Status: {'CLEARED' if current_trades >= live_trades_needed and current_days >= live_days_needed else 'IN PROGRESS'}")
        
        # Parameter stability check
        print(f"\n=== PARAMETER STABILITY ===")
        
        # Check signal strength distribution
        signal_strengths = [t['signal_strength'] for t in trade_details]
        avg_strength = np.mean(signal_strengths)
        strength_std = np.std(signal_strengths)
        
        print(f"Signal strength: {avg_strength:.2%} ± {strength_std:.2%}")
        
        # Check slippage drift
        slippages = [t['slippage'] for t in trade_details]
        avg_slippage_current = np.mean(slippages)
        slippage_std = np.std(slippages)
        
        print(f"Slippage: {avg_slippage_current:.2%} ± {slippage_std:.2%}")
        
        # Stability assessment
        strength_stable = strength_std < avg_strength * 0.5  # CV < 0.5
        slippage_stable = slippage_std < avg_slippage_current * 0.5  # CV < 0.5
        
        print(f"Signal strength stability: {'STABLE' if strength_stable else 'UNSTABLE'}")
        print(f"Slippage stability: {'STABLE' if slippage_stable else 'UNSTABLE'}")
        
        # Final status
        print(f"\n=== FINAL STATUS ===")
        
        if current_trades >= live_trades_needed and current_days >= live_days_needed:
            status = "LIVE-READY"
        elif current_trades >= pilot_trades_needed:
            status = "SMALL-CAPITAL PILOT CANDIDATE"
        else:
            status = "FROZEN PAPER-TRADING"
        
        print(f"Current status: {status}")
        print(f"Next milestone: {'LIVE-READY' if current_trades < live_trades_needed else 'MAINTENANCE'}")
        
        # Recommendations
        print(f"\n=== RECOMMENDATIONS ===")
        
        if current_trades < pilot_trades_needed:
            trades_needed = pilot_trades_needed - current_trades
            print(f"Continue frozen paper-trading until {trades_needed} more trades accumulated")
            print(f"Estimated time: {trades_needed / (current_trades / current_days) * 30:.0f} days at current rate")
        elif current_trades < live_trades_needed:
            trades_needed = live_trades_needed - current_trades
            print(f"Continue accumulation for live-ready: {trades_needed} more trades needed")
            print(f"Estimated time: {trades_needed / (current_trades / current_days) * 30:.0f} days at current rate")
        else:
            print(f"All gates cleared - ready for live deployment consideration")
        
        return {
            'current_trades': current_trades,
            'current_days': current_days,
            'portfolio_return': portfolio_return,
            'win_rate': win_rate,
            'max_drawdown': max_drawdown,
            'sharpe': sharpe,
            'status': status,
            'regime_performance': regime_performance,
            'parameter_stability': {
                'signal_strength': strength_stable,
                'slippage': slippage_stable
            }
        }
    
    return None

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    accumulation_monitor()
