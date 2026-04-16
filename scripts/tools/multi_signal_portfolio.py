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
    
    # Calculate volatility for regime
    market_data['rolling_vol'] = market_data['market_return'].rolling(window=window_days).std() * np.sqrt(252)
    
    # Classify regimes
    regimes = []
    for idx, row in market_data.iterrows():
        if pd.notna(row['rolling_return']):
            # Annualize the rolling return
            annualized_return = (1 + row['rolling_return']) ** (365/window_days) - 1
            
            # Regime classification
            if annualized_return > 0.15:
                market_regime = 'bull'
            elif annualized_return < -0.10:
                market_regime = 'bear'
            else:
                market_regime = 'neutral'
            
            # Volatility classification
            if pd.notna(row['rolling_vol']):
                if row['rolling_vol'] > 0.25:
                    vol_regime = 'high'
                elif row['rolling_vol'] < 0.15:
                    vol_regime = 'low'
                else:
                    vol_regime = 'medium'
            else:
                vol_regime = 'unknown'
        else:
            market_regime = 'unknown'
            vol_regime = 'unknown'
        
        regimes.append({
            'date': row['date'],
            'market_regime': market_regime,
            'vol_regime': vol_regime,
            'annualized_return': annualized_return if pd.notna(row['rolling_return']) else 0,
            'volatility': row['rolling_vol'] if pd.notna(row['rolling_vol']) else 0
        })
    
    return pd.DataFrame(regimes)

def momentum_5d_signals(df, hold_days=3, threshold=0.012):
    """5-day momentum signals with lower threshold"""
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        ticker_df = ticker_df.reset_index(drop=True)
        
        # Calculate 5-day returns
        ticker_df['return_5d'] = ticker_df['close'].pct_change(5)
        
        # Calculate trend strength (20-day momentum)
        ticker_df['trend_20d'] = ticker_df['close'].pct_change(20)
        
        # Calculate volatility
        ticker_df['volatility'] = ticker_df['close'].pct_change().rolling(20).std() * np.sqrt(252)
        
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
                    
                    # Calculate signal quality score
                    signal_strength = row['return_5d']
                    trend_strength = row['trend_20d'] if pd.notna(row['trend_20d']) else 0
                    volatility = row['volatility'] if pd.notna(row['volatility']) else 0.25
                    
                    # Quality score: higher momentum + stronger trend + moderate volatility
                    quality_score = signal_strength * 0.5 + trend_strength * 0.3 - abs(volatility - 0.2) * 0.2
                    
                    results.append({
                        'date': pd.to_datetime(signal_date).date(),
                        'ticker': ticker,
                        'signal_type': 'momentum_5d',
                        'hold_days': hold_days,
                        'signal_return': row['return_5d'],
                        'trade_return': trade_return,
                        'signal_strength': signal_strength,
                        'trend_strength': trend_strength,
                        'volatility': volatility,
                        'quality_score': quality_score,
                        'entry_price': entry_price,
                        'exit_price': exit_price
                    })
    
    return results

def mean_reversion_signals(df, hold_days=2, threshold=-0.03):
    """Mean reversion signals (independent from momentum)"""
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        ticker_df = ticker_df.reset_index(drop=True)
        
        # Calculate 1-day returns
        ticker_df['return_1d'] = ticker_df['close'].pct_change()
        
        # Calculate short-term mean reversion (5-day average)
        ticker_df['mean_5d'] = ticker_df['close'].rolling(5).mean()
        ticker_df['deviation'] = (ticker_df['close'] - ticker_df['mean_5d']) / ticker_df['mean_5d']
        
        # Calculate volatility
        ticker_df['volatility'] = ticker_df['close'].pct_change().rolling(20).std() * np.sqrt(252)
        
        # Find signals (large drops)
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['deviation']) and row['deviation'] < threshold:
                
                # Calculate hold period return
                signal_date = row['date']
                entry_price = row['close']
                
                # Find exit date
                exit_idx = idx + hold_days
                if exit_idx < len(ticker_df):
                    exit_price = ticker_df.iloc[exit_idx]['close']
                    trade_return = (exit_price / entry_price) - 1
                    
                    # Calculate signal quality score
                    signal_strength = abs(row['deviation'])
                    volatility = row['volatility'] if pd.notna(row['volatility']) else 0.25
                    
                    # Quality score: larger deviation + moderate volatility
                    quality_score = signal_strength * 0.7 - abs(volatility - 0.2) * 0.3
                    
                    results.append({
                        'date': pd.to_datetime(signal_date).date(),
                        'ticker': ticker,
                        'signal_type': 'mean_reversion',
                        'hold_days': hold_days,
                        'signal_return': row['deviation'],
                        'trade_return': trade_return,
                        'signal_strength': signal_strength,
                        'trend_strength': 0,  # Not applicable for mean reversion
                        'volatility': volatility,
                        'quality_score': quality_score,
                        'entry_price': entry_price,
                        'exit_price': exit_price
                    })
    
    return results

def volatility_breakout_signals(df, hold_days=3, threshold=0.04):
    """Volatility breakout signals (independent from momentum)"""
    
    results = []
    
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        ticker_df = ticker_df.reset_index(drop=True)
        
        # Calculate daily returns
        ticker_df['return_1d'] = ticker_df['close'].pct_change()
        
        # Calculate rolling volatility
        ticker_df['rolling_vol'] = ticker_df['return_1d'].rolling(20).std() * np.sqrt(252)
        
        # Calculate volatility change
        ticker_df['vol_change'] = ticker_df['rolling_vol'].pct_change(5)
        
        # Find signals (volatility breakout)
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['vol_change']) and row['vol_change'] > threshold:
                
                # Calculate hold period return
                signal_date = row['date']
                entry_price = row['close']
                
                # Find exit date
                exit_idx = idx + hold_days
                if exit_idx < len(ticker_df):
                    exit_price = ticker_df.iloc[exit_idx]['close']
                    trade_return = (exit_price / entry_price) - 1
                    
                    # Calculate signal quality score
                    signal_strength = row['vol_change']
                    volatility = row['rolling_vol'] if pd.notna(row['rolling_vol']) else 0.25
                    
                    # Quality score: higher vol change + base volatility
                    quality_score = signal_strength * 0.6 + volatility * 0.4
                    
                    results.append({
                        'date': pd.to_datetime(signal_date).date(),
                        'ticker': ticker,
                        'signal_type': 'volatility_breakout',
                        'hold_days': hold_days,
                        'signal_return': row['vol_change'],
                        'trade_return': trade_return,
                        'signal_strength': signal_strength,
                        'trend_strength': 0,  # Not applicable for volatility breakout
                        'volatility': volatility,
                        'quality_score': quality_score,
                        'entry_price': entry_price,
                        'exit_price': exit_price
                    })
    
    return results

def realistic_slippage_model(trade_return, ticker, trade_size, market_volatility):
    """Realistic slippage model"""
    
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

def calculate_volatility_sizing(trade, base_size=0.05):
    """Calculate volatility-adjusted position size (smaller for portfolio)"""
    
    # Use signal strength as volatility proxy
    signal_vol = trade['volatility']
    
    # Higher volatility = smaller position
    vol_adjustment = 1 / (1 + signal_vol * 2)
    position_size = base_size * vol_adjustment
    
    return position_size

def apply_stop_loss(trade_return, stop_level=0.02):
    """Apply stop-loss rule"""
    
    if trade_return < -stop_level:
        return -stop_level
    else:
        return trade_return

def portfolio_position_management(signals, max_positions=3):
    """Portfolio position management with overlap control"""
    
    # Sort all signals by date and quality score
    sorted_signals = sorted(signals, key=lambda x: (x['date'], -x['quality_score']))
    
    selected_signals = []
    active_positions = {}  # ticker -> exit_date
    
    for signal in sorted_signals:
        signal_date = signal['date']
        
        # Remove expired positions
        expired_tickers = [ticker for ticker, exit_date in active_positions.items() 
                          if signal_date > exit_date]
        for ticker in expired_tickers:
            del active_positions[ticker]
        
        # Check position limit
        if len(active_positions) >= max_positions:
            continue
        
        # Check for same ticker (no overlap)
        if signal['ticker'] in active_positions:
            continue
        
        # Add position
        selected_signals.append(signal)
        active_positions[signal['ticker']] = signal_date + timedelta(days=signal['hold_days'])
    
    return selected_signals

def multi_signal_portfolio_test():
    """Multi-signal portfolio test with scaling"""
    
    print("=== MULTI-SIGNAL PORTFOLIO TEST ===")
    print("Scaling observations without breaking discipline\n")
    
    # Get data
    df = get_price_data()
    df['date'] = pd.to_datetime(df['date'])
    
    # Calculate market regimes
    regimes_df = calculate_market_regime(df)
    
    # Use accumulation period
    max_date = df['date'].max()
    test_start = max_date - timedelta(days=365)
    test_end = max_date
    
    print(f"Test period: {test_start.strftime('%Y-%m-%d')} to {test_end.strftime('%Y-%m-%d')}")
    print(f"Duration: {(test_end - test_start).days} days")
    
    # Generate signals from multiple strategies
    print(f"\n=== SIGNAL GENERATION ===")
    
    # Momentum variants (different hold periods)
    momentum_3d = momentum_5d_signals(df, hold_days=2, threshold=0.012)
    momentum_4d = momentum_5d_signals(df, hold_days=4, threshold=0.012)
    
    # Mean reversion
    mean_reversion = mean_reversion_signals(df, hold_days=2, threshold=-0.03)
    
    # Volatility breakout
    vol_breakout = volatility_breakout_signals(df, hold_days=3, threshold=0.04)
    
    # Combine all signals
    all_signals = momentum_3d + momentum_4d + mean_reversion + vol_breakout
    
    # Filter to test period
    test_signals = [s for s in all_signals if test_start <= pd.to_datetime(s['date']) <= test_end]
    
    print(f"Momentum (2d hold): {len(momentum_3d)} signals")
    print(f"Momentum (4d hold): {len(momentum_4d)} signals")
    print(f"Mean reversion: {len(mean_reversion)} signals")
    print(f"Volatility breakout: {len(vol_breakout)} signals")
    print(f"Total signals: {len(all_signals)}")
    print(f"Test period signals: {len(test_signals)}")
    
    # Apply portfolio position management
    selected_signals = portfolio_position_management(test_signals, max_positions=3)
    
    print(f"\nPortfolio management:")
    print(f"Selected signals: {len(selected_signals)}")
    print(f"Selection rate: {len(selected_signals)/len(test_signals):.1%}")
    
    # Prepare trade details with regime tagging
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
        
        # Get regime information
        signal_date = pd.to_datetime(signal['date'])
        regime_info = regimes_df[regimes_df['date'] == signal_date]
        
        if len(regime_info) > 0:
            market_regime = regime_info.iloc[0]['market_regime']
            vol_regime = regime_info.iloc[0]['vol_regime']
        else:
            market_regime = 'unknown'
            vol_regime = 'unknown'
        
        trade_details.append({
            'date': signal['date'],
            'ticker': signal['ticker'],
            'signal_type': signal['signal_type'],
            'raw_return': signal['trade_return'],
            'adjusted_return': adjusted_return,
            'slippage': slippage,
            'signal_strength': signal['signal_strength'],
            'quality_score': signal['quality_score'],
            'volatility': signal['volatility'],
            'market_regime': market_regime,
            'vol_regime': vol_regime
        })
    
    # Apply portfolio execution
    portfolio_returns = []
    stop_frequency = 0
    
    for trade in trade_details:
        # Apply stop-loss
        stopped_return = apply_stop_loss(trade['adjusted_return'], 0.02)
        
        # Check if stop was triggered
        if stopped_return == -0.02:
            stop_frequency += 1
        
        # Volatility-adjusted sizing
        position_size = calculate_volatility_sizing(trade, 0.05)  # 5% base size
        portfolio_return = stopped_return * position_size
        portfolio_returns.append(portfolio_return)
    
    # Calculate performance metrics
    if portfolio_returns:
        # Portfolio-level returns
        total_return = sum(portfolio_returns)
        win_rate = sum(1 for r in portfolio_returns if r > 0) / len(portfolio_returns)
        
        # Calculate drawdown
        cumulative = np.cumsum(portfolio_returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdowns = cumulative - running_max
        max_drawdown = min(drawdowns) if len(drawdowns) > 0 else 0
        
        # Calculate volatility and Sharpe
        returns_array = np.array(portfolio_returns)
        volatility = np.std(returns_array) * np.sqrt(252)  # Annualized
        sharpe = np.mean(returns_array) / np.std(returns_array) * np.sqrt(252) if np.std(returns_array) != 0 else 0
        
        # Calculate costs
        avg_slippage = np.mean([t['slippage'] for t in trade_details])
        stop_freq_pct = stop_frequency / len(portfolio_returns)
        
        print(f"\n=== PORTFOLIO PERFORMANCE ===")
        print(f"Total return: {total_return:+.2%}")
        print(f"Win rate: {win_rate:.1%}")
        print(f"Max drawdown: {max_drawdown:+.2%}")
        print(f"Volatility: {volatility:.2%}")
        print(f"Sharpe ratio: {sharpe:.2f}")
        print(f"Avg slippage: {avg_slippage:.2%}")
        print(f"Stop frequency: {stop_freq_pct:.1%}")
        
        # Signal type analysis
        print(f"\n=== SIGNAL TYPE ANALYSIS ===")
        signal_performance = {}
        
        for signal_type in ['momentum_5d', 'mean_reversion', 'volatility_breakout']:
            type_trades = [t for t in trade_details if t['signal_type'] == signal_type]
            
            if type_trades:
                type_returns = [portfolio_returns[i] for i, t in enumerate(trade_details) 
                              if t['signal_type'] == signal_type]
                
                type_return = sum(type_returns)
                type_win_rate = sum(1 for r in type_returns if r > 0) / len(type_returns)
                
                signal_performance[signal_type] = {
                    'trades': len(type_trades),
                    'return': type_return,
                    'win_rate': type_win_rate
                }
                
                print(f"{signal_type}: {len(type_trades)} trades, {type_return:+.2%} return, {type_win_rate:.1%} win rate")
        
        # Regime analysis
        print(f"\n=== REGIME ANALYSIS ===")
        regime_performance = {}
        
        for regime in ['bull', 'bear', 'neutral']:
            regime_trades = [t for t in trade_details if t['market_regime'] == regime]
            
            if regime_trades:
                regime_returns = [portfolio_returns[i] for i, t in enumerate(trade_details) 
                                if t['market_regime'] == regime]
                
                regime_return = sum(regime_returns)
                regime_win_rate = sum(1 for r in regime_returns if r > 0) / len(regime_returns)
                
                regime_performance[regime] = {
                    'trades': len(regime_trades),
                    'return': regime_return,
                    'win_rate': regime_win_rate
                }
                
                print(f"{regime.upper()}: {len(regime_trades)} trades, {regime_return:+.2%} return, {regime_win_rate:.1%} win rate")
        
        # Quality score analysis
        print(f"\n=== QUALITY SCORE ANALYSIS ===")
        
        # Sort by quality score
        quality_sorted = sorted(trade_details, key=lambda x: x['quality_score'], reverse=True)
        
        # Analyze by decile
        decile_size = len(quality_sorted) // 10
        for i in range(0, min(5, len(quality_sorted) // decile_size)):  # Top 5 deciles
            start_idx = i * decile_size
            end_idx = (i + 1) * decile_size
            
            decile_trades = quality_sorted[start_idx:end_idx]
            decile_returns = [portfolio_returns[trade_details.index(t)] for t in decile_trades]
            
            decile_return = sum(decile_returns)
            decile_win_rate = sum(1 for r in decile_returns if r > 0) / len(decile_returns)
            avg_quality = np.mean([t['quality_score'] for t in decile_trades])
            
            print(f"Decile {i+1}: {len(decile_trades)} trades, {decile_return:+.2%} return, {decile_win_rate:.1%} win rate, avg quality: {avg_quality:.3f}")
        
        # Final assessment
        print(f"\n=== SCALING ASSESSMENT ===")
        
        # Trade count improvement
        original_single_signal = 76  # From previous analysis
        current_multi_signal = len(selected_signals)
        trade_improvement = (current_multi_signal / original_single_signal - 1) * 100
        
        print(f"Trade frequency improvement: {trade_improvement:+.1f}%")
        print(f"Original single signal: {original_single_signal} trades/year")
        print(f"Current multi-signal: {current_multi_signal} trades/year")
        
        # Gate progress
        print(f"\n=== GATE PROGRESS ===")
        
        if current_multi_signal >= 100:
            gate_status = "LIVE-READY GATE CLEARED"
        elif current_multi_signal >= 50:
            gate_status = "SMALL-CAPITAL PILOT GATE CLEARED"
        else:
            gate_status = f"NEED {50 - current_multi_signal} MORE TRADES FOR PILOT GATE"
        
        print(f"Status: {gate_status}")
        
        return {
            'total_trades': len(selected_signals),
            'total_return': total_return,
            'win_rate': win_rate,
            'max_drawdown': max_drawdown,
            'sharpe': sharpe,
            'signal_performance': signal_performance,
            'regime_performance': regime_performance,
            'trade_improvement': trade_improvement
        }
    
    return None

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    multi_signal_portfolio_test()
