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
    """Realistic slippage model with spreads and gap risk"""
    
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

def outlier_trimmed_analysis(trade_details, trim_percentile=95):
    """Analyze edge with outlier trimming"""
    
    print(f"=== OUTLIER TRIMMED ANALYSIS (top {trim_percentile} percentile) ===")
    
    if not trade_details:
        print("No trade details for analysis")
        return None
    
    returns = [t['adjusted_return'] for t in trade_details]
    
    # Calculate percentile threshold
    threshold = np.percentile(returns, trim_percentile)
    
    # Separate trimmed and regular returns
    regular_returns = returns
    trimmed_returns = [r for r in returns if r <= threshold]
    
    print(f"Regular analysis: {len(regular_returns)} trades")
    print(f"Trimmed analysis: {len(trimmed_returns)} trades")
    print(f"Trim threshold: {threshold:.2%}")
    print(f"Trades removed: {len(regular_returns) - len(trimmed_returns)}")
    
    # Calculate performance metrics
    def calculate_metrics(returns_list):
        if not returns_list:
            return {'win_rate': 0, 'total_return': 0, 'avg_return': 0}
        
        win_rate = sum(1 for r in returns_list if r > 0) / len(returns_list)
        total_return = np.prod(1 + np.array(returns_list)) - 1
        avg_return = np.mean(returns_list)
        
        return {
            'win_rate': win_rate,
            'total_return': total_return,
            'avg_return': avg_return
        }
    
    regular_metrics = calculate_metrics(regular_returns)
    trimmed_metrics = calculate_metrics(trimmed_returns)
    
    print(f"\nRegular performance:")
    print(f"  Win rate: {regular_metrics['win_rate']:.1%}")
    print(f"  Total return: {regular_metrics['total_return']:+.2%}")
    print(f"  Avg return: {regular_metrics['avg_return']:+.2%}")
    
    print(f"\nTrimmed performance:")
    print(f"  Win rate: {trimmed_metrics['win_rate']:.1%}")
    print(f"  Total return: {trimmed_metrics['total_return']:+.2%}")
    print(f"  Avg return: {trimmed_metrics['avg_return']:+.2%}")
    
    # Check robustness
    if trimmed_metrics['total_return'] > 0 and trimmed_metrics['win_rate'] > 0.52:
        robust = True
        print(f"\n>>> ROBUST: Edge survives outlier trimming")
    else:
        robust = False
        print(f"\n>>> FRAGILE: Edge depends on outliers")
    
    return {
        'regular': regular_metrics,
        'trimmed': trimmed_metrics,
        'robust': robust,
        'threshold': threshold
    }

def capital_efficiency_test(trade_details, sizing_methods=['fixed', 'volatility']):
    """Test different capital efficiency methods"""
    
    print(f"=== CAPITAL EFFICIENCY TEST ===")
    
    if not trade_details:
        print("No trade details for analysis")
        return None
    
    results = {}
    
    for method in sizing_methods:
        print(f"\n--- {method.upper()} SIZING ---")
        
        # Calculate position sizes
        sized_returns = []
        
        for trade in trade_details:
            if method == 'fixed':
                # Fixed fractional sizing (10% per trade)
                position_size = 0.1
            elif method == 'volatility':
                # Volatility-adjusted sizing
                # Estimate volatility from signal strength
                signal_vol = trade['signal_strength']
                base_size = 0.1
                # Higher momentum = higher volatility = smaller position
                vol_adjustment = 1 / (1 + signal_vol * 10)
                position_size = base_size * vol_adjustment
            
            # Apply position sizing
            sized_return = trade['adjusted_return'] * position_size
            sized_returns.append(sized_return)
        
        # Calculate portfolio metrics
        if sized_returns:
            # Aggregate by date (simple approach)
            portfolio_return = sum(sized_returns)
            
            # Calculate win rate on portfolio level
            daily_pnl = []
            for i, trade in enumerate(trade_details):
                daily_pnl.append(sized_returns[i])
            
            win_rate = sum(1 for r in daily_pnl if r > 0) / len(daily_pnl)
            
            # Calculate risk metrics
            returns_array = np.array(daily_pnl)
            volatility = np.std(returns_array) * np.sqrt(252)  # Annualized
            sharpe = np.mean(returns_array) / np.std(returns_array) * np.sqrt(252) if np.std(returns_array) != 0 else 0
            
            results[method] = {
                'portfolio_return': portfolio_return,
                'win_rate': win_rate,
                'volatility': volatility,
                'sharpe': sharpe,
                'avg_position_size': np.mean([s / trade['adjusted_return'] for s, trade in zip(sized_returns, trade_details)])
            }
            
            print(f"Portfolio return: {portfolio_return:+.2%}")
            print(f"Win rate: {win_rate:.1%}")
            print(f"Volatility: {volatility:.2%}")
            print(f"Sharpe ratio: {sharpe:.2f}")
            print(f"Avg position size: {results[method]['avg_position_size']:.1%}")
    
    return results

def gap_risk_filtering(trade_details, gap_threshold=0.05):
    """Filter trades by gap risk"""
    
    print(f"=== GAP RISK FILTERING (threshold: {gap_threshold:.1%}) ===")
    
    if not trade_details:
        print("No trade details for analysis")
        return None
    
    # Calculate gap risk (simplified - use signal strength as proxy)
    filtered_trades = []
    excluded_trades = []
    
    for trade in trade_details:
        # Estimate gap risk from signal strength
        # Higher momentum = higher gap risk
        estimated_gap_risk = trade['signal_strength'] * 0.5  # Rough estimate
        
        if estimated_gap_risk <= gap_threshold:
            filtered_trades.append(trade)
        else:
            excluded_trades.append(trade)
    
    print(f"Original trades: {len(trade_details)}")
    print(f"Filtered trades: {len(filtered_trades)}")
    print(f"Excluded trades: {len(excluded_trades)}")
    print(f"Exclusion rate: {len(excluded_trades)/len(trade_details):.1%}")
    
    # Calculate performance comparison
    def calculate_performance(trades):
        if not trades:
            return {'win_rate': 0, 'total_return': 0}
        
        returns = [t['adjusted_return'] for t in trades]
        win_rate = sum(1 for r in returns if r > 0) / len(returns)
        total_return = np.prod(1 + np.array(returns)) - 1
        
        return {'win_rate': win_rate, 'total_return': total_return}
    
    original_perf = calculate_performance(trade_details)
    filtered_perf = calculate_performance(filtered_trades)
    
    print(f"\nOriginal performance:")
    print(f"  Win rate: {original_perf['win_rate']:.1%}")
    print(f"  Total return: {original_perf['total_return']:+.2%}")
    
    print(f"\nFiltered performance:")
    print(f"  Win rate: {filtered_perf['win_rate']:.1%}")
    print(f"  Total return: {filtered_perf['total_return']:+.2%}")
    
    # Check if filtering helps
    if filtered_perf['total_return'] > original_perf['total_return'] and filtered_perf['win_rate'] >= original_perf['win_rate']:
        print(f"\n>>> FILTERING HELPS: Improves risk-adjusted returns")
        effective = True
    else:
        print(f"\n>>> FILTERING HURTS: Reduces performance")
        effective = False
    
    return {
        'original': original_perf,
        'filtered': filtered_perf,
        'effective': effective,
        'filtered_trades': filtered_trades
    }

def stop_loss_ev_analysis(trade_details, stop_levels=[0.02, 0.03, 0.05]):
    """Test stop-loss rules against expected value"""
    
    print(f"=== STOP-LOSS EV ANALYSIS ===")
    
    if not trade_details:
        print("No trade details for analysis")
        return None
    
    results = {}
    
    for stop_level in stop_levels:
        print(f"\n--- STOP LOSS: {stop_level:.1%} ---")
        
        stopped_returns = []
        
        for trade in trade_details:
            # Simulate stop-loss
            if trade['adjusted_return'] < -stop_level:
                # Stop triggered
                stopped_return = -stop_level
            else:
                # No stop triggered
                stopped_return = trade['adjusted_return']
            
            stopped_returns.append(stopped_return)
        
        # Calculate metrics
        win_rate = sum(1 for r in stopped_returns if r > 0) / len(stopped_returns)
        total_return = np.prod(1 + np.array(stopped_returns)) - 1
        avg_return = np.mean(stopped_returns)
        
        # Calculate EV
        ev = avg_return
        
        # Calculate stop frequency
        stop_frequency = sum(1 for r in stopped_returns if r <= -stop_level) / len(stopped_returns)
        
        results[stop_level] = {
            'win_rate': win_rate,
            'total_return': total_return,
            'avg_return': avg_return,
            'ev': ev,
            'stop_frequency': stop_frequency
        }
        
        print(f"Win rate: {win_rate:.1%}")
        print(f"Total return: {total_return:+.2%}")
        print(f"Average return: {avg_return:+.2%}")
        print(f"Expected value: {ev:+.2%}")
        print(f"Stop frequency: {stop_frequency:.1%}")
        
        # EV assessment
        if ev > 0:
            print(f">>> POSITIVE EV: Stop-loss preserves edge")
        else:
            print(f">>> NEGATIVE EV: Stop-loss destroys edge")
    
    return results

def optimization_pipeline():
    """Optimization pipeline for edge improvement"""
    
    print("=== EDGE OPTIMIZATION PIPELINE ===")
    print("Optimizing: 1) Outlier dependence, 2) Capital efficiency, 3) Drawdown control\n")
    
    # Get data
    df = get_price_data()
    df['date'] = pd.to_datetime(df['date'])
    
    # Use extended paper trading period
    max_date = df['date'].max()
    paper_start = max_date - timedelta(days=365)
    paper_end = max_date
    
    # Generate signals (frozen rule)
    signals = multi_day_momentum_5d_fixed(df, 3, 0.03)
    
    # Filter to paper trading period
    paper_signals = [s for s in signals if paper_start <= pd.to_datetime(s['date']) <= paper_end]
    
    # Apply overlap control
    selected_signals = deterministic_overlap_control(paper_signals)
    
    # Calculate realistic returns
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
            'signal_strength': signal['signal_return']
        })
    
    print(f"Base analysis: {len(trade_details)} trades")
    
    # Step 1: Outlier dependence analysis
    outlier_results = outlier_trimmed_analysis(trade_details)
    
    # Step 2: Capital efficiency test
    cap_results = capital_efficiency_test(trade_details)
    
    # Step 3: Gap risk filtering
    gap_results = gap_risk_filtering(trade_details)
    
    # Step 4: Stop-loss EV analysis
    stop_results = stop_loss_ev_analysis(trade_details)
    
    # Final optimization recommendations
    print(f"\n=== OPTIMIZATION RECOMMENDATIONS ===")
    
    recommendations = []
    
    # Outlier analysis
    if outlier_results and outlier_results['robust']:
        recommendations.append("Outlier trimming: Edge is robust - can filter extreme winners")
    else:
        recommendations.append("Outlier trimming: Edge depends on outliers - keep current approach")
    
    # Capital efficiency
    if cap_results:
        best_method = max(cap_results.keys(), key=lambda x: cap_results[x]['sharpe'])
        recommendations.append(f"Capital efficiency: Use {best_method} sizing (Sharpe: {cap_results[best_method]['sharpe']:.2f})")
    
    # Gap risk filtering
    if gap_results and gap_results['effective']:
        recommendations.append("Gap risk filtering: Helps performance - implement filtering")
    else:
        recommendations.append("Gap risk filtering: Hurts performance - avoid filtering")
    
    # Stop-loss
    if stop_results:
        positive_stop_levels = [level for level, result in stop_results.items() if result['ev'] > 0]
        if positive_stop_levels:
            best_stop = min(positive_stop_levels)  # Tightest positive EV stop
            recommendations.append(f"Stop-loss: Use {best_stop:.1%} stop (positive EV)")
        else:
            recommendations.append("Stop-loss: No positive EV levels - avoid stops")
    
    for rec in recommendations:
        print(f"  {rec}")
    
    return {
        'outlier': outlier_results,
        'capital': cap_results,
        'gap': gap_results,
        'stop': stop_results,
        'recommendations': recommendations
    }

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    optimization_pipeline()
