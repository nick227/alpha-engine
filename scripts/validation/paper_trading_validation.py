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
    """Realistic slippage model based on trade characteristics"""
    
    # Base slippage components
    base_spread = 0.0005  # 0.05% base spread for liquid stocks
    impact_factor = 0.0001  # 0.01% per $1M traded
    
    # Adjust for volatility (higher vol = higher slippage)
    volatility_multiplier = 1 + (market_volatility - 0.2)  # 20% vol baseline
    
    # Adjust for trade size
    size_multiplier = 1 + (trade_size / 1000000)  # $1M baseline
    
    # Calculate total slippage
    total_slippage = (base_spread + impact_factor * trade_size / 1000000) * volatility_multiplier * size_multiplier
    
    # Apply slippage to return
    adjusted_return = trade_return - total_slippage
    
    return adjusted_return, total_slippage

def paper_trading_period(df, start_date, end_date):
    """Paper trading on unseen forward period"""
    
    print(f"=== PAPER TRADING: {start_date} to {end_date} ===")
    
    # Filter to paper trading period
    paper_data = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    
    if len(paper_data) == 0:
        print("No data for paper trading period")
        return None
    
    # Generate signals (frozen rule)
    signals = multi_day_momentum_5d_fixed(paper_data, 3, 0.03)
    
    if len(signals) < 10:
        print(f"Insufficient signals: {len(signals)}")
        return None
    
    # Apply overlap control (frozen rule)
    selected_signals = deterministic_overlap_control(signals)
    
    print(f"Paper trading signals: {len(selected_signals)}")
    
    # Calculate realistic returns
    realistic_returns = []
    slippage_costs = []
    
    for signal in selected_signals:
        # Estimate market volatility (simplified)
        ticker_data = paper_data[paper_data['ticker'] == signal['ticker']]
        if len(ticker_data) > 20:
            daily_returns = ticker_data['close'].pct_change().dropna()
            market_volatility = daily_returns.std() * np.sqrt(252)
        else:
            market_volatility = 0.25  # Default 25% annual vol
        
        # Estimate trade size (simplified - equal weight)
        portfolio_size = 1000000  # $1M portfolio
        num_positions = len(selected_signals)
        trade_size = portfolio_size / num_positions if num_positions > 0 else portfolio_size
        
        # Apply realistic slippage
        adjusted_return, slippage = realistic_slippage_model(
            signal['trade_return'], 
            signal['ticker'], 
            trade_size, 
            market_volatility
        )
        
        realistic_returns.append(adjusted_return)
        slippage_costs.append(slippage)
    
    if not realistic_returns:
        print("No realistic returns calculated")
        return None
    
    # Calculate performance metrics
    win_rate = sum(1 for r in realistic_returns if r > 0) / len(realistic_returns)
    total_return = np.prod(1 + np.array(realistic_returns)) - 1
    avg_slippage = np.mean(slippage_costs)
    
    print(f"Win rate: {win_rate:.1%}")
    print(f"Total return: {total_return:+.2%}")
    print(f"Average slippage: {avg_slippage:.2%}")
    
    return {
        'signals': len(selected_signals),
        'win_rate': win_rate,
        'return': total_return,
        'avg_slippage': avg_slippage,
        'returns': realistic_returns
    }

def capacity_turnover_analysis(signals):
    """Analyze capacity and turnover characteristics"""
    
    print("\n=== CAPACITY AND TURNOVER ANALYSIS ===")
    
    if not signals:
        print("No signals for analysis")
        return None
    
    # Calculate turnover
    selected_signals = deterministic_overlap_control(signals)
    
    # Average holding period
    holding_period = 3  # 3 days
    
    # Number of trades per year
    trades_per_year = len(selected_signals) / 4  # 4 years of data
    
    # Annual turnover
    annual_turnover = (trades_per_year * holding_period) / 365
    
    # Position concentration
    ticker_counts = {}
    for signal in selected_signals:
        ticker_counts[signal['ticker']] = ticker_counts.get(signal['ticker'], 0) + 1
    
    max_concentration = max(ticker_counts.values()) / len(selected_signals)
    
    print(f"Trades per year: {trades_per_year:.1f}")
    print(f"Annual turnover: {annual_turnover:.1%}")
    print(f"Max ticker concentration: {max_concentration:.1%}")
    
    # Capacity estimate (simplified)
    # Assume max 5% of daily volume for large caps
    avg_daily_volume = 10000000  # $10M daily volume (assumption)
    max_position_size = avg_daily_volume * 0.05
    
    # Estimated capacity
    estimated_capacity = max_position_size * len(selected_signals) / trades_per_year
    
    print(f"Estimated capacity: ${estimated_capacity:,.0f}")
    
    return {
        'trades_per_year': trades_per_year,
        'annual_turnover': annual_turnover,
        'max_concentration': max_concentration,
        'estimated_capacity': estimated_capacity
    }

def drawdown_analysis(returns):
    """Analyze drawdown characteristics"""
    
    print("\n=== DRAWDOWN ANALYSIS ===")
    
    if not returns:
        print("No returns for analysis")
        return None
    
    # Calculate cumulative returns
    cumulative = np.cumprod(1 + np.array(returns))
    
    # Calculate running maximum
    running_max = np.maximum.accumulate(cumulative)
    
    # Calculate drawdowns
    drawdowns = (cumulative - running_max) / running_max
    
    # Find maximum drawdown
    max_drawdown = min(drawdowns)
    
    # Find drawdown periods
    in_drawdown = drawdowns < 0
    drawdown_periods = []
    
    current_dd_start = None
    for i, is_dd in enumerate(in_drawdown):
        if is_dd and current_dd_start is None:
            current_dd_start = i
        elif not is_dd and current_dd_start is not None:
            drawdown_periods.append((current_dd_start, i))
            current_dd_start = None
    
    # Handle ongoing drawdown
    if current_dd_start is not None:
        drawdown_periods.append((current_dd_start, len(drawdowns)))
    
    # Calculate drawdown statistics
    avg_drawdown = np.mean([drawdowns[start:end].min() for start, end in drawdown_periods]) if drawdown_periods else 0
    max_dd_duration = max(end - start for start, end in drawdown_periods) if drawdown_periods else 0
    
    print(f"Maximum drawdown: {max_drawdown:+.2%}")
    print(f"Average drawdown: {avg_drawdown:+.2%}")
    print(f"Max drawdown duration: {max_dd_duration} days")
    print(f"Number of drawdown periods: {len(drawdown_periods)}")
    
    return {
        'max_drawdown': max_drawdown,
        'avg_drawdown': avg_drawdown,
        'max_duration': max_dd_duration,
        'periods': len(drawdown_periods)
    }

def trade_clustering_analysis(signals):
    """Analyze trade clustering patterns"""
    
    print("\n=== TRADE CLUSTERING ANALYSIS ===")
    
    if not signals:
        print("No signals for analysis")
        return None
    
    selected_signals = deterministic_overlap_control(signals)
    
    # Group by date
    signals_by_date = {}
    for signal in selected_signals:
        date = signal['date']
        if date not in signals_by_date:
            signals_by_date[date] = []
        signals_by_date[date].append(signal)
    
    # Calculate clustering metrics
    dates_with_signals = len(signals_by_date)
    total_signals = len(selected_signals)
    avg_signals_per_day = total_signals / dates_with_signals if dates_with_signals > 0 else 0
    
    # Find maximum clustering
    max_signals_day = max(len(signals) for signals in signals_by_date.values()) if signals_by_date else 0
    
    # Calculate date gaps
    sorted_dates = sorted(signals_by_date.keys())
    gaps = []
    for i in range(1, len(sorted_dates)):
        gap = (sorted_dates[i] - sorted_dates[i-1]).days
        gaps.append(gap)
    
    avg_gap = np.mean(gaps) if gaps else 0
    max_gap = max(gaps) if gaps else 0
    
    print(f"Trading days with signals: {dates_with_signals}")
    print(f"Average signals per day: {avg_signals_per_day:.1f}")
    print(f"Maximum signals in one day: {max_signals_day}")
    print(f"Average gap between trading days: {avg_gap:.1f} days")
    print(f"Maximum gap: {max_gap} days")
    
    return {
        'trading_days': dates_with_signals,
        'avg_signals_per_day': avg_signals_per_day,
        'max_signals_day': max_signals_day,
        'avg_gap': avg_gap,
        'max_gap': max_gap
    }

def comprehensive_paper_trading_validation():
    """Comprehensive paper trading validation"""
    
    print("=== COMPREHENSIVE PAPER TRADING VALIDATION ===")
    print("Testing frozen rule set on unseen data with realistic costs\n")
    
    # Get data
    df = get_price_data()
    df['date'] = pd.to_datetime(df['date'])
    
    # Use last 6 months as paper trading period (unseen)
    max_date = df['date'].max()
    paper_start = max_date - timedelta(days=180)
    paper_end = max_date
    
    print(f"Paper trading period: {paper_start.strftime('%Y-%m-%d')} to {paper_end.strftime('%Y-%m-%d')}")
    
    # Generate all signals for analysis
    all_signals = multi_day_momentum_5d_fixed(df, 3, 0.03)
    
    # Test 1: Paper trading period
    paper_results = paper_trading_period(df, paper_start, paper_end)
    
    # Test 2: Capacity and turnover
    capacity_results = capacity_turnover_analysis(all_signals)
    
    # Test 3: Drawdown analysis (if paper results available)
    drawdown_results = None
    if paper_results and paper_results['returns']:
        drawdown_results = drawdown_analysis(paper_results['returns'])
    
    # Test 4: Trade clustering
    clustering_results = trade_clustering_analysis(all_signals)
    
    # Final assessment
    print(f"\n=== PAPER TRADING ASSESSMENT ===")
    
    criteria_met = []
    
    # Paper trading performance
    if paper_results:
        if paper_results['return'] > 0.05:  # 5% threshold
            criteria_met.append("Paper trading: PROFITABLE")
        elif paper_results['return'] > 0:
            criteria_met.append("Paper trading: POSITIVE")
        else:
            criteria_met.append("Paper trading: NEGATIVE")
        
        if paper_results['avg_slippage'] < 0.005:  # 0.5% threshold
            criteria_met.append("Slippage: ACCEPTABLE")
        else:
            criteria_met.append("Slippage: HIGH")
    else:
        criteria_met.append("Paper trading: INSUFFICIENT DATA")
    
    # Capacity
    if capacity_results:
        if capacity_results['estimated_capacity'] > 10000000:  # $10M threshold
            criteria_met.append("Capacity: GOOD")
        elif capacity_results['estimated_capacity'] > 1000000:  # $1M threshold
            criteria_met.append("Capacity: MARGINAL")
        else:
            criteria_met.append("Capacity: POOR")
        
        if capacity_results['annual_turnover'] < 2.0:  # 200% threshold
            criteria_met.append("Turnover: LOW")
        elif capacity_results['annual_turnover'] < 5.0:  # 500% threshold
            criteria_met.append("Turnover: MEDIUM")
        else:
            criteria_met.append("Turnover: HIGH")
    
    # Drawdown
    if drawdown_results:
        if drawdown_results['max_drawdown'] > -0.15:  # 15% threshold
            criteria_met.append("Drawdown: ACCEPTABLE")
        elif drawdown_results['max_drawdown'] > -0.25:  # 25% threshold
            criteria_met.append("Drawdown: HIGH")
        else:
            criteria_met.append("Drawdown: EXTREME")
    
    for criterion in criteria_met:
        print(f"  {criterion}")
    
    # Overall verdict
    print(f"\n=== DEPLOYMENT READINESS ===")
    
    positive_criteria = sum(1 for c in criteria_met if "PROFITABLE" in c or "POSITIVE" in c or "GOOD" in c or "ACCEPTABLE" in c or "LOW" in c)
    total_criteria = len(criteria_met)
    
    if positive_criteria >= total_criteria * 0.75:
        print(">>> READY FOR PAPER TRADING")
        print("Signal passes realistic trading validation")
    elif positive_criteria >= total_criteria * 0.5:
        print(">>> NEEDS OPTIMIZATION")
        print("Signal shows potential but requires refinement")
    else:
        print(">>> NOT READY FOR TRADING")
        print("Signal fails realistic trading validation")
    
    return {
        'paper_trading': paper_results,
        'capacity': capacity_results,
        'drawdown': drawdown_results,
        'clustering': clustering_results,
        'criteria': criteria_met
    }

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    comprehensive_paper_trading_validation()
