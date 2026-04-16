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
    base_spread = 0.0008  # 0.08% base spread (more conservative)
    impact_factor = 0.00015  # 0.015% per $1M traded
    
    # Gap risk component (higher for momentum trades)
    gap_risk = 0.0002  # 0.02% gap risk
    
    # Adjust for volatility (higher vol = higher slippage)
    volatility_multiplier = 1 + (market_volatility - 0.2)
    
    # Adjust for trade size
    size_multiplier = 1 + (trade_size / 1000000)
    
    # Calculate total slippage
    total_slippage = (base_spread + impact_factor * trade_size / 1000000 + gap_risk) * volatility_multiplier * size_multiplier
    
    # Apply slippage to return
    adjusted_return = trade_return - total_slippage
    
    return adjusted_return, total_slippage

def extended_paper_trading(df, start_date, end_date):
    """Extended paper trading on longer unseen period"""
    
    print(f"=== EXTENDED PAPER TRADING: {start_date} to {end_date} ===")
    
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
    
    # Calculate realistic returns with conservative costs
    realistic_returns = []
    slippage_costs = []
    trade_details = []
    
    for signal in selected_signals:
        # Estimate market volatility
        ticker_data = paper_data[paper_data['ticker'] == signal['ticker']]
        if len(ticker_data) > 20:
            daily_returns = ticker_data['close'].pct_change().dropna()
            market_volatility = daily_returns.std() * np.sqrt(252)
        else:
            market_volatility = 0.25  # Default 25% annual vol
        
        # Estimate trade size (equal weight portfolio)
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
        
        trade_details.append({
            'date': signal['date'],
            'ticker': signal['ticker'],
            'raw_return': signal['trade_return'],
            'adjusted_return': adjusted_return,
            'slippage': slippage,
            'signal_strength': signal['signal_return']
        })
    
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
        'returns': realistic_returns,
        'trade_details': trade_details
    }

def payoff_structure_analysis(trade_details):
    """Analyze payoff structure and outlier dependence"""
    
    print("\n=== PAYOFF STRUCTURE ANALYSIS ===")
    
    if not trade_details:
        print("No trade details for analysis")
        return None
    
    returns = [t['adjusted_return'] for t in trade_details]
    
    # Separate wins and losses
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    
    # Basic payoff metrics
    avg_win = np.mean(wins) if wins else 0
    avg_loss = np.mean(losses) if losses else 0
    median_return = np.median(returns)
    
    print(f"Avg win: {avg_win:+.2%}")
    print(f"Avg loss: {avg_loss:+.2%}")
    print(f"Median return: {median_return:+.2%}")
    print(f"Win/Loss ratio: {abs(avg_win/avg_loss):.2f}" if avg_loss != 0 else "Win/Loss ratio: N/A")
    
    # Worst 5 trades
    sorted_returns = sorted(returns)
    worst_5 = sorted_returns[:5]
    best_5 = sorted_returns[-5:]
    
    print(f"\nWorst 5 trades:")
    for i, r in enumerate(worst_5):
        print(f"  {i+1}: {r:+.2%}")
    
    print(f"\nBest 5 trades:")
    for i, r in enumerate(reversed(best_5)):
        print(f"  {i+1}: {r:+.2%}")
    
    # Contribution of top 5 winners
    total_return = sum(returns)
    top5_contribution = sum(best_5) / total_return * 100 if total_return != 0 else 0
    
    print(f"\nTop 5 winners contribution: {top5_contribution:.1f}%")
    
    # Outlier dependence check
    if top5_contribution > 30:
        print(">>> WARNING: High outlier dependence")
        robust = False
    elif top5_contribution > 20:
        print(">>> CAUTION: Moderate outlier dependence")
        robust = True
    else:
        print(">>> GOOD: Low outlier dependence")
        robust = True
    
    return {
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'median_return': median_return,
        'worst_5': worst_5,
        'best_5': best_5,
        'top5_contribution': top5_contribution,
        'robust': robust
    }

def minimal_risk_overlays(trade_details, max_positions=1, capital_per_trade=0.1, portfolio_heat_cap=0.5):
    """Apply minimal risk overlays"""
    
    print("\n=== MINIMAL RISK OVERLAYS ===")
    print(f"Max positions: {max_positions}")
    print(f"Capital per trade: {capital_per_trade:.1%}")
    print(f"Portfolio heat cap: {portfolio_heat_cap:.1%}")
    
    # Sort by date and signal strength
    sorted_trades = sorted(trade_details, key=lambda x: (x['date'], -x['signal_strength']))
    
    # Apply position limits
    selected_trades = []
    current_positions = {}
    current_heat = 0
    
    for trade in sorted_trades:
        # Check position limits
        if len(current_positions) >= max_positions:
            # Remove oldest position
            oldest_date = min(current_positions.values())
            current_positions = {k: v for k, v in current_positions.items() if v > oldest_date}
            current_heat = len(current_positions) * capital_per_trade
        
        # Check portfolio heat
        if current_heat + capital_per_trade > portfolio_heat_cap:
            continue
        
        # Add position
        trade_date = trade['date']
        exit_date = trade_date + timedelta(days=3)  # 3-day hold
        
        # Calculate adjusted return with position sizing
        sized_return = trade['adjusted_return'] * capital_per_trade
        
        selected_trades.append({
            'date': trade['date'],
            'ticker': trade['ticker'],
            'raw_return': trade['raw_return'],
            'adjusted_return': trade['adjusted_return'],
            'sized_return': sized_return,
            'slippage': trade['slippage'],
            'signal_strength': trade['signal_strength'],
            'position_size': capital_per_trade
        })
        
        current_positions[trade['ticker']] = exit_date
        current_heat += capital_per_trade
    
    print(f"Trades after risk overlays: {len(selected_trades)}")
    
    # Calculate performance with risk overlays
    if selected_trades:
        sized_returns = [t['sized_return'] for t in selected_trades]
        
        # Aggregate returns by day (portfolio level)
        daily_returns = {}
        for trade in selected_trades:
            date = trade['date']
            if date not in daily_returns:
                daily_returns[date] = []
            daily_returns[date].append(trade['sized_return'])
        
        # Calculate daily portfolio returns
        portfolio_returns = []
        for date in sorted(daily_returns.keys()):
            day_return = sum(daily_returns[date])
            portfolio_returns.append(day_return)
        
        # Calculate portfolio metrics
        portfolio_total_return = np.prod(1 + np.array(portfolio_returns)) - 1
        win_rate = sum(1 for r in portfolio_returns if r > 0) / len(portfolio_returns)
        
        print(f"Portfolio win rate: {win_rate:.1%}")
        print(f"Portfolio total return: {portfolio_total_return:+.2%}")
        
        return {
            'trades': len(selected_trades),
            'portfolio_returns': portfolio_returns,
            'win_rate': win_rate,
            'total_return': portfolio_total_return
        }
    
    return None

def extended_validation_pipeline():
    """Extended validation pipeline with frozen rules"""
    
    print("=== EXTENDED PAPER TRADING VALIDATION ===")
    print("Testing frozen rule set on extended unseen period\n")
    
    # Get data
    df = get_price_data()
    df['date'] = pd.to_datetime(df['date'])
    
    # Use last 12 months as extended paper trading period
    max_date = df['date'].max()
    paper_start = max_date - timedelta(days=365)
    paper_end = max_date
    
    print(f"Extended paper trading period: {paper_start.strftime('%Y-%m-%d')} to {paper_end.strftime('%Y-%m-%d')}")
    print(f"Duration: {(paper_end - paper_start).days} days")
    
    # Step 1: Extended paper trading
    paper_results = extended_paper_trading(df, paper_start, paper_end)
    
    if not paper_results:
        print("Failed extended paper trading")
        return None
    
    # Step 2: Payoff structure analysis
    payoff_results = payoff_structure_analysis(paper_results['trade_details'])
    
    # Step 3: Minimal risk overlays
    risk_results = minimal_risk_overlays(paper_results['trade_details'])
    
    # Final assessment
    print(f"\n=== EXTENDED VALIDATION ASSESSMENT ===")
    
    criteria_met = []
    
    # Extended paper trading
    if paper_results['signals'] >= 50:  # Minimum 50 trades
        criteria_met.append("Sample size: ADEQUATE")
    else:
        criteria_met.append("Sample size: SMALL")
    
    if paper_results['win_rate'] >= 0.55:  # 55% threshold
        criteria_met.append("Win rate: STRONG")
    elif paper_results['win_rate'] >= 0.52:  # 52% threshold
        criteria_met.append("Win rate: MARGINAL")
    else:
        criteria_met.append("Win rate: POOR")
    
    if paper_results['return'] >= 0.05:  # 5% threshold
        criteria_met.append("Return: STRONG")
    elif paper_results['return'] >= 0.02:  # 2% threshold
        criteria_met.append("Return: POSITIVE")
    else:
        criteria_met.append("Return: NEGATIVE")
    
    # Payoff structure
    if payoff_results and payoff_results['robust']:
        criteria_met.append("Payoff: ROBUST")
    else:
        criteria_met.append("Payoff: FRAGILE")
    
    # Risk overlays
    if risk_results and risk_results['total_return'] > 0:
        criteria_met.append("Risk overlays: EFFECTIVE")
    else:
        criteria_met.append("Risk overlays: INEFFECTIVE")
    
    for criterion in criteria_met:
        print(f"  {criterion}")
    
    # Overall verdict
    print(f"\n=== EXTENDED VALIDATION VERDICT ===")
    
    positive_criteria = sum(1 for c in criteria_met if "STRONG" in c or "ADEQUATE" in c or "ROBUST" in c or "EFFECTIVE" in c)
    total_criteria = len(criteria_met)
    
    if positive_criteria >= total_criteria * 0.75:
        print(">>> READY FOR OPTIMIZATION")
        print("Extended validation confirms edge quality")
    elif positive_criteria >= total_criteria * 0.5:
        print(">>> PROMISING - NEEDS MORE VALIDATION")
        print("Edge shows potential but needs further testing")
    else:
        print(">>> INSUFFICIENT EVIDENCE")
        print("Extended validation does not confirm edge")
    
    return {
        'paper_trading': paper_results,
        'payoff': payoff_results,
        'risk_overlays': risk_results,
        'criteria': criteria_met
    }

if __name__ == "__main__":
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    extended_validation_pipeline()
