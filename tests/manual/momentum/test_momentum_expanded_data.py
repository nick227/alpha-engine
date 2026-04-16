import sqlite3
import numpy as np
from datetime import datetime, timedelta
import pandas as pd

def get_momentum_signals_expanded(threshold=0.015):
    """Get momentum signals from expanded price_data table"""
    
    conn = sqlite3.connect("data/alpha.db")
    
    # Get all price data
    query = """
    SELECT ticker, date, close
    FROM price_data
    ORDER BY ticker, date
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Group by ticker for daily return calculation
    ticker_data = {}
    for ticker in df['ticker'].unique():
        ticker_df = df[df['ticker'] == ticker].copy()
        ticker_df = ticker_df.sort_values('date')
        ticker_data[ticker] = ticker_df
    
    # Calculate daily returns and find momentum signals
    momentum_signals = []
    
    for ticker, ticker_df in ticker_data.items():
        # Calculate daily returns
        ticker_df['daily_return'] = ticker_df['close'].pct_change()
        
        # Find momentum signals
        for idx, row in ticker_df.iterrows():
            if pd.notna(row['daily_return']) and row['daily_return'] > threshold:
                momentum_signals.append({
                    'date': pd.to_datetime(row['date']).date(),
                    'ticker': ticker,
                    'signal_return': row['daily_return'],
                    'close_price': row['close']
                })
    
    return momentum_signals

def get_next_day_return(ticker, signal_date):
    """Get return from close(T) to close(T+1) from price_data"""
    
    conn = sqlite3.connect("data/alpha.db")
    
    # Get entry price (close at signal date)
    entry_query = """
    SELECT close
    FROM price_data
    WHERE ticker = ?
    AND date = ?
    LIMIT 1
    """
    
    cursor = conn.execute(entry_query, (ticker, signal_date))
    entry_result = cursor.fetchone()
    
    if not entry_result:
        conn.close()
        return None
    
    entry_price = entry_result[0]
    
    # Get exit price (next day's close)
    next_day = signal_date + timedelta(days=1)
    exit_query = """
    SELECT close
    FROM price_data
    WHERE ticker = ?
    AND date = ?
    LIMIT 1
    """
    
    cursor = conn.execute(exit_query, (ticker, next_day))
    exit_result = cursor.fetchone()
    
    if not exit_result:
        conn.close()
        return None
    
    exit_price = exit_result[0]
    
    # Compute return
    return_pct = (exit_price / entry_price) - 1
    
    conn.close()
    return return_pct

def analyze_return_distribution(trade_returns):
    """Analyze return distribution to check for outlier dominance"""
    
    if not trade_returns:
        return {}
    
    returns = np.array(trade_returns)
    
    # Basic stats
    median = np.median(returns)
    q25 = np.percentile(returns, 25)
    q75 = np.percentile(returns, 75)
    
    # Top 3 winners
    sorted_returns = sorted(returns, reverse=True)
    top3_sum = sum(sorted_returns[:3])
    total_sum = sum(returns)
    top3_contribution = (top3_sum / total_sum) * 100 if total_sum != 0 else 0
    
    return {
        'median': median,
        'q25': q25,
        'q75': q75,
        'top3_contribution': top3_contribution,
        'top3_returns': sorted_returns[:3]
    }

def test_momentum_expanded_data():
    """Test momentum with expanded dataset"""
    
    print("=== MOMENTUM TEST WITH EXPANDED DATASET ===")
    print("Using 30 tickers, 4 years of data (2021-2024)\n")
    
    # Test different thresholds
    thresholds = [0.01, 0.0125, 0.015, 0.02, 0.025]
    
    results = {}
    
    for threshold in thresholds:
        print(f"=== THRESHOLD: >{threshold*100:.1f}% ===")
        
        # Get signals
        signals = get_momentum_signals_expanded(threshold)
        print(f"Signals found: {len(signals)}")
        
        if len(signals) < 50:
            print(f"Insufficient signals - need at least 50")
            continue
        
        # Group by date for portfolio construction
        signals_by_date = {}
        for signal in signals:
            date = signal['date']
            if date not in signals_by_date:
                signals_by_date[date] = []
            signals_by_date[date].append(signal)
        
        # Portfolio variables
        equity = 1.0
        daily_returns = []
        all_trade_returns = []
        
        # Process each trading day
        trading_dates = sorted(signals_by_date.keys())
        
        for date in trading_dates:
            day_signals = signals_by_date[date]
            
            # Get returns for all signals on this day
            trade_returns = []
            
            for signal in day_signals:
                trade_return = get_next_day_return(signal['ticker'], date)
                
                if trade_return is not None:
                    # Apply friction
                    trade_return -= 0.0015
                    trade_returns.append(trade_return)
                    all_trade_returns.append(trade_return)
            
            if len(trade_returns) == 0:
                continue
            
            # Equal weight across trades
            day_return = sum(trade_returns) / len(trade_returns)
            daily_returns.append(day_return)
            
            # Compound
            equity *= (1 + day_return)
        
        # Calculate results
        total_return = equity - 1
        win_rate = sum(1 for r in all_trade_returns if r > 0) / len(all_trade_returns) if all_trade_returns else 0
        
        # Analyze distribution
        dist_analysis = analyze_return_distribution(all_trade_returns)
        
        print(f"Trading days: {len(daily_returns)}")
        print(f"Total trades: {len(all_trade_returns)}")
        print(f"Win rate: {win_rate:.1%}")
        print(f"Portfolio return: {total_return:+.2%}")
        print(f"Median return: {dist_analysis.get('median', 0):+.2%}")
        print(f"Q25/Q75: {dist_analysis.get('q25', 0):+.2%}/{dist_analysis.get('q75', 0):+.2%}")
        print(f"Top 3 contribution: {dist_analysis.get('top3_contribution', 0):.1f}%")
        
        # Check if meets minimum criteria
        if len(all_trade_returns) >= 100 and win_rate > 0.52 and total_return > 0:
            print(f"*** MEETS 100+ TRADE CRITERIA ***")
        
        results[threshold] = {
            'signals': len(signals),
            'trades': len(all_trade_returns),
            'win_rate': win_rate,
            'return': total_return,
            'median': dist_analysis.get('median', 0),
            'q25': dist_analysis.get('q25', 0),
            'q75': dist_analysis.get('q75', 0),
            'top3_contribution': dist_analysis.get('top3_contribution', 0)
        }
        
        print()
    
    # Summary analysis
    print("=== EXPANDED DATASET ANALYSIS ===")
    print("Finding thresholds with 100+ trades and positive returns\n")
    
    viable_combinations = []
    
    for threshold in sorted(results.keys()):
        r = results[threshold]
        
        if r['trades'] >= 100 and r['win_rate'] > 0.52 and r['return'] > 0:
            viable_combinations.append({
                'threshold': threshold,
                'trades': r['trades'],
                'win_rate': r['win_rate'],
                'return': r['return'],
                'top3_contribution': r['top3_contribution'],
                'median': r['median']
            })
    
    if viable_combinations:
        print("VIABLE BASELINES FOUND:")
        print("Threshold  Trades  Win Rate  Return  Top3%  Median")
        print("-" * 55)
        
        for combo in viable_combinations:
            print(f"{combo['threshold']*100:>8.1f}%   {combo['trades']:>6}   {combo['win_rate']:>7.1%}   {combo['return']:>+6.2f}   {combo['top3_contribution']:>5.1f}%   {combo['median']:>+6.2f}")
        
        # Find best option
        best = max(viable_combinations, key=lambda x: x['return'])
        
        print(f"\n=== RECOMMENDED BASELINE ===")
        print(f"Threshold: >{best['threshold']*100:.1f}%")
        print(f"Trades: {best['trades']}")
        print(f"Win rate: {best['win_rate']:.1%}")
        print(f"Return: {best['return']:+.2%}")
        print(f"Top 3 contribution: {best['top3_contribution']:.1f}%")
        print(f"Median return: {best['median']:+.2%}")
        
        if best['top3_contribution'] < 30:
            print(">>> ROBUST EDGE: Low outlier dependence")
        else:
            print(">>> FRAGILE EDGE: High outlier dependence")
        
        return best
        
    else:
        print("No combinations meet 100+ trade criteria")
        
        # Show best available
        if results:
            best_by_trades = max(results.values(), key=lambda x: x['trades'])
            print(f"\n=== BEST AVAILABLE ===")
            print(f"Most trades: {best_by_trades['trades']} at threshold {max(results.keys())*100:.1f}%")
            print(f"Win rate: {best_by_trades['win_rate']:.1%}")
            print(f"Return: {best_by_trades['return']:+.2%}")
        
        return None

def test_time_shift_expanded(threshold=0.015):
    """Time-shift test with expanded dataset"""
    
    print(f"\n=== TIME-SHIFT TEST: >{threshold*100:.1f}% ===")
    
    # Get signals
    signals = get_momentum_signals_expanded(threshold)
    
    # Group by date
    signals_by_date = {}
    for signal in signals:
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
            trade_return = get_next_day_return(signal['ticker'], date)
            if trade_return is not None:
                normal_trade_returns.append(trade_return - 0.0015)
        
        if normal_trade_returns:
            normal_returns.append(sum(normal_trade_returns) / len(normal_trade_returns))
        
        # Time-shift test (shift by 2 days)
        shifted_date = date + timedelta(days=2)
        shifted_trade_returns = []
        for signal in day_signals:
            shifted_return = get_next_day_return(signal['ticker'], shifted_date)
            if shifted_return is not None:
                shifted_trade_returns.append(shifted_return - 0.0015)
        
        if shifted_trade_returns:
            shifted_returns.append(sum(shifted_trade_returns) / len(shifted_trade_returns))
    
    # Calculate portfolio returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    print(f"Normal portfolio return: {normal_portfolio:+.2%}")
    print(f"Time-shift portfolio return: {shifted_portfolio:+.2%}")
    print(f"Time-shift alpha collapse: {(normal_portfolio - shifted_portfolio):+.2%}")
    
    passes = shifted_portfolio <= 0.01
    print(f"TIME-SHIFT TEST: {'PASSED' if passes else 'FAILED'}")
    
    return normal_portfolio, shifted_portfolio, passes

if __name__ == "__main__":
    best_baseline = test_momentum_expanded_data()
    
    if best_baseline:
        test_time_shift_expanded(best_baseline['threshold'])
