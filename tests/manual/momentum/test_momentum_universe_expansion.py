import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def get_all_tickers():
    """Get all available tickers in the dataset"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    query = """
    SELECT DISTINCT ticker
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    ORDER BY ticker
    """
    
    cursor = simulator.conn.execute(query)
    tickers = [row['ticker'] for row in cursor.fetchall()]
    
    simulator.close()
    return tickers

def get_momentum_signals_expanded(threshold=0.015, ticker_list=None):
    """Get momentum signals with expanded ticker universe"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Build ticker filter
    ticker_filter = ""
    params = []
    
    if ticker_list:
        placeholders = ",".join(["?" for _ in ticker_list])
        ticker_filter = f" AND p.ticker IN ({placeholders})"
        params.extend(ticker_list)
    
    # Get all predictions to calculate daily returns
    price_query = f"""
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    {ticker_filter}
    ORDER BY ticker, timestamp
    """
    
    cursor = simulator.conn.execute(price_query, params)
    all_predictions = cursor.fetchall()
    
    # Group by ticker for daily return calculation
    ticker_data = {}
    for pred in all_predictions:
        ticker = pred['ticker']
        if ticker not in ticker_data:
            ticker_data[ticker] = []
        ticker_data[ticker].append(pred)
    
    # Calculate daily returns and find momentum signals
    momentum_signals = []
    
    for ticker, predictions in ticker_data.items():
        # Sort by date
        predictions.sort(key=lambda x: datetime.strptime(x['signal_date'], '%Y-%m-%d').date())
        
        for i, pred in enumerate(predictions):
            signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
            
            # Calculate daily return
            if i > 0:
                prev_price = predictions[i-1]['entry_price']
                current_price = pred['entry_price']
                daily_return = (current_price - prev_price) / prev_price
                
                # Momentum signal with adjustable threshold
                if daily_return > threshold:
                    momentum_signals.append({
                        'date': signal_date,
                        'ticker': ticker,
                        'signal_return': daily_return
                    })
    
    simulator.close()
    return momentum_signals

def get_next_day_return(ticker, signal_date):
    """Get return from close(T) to close(T+1)"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get entry price (close at signal date)
    entry_query = """
    SELECT p.entry_price
    FROM predictions p
    WHERE p.ticker = ?
    AND DATE(p.timestamp) = DATE(?)
    AND p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    LIMIT 1
    """
    
    cursor = simulator.conn.execute(entry_query, (ticker, signal_date))
    entry_result = cursor.fetchone()
    
    if not entry_result:
        simulator.close()
        return None
    
    entry_price = entry_result['entry_price']
    
    # Get exit price (next day's close)
    next_day = signal_date + timedelta(days=1)
    exit_query = """
    SELECT p.entry_price as close_price
    FROM predictions p
    WHERE p.ticker = ?
    AND DATE(p.timestamp) = DATE(?)
    AND p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    LIMIT 1
    """
    
    cursor = simulator.conn.execute(exit_query, (ticker, next_day))
    exit_result = cursor.fetchone()
    
    if not exit_result:
        simulator.close()
        return None
    
    exit_price = exit_result['close_price']
    
    # Compute return
    return_pct = (exit_price / entry_price) - 1
    
    simulator.close()
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

def test_momentum_universe_expansion():
    """Test momentum with expanded universe to achieve 100+ trades"""
    
    print("=== MOMENTUM UNIVERSE EXPANSION TEST ===")
    print("Expanding ticker universe to increase sample size\n")
    
    # Get all available tickers
    all_tickers = get_all_tickers()
    print(f"Total tickers available: {len(all_tickers)}")
    
    # Test different universe sizes
    universe_sizes = [
        len(all_tickers),  # Full universe
        min(50, len(all_tickers)),  # Top 50
        min(20, len(all_tickers)),  # Top 20
        min(10, len(all_tickers)),  # Top 10
    ]
    
    thresholds = [0.01, 0.0125, 0.015]
    
    results = {}
    
    for threshold in thresholds:
        print(f"\n=== THRESHOLD: >{threshold*100:.1f}% ===")
        
        threshold_results = {}
        
        for size in universe_sizes:
            # Use first N tickers
            ticker_subset = all_tickers[:size]
            
            # Get signals with expanded universe
            signals = get_momentum_signals_expanded(threshold, ticker_subset)
            
            print(f"Universe size {size:3d}: {len(signals):3d} signals")
            
            if len(signals) < 10:
                print(f"  Insufficient signals - skipping")
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
            all_trade_returns = []  # For distribution analysis
            
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
            
            print(f"  Trades: {len(all_trade_returns):3d}")
            print(f"  Win rate: {win_rate:.1%}")
            print(f"  Return: {total_return:+.2%}")
            print(f"  Median: {dist_analysis.get('median', 0):+.2%}")
            print(f"  Q25/Q75: {dist_analysis.get('q25', 0):+.2%}/{dist_analysis.get('q75', 0):+.2%}")
            print(f"  Top 3 contribution: {dist_analysis.get('top3_contribution', 0):.1f}%")
            
            threshold_results[f"universe_{size}"] = {
                'signals': len(signals),
                'trades': len(all_trade_returns),
                'win_rate': win_rate,
                'return': total_return,
                'median': dist_analysis.get('median', 0),
                'q25': dist_analysis.get('q25', 0),
                'q75': dist_analysis.get('q75', 0),
                'top3_contribution': dist_analysis.get('top3_contribution', 0)
            }
            
            # Check if meets minimum criteria
            if len(all_trade_returns) >= 100 and win_rate > 0.52 and total_return > 0:
                print(f"  *** MEETS 100+ TRADE CRITERIA ***")
        
        results[threshold] = threshold_results
    
    # Find best combinations
    print(f"\n=== BEST COMBINATIONS (100+ trades) ===")
    print("Threshold  Universe  Trades  Win Rate  Return  Top3%  Median")
    print("-" * 65)
    
    best_combinations = []
    
    for threshold in sorted(results.keys()):
        threshold_results = results[threshold]
        
        for universe_key, r in threshold_results.items():
            if r['trades'] >= 100 and r['win_rate'] > 0.52 and r['return'] > 0:
                universe_size = universe_key.split('_')[1]
                print(f"{threshold*100:>8.1f}%   {universe_size:>8}   {r['trades']:>6}   {r['win_rate']:>7.1%}   {r['return']:>+6.2f}   {r['top3_contribution']:>5.1f}%   {r['median']:>+6.2f}")
                
                best_combinations.append({
                    'threshold': threshold,
                    'universe_size': universe_size,
                    'trades': r['trades'],
                    'win_rate': r['win_rate'],
                    'return': r['return'],
                    'top3_contribution': r['top3_contribution'],
                    'median': r['median']
                })
    
    # Recommend best option
    if best_combinations:
        # Score by combination of trades, win_rate, return, and low outlier dependence
        best = None
        best_score = -999
        
        for combo in best_combinations:
            # Penalize high outlier dependence
            score = combo['trades'] * 0.1 + combo['win_rate'] * 10 + combo['return'] * 100 - combo['top3_contribution']
            if score > best_score:
                best_score = score
                best = combo
        
        print(f"\n=== RECOMMENDED BASELINE ===")
        print(f"Threshold: >{best['threshold']*100:.1f}%")
        print(f"Universe size: {best['universe_size']} tickers")
        print(f"Trades: {best['trades']}")
        print(f"Win rate: {best['win_rate']:.1%}")
        print(f"Return: {best['return']:+.2%}")
        print(f"Top 3 contribution: {best['top3_contribution']:.1f}%")
        print(f"Median return: {best['median']:+.2%}")
        
        if best['top3_contribution'] < 30:
            print("✅ Low outlier dependence - robust edge")
        else:
            print("⚠️ High outlier dependence - fragile edge")
        
        return best
    
    else:
        print("No combinations meet 100+ trade criteria")
        return None

if __name__ == "__main__":
    test_momentum_universe_expansion()
