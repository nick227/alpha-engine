import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def get_momentum_signals(threshold=0.015, start_date=None, end_date=None):
    """Get momentum signals with expanded date range"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Build query with date range
    date_filter = ""
    params = []
    
    if start_date:
        date_filter += " AND DATE(p.timestamp) >= DATE(?)"
        params.append(start_date)
    
    if end_date:
        date_filter += " AND DATE(p.timestamp) <= DATE(?)"
        params.append(end_date)
    
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
    {date_filter}
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

def test_momentum_expanded():
    """Test momentum with expanded sample size"""
    
    print("=== MOMENTUM EXPANDED SAMPLE TEST ===")
    print("Testing lower thresholds and expanded date range\n")
    
    # Test parameters
    thresholds = [0.01, 0.0125, 0.015]
    
    # Try expanding date range
    date_ranges = [
        (None, None),  # Full range
        ("2025-01-01", None),  # From 2025
        ("2024-01-01", None),  # From 2024
    ]
    
    results = {}
    
    for threshold in thresholds:
        print(f"=== THRESHOLD: >{threshold*100:.1f}% ===")
        
        threshold_results = {}
        
        for start_date, end_date in date_ranges:
            range_desc = "Full range" if not start_date else f"From {start_date}"
            
            # Get signals
            signals = get_momentum_signals(threshold, start_date, end_date)
            print(f"{range_desc}: {len(signals)} signals")
            
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
            wins = []
            losses = []
            
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
                
                if len(trade_returns) == 0:
                    continue
                
                # Equal weight across trades
                day_return = sum(trade_returns) / len(trade_returns)
                daily_returns.append(day_return)
                
                # Compound
                equity *= (1 + day_return)
                
                # Track wins/losses
                for r in trade_returns:
                    if r > 0:
                        wins.append(r)
                    else:
                        losses.append(r)
            
            # Calculate results
            total_return = equity - 1
            win_rate = len(wins) / (len(wins) + len(losses)) if (wins or losses) else 0
            avg_win = sum(wins) / len(wins) if wins else 0
            avg_loss = sum(losses) / len(losses) if losses else 0
            
            print(f"  Trading days: {len(daily_returns)}")
            print(f"  Total trades: {len(wins) + len(losses)}")
            print(f"  Win rate: {win_rate:.1%}")
            print(f"  Portfolio return: {total_return:+.2%}")
            print(f"  Avg win: {avg_win:+.2%}")
            print(f"  Avg loss: {avg_loss:+.2%}")
            
            threshold_results[range_desc] = {
                'signals': len(signals),
                'trades': len(wins) + len(losses),
                'win_rate': win_rate,
                'return': total_return,
                'avg_win': avg_win,
                'avg_loss': avg_loss
            }
            
            # Check if meets minimum criteria
            if (len(wins) + len(losses)) >= 30 and win_rate > 0.52 and total_return > 0:
                print(f"  *** MEETS MINIMUM CRITERIA ***")
            
            print()
        
        results[threshold] = threshold_results
    
    # Summary analysis
    print("=== EXPANDED SAMPLE ANALYSIS ===")
    print("Finding thresholds with 30+ trades and positive returns\n")
    
    viable_combinations = []
    
    for threshold in sorted(results.keys()):
        threshold_results = results[threshold]
        
        for range_desc, r in threshold_results.items():
            if r['trades'] >= 30 and r['win_rate'] > 0.52 and r['return'] > 0:
                viable_combinations.append({
                    'threshold': threshold,
                    'range': range_desc,
                    'trades': r['trades'],
                    'win_rate': r['win_rate'],
                    'return': r['return']
                })
    
    if viable_combinations:
        print("VIABLE BASELINES FOUND:")
        print("Threshold  Range        Trades  Win Rate  Return")
        print("-" * 50)
        
        for combo in viable_combinations:
            print(f"{combo['threshold']*100:>8.1f}%   {combo['range']:<12} {combo['trades']:>6}   {combo['win_rate']:>7.1%}   {combo['return']:>+6.2f}")
        
        print(f"\nBest option: {viable_combinations[0]['threshold']*100:.1f}% threshold, {viable_combinations[0]['range']}")
        print(f"Ready for time-shift validation and filter testing")
        
    else:
        print("No combinations meet minimum criteria")
        print("Consider further expanding dataset or adjusting signal definition")
    
    return results, viable_combinations

def test_time_shift_validation(threshold, start_date=None, end_date=None):
    """Time-shift validation for promising baseline"""
    
    print(f"=== TIME-SHIFT VALIDATION: >{threshold*100:.1f}% ===")
    
    # Get signals
    signals = get_momentum_signals(threshold, start_date, end_date)
    
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
    results, viable = test_momentum_expanded()
    
    # If we found viable baselines, run time-shift validation
    if viable:
        print("\n" + "="*60)
        best = viable[0]
        test_time_shift_validation(best['threshold'], None, None)
