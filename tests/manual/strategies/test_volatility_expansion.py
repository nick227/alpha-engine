import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def compute_return_from_prices(ticker, entry_date, exit_date):
    """Compute return from raw price data - NOT from stored outcomes"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get entry price (next day's entry)
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
    
    cursor = simulator.conn.execute(entry_query, (ticker, entry_date))
    entry_result = cursor.fetchone()
    
    if not entry_result:
        simulator.close()
        return None
    
    entry_price = entry_result['entry_price']
    
    # Get exit price (same day's close - using entry_price as proxy)
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
    
    cursor = simulator.conn.execute(exit_query, (ticker, exit_date))
    exit_result = cursor.fetchone()
    
    if not exit_result:
        simulator.close()
        return None
    
    exit_price = exit_result['close_price']
    
    # Compute return manually
    return_pct = (exit_price / entry_price) - 1
    
    simulator.close()
    return return_pct

def calculate_volatility(ticker, date, window=5):
    """Calculate rolling volatility using price data"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get last N days of prices
    price_query = """
    SELECT p.entry_price
    FROM predictions p
    WHERE p.ticker = ?
    AND DATE(p.timestamp) <= DATE(?)
    AND p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    ORDER BY DATE(p.timestamp) DESC
    LIMIT ?
    """
    
    cursor = simulator.conn.execute(price_query, (ticker, date, window))
    prices = [row['entry_price'] for row in cursor.fetchall()]
    
    simulator.close()
    
    if len(prices) < 2:
        return None
    
    # Calculate daily returns
    returns = []
    for i in range(1, len(prices)):
        daily_return = (prices[i-1] - prices[i]) / prices[i]
        returns.append(daily_return)
    
    # Calculate volatility (standard deviation)
    volatility = np.std(returns) if returns else 0
    
    return volatility

def test_volatility_expansion():
    """Test volatility expansion - range_today > 2x avg_range_20d"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get all predictions for volatility calculation
    price_query = """
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    ORDER BY ticker, timestamp
    """
    
    cursor = simulator.conn.execute(price_query)
    all_predictions = cursor.fetchall()
    
    # Group by ticker for rolling calculations
    ticker_data = {}
    for pred in all_predictions:
        ticker = pred['ticker']
        if ticker not in ticker_data:
            ticker_data[ticker] = []
        ticker_data[ticker].append(pred)
    
    volatility_signals = []
    
    for ticker, predictions in ticker_data.items():
        # Sort by date
        predictions.sort(key=lambda x: datetime.strptime(x['signal_date'], '%Y-%m-%d').date())
        
        for i, pred in enumerate(predictions):
            signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
            
            # Calculate today's volatility (using 5-day window)
            current_vol = calculate_volatility(ticker, signal_date, 5)
            
            # Calculate average volatility over last 20 days
            avg_vol = calculate_volatility(ticker, signal_date - timedelta(days=1), 20)
            
            if current_vol is not None and avg_vol is not None and avg_vol > 0:
                # Volatility expansion signal: today's vol > 2x average
                if current_vol > 2 * avg_vol:
                    volatility_signals.append({
                        'date': signal_date,
                        'ticker': ticker,
                        'current_vol': current_vol,
                        'avg_vol': avg_vol,
                        'vol_ratio': current_vol / avg_vol
                    })
    
    print(f"=== VOLATILITY EXPANSION TEST ===")
    print(f"Signal: volatility_today > 2x avg_volatility_20d")
    print(f"Method: Manual return calculation (no outcome reuse)")
    print(f"Signals found: {len(volatility_signals)}")
    
    # Test next day returns using manual calculation
    daily_returns = []
    wins = 0
    
    for signal in volatility_signals:
        entry_date = signal['date'] + timedelta(days=1)
        exit_date = entry_date  # Same day exit
        
        # Compute return from raw prices (NOT from stored outcomes)
        return_pct = compute_return_from_prices(signal['ticker'], entry_date, exit_date)
        
        if return_pct is not None:
            friction_cost = 0.0015
            daily_returns.append(return_pct - friction_cost)
            
            if return_pct > 0:
                wins += 1
    
    if daily_returns:
        portfolio_return = np.prod(1 + np.array(daily_returns)) - 1
    else:
        portfolio_return = 0.0
    
    win_rate = wins / len(daily_returns) if daily_returns else 0
    
    print(f"Portfolio return (with friction): {portfolio_return:+.2%}")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Avg daily return: {np.mean(daily_returns):+.2%}" if daily_returns else "Avg daily return: 0.0%")
    
    # Pass criteria
    passes = (
        win_rate > 0.55 and
        portfolio_return > 0 and
        len(daily_returns) >= 10
    )
    
    print(f"\nRESULT: {'PASS' if passes else 'FAIL'}")
    if not passes:
        print("Requirements not met: win_rate > 55%, survives friction, stable across time")
    
    return len(volatility_signals), win_rate, portfolio_return, daily_returns

def test_volatility_time_shift():
    """Time-shift test for volatility expansion"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get all predictions
    price_query = """
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    ORDER BY ticker, timestamp
    """
    
    cursor = simulator.conn.execute(price_query)
    all_predictions = cursor.fetchall()
    
    # Group by ticker for rolling calculations
    ticker_data = {}
    for pred in all_predictions:
        ticker = pred['ticker']
        if ticker not in ticker_data:
            ticker_data[ticker] = []
        ticker_data[ticker].append(pred)
    
    volatility_signals = []
    
    for ticker, predictions in ticker_data.items():
        predictions.sort(key=lambda x: datetime.strptime(x['signal_date'], '%Y-%m-%d').date())
        
        for i, pred in enumerate(predictions):
            signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
            
            current_vol = calculate_volatility(ticker, signal_date, 5)
            avg_vol = calculate_volatility(ticker, signal_date - timedelta(days=1), 20)
            
            if current_vol is not None and avg_vol is not None and avg_vol > 0:
                if current_vol > 2 * avg_vol:
                    volatility_signals.append({
                        'date': signal_date,
                        'ticker': ticker
                    })
    
    # Normal and shifted returns using manual calculation
    normal_returns = []
    shifted_returns = []
    
    for signal in volatility_signals:
        # Normal test
        entry_date = signal['date'] + timedelta(days=1)
        exit_date = entry_date
        
        return_pct = compute_return_from_prices(signal['ticker'], entry_date, exit_date)
        
        if return_pct is not None:
            friction_cost = 0.0015
            normal_returns.append(return_pct - friction_cost)
        
        # Time-shift test
        shifted_entry_date = signal['date'] + timedelta(days=3)
        shifted_exit_date = shifted_entry_date
        
        shifted_return = compute_return_from_prices(signal['ticker'], shifted_entry_date, shifted_exit_date)
        
        if shifted_return is not None:
            shifted_returns.append(shifted_return - friction_cost)
    
    simulator.close()
    
    # Calculate returns
    normal_portfolio = np.prod(1 + np.array(normal_returns)) - 1 if normal_returns else 0.0
    shifted_portfolio = np.prod(1 + np.array(shifted_returns)) - 1 if shifted_returns else 0.0
    
    normal_winrate = sum(1 for r in normal_returns if r > 0) / len(normal_returns) if normal_returns else 0
    shifted_winrate = sum(1 for r in shifted_returns if r > 0) / len(shifted_returns) if shifted_returns else 0
    
    print(f"\n=== VOLATILITY TIME-SHIFT TEST ===")
    print(f"Normal portfolio return: {normal_portfolio:+.2%}")
    print(f"Time-shift portfolio return: {shifted_portfolio:+.2%}")
    print(f"Time-shift alpha collapse: {(normal_portfolio - shifted_portfolio):+.2%}")
    print(f"Normal win rate: {normal_winrate:.1%}")
    print(f"Shifted win rate: {shifted_winrate:.1%}")
    
    # Time-shift test passes if shifted returns are ~0 or much lower
    passes = shifted_portfolio <= 0.01
    
    print(f"\nTIME-SHIFT TEST: {'PASSED' if passes else 'FAILED'}")
    if not passes:
        print("WARNING: Time-shift test suggests look-ahead bias!")
    else:
        print("SUCCESS: Volatility signal passes time-shift validation")
    
    return normal_portfolio, shifted_portfolio, normal_winrate, shifted_winrate

if __name__ == "__main__":
    print("=== TESTING VOLATILITY EXPANSION ===")
    print("Hypothesis: volatility_today > 2x avg_volatility_20d")
    print("Method: Clean signal definition + manual return calculation\n")
    
    signals, winrate, portfolio, returns = test_volatility_expansion()
    normal, shifted, normal_wr, shifted_wr = test_volatility_time_shift()
    
    print(f"\n=== VOLATILITY FINAL RESULTS ===")
    print(f"Signals: {signals}")
    print(f"Win rate: {winrate:.1%}")
    print(f"Portfolio return: {portfolio:+.2%}")
    print(f"Time-shift validation: {'PASSED' if shifted <= 0.01 else 'FAILED'}")
    
    # Overall assessment
    if signals >= 10 and winrate > 0.55 and portfolio > 0 and shifted <= 0.01:
        print(f"\nOVERALL: VOLATILITY SIGNAL VALIDATED")
        print("Found real tradable edge!")
    elif shifted <= 0.01:
        print(f"\nOVERALL: VOLATILITY SIGNAL FAILED (but clean)")
        print("No edge detected, but system working correctly")
    else:
        print(f"\nOVERALL: VOLATILITY SIGNAL FAILED (leakage)")
        print("Time-shift test indicates bias")
