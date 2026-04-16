import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def compute_next_day_return(ticker, signal_date):
    """Compute return from close(T) to close(T+1) - CORRECT execution"""
    
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
    
    # Compute return manually: close(T) → close(T+1)
    return_pct = (exit_price / entry_price) - 1
    
    simulator.close()
    return return_pct

def test_mean_reversion_corrected():
    """Test mean reversion with correct execution: close(T) → close(T+1)"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find -2% drops
    price_query = """
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    """
    
    cursor = simulator.conn.execute(price_query)
    all_predictions = cursor.fetchall()
    
    drop_signals = []
    
    for pred in all_predictions:
        signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
        
        # Get previous day's price to calculate drop
        prev_day_query = """
        SELECT p.entry_price as prev_price
        FROM predictions p
        WHERE p.ticker = ?
        AND DATE(p.timestamp) = DATE(?)
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND p.entry_price IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(prev_day_query, (pred['ticker'], signal_date - timedelta(days=1)))
        prev_day = cursor.fetchone()
        
        if prev_day and prev_day['prev_price']:
            daily_return = (pred['entry_price'] - prev_day['prev_price']) / prev_day['prev_price']
            
            # Signal: extreme drop
            if daily_return < -0.02:
                drop_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker']
                })
    
    print(f"=== MEAN REVERSION (CORRECTED) ===")
    print(f"Signal: daily_return < -2%")
    print(f"Execution: close(T) → close(T+1)")
    print(f"Signals found: {len(drop_signals)}")
    
    # Test next day returns using CORRECT execution
    daily_returns = []
    wins = 0
    
    for signal in drop_signals:
        # Compute return from close(T) to close(T+1)
        return_pct = compute_next_day_return(signal['ticker'], signal['date'])
        
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
    
    return len(drop_signals), win_rate, portfolio_return

def test_momentum_corrected():
    """Test momentum with correct execution: close(T) → close(T+1)"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find +2% gains
    price_query = """
    SELECT 
        DATE(p.timestamp) as signal_date,
        p.ticker,
        p.entry_price
    FROM predictions p
    WHERE p.mode = 'backtest'
    AND p.horizon = '7d'
    AND p.entry_price IS NOT NULL
    """
    
    cursor = simulator.conn.execute(price_query)
    all_predictions = cursor.fetchall()
    
    momentum_signals = []
    
    for pred in all_predictions:
        signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
        
        # Get previous day's price to calculate gain
        prev_day_query = """
        SELECT p.entry_price as prev_price
        FROM predictions p
        WHERE p.ticker = ?
        AND DATE(p.timestamp) = DATE(?)
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND p.entry_price IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(prev_day_query, (pred['ticker'], signal_date - timedelta(days=1)))
        prev_day = cursor.fetchone()
        
        if prev_day and prev_day['prev_price']:
            daily_return = (pred['entry_price'] - prev_day['prev_price']) / prev_day['prev_price']
            
            # Signal: momentum gain
            if daily_return > 0.02:
                momentum_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker']
                })
    
    print(f"\n=== MOMENTUM (CORRECTED) ===")
    print(f"Signal: daily_return > +2%")
    print(f"Execution: close(T) → close(T+1)")
    print(f"Signals found: {len(momentum_signals)}")
    
    # Test next day returns using CORRECT execution
    daily_returns = []
    wins = 0
    
    for signal in momentum_signals:
        # Compute return from close(T) to close(T+1)
        return_pct = compute_next_day_return(signal['ticker'], signal['date'])
        
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
    
    return len(momentum_signals), win_rate, portfolio_return

def test_volatility_corrected():
    """Test volatility expansion with correct execution: close(T) → close(T+1)"""
    
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
            if i >= 5:
                prices_5d = [p['entry_price'] for p in predictions[i-5:i]]
                returns_5d = [(prices_5d[j-1] - prices_5d[j]) / prices_5d[j] for j in range(1, len(prices_5d))]
                current_vol = np.std(returns_5d) if returns_5d else 0
                
                # Calculate average volatility over last 20 days
                if i >= 25:
                    prices_20d = [p['entry_price'] for p in predictions[i-25:i]]
                    returns_20d = [(prices_20d[j-1] - prices_20d[j]) / prices_20d[j] for j in range(1, len(prices_20d))]
                    avg_vol = np.std(returns_20d) if returns_20d else 0
                    
                    if avg_vol > 0 and current_vol > 2 * avg_vol:
                        volatility_signals.append({
                            'date': signal_date,
                            'ticker': ticker
                        })
    
    print(f"\n=== VOLATILITY EXPANSION (CORRECTED) ===")
    print(f"Signal: volatility_today > 2x avg_volatility_20d")
    print(f"Execution: close(T) → close(T+1)")
    print(f"Signals found: {len(volatility_signals)}")
    
    # Test next day returns using CORRECT execution
    daily_returns = []
    wins = 0
    
    for signal in volatility_signals:
        # Compute return from close(T) to close(T+1)
        return_pct = compute_next_day_return(signal['ticker'], signal['date'])
        
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
    
    return len(volatility_signals), win_rate, portfolio_return

def run_all_corrected():
    """Run all 3 signals with correct execution"""
    
    print("=== TESTING SIGNALS WITH CORRECT EXECUTION ===")
    print("Execution model: close(T) → close(T+1)")
    print("This aligns signal definition with trading logic\n")
    
    # Run all tests
    mr_signals, mr_winrate, mr_return = test_mean_reversion_corrected()
    mom_signals, mom_winrate, mom_return = test_momentum_corrected()
    vol_signals, vol_winrate, vol_return = test_volatility_corrected()
    
    # Summary
    print(f"\n=== CORRECTED RESULTS SUMMARY ===")
    print(f"Mean Reversion: {mr_signals:3d} signals, {mr_winrate:5.1%} win rate, {mr_return:+6.2%} return")
    print(f"Momentum:       {mom_signals:3d} signals, {mom_winrate:5.1%} win rate, {mom_return:+6.2%} return")
    print(f"Volatility:      {vol_signals:3d} signals, {vol_winrate:5.1%} win rate, {vol_return:+6.2%} return")
    
    # Analysis
    print(f"\n=== ANALYSIS ===")
    
    # Check for any real edges
    edges = []
    if mr_winrate > 0.55 and mr_return > 0:
        edges.append("Mean Reversion")
    if mom_winrate > 0.55 and mom_return > 0:
        edges.append("Momentum")
    if vol_winrate > 0.55 and vol_return > 0:
        edges.append("Volatility Expansion")
    
    if edges:
        print(f"REAL EDGES DETECTED: {', '.join(edges)}")
        print("These signals show genuine predictive power")
    else:
        print("NO GENUINE EDGES DETECTED")
        print("All signals show random behavior")
    
    return {
        'mean_reversion': {'signals': mr_signals, 'winrate': mr_winrate, 'return': mr_return},
        'momentum': {'signals': mom_signals, 'winrate': mom_winrate, 'return': mom_return},
        'volatility': {'signals': vol_signals, 'winrate': vol_winrate, 'return': vol_return}
    }

if __name__ == "__main__":
    run_all_corrected()
