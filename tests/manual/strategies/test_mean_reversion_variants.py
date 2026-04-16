import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def test_variant_1_broad():
    """Variant 1: Broad baseline - daily_return < -2%"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Find all predictions to calculate daily returns
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
    
    # Calculate daily returns and find drops
    drop_signals = []
    
    for pred in all_predictions:
        signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
        
        # Get previous day's price
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
            
            # Variant 1: < -2% drop
            if daily_return < -0.02:
                drop_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker'],
                    'drop_return': daily_return
                })
    
    print(f"=== VARIANT 1: BROAD BASELINE ===")
    print(f"Signal: daily_return < -2%")
    print(f"Signals found: {len(drop_signals)}")
    
    # Test next day returns
    daily_returns = []
    wins = 0
    
    for signal in drop_signals:
        next_day = signal['date'] + timedelta(days=1)
        
        outcome_query = """
        SELECT po.return_pct
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?)
        AND p.ticker = ?
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND po.return_pct IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(outcome_query, (next_day, signal['ticker']))
        outcome = cursor.fetchone()
        
        if outcome:
            friction_cost = 0.0015
            daily_returns.append(outcome['return_pct'] - friction_cost)
            
            if outcome['return_pct'] > 0:
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

def test_variant_2_stronger():
    """Variant 2: Stronger drops - daily_return < -3%"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Same logic but with -3% threshold
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
            
            # Variant 2: < -3% drop
            if daily_return < -0.03:
                drop_signals.append({
                    'date': signal_date,
                    'ticker': pred['ticker'],
                    'drop_return': daily_return
                })
    
    print(f"\n=== VARIANT 2: STRONGER DROPS ===")
    print(f"Signal: daily_return < -3%")
    print(f"Signals found: {len(drop_signals)}")
    
    # Test next day returns
    daily_returns = []
    wins = 0
    
    for signal in drop_signals:
        next_day = signal['date'] + timedelta(days=1)
        
        outcome_query = """
        SELECT po.return_pct
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?)
        AND p.ticker = ?
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND po.return_pct IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(outcome_query, (next_day, signal['ticker']))
        outcome = cursor.fetchone()
        
        if outcome:
            friction_cost = 0.0015
            daily_returns.append(outcome['return_pct'] - friction_cost)
            
            if outcome['return_pct'] > 0:
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

def test_variant_3_exhaustion():
    """Variant 3: Exhaustion moves - daily_return < -3% AND close < rolling_5d_low"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get price history for rolling calculations
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
    
    drop_signals = []
    
    for ticker, predictions in ticker_data.items():
        # Sort by date
        predictions.sort(key=lambda x: datetime.strptime(x['signal_date'], '%Y-%m-%d').date())
        
        for i, pred in enumerate(predictions):
            signal_date = datetime.strptime(pred['signal_date'], '%Y-%m-%d').date()
            
            # Calculate daily return
            if i > 0:  # Need previous day
                prev_price = predictions[i-1]['entry_price']
                daily_return = (pred['entry_price'] - prev_price) / prev_price
                
                # Calculate rolling 5-day low
                if i >= 5:  # Need 5 days of history
                    last_5_closes = [p['entry_price'] for p in predictions[i-5:i]]
                    rolling_5d_low = min(last_5_closes)
                    
                    # Variant 3: < -3% drop AND close < rolling_5d_low
                    if daily_return < -0.03 and pred['entry_price'] < rolling_5d_low:
                        drop_signals.append({
                            'date': signal_date,
                            'ticker': ticker,
                            'drop_return': daily_return
                        })
    
    print(f"\n=== VARIANT 3: EXHAUSTION MOVES ===")
    print(f"Signal: daily_return < -3% AND close < rolling_5d_low")
    print(f"Signals found: {len(drop_signals)}")
    
    # Test next day returns
    daily_returns = []
    wins = 0
    
    for signal in drop_signals:
        next_day = signal['date'] + timedelta(days=1)
        
        outcome_query = """
        SELECT po.return_pct
        FROM predictions p
        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
        WHERE DATE(p.timestamp) = DATE(?)
        AND p.ticker = ?
        AND p.mode = 'backtest'
        AND p.horizon = '7d'
        AND po.return_pct IS NOT NULL
        LIMIT 1
        """
        
        cursor = simulator.conn.execute(outcome_query, (next_day, signal['ticker']))
        outcome = cursor.fetchone()
        
        if outcome:
            friction_cost = 0.0015
            daily_returns.append(outcome['return_pct'] - friction_cost)
            
            if outcome['return_pct'] > 0:
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

def run_all_variants():
    """Run all 3 variants and compare"""
    
    print("=== MEAN REVERSION VARIANT COMPARISON ===")
    print("Testing progressively stronger mean-reversion signals\n")
    
    # Run all variants
    v1_signals, v1_winrate, v1_return = test_variant_1_broad()
    v2_signals, v2_winrate, v2_return = test_variant_2_stronger()
    v3_signals, v3_winrate, v3_return = test_variant_3_exhaustion()
    
    # Summary
    print(f"\n=== SUMMARY COMPARISON ===")
    print(f"Variant 1 (-2%):  {v1_signals:3d} signals, {v1_winrate:5.1%} win rate, {v1_return:+6.2%} return")
    print(f"Variant 2 (-3%):  {v2_signals:3d} signals, {v2_winrate:5.1%} win rate, {v2_return:+6.2%} return")
    print(f"Variant 3 (exh):  {v3_signals:3d} signals, {v3_winrate:5.1%} win rate, {v3_return:+6.2%} return")
    
    # Analysis
    print(f"\n=== ANALYSIS ===")
    
    if all(abs(winrate - 0.5) < 0.05 for winrate in [v1_winrate, v2_winrate, v3_winrate]):
        print("CONCLUSION: No mean reversion edge detected in any variant")
        print("All variants ~50% win rate suggests random behavior")
    elif v3_winrate > 0.55:
        print("CONCLUSION: Edge exists only in exhaustion moves")
        print("Variant 3 shows significant outperformance")
    elif any(winrate > 0.52 for winrate in [v1_winrate, v2_winrate, v3_winrate]):
        print("CONCLUSION: Weak mean reversion edge detected")
        print("Some variants show slight outperformance")
    else:
        print("CONCLUSION: Mixed results - need more analysis")
    
    return {
        'variant1': {'signals': v1_signals, 'winrate': v1_winrate, 'return': v1_return},
        'variant2': {'signals': v2_signals, 'winrate': v2_winrate, 'return': v2_return},
        'variant3': {'signals': v3_signals, 'winrate': v3_winrate, 'return': v3_return}
    }

if __name__ == "__main__":
    run_all_variants()
