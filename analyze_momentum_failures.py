import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

def get_momentum_trades():
    """Get all momentum trades with detailed data"""
    
    simulator = PortfolioSimulator("data/alpha.db")
    simulator.connect()
    
    # Get all predictions to calculate daily returns
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
    
    # Group by ticker for daily return calculation
    ticker_data = {}
    for pred in all_predictions:
        ticker = pred['ticker']
        if ticker not in ticker_data:
            ticker_data[ticker] = []
        ticker_data[ticker].append(pred)
    
    # Calculate daily returns and find momentum signals
    momentum_trades = []
    
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
                
                # Momentum signal: > +2% gain
                if daily_return > 0.02:
                    # Get trade details
                    entry_price = current_price
                    
                    # Get next day's close
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
                    
                    if exit_result:
                        exit_price = exit_result['close_price']
                        trade_return = (exit_price / entry_price) - 1
                        
                        # Get additional context
                        context_query = """
                        SELECT 
                            p.entry_price as prev_price,
                            po.return_pct as outcome_return
                        FROM predictions p
                        LEFT JOIN prediction_outcomes po ON p.id = po.prediction_id
                        WHERE p.ticker = ?
                        AND DATE(p.timestamp) = DATE(?)
                        AND p.mode = 'backtest'
                        AND p.horizon = '7d'
                        AND p.entry_price IS NOT NULL
                        LIMIT 1
                        """
                        
                        cursor = simulator.conn.execute(context_query, (ticker, signal_date - timedelta(days=1)))
                        context = cursor.fetchone()
                        
                        momentum_trades.append({
                            'date': signal_date,
                            'ticker': ticker,
                            'signal_return': daily_return,
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'trade_return': trade_return,
                            'prev_price': context['prev_price'] if context else None,
                            'outcome_return': context['outcome_return'] if context else None
                        })
    
    simulator.close()
    return momentum_trades

def analyze_momentum_failures():
    """Analyze differences between winning and losing momentum trades"""
    
    trades = get_momentum_trades()
    
    print(f"=== MOMENTUM FAILURE ANALYSIS ===")
    print(f"Total momentum trades: {len(trades)}")
    
    # Separate winners and losers
    winners = [t for t in trades if t['trade_return'] > 0]
    losers = [t for t in trades if t['trade_return'] <= 0]
    
    print(f"Winning trades: {len(winners)}")
    print(f"Losing trades: {len(losers)}")
    
    if not winners or not losers:
        print("Insufficient data for analysis")
        return
    
    # Analyze characteristics
    print(f"\n=== WINNERS ANALYSIS ===")
    print(f"Signal return (avg): {np.mean([w['signal_return'] for w in winners]):+.2%}")
    print(f"Trade return (avg): {np.mean([w['trade_return'] for w in winners]):+.2%}")
    print(f"Signal return (max): {max([w['signal_return'] for w in winners]):+.2%}")
    print(f"Trade return (max): {max([w['trade_return'] for w in winners]):+.2%}")
    
    print(f"\n=== LOSERS ANALYSIS ===")
    print(f"Signal return (avg): {np.mean([l['signal_return'] for l in losers]):+.2%}")
    print(f"Trade return (avg): {np.mean([l['trade_return'] for l in losers]):+.2%}")
    print(f"Signal return (max): {max([l['signal_return'] for l in losers]):+.2%}")
    print(f"Trade return (min): {min([l['trade_return'] for l in losers]):+.2%}")
    print(f"Trade return (max loss): {min([l['trade_return'] for l in losers]):+.2%}")
    
    # Look for patterns in losers
    print(f"\n=== LOSER PATTERNS ===")
    
    # Check for extreme signal returns in losers
    extreme_losers = [l for l in losers if l['signal_return'] > 0.05]  # >5% signal
    normal_losers = [l for l in losers if l['signal_return'] <= 0.05]
    
    print(f"Extreme signal losers (>5%): {len(extreme_losers)}")
    print(f"Normal signal losers (≤5%): {len(normal_losers)}")
    
    if extreme_losers:
        print(f"Extreme losers avg trade return: {np.mean([l['trade_return'] for l in extreme_losers]):+.2%}")
        print(f"Normal losers avg trade return: {np.mean([l['trade_return'] for l in normal_losers]):+.2%}")
    
    # Check for gap reversals
    gap_reversals = []
    for loser in losers:
        if loser['prev_price']:
            gap = (loser['entry_price'] - loser['prev_price']) / loser['prev_price']
            if gap > 0.03:  # >3% gap up
                gap_reversals.append(loser)
    
    print(f"Gap reversal losers: {len(gap_reversals)}")
    
    # Summary insights
    print(f"\n=== KEY INSIGHTS ===")
    
    # Signal strength vs performance
    winner_signals = [w['signal_return'] for w in winners]
    loser_signals = [l['signal_return'] for l in losers]
    
    print(f"Avg signal strength - Winners: {np.mean(winner_signals):+.2%}")
    print(f"Avg signal strength - Losers: {np.mean(loser_signals):+.2%}")
    
    # Check if losers have stronger signals
    if np.mean(loser_signals) > np.mean(winner_signals):
        print("🔍 LOSERS HAVE STRONGER SIGNALS ON AVERAGE")
        print("   Suggests: Strong momentum = reversals")
    else:
        print("🔍 WINNERS HAVE STRONGER SIGNALS ON AVERAGE")
        print("   Suggests: Signal strength correlates with success")
    
    # Check for extreme loser concentration
    if len(extreme_losers) > len(losers) * 0.5:
        print("🔍 EXTREME SIGNALS DOMINATE LOSERS")
        print("   Suggests: Filter out >5% signals")
    
    # Gap reversal impact
    if len(gap_reversals) > len(losers) * 0.3:
        print("🔍 GAP REVERSALS DOMINATE LOSERS")
        print("   Suggests: Avoid gap-up signals")

if __name__ == "__main__":
    analyze_momentum_failures()
