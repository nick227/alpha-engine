import sqlite3
import numpy as np
from datetime import datetime, timedelta
from app.simulation.portfolio_simulator import PortfolioSimulator

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

def get_momentum_signals():
    """Get all momentum signals: daily_return > +2%"""
    
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
                
                # Momentum signal: > +2% gain
                if daily_return > 0.02:
                    momentum_signals.append({
                        'date': signal_date,
                        'ticker': ticker,
                        'daily_return': daily_return
                    })
    
    simulator.close()
    return momentum_signals

def test_momentum_correct_portfolio():
    """Test momentum with correct portfolio math"""
    
    print("=== MOMENTUM - CORRECT PORTFOLIO MATH ===")
    print("Execution: close(T) → close(T+1)")
    print("Portfolio: Equal weight per day, geometric compounding\n")
    
    # Get momentum signals
    momentum_signals = get_momentum_signals()
    print(f"Total momentum signals found: {len(momentum_signals)}")
    
    # Group signals by date for portfolio construction
    signals_by_date = {}
    for signal in momentum_signals:
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
            # Get return from close(T) to close(T+1)
            trade_return = get_next_day_return(signal['ticker'], date)
            
            if trade_return is not None:
                # Apply friction
                trade_return -= 0.0015
                trade_returns.append(trade_return)
        
        if len(trade_returns) == 0:
            continue
        
        # 🔑 Normalize capital: equal weight across trades
        day_return = sum(trade_returns) / len(trade_returns)
        daily_returns.append(day_return)
        
        # 🔑 Compound (not sum)
        equity *= (1 + day_return)
        
        # Track wins/losses
        for r in trade_returns:
            if r > 0:
                wins.append(r)
            else:
                losses.append(r)
    
    # Final calculations
    total_return = equity - 1
    win_rate = len(wins) / (len(wins) + len(losses)) if (wins or losses) else 0
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    max_loss = min(losses) if losses else 0
    
    print(f"\n=== PORTFOLIO RESULTS ===")
    print(f"Trading days: {len(daily_returns)}")
    print(f"Total trades: {len(wins) + len(losses)}")
    print(f"Win rate: {win_rate:.1%}")
    print(f"Portfolio return: {total_return:+.2%}")
    print(f"Avg win: {avg_win:+.2%}")
    print(f"Avg loss: {avg_loss:+.2%}")
    print(f"Max loss: {max_loss:+.2%}")
    
    # Analysis
    print(f"\n=== ANALYSIS ===")
    
    if win_rate > 0.70 and total_return < 0:
        print("CASE B: Fake 'high win rate' - avg_loss dominates avg_win")
        print("Signal looks good but loses money due to asymmetric payoffs")
    elif win_rate > 0.55 and total_return > 0:
        print("CASE A: Real edge - profitable strategy")
        print("Signal shows genuine predictive power")
    elif abs(win_rate - 0.5) < 0.1 and abs(total_return) < 0.02:
        print("CASE C: No edge - random behavior")
        print("Signal shows no predictive power")
    else:
        print("MIXED: Need further analysis")
    
    return {
        'total_return': total_return,
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'max_loss': max_loss,
        'total_trades': len(wins) + len(losses)
    }

if __name__ == "__main__":
    test_momentum_correct_portfolio()
