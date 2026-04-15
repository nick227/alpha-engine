# Alpaca Paper Trading Automation Guide

## Overview

This guide explains how to set up and automate paper trading using Alpaca's paper trading API with the Alpha Engine system.

## Prerequisites

### 1. Alpaca Account Setup
- Create a free Alpaca account at [alpaca.markets](https://alpaca.markets)
- Enable paper trading in your account dashboard
- Generate API keys for paper trading

### 2. Environment Configuration
Add your Alpaca paper trading credentials to `.env`:

```bash
# Alpaca Paper Trading
ALPACA_PAPER_KEY=your_paper_api_key_here
ALPACA_PAPER_SECRET=your_paper_api_secret_here
```

### 3. Install Dependencies
```bash
pip install alpaca-py
```

## Quick Start

### 1. Test Connection
```bash
python dev_scripts/scripts/smoke_test_alpaca.py
```

This will:
- Connect to your Alpaca paper account
- Buy 1 share of SPY
- Verify the position syncs to local database
- Sell 1 share of SPY
- Complete the test trade cycle

### 2. Verify Setup
After running the smoke test, check:
- Alpaca dashboard shows the test trades
- Local database has the position records
- No errors in the logs

## Automation Components

### Core Files

#### `app/trading/providers/alpaca.py`
- **Purpose**: Alpaca trading provider implementation
- **Features**: Order submission, position tracking, account sync
- **Safety**: Built-in limits for testing (1 share max, $10 notional max)

#### `dev_scripts/scripts/smoke_test_alpaca.py`
- **Purpose**: Connection and basic trade testing
- **Use**: Verify API connectivity and basic functionality

## Automated Trading Workflow

### 1. Daily Automation Setup

Create a new batch file for automated paper trading:

```batch
@echo off
cd /d C:\wamp64\www\alpha-engine-poc
call .venv\Scripts\activate
python scripts/auto_paper_trading.py >> logs\paper_trading.log 2>&1
```

Save as `run_paper_trading_automation.bat`

### 2. Task Scheduler Integration

Add to your daily automation schedule:

```powershell
# Task 4 - Paper Trading (8:00 AM)
$action = New-ScheduledTaskAction -Execute "C:\wamp64\www\alpha-engine-poc\run_paper_trading_automation.bat"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 8:00AM
Register-ScheduledTask -TaskName "AlphaEngine - Paper Trading" -Action $action -Trigger $trigger -RunLevel Highest -Force
```

### 3. Trading Script

Create `scripts/auto_paper_trading.py`:

```python
import asyncio
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

from app.trading.providers import AlpacaProvider
from app.db.repository import AlphaRepository
from app.trading.trade_lifecycle import OrderType

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AutoPaperTrading")

async def run_daily_trading():
    load_dotenv()
    
    api_key = os.getenv('ALPACA_PAPER_KEY')
    api_secret = os.getenv('ALPACA_PAPER_SECRET')
    
    if not api_key or not api_secret:
        logger.error("ALPACA_PAPER_KEY or ALPACA_PAPER_SECRET not found in .env")
        return

    # Initialize components
    repo = AlphaRepository("data/alpha.db")
    provider = AlpacaProvider(api_key, api_secret, repo, paper=True)
    
    try:
        logger.info("Starting daily paper trading automation...")
        
        # 1. Sync current state
        await provider.sync()
        logger.info("Account and positions synchronized")
        
        # 2. Get today's predictions from the system
        # This would integrate with your prediction system
        predictions = get_today_predictions(repo)
        logger.info(f"Found {len(predictions)} predictions for today")
        
        # 3. Execute trades based on predictions
        for pred in predictions:
            if should_trade_prediction(pred):
                await execute_trade(provider, pred)
        
        # 4. Final sync
        await provider.sync()
        logger.info("Daily paper trading completed successfully")
        
    except Exception as e:
        logger.exception(f"Paper trading automation failed: {e}")
    finally:
        repo.close()

def get_today_predictions(repo):
    """Get today's trading predictions from the system."""
    # This would integrate with your prediction system
    # For now, return empty list
    return []

def should_trade_prediction(prediction):
    """Determine if a prediction should be traded."""
    # Add your trading logic here
    # Example: Only trade if confidence > 0.7
    return prediction.get('confidence', 0) > 0.7

async def execute_trade(provider, prediction):
    """Execute a trade based on a prediction."""
    ticker = prediction['symbol']
    direction = 'long' if prediction['prediction'] > 0 else 'short'
    quantity = 1  # Fixed quantity for paper trading
    
    try:
        order = await provider.submit_order(
            ticker=ticker,
            quantity=quantity,
            direction=direction,
            order_type=OrderType.MARKET
        )
        logger.info(f"Executed {direction} order for {ticker}: {order['id']}")
    except Exception as e:
        logger.error(f"Failed to execute trade for {ticker}: {e}")

if __name__ == "__main__":
    asyncio.run(run_daily_trading())
```

## Integration with Prediction System

### 1. Connect to Daily Predictions

Modify the `get_today_predictions()` function to pull from your actual prediction system:

```python
def get_today_predictions(repo):
    """Get today's trading predictions from the system."""
    # Example: Pull from dimensional_predictions table
    today = datetime.now().strftime('%Y-%m-%d')
    
    query = """
    SELECT symbol, prediction, confidence, axis_key
    FROM dimensional_predictions 
    WHERE prediction_date = ? 
    AND confidence >= 0.7
    ORDER BY confidence DESC
    LIMIT 10
    """
    
    predictions = repo.execute_query(query, (today,))
    return predictions
```

### 2. Position Sizing Logic

Add position sizing based on confidence:

```python
def calculate_position_size(prediction, account_balance):
    """Calculate position size based on prediction confidence."""
    confidence = prediction.get('confidence', 0)
    
    # Scale position size based on confidence
    if confidence >= 0.9:
        return 2  # Max 2 shares for high confidence
    elif confidence >= 0.8:
        return 1  # 1 share for medium confidence
    else:
        return 0  # Skip low confidence predictions
```

### 3. Risk Management

Add risk management rules:

```python
def should_trade_prediction(prediction, current_positions):
    """Apply risk management rules."""
    ticker = prediction['symbol']
    
    # Rule 1: Don't over-concentrate
    position_count = sum(1 for pos in current_positions if pos['ticker'] == ticker)
    if position_count >= 2:
        return False
    
    # Rule 2: Respect daily position limits
    if len(current_positions) >= 10:
        return False
    
    # Rule 3: Minimum confidence threshold
    if prediction.get('confidence', 0) < 0.7:
        return False
    
    return True
```

## Monitoring and Logging

### 1. Log File Monitoring

Check daily trading logs:

```bash
# View today's paper trading log
type logs\paper_trading.log

# Check for errors
findstr "ERROR" logs\paper_trading.log
```

### 2. Position Verification

Verify positions in both systems:

```bash
# Check local database
python -c "
from app.db.repository import AlphaRepository
repo = AlphaRepository('data/alpha.db')
positions = repo.get_paper_positions()
for pos in positions:
    print(f'{pos[\"ticker\"]}: {pos[\"quantity\"]} shares')
repo.close()
"

# Check Alpaca dashboard
# Visit https://app.alpaca.markets/paper/dashboard
```

### 3. Performance Tracking

Add performance tracking to your automation:

```python
def track_performance(repo, provider):
    """Track and log performance metrics."""
    try:
        account = await provider.get_account()
        positions = await provider.get_positions()
        
        total_pnl = sum(pos.unrealized_pnl for pos in positions)
        
        logger.info(f"Performance Summary:")
        logger.info(f"  Account Equity: ${account['equity']:,.2f}")
        logger.info(f"  Total P&L: ${total_pnl:,.2f}")
        logger.info(f"  Open Positions: {len(positions)}")
        
        # Store performance in database
        repo.store_performance_metrics({
            'date': datetime.now().strftime('%Y-%m-%d'),
            'equity': account['equity'],
            'pnl': total_pnl,
            'position_count': len(positions)
        })
        
    except Exception as e:
        logger.error(f"Failed to track performance: {e}")
```

## Safety Features

### 1. Built-in Limits

The AlpacaProvider includes safety limits:
- **Maximum quantity**: 1 share per order
- **Maximum notional**: $10 per order
- **Paper trading only**: Prevents accidental live trading

### 2. Additional Safety Checks

Add these to your automation:

```python
def safety_check_before_trade(provider, ticker, quantity):
    """Perform safety checks before trading."""
    try:
        account = await provider.get_account()
        
        # Check 1: Minimum buying power
        if account['buying_power'] < 100:  # $100 minimum
            logger.warning(f"Insufficient buying power: ${account['buying_power']}")
            return False
        
        # Check 2: Reasonable stock price
        # Get current price (you'd need to implement this)
        current_price = get_current_price(ticker)
        if current_price > 100:  # Don't trade expensive stocks
            logger.warning(f"Stock price too high: ${current_price}")
            return False
        
        # Check 3: Daily trade count
        today_trades = get_today_trade_count()
        if today_trades >= 20:  # Max 20 trades per day
            logger.warning(f"Daily trade limit reached: {today_trades}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Safety check failed: {e}")
        return False
```

## Troubleshooting

### Common Issues

#### 1. API Key Errors
```bash
# Check environment variables
echo $ALPACA_PAPER_KEY
echo $ALPACA_PAPER_SECRET

# Verify .env file exists
ls -la .env
```

#### 2. Connection Issues
```bash
# Test API connectivity
python -c "
import alpaca
from alpaca.trading.client import TradingClient
client = TradingClient('key', 'secret', paper=True)
account = client.get_account()
print('Connected successfully')
"
```

#### 3. Order Failures
```bash
# Check logs for specific error messages
type logs\paper_trading.log | findstr "ERROR"

# Common causes:
# - Insufficient buying power
# - Market closed
# - Invalid ticker symbol
```

### Recovery Procedures

#### 1. Sync Issues
```bash
# Force full sync
python -c "
import asyncio
from app.trading.providers import AlpacaProvider
from app.db.repository import AlphaRepository

async def force_sync():
    repo = AlphaRepository('data/alpha.db')
    provider = AlpacaProvider('key', 'secret', repo, paper=True)
    await provider.sync()
    repo.close()

asyncio.run(force_sync())
"
```

#### 2. Stuck Orders
```bash
# Check for open orders
python -c "
import alpaca
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus

client = TradingClient('key', 'secret', paper=True)
orders = client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
for order in orders:
    print(f'Open order: {order.symbol} - {order.status}')
"
```

## Best Practices

### 1. Daily Checklist
- [ ] Check paper trading log for errors
- [ ] Verify positions in Alpaca dashboard
- [ ] Monitor account equity and P&L
- [ ] Review trade execution quality

### 2. Weekly Review
- [ ] Analyze weekly performance
- [ ] Check for position concentration
- [ ] Review prediction accuracy
- [ ] Adjust risk parameters if needed

### 3. Monthly Maintenance
- [ ] Update API keys if needed
- [ ] Review and rotate positions
- [ ] Archive old trading data
- [ ] Update trading strategies

## Advanced Features

### 1. Multi-Strategy Trading
```python
def execute_strategy_trades(provider, strategy_predictions):
    """Execute trades for multiple strategies."""
    for strategy_name, predictions in strategy_predictions.items():
        logger.info(f"Executing trades for strategy: {strategy_name}")
        
        for pred in predictions:
            if should_trade_prediction(pred):
                # Add strategy tag to order
                await execute_trade_with_strategy(provider, pred, strategy_name)
```

### 2. Portfolio Rebalancing
```python
def rebalance_portfolio(provider, target_allocation):
    """Rebalance portfolio to match target allocation."""
    current_positions = await provider.get_positions()
    
    for ticker, target_weight in target_allocation.items():
        current_pos = next((p for p in current_positions if p.ticker == ticker), None)
        
        if not current_pos and target_weight > 0:
            # Buy to establish position
            await buy_to_target_weight(provider, ticker, target_weight)
        elif current_pos and target_weight == 0:
            # Sell entire position
            await sell_position(provider, ticker, current_pos.total_quantity)
```

### 3. Performance Analytics
```python
def generate_performance_report(repo, start_date, end_date):
    """Generate detailed performance report."""
    trades = repo.get_trades_in_period(start_date, end_date)
    
    metrics = {
        'total_trades': len(trades),
        'winning_trades': len([t for t in trades if t['pnl'] > 0]),
        'total_pnl': sum(t['pnl'] for t in trades),
        'max_drawdown': calculate_max_drawdown(trades),
        'sharpe_ratio': calculate_sharpe_ratio(trades)
    }
    
    return metrics
```

## Conclusion

This automation framework provides a complete paper trading solution that:
- Executes trades based on your prediction system
- Maintains safety limits and risk management
- Provides comprehensive monitoring and logging
- Integrates seamlessly with your existing Alpha Engine workflow

Start with the smoke test to verify connectivity, then gradually build up your automation with the provided components and safety features.
