# Alpha Engine Paper Trading System

A comprehensive paper trading system that integrates with the Alpha Engine pipeline to execute simulated trades with multi-layer qualification and risk management.

## Overview

The paper trading system provides:

- **Feature-Rich Signal Processing**: 50+ predictive features across 10 categories
- **Multi-Layer Qualification**: Signal quality, risk management, and optional LLM validation
- **Portfolio Management**: Real-time position tracking and risk controls
- **Performance Analytics**: Comprehensive trade tracking and win rate analysis
- **Alpha Engine Integration**: Seamless integration with consensus signals and strategy predictions

## Architecture

### Core Components

1. **FeatureEngine** (`app/core/feature_engine.py`)
   - Comprehensive predictive state feature engineering
   - Multi-timeframe analysis (1m to 30d)
   - Advanced volatility, trend, and momentum indicators
   - Cross-asset signals (VIX, DXY, BTC, Oil)

2. **FeatureIntegration** (`app/core/feature_integration.py`)
   - Backward-compatible integration with existing pipeline
   - Legacy mode support for gradual migration
   - Feature validation and importance reporting

3. **PaperTrader** (`app/trading/paper_trader.py`)
   - Core paper trading execution engine
   - Multi-layer qualification pipeline
   - Portfolio state management
   - Risk controls and position sizing

4. **PositionSizer** (`app/trading/position_sizing.py`)
   - Advanced position sizing models
   - Kelly criterion, volatility targeting, risk parity
   - Confidence, stability, volatility, drawdown adjustments
   - Comprehensive risk management integration

5. **RiskEngine** (`app/trading/risk_engine.py`)
   - Comprehensive risk management system
   - Position limits and exposure controls
   - Daily loss limits and drawdown protection
   - Trading frequency controls and cooldowns
   - Emergency halt mechanisms

6. **AlphaEngineIntegration** (`app/trading/alpha_integration.py`)
   - Integration layer with Alpha Engine pipeline
   - Consensus signal processing
   - Strategy prediction handling

7. **TradeLifecycleManager** (`app/trading/trade_lifecycle.py`)
   - Complete trade lifecycle management
   - Multi-state position tracking
   - Automated exit handling
   - Partial exit support
   - Stop loss and trailing stops

8. **PaperTradingOrchestrator** (`app/trading/alpha_integration.py`)
   - Complete workflow orchestration
   - Session management
   - Performance tracking

### Qualification Pipeline

```
Signal Generation -> Signal Quality Filter -> Risk Management -> LLM Validation -> Position Sizing -> Execution
```

#### Signal Quality Filter
- Minimum confidence thresholds
- Consensus score validation
- Volume and liquidity checks
- Volatility limits

#### Risk Management Layer
- Position size limits (default: 1% base, 2% max)
- Ticker exposure caps (default: 10% max)
- Sector and strategy exposure limits
- Daily loss limits (default: 2% max)
- Correlation checks

#### LLM Validation Layer (Optional)
- High-conviction trade analysis (>80% confidence)
- Natural language reasoning
- Market context evaluation
- Anomaly detection

## Position Sizing Model

The system converts Alpha Engine predictions to actual trade sizes using a comprehensive model:

### Core Formula

```
position_size = f(confidence, stability, volatility, drawdown, regime)
```

### Sizing Methods

1. **Fixed Percentage**: Base size scaled by confidence and stability
2. **Kelly Criterion**: Optimal sizing based on historical win/loss ratios
3. **Volatility Targeting**: Target constant volatility exposure
4. **Risk Parity**: Equalize risk contribution across positions
5. **Confidence Scaled**: Non-linear confidence scaling
6. **Adaptive Kelly**: Kelly with market condition adjustments

### Adjustment Factors

- **Confidence**: `size *= confidence^1.5` (convex scaling)
- **Stability**: `size *= stability` (linear scaling)
- **Volatility**: `size *= target_volatility / current_volatility` (inverse scaling)
- **Drawdown**: `size *= max(0.3, 1.0 - drawdown * 2)` (reduction in drawdowns)
- **Regime**: Risk-on/risk-off adjustments (1.3x for risk-on, 0.5x for risk-off)

### Risk Limits

- Maximum position size: 2% of portfolio
- Maximum ticker exposure: 10% of portfolio
- Maximum risk per trade: 2% of portfolio
- Daily loss limits: 2% maximum daily loss

## Risk Management Engine

The comprehensive risk engine prevents paper trading from "exploding" through multiple safeguards:

### Core Risk Controls

- **Stop Loss**: Automatic position exit at 2% loss (configurable)
- **Max Exposure**: Total exposure capped at 80% of portfolio
- **Per-Ticker Cap**: Maximum 10% exposure per symbol
- **Per-Day Loss Cap**: Trading halt after 2% daily loss
- **Max Concurrent Trades**: Limit of 10 simultaneous positions
- **Cooldown After Loss**: 30-minute cooldown after consecutive losses
- **Confidence Floor**: Minimum 60% confidence after losses

### Advanced Risk Features

#### **Emergency Controls**
- Emergency halt at 5% daily loss
- Drawdown protection (15% maximum)
- Circuit breaker for high volatility
- Position size emergency caps (1% during emergencies)

#### **Trading Frequency Limits**
- Maximum 50 trades per day
- Maximum 10 trades per hour
- Automatic trade record cleanup
- Frequency-based rejection logic

#### **Exposure Management**
- Sector exposure limits (20% maximum)
- Strategy exposure limits (15% maximum)
- Correlation checks (15% maximum correlated exposure)
- Real-time exposure tracking

#### **Recovery Mechanisms**
- Automatic trading halt on critical losses
- Gradual position size reduction in drawdowns
- Confidence floor activation after losses
- Cooldown periods for risk reduction

### Risk Monitoring

```python
# Real-time risk metrics
risk_metrics = risk_engine.get_risk_summary()

# Risk alerts and warnings
alerts = risk_engine.get_risk_alerts()

# Comprehensive risk checks
risk_checks = risk_engine.check_trade_risk(
    ticker="AAPL",
    direction="long", 
    position_size=100,
    entry_price=175.50,
    confidence=0.8
)
```

### Risk Actions

The risk engine can trigger multiple actions:

- **ALLOW**: Trade passes all checks
- **REDUCE**: Suggest smaller position size
- **REJECT**: Block trade due to risk limits
- **HALT_TRADING**: Emergency trading halt
- **CLOSE_ALL**: Liquidate all positions

### Configuration

```json
{
  "risk_limits": {
    "max_position_size": 0.02,
    "max_ticker_exposure": 0.10,
    "max_daily_loss_pct": 0.02,
    "max_concurrent_trades": 10,
    "loss_cooldown_minutes": 30,
    "consecutive_loss_limit": 3,
    "confidence_floor": 0.6,
    "emergency_halt_loss_pct": 0.05,
    "max_drawdown_pct": 0.15
  }
}
```

## Trade Lifecycle Model

The system manages the complete trade lifecycle from signal generation through final exit:

```
Signal → Entry → Hold → Partial Exit/Stop → Close
```

### Trade States

1. **SIGNAL**: Initial signal received from Alpha Engine
2. **PENDING_ENTRY**: Trade created, awaiting execution
3. **ENTERED**: Position opened and active
4. **HOLDING**: Position being managed
5. **PARTIAL_EXIT**: Partial position closed
6. **STOPPED**: Stop loss triggered
7. **CLOSED**: Position completely closed
8. **CANCELLED**: Trade cancelled before execution
9. **REJECTED**: Trade rejected by risk engine

### Lifecycle Management

#### **Signal Generation**
```python
# Create trade from Alpha Engine signal
trade = lifecycle_manager.create_trade_from_signal(
    signal_id="signal_123",
    strategy_id="consensus_v1",
    ticker="AAPL",
    direction="long",
    entry_price=175.50,
    quantity=100,
    confidence=0.8,
    regime="BULLISH_MODERATE"
)
```

#### **Entry Execution**
```python
# Execute market entry
success = lifecycle_manager.execute_entry(
    trade_id="trade_123",
    execution_price=175.45,
    execution_quantity=100
)
```

#### **Position Holding**
```python
# Update position with market prices
position_update = lifecycle_manager.update_position(
    trade_id="trade_123",
    current_price=176.20
)
```

#### **Partial Exit**
```python
# Execute partial exit at profit target
success = lifecycle_manager.execute_partial_exit(
    trade_id="trade_123",
    exit_quantity=50,  # 50% of position
    exit_price=180.00,
    reason="Partial exit at 50% level"
)
```

#### **Stop Loss**
```python
# Execute stop loss
success = lifecycle_manager.execute_stop_loss(
    trade_id="trade_123",
    stop_price=172.50,
    reason="Stop loss triggered at 2%"
)
```

#### **Trade Close**
```python
# Close remaining position
success = lifecycle_manager.close_trade(
    trade_id="trade_123",
    close_price=182.00,
    reason=ExitReason.TARGET_REACHED
)
```

### Exit Conditions

#### **Target Reached**
- Price hits target level
- Automatic position closure
- Full P&L realization

#### **Stop Loss**
- Price hits stop loss level
- Immediate position closure
- Risk protection mechanism

#### **Trailing Stop**
- Dynamic stop loss adjustment
- Tracks favorable price movements
- Locks in profits

#### **Time Exit**
- Maximum hold time exceeded
- Prevents overexposure
- Configurable time limits

#### **Partial Exit Levels**
- Multiple profit-taking levels
- Configurable percentages (e.g., 25%, 50%, 75%)
- Reduces position size gradually

### Position Tracking

#### **Real-time Monitoring**
```python
# Get current portfolio positions
positions = lifecycle_manager.get_portfolio_positions()

# Returns:
{
    "AAPL": {
        "quantity": 100,
        "direction": "long",
        "entry_price": 175.50,
        "current_price": 176.20,
        "unrealized_pnl": 70.00,
        "unrealized_pnl_pct": 0.04,
        "remaining_quantity": 100
    }
}
```

#### **Trade History**
```python
# Get complete trade history
history = lifecycle_manager.get_trade_history(limit=50)

# Includes all legs, state changes, and performance metrics
```

#### **Performance Metrics**
- **Realized P&L**: Actual profit/loss on closed positions
- **Unrealized P&L**: Current profit/loss on open positions
- **Maximum Runup**: Best price achieved during trade
- **Maximum Drawdown**: Worst price movement during trade
- **Trade Duration**: Time from entry to exit
- **Win Rate**: Percentage of profitable trades

### Automated Features

#### **Price Update Processing**
```python
# Bulk update all market prices
price_updates = {
    "AAPL": 176.20,
    "MSFT": 380.50,
    "GOOGL": 140.75
}

lifecycle_manager.update_market_prices(price_updates)
# Automatically checks exit conditions and executes actions
```

#### **Callback System**
```python
# Define custom callbacks
def on_entry(trade, leg):
    print(f"Entered {trade.ticker} at {leg.price}")

def on_partial_exit(trade, leg, pnl):
    print(f"Partial exit: {pnl:.2f}")

def on_stop(trade, leg, pnl):
    print(f"Stop loss: {pnl:.2f}")

def on_exit(trade, leg, pnl):
    print(f"Trade closed: {pnl:.2f}")

# Attach callbacks to trade
trade.on_entry = on_entry
trade.on_partial_exit = on_partial_exit
trade.on_stop = on_stop
trade.on_exit = on_exit
```

### Configuration

```json
{
  "trade_lifecycle": {
    "stop_loss_pct": 0.02,
    "target_pct": 0.04,
    "trailing_stop_pct": 0.015,
    "max_hold_time_hours": 24,
    "partial_exit_enabled": true,
    "partial_exit_levels": [0.25, 0.5, 0.75]
  }
}
```

## Features

### Predictive Features (50+ indicators)

#### Multi-Timeframe Returns
- 1m, 5m, 15m, 1h, 4h, 1d, 7d, 30d returns

#### Advanced Volatility
- Realized volatility (5, 10, 20, 50 periods)
- Parkinson volatility estimator
- Garman-Klass volatility
- Volatility percentiles and regimes

#### Trend Strength
- ADX (14-period) with directional movement
- Trend classification (WEAK/MODERATE/STRONG)
- Trend direction (BULLISH/BEARISH/NEUTRAL)
- Momentum acceleration

#### Volume Analysis
- Volume ratios (5, 10, 20 periods)
- Volume anomaly detection
- Price-volume divergence
- Volume trend analysis

#### Mean Reversion
- Distance from multiple moving averages
- Bollinger Band position and width
- Mean reversion scoring
- Statistical distance measures

#### Gap Detection
- Gap size measurement
- Gap fill probability
- Historical gap patterns
- Overnight gap analysis

#### Cross-Asset Signals
- VIX, DXY, BTC, Oil correlations
- Cross-asset regime classification
- Risk-on/risk-off signals

#### Market Microstructure
- Intraday return patterns
- Range expansion analysis
- Close position within range
- Overnight gap effects

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Save default configuration
python -m app.cli.paper_trade save-config --output config/paper_trading.json

# Initialize system
python -m app.cli.paper_trade init --config config/paper_trading.json
```

## Quick Start

### Demo Session

```bash
# Run demo with default settings
python -m app.cli.paper_trade demo

# Run with custom configuration
python -m app.cli.paper_trade run --config config/paper_trading.json
```

### Using with Alpha Engine Pipeline

```python
import asyncio
from app.trading import PaperTradingSystem
from app.core.types import RawEvent

async def run_paper_trading():
    # Initialize system
    system = PaperTradingSystem("config/paper_trading.json")
    
    # Your Alpha Engine pipeline data
    raw_events = [...]  # List of RawEvent objects
    price_contexts = {...}  # Price contexts dict
    
    # Run paper trading session
    results = await system.run_with_pipeline_data(
        raw_events=raw_events,
        price_contexts=price_contexts,
        market_data={'prices': {'AAPL': 175.50, 'MSFT': 380.25}}
    )
    
    print(f"Executed {results['total_trades']} trades")
    return results

# Run the session
asyncio.run(run_paper_trading())
```

## Configuration

### Basic Configuration

```json
{
  "initial_cash": 100000.0,
  "tenant_id": "paper_trading",
  "base_position_pct": 0.01,
  "max_position_pct": 0.02,
  "min_confidence": 0.6,
  "min_consensus": 0.5,
  "max_ticker_exposure": 0.10,
  "max_daily_loss_pct": 0.02,
  "llm_validation_enabled": false,
  "simulation_mode": true
}
```

### Risk Management Configuration

```json
{
  "risk_management": {
    "max_position_pct": 0.02,
    "max_ticker_exposure": 0.10,
    "max_sector_exposure": 0.20,
    "max_strategy_exposure": 0.15,
    "max_daily_loss_pct": 0.02,
    "max_correlation_exposure": 0.15
  }
}
```

### LLM Validation Configuration

```json
{
  "llm_validation": {
    "enabled": true,
    "min_confidence_for_llm": 0.8,
    "provider": "openai",
    "model": "gpt-4"
  }
}
```

## CLI Commands

### System Management

```bash
# Initialize with environment preset
python -m app.cli.paper_trade init --env dev

# Check system status
python -m app.cli.paper_trade status --config config/paper_trading.json

# Validate configuration
python -m app.cli.paper_trade validate --config config/paper_trading.json
```

### Trading Operations

```bash
# Run paper trading session
python -m app.cli.paper_trade run --config config/paper_trading.json --output results.json

# View trade history
python -m app.cli.paper_trade history --config config/paper_trading.json --limit 20

# Run demo session
python -m app.cli.paper_trade demo
```

### Configuration Management

```bash
# Save default configuration
python -m app.cli.paper_trade save-config --output config/paper_trading.json

# Use environment presets
python -m app.cli.paper_trade init --env prod
```

## API Reference

### PaperTradingSystem

```python
class PaperTradingSystem:
    def __init__(self, config_path: Optional[str] = None)
    
    async def run_with_pipeline_data(
        self,
        raw_events: List[RawEvent],
        price_contexts: Dict[str, Any],
        market_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]
    
    async def run_demo_session(self) -> Dict[str, Any]
    
    def get_system_status(self) -> Dict[str, Any]
```

### PaperTrader

```python
class PaperTrader:
    def __init__(self, config: Dict[str, Any])
    
    async def process_signal(
        self,
        ticker: str,
        strategy_id: str,
        direction: TradeDirection,
        confidence: float,
        consensus_score: float,
        alpha_score: float,
        feature_snapshot: Dict[str, Any],
        entry_price: float,
        regime: str = "UNKNOWN"
    ) -> Optional[Dict[str, Any]]
    
    def get_portfolio_summary(self) -> Dict[str, Any]
    
    def get_trade_history(self, limit: int = 100) -> List[Dict[str, Any]]
```

### FeatureEngine

```python
class FeatureEngine:
    def build_feature_set(
        self,
        ticker_bars: pd.DataFrame,
        event_ts: datetime,
        cross_asset_data: Optional[Dict[str, pd.DataFrame]] = None,
        lookback_days: int = 30
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]
```

## Performance Metrics

### Key Metrics Tracked

- **Win Rate**: Percentage of profitable trades
- **Average Return**: Mean return per trade
- **Sharpe Ratio**: Risk-adjusted returns
- **Maximum Drawdown**: Largest peak-to-trough decline
- **Trade Frequency**: Number of trades per day
- **Position Turnover**: Portfolio turnover rate

### Risk Metrics

- **Portfolio Exposure**: Total market exposure
- **Sector Concentration**: Exposure by sector
- **Strategy Concentration**: Exposure by strategy
- **Correlation Risk**: Correlated position exposure
- **Daily Loss Tracking**: Real-time P&L monitoring

## Integration Examples

### With Alpaca (Future Enhancement)

```python
# Future: Real broker integration
from app.trading.brokers import AlpacaBroker

broker = AlpacaBroker(api_key, secret_key)
system = PaperTradingSystem(config, broker=broker)
```

### With Custom Strategies

```python
# Custom strategy integration
from app.strategies.custom import MyStrategy

strategy = MyStrategy(config)
system.add_strategy(strategy)
```

## Monitoring and Logging

### Log Levels

- `DEBUG`: Detailed execution information
- `INFO`: General system status and trade execution
- `WARNING`: Risk limit warnings and qualification failures
- `ERROR`: System errors and exceptions

### Performance Monitoring

```python
# Enable metrics collection
config.enable_metrics = True
config.metrics_port = 8080

# Access metrics
system.get_performance_metrics()
```

## Troubleshooting

### Common Issues

1. **Configuration Validation Errors**
   - Check configuration file format
   - Validate parameter ranges
   - Ensure required fields are present

2. **Trade Rejections**
   - Check confidence thresholds
   - Verify risk limits
   - Review qualification layer logs

3. **Feature Integration Issues**
   - Ensure backward compatibility mode
   - Check feature completeness
   - Validate data formats

### Debug Mode

```json
{
  "debug_mode": true,
  "log_level": "DEBUG",
  "dry_run": true
}
```

## Development

### Running Tests

```bash
# Run unit tests
python -m pytest tests/trading/

# Run integration tests
python -m pytest tests/trading_integration/

# Run with coverage
python -m pytest --cov=app/trading tests/
```

### Development Configuration

```python
from app.trading.config import get_development_config

config = get_development_config()
config.debug_mode = True
config.dry_run = True
```

## License

This project is part of the Alpha Engine system. See the main project license for details.

## Support

For questions and support:

1. Check the troubleshooting section
2. Review the API documentation
3. Enable debug mode for detailed logging
4. Contact the Alpha Engine development team
