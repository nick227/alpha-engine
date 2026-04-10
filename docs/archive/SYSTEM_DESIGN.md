# Alpha Engine System Design

## Overview

The Alpha Engine is a comprehensive trading system that processes market data, generates trading signals, and executes trades through paper trading and live trading interfaces. The system follows a clean architectural pattern with a single decision engine and dedicated execution layer.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Alpha Engine (Brain)                      │
├─────────────────────────────────────────────────────────────────┤
│  Feature Engine    │  Strategies  │  Consensus  │  Weight  │
│                   │             │   Engine    │  Engine  │
│  • Returns        │  • Momentum │  • Signal   │  • Regime│
│  • Technical      │  • Mean Rev │  Fusion     │  Match   │
│  • Volatility     │  • News     │  • Agreement│  • Decay │
│  • Macro Data     │  • Macro    │  • Scoring  │          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     FINAL SIGNALS                            │
│  Ticker │ Direction │ Confidence │ Weight │ Strategy_ID │ Regime │
│         │             │             │ (Signal    │           │         │
│         │             │             │ Importance)│           │         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                Execution Planner (Filter)                       │
│  • Validate Signals  │  • Enforce Limits  │  • Filter    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│              Portfolio Allocator (Size)                        │
│  • Normalize Weights  │  • Convert to Size  │  • Caps   │
│  • Risk Adjustments   │  • Capital Allocation │           │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│              Paper Trader / Alpaca (Execute)                 │
│  • Execute Orders  │  • Track P&L  │  • Update Portfolio │
└─────────────────────────────────────────────────────────────────┘
```

**Learning Loop (Temporal, Not Inline)**:
```
Strategies → Predictions → Outcomes → Performance Learning → Weight Update → NEXT Consensus
```

## Process Flow

### 1. Data Ingestion (Backfill)

**Purpose**: Populate system with historical and real-time market data

**Process**:
```bash
# Interactive launcher
python start.py

# Direct CLI
python -m app.ingest.backfill_cli run --days 90
python -m app.ingest.backfill_cli backfill-range --start 2024-01-01 --end 2024-03-31
```

**Data Sources**:
- **Market Data**: Yahoo Finance (OHLCV, technical indicators)
- **News Data**: Alpaca News (sentiment analysis)
- **Macro Data**: VIX, DXY, Oil, Bitcoin (cross-asset signals)

**Output**: Time-series data in database for strategy processing

### 2. Feature Engineering

**Purpose**: Transform raw data into predictive features

**Core Features**:
```python
# Price-based features
returns_1m, returns_5m, returns_15m, returns_1h, returns_4h, returns_1d
realized_volatility, historical_volatility_window
zscore_20, range_expansion
vwap_distance, volume_ratio

# Technical indicators
rsi_14, macd_signal, bollinger_zscore
adx_value, short_trend, medium_trend

# Macro features
vix_return_1d, dxy_return_1d, oil_return_1d, btc_return_1d
cross_asset_correlation, regime_classification
```

**Feature Pipeline**:
```python
# app/core/price_context.py
def build_price_context(bars, entry_price, timestamp):
    ctx = {}
    ctx["returns_1m"] = calculate_returns(bars, "1m")
    ctx["realized_volatility"] = calculate_volatility(bars)
    ctx["rsi_14"] = calculate_rsi(bars, 14)
    # ... all features
    return ctx
```

### 3. Strategy Generation

**Purpose**: Generate trading signals using diverse strategy families

**Strategy Families**:
- **Momentum**: Trend following, breakout, acceleration
- **Mean Reversion**: RSI, Bollinger, statistical
- **Event-Driven**: News drift, earnings volatility, macro sensitivity
- **Volatility**: Expansion, crush, regime-based
- **Cross-Asset**: Correlation, relative strength, pairs trading

**Strategy Interface**:
```python
class StrategyBase(ABC):
    def maybe_predict(self, scored_event, mra, price_context, timestamp) -> Prediction:
        # Generate prediction with confidence
        return Prediction(
            ticker=ticker,
            direction="up/down/flat",
            confidence=0.0-1.0,
            horizon="15m/1h/4h/1d",
            entry_price=current_price
        )
```

### 4. Consensus Building

**Purpose**: Combine multiple strategy signals into unified predictions

**Process**:
```python
# app/engine/consensus_engine.py
class ConsensusEngine:
    def build_consensus(self, sentiment_signal, quant_signal, realized_vol, adx_value):
        # Combine signals with regime-aware weights
        base_weights = self.regime_manager.get_weights(volatility_regime)
        agreement_bonus = self.calculate_agreement(sentiment_signal, quant_signal)
        
        consensus = ConsensusPrediction(
            direction=self.resolve_direction(sentiment_signal, quant_signal),
            confidence=self.calculate_consensus_confidence(sentiment_signal, quant_signal),
            weighted_consensus=base_weights * agreement_bonus,
            regime=current_regime
        )
        return consensus
```

### 5. Performance Learning

**Purpose**: Continuously learn and improve strategy weights

**Components**:
- **Continuous Learning**: Track strategy performance by regime
- **Weight Engine**: Dynamic weight allocation based on performance
- **Champion Selection**: Promote best performing strategies

**Learning Loop**:
```python
# app/engine/continuous_learning.py
class ContinuousLearner:
    def update_strategy_performance(self, strategy_id, outcome):
        # Update performance metrics
        win_rate = calculate_win_rate(outcomes)
        stability = calculate_stability(returns)
        regime_strength = calculate_regime_performance(outcomes)
        
        # Update weights
        new_weight = win_rate * max(0.1, stability)
        self.weight_engine.update_weight(strategy_id, new_weight)
```

### 6. Final Signal Generation

**Purpose**: Produce final trading signals with capital allocation

**Output Format**:
```python
final_signals = [
    {
        "ticker": "AAPL",
        "direction": "up",
        "confidence": 0.72,
        "weight": 0.21,        # From WeightEngine (capital allocation)
        "strategy_id": "momentum_breakout_v1",
        "regime": "NORMAL_VOLATILITY"
    },
    {
        "ticker": "NVDA", 
        "direction": "down",
        "confidence": 0.65,
        "weight": 0.18,
        "strategy_id": "news_drift_v2",
        "regime": "NORMAL_VOLATILITY"
    }
]
```

### 7. Execution Planning

**Purpose**: Filter and validate signals without re-deciding

**Key Principles**:
- **NO re-computation**: Use weights directly from WeightEngine
- **NO re-ranking**: Trust Alpha Engine decisions
- **Filter only**: Apply execution constraints

**Process**:
```python
# app/trading/execution_planner.py
class ExecutionPlanner:
    def plan(self, final_signals):
        filtered_signals = []
        
        for signal in final_signals:
            # Filter checks (no recompute)
            if signal["weight"] < 0.05:  # Minimum weight
                continue
                
            if signal["confidence"] < 0.52:  # Minimum confidence
                continue
            
            filtered_signals.append(signal)
        
        return filtered_signals  # Pass to Portfolio Allocator
```

### 8. Portfolio Allocation

**Purpose**: Convert signal weights to position sizes with risk adjustments

**Key Principles**:
- **Weight ≠ Size**: Weight is signal importance, not final position size
- **Risk Adjustments**: Apply position sizing based on capital and risk
- **Normalization**: Ensure weights sum to 1.0

**Process**:
```python
# app/trading/portfolio_allocator.py
class PortfolioAllocator:
    def allocate(self, filtered_signals, portfolio_state):
        allocations = []
        
        # Normalize weights to sum to 1.0
        total_weight = sum(s["weight"] for s in filtered_signals)
        normalized_signals = [
            {**s, "normalized_weight": s["weight"] / total_weight}
            for s in filtered_signals
        ]
        
        for signal in normalized_signals:
            # Convert to position size
            capital = portfolio_state.total_value
            base_allocation = signal["normalized_weight"] * capital
            
            # Risk adjustments
            risk_adjusted_size = self.apply_risk_limits(
                base_allocation, signal, portfolio_state
            )
            
            allocations.append({
                "ticker": signal["ticker"],
                "direction": signal["direction"],
                "position_size": risk_adjusted_size,
                "allocation_pct": signal["normalized_weight"]
            })
        
        return allocations
```

### 9. Paper Trading Execution

**Purpose**: Execute trades with realistic market simulation

**Key Features**:
- **Realistic Execution**: Slippage, spread, partial fills, latency
- **Portfolio Management**: Real-time P&L tracking, position management
- **Risk Controls**: Position limits, exposure caps, correlation checks

**Execution Process**:
```python
# app/trading/paper_trader.py
class PaperTrader:
    def execute(self, execution_plan):
        for order in execution_plan.buy_orders:
            # Get market conditions
            market_conditions = self.market_simulator.get_conditions(order.ticker)
            
            # Execute with realistic simulation
            execution_result = await self.execution_engine.execute_market_order(
                order, market_conditions
            )
            
            # Update portfolio
            self.update_portfolio(execution_result)
            
        # Manage existing positions (stop losses, targets)
        self.manage_positions(current_prices)
```

### 10. Trading Loop

**Purpose**: Continuous trading cycle automation

**Main Loop**:
```python
# main.py
class ProductionTradingSystem:
    def run_trading_loop(self):
        while True:
            # 1. Alpha Engine decides (brain)
            final_signals = self.alpha_engine.run()
            
            # 2. Execution Planner filters (no re-deciding)
            filtered_signals = self.execution_planner.plan(final_signals)
            
            # 3. Portfolio Allocator converts to position sizes
            allocations = self.portfolio_allocator.allocate(filtered_signals, self.paper_trader.portfolio_state)
            
            # 4. Paper Trader executes (dumb executor)
            self.paper_trader.execute(allocations)
            
            # 5. Portfolio updates automatically
            self.paper_trader.update_portfolio()
            
            # 6. Wait for next cycle
            time.sleep(60)  # 1-minute cycles
```

## Data Flow Architecture

### **Database Schema**
```sql
-- Raw data storage
events (id, source, timestamp, ticker, text, metadata)
price_bars (id, ticker, timestamp, open, high, low, close, volume)

-- Strategy outputs
strategies (id, name, strategy_type, config, active)
predictions (id, strategy_id, ticker, timestamp, direction, confidence, horizon)
signals (id, prediction_id, strategy_id, ticker, direction, confidence, track, regime)

-- Performance tracking
prediction_outcomes (id, prediction_id, exit_price, realized_return, direction_correct, evaluated_at)
strategy_performance (id, strategy_id, win_rate, alpha, stability, regime_strength, confidence_weight)

-- Trading
portfolio_state (id, tenant_id, cash, total_value, leverage, realized_pnl, unrealized_pnl)
positions (id, tenant_id, ticker, quantity, entry_price, current_price, unrealized_pnl)
trades (id, tenant_id, ticker, strategy_id, direction, quantity, entry_price, exit_price, pnl, status)
```

### **API Interfaces**

**Alpha Engine API**:
```python
# Get current signals
GET /api/v1/signals
Response: {"signals": [...], "timestamp": "2024-03-15T10:30:00Z"}

# Get portfolio state
GET /api/v1/portfolio
Response: {"cash": 85000, "positions": {...}, "total_value": 100000, "pnl": 5000}

# Get strategy performance
GET /api/v1/performance
Response: {"strategies": {...}, "win_rate": 0.65, "sharpe": 1.2}
```

**Execution API**:
```python
# Submit execution plan
POST /api/v1/execute
Request: {"buy_orders": [...], "sell_orders": [...]}
Response: {"trades": [...], "execution_cost": 125.50}
```

## Configuration Management

### **System Configuration**
```yaml
# config/system.yaml
database:
  path: "data/alpha_engine.db"
  connection_pool_size: 10

data_sources:
  yahoo_finance:
    enabled: true
    rate_limit: 2000/hour
  alpaca_news:
    enabled: true
    api_key: "${ALPACA_API_KEY}"
    rate_limit: 60/minute

trading:
  market_hours_only: true
  execution_delay_seconds: 0.1
  slippage_bps: 5.0
  max_position_size: 0.15

performance:
  update_interval: 60  # seconds
  min_samples: 100
  decay_factor: 0.95
```

### **Strategy Configuration**
```yaml
# config/strategies/momentum_breakout.yaml
strategy_type: "momentum_breakout"
parameters:
  consolidation_threshold: 0.02
  volume_breakout_threshold: 2.0
  acceleration_threshold: 0.001
  horizon: "1h"
  min_confidence: 0.52

risk_limits:
  max_position_size: 0.10
  max_daily_loss: 0.02
  correlation_limit: 0.7
```

## Monitoring & Analytics

### **System Health Metrics**
- **Data Pipeline**: Ingestion rates, data quality, latency
- **Strategy Performance**: Win rates, Sharpe ratios, regime performance
- **Execution Quality**: Fill rates, slippage, latency
- **Portfolio Metrics**: P&L, drawdown, leverage, exposure

### **Alerting System**
```python
# System alerts
alerts = [
    {"type": "data_gap", "threshold": 5, "window": "1h"},
    {"type": "strategy_degradation", "threshold": -0.10, "window": "7d"},
    {"type": "execution_latency", "threshold": 500, "window": "5m"},
    {"type": "portfolio_drawdown", "threshold": 0.15, "window": "1d"}
]
```

## Deployment Architecture

### **Development Environment**
```bash
# Local development
python start.py  # Interactive launcher

# Testing
pytest tests/
python -m app.ingest.backfill_cli run --days 7  # Small test dataset
```

### **Production Environment**
```bash
# Production deployment
docker-compose up -d  # Full stack deployment

# Monitoring
docker logs alpha_engine
curl http://localhost:8000/health  # Health check
```

## Migration Path

### **Phase 1: Paper Trading (Week 1-2)**
1. Complete feature engineering implementation
2. Fix regime engine placeholder
3. Implement execution planner
4. Deploy paper trading loop

### **Phase 2: Live Trading (Week 3-4)**
1. Replace PaperTrader with AlpacaExecutor
2. Implement realistic execution model
3. Add comprehensive monitoring
4. Deploy to production

### **Phase 3: Optimization (Week 5-6)**
1. Add strategy diversity (momentum breakout, news drift, macro sensitivity)
2. Implement advanced risk management
3. Add portfolio optimization
4. Performance tuning

## Success Metrics

### **System Performance**
- **Signal Quality**: Win rate >55%, confidence calibration <5% error
- **Execution Quality**: Fill rate >95%, slippage <10bps
- **Portfolio Performance**: Sharpe ratio >1.5, max drawdown <15%

### **Operational Excellence**
- **Data Reliability**: >99.9% uptime, <1% data gaps
- **System Latency**: End-to-end signal generation <100ms
- **Scalability**: Support 1000+ concurrent strategies

---

**Document Version**: 1.1  
**Last Updated**: 2025-04-10  
**Status**: Paper-trading ready architecture

**Remaining Implementation Tasks**:
- Signal calibration (confidence → probability mapping)
- Regime engine completion (remove hardcoded placeholder)
- Realistic execution model (slippage, spread, partial fills)
- Position lifecycle management (entry → exit → P&L)
- Portfolio state engine (real-time tracking)
