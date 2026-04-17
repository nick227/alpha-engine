# Strategy Performance Analysis Tools

This directory contains comprehensive tools for analyzing strategy performance periods, streaks, and temporal patterns.

## Overview

The performance analysis suite helps you identify:
- **Best performing time periods** - When does your strategy excel?
- **Longest winning/losing runs** - Track streaks and patterns
- **Monthly/quarterly patterns** - Seasonal performance insights
- **Real-time streak monitoring** - Live alerts for significant streaks
- **Visual dashboards** - Interactive performance visualizations

## Core Components

### 1. Strategy Performance Periods Analyzer (`strategy_performance_periods.py`)

**Purpose**: Identifies periods when strategies performed best and analyzes temporal performance patterns.

**Key Features**:
- Rolling window performance analysis (customizable window sizes)
- Best/worst period identification
- Monthly and quarterly pattern analysis
- Comprehensive streak detection
- Performance consistency metrics

**Usage**:
```python
from scripts.analysis.strategy_performance_periods import StrategyPerformanceAnalyzer

analyzer = StrategyPerformanceAnalyzer()
analyzer.load_historical_data()
analyzer.calculate_regimes()
analyzer.simulate_bear_expansion_strategy()

# Generate comprehensive report
report = analyzer.generate_performance_report()

# Analyze specific periods
performance_periods = analyzer.analyze_performance_periods(window_days=30)
streaks = analyzer.analyze_streaks()
monthly_patterns = analyzer.analyze_monthly_patterns()
```

**Key Outputs**:
- **Best Period**: Date range with highest P&L
- **Worst Period**: Date range with lowest P&L  
- **Longest Winning Streak**: Consecutive winning trades
- **Longest Losing Streak**: Consecutive losing trades
- **Monthly Performance**: Performance by month/quarter
- **Performance Consistency**: % of positive periods

### 2. Streak Monitor (`streak_monitor.py`)

**Purpose**: Real-time streak detection and alerting system for live trading.

**Key Features**:
- Real-time streak tracking as trades complete
- Configurable alerts for streak lengths
- P&L threshold monitoring
- Streak history and statistics
- Export capabilities

**Usage**:
```python
from scripts.analysis.streak_monitor import StreakMonitor

monitor = StreakMonitor()

# Add trades as they complete
alerts = monitor.add_trade({
    'pnl': 500,
    'ticker': 'AAPL',
    'entry_date': '2024-01-01',
    'exit_date': '2024-01-02'
})

# Check current streak
current_streak = monitor.get_current_streak_info()

# Get statistics
stats = monitor.get_streak_statistics()

# Export data
filename = monitor.export_data()
```

**Alert Configuration**:
```json
{
  "alerts": [
    {
      "streak_type": "winning",
      "min_length": 3,
      "message_template": "Winning streak of {length} trades! Total P&L: ${pnl:,.0f}",
      "severity": "info"
    },
    {
      "streak_type": "losing", 
      "min_length": 3,
      "message_template": "Losing streak of {length} trades. Total P&L: ${pnl:,.0f}",
      "severity": "warning"
    }
  ]
}
```

### 3. Performance Dashboard (`performance_dashboard.py`)

**Purpose**: Visual dashboard for performance periods and streaks.

**Key Visualizations**:
- Cumulative P&L over time
- Trade P&L distribution
- Rolling win rate timeline
- Monthly performance heatmap
- Streak analysis charts
- Performance period comparison
- Risk metrics summary
- Position in range analysis

**Usage**:
```python
from scripts.analysis.performance_dashboard import PerformanceDashboard

# Create dashboard
dashboard = PerformanceDashboard(analyzer)
dashboard.create_performance_overview('performance_dashboard.png')
dashboard.create_streak_dashboard('streak_dashboard.png')
```

## Integration with Live Trading

### Real-time Integration

```python
# In your trading system
from scripts.analysis.streak_monitor import StreakMonitor

monitor = StreakMonitor()

def on_trade_complete(trade_data):
    """Called when a trade completes"""
    alerts = monitor.add_trade(trade_data)
    
    # Handle alerts
    for alert in alerts:
        if alert['severity'] == 'critical':
            send_email_alert(alert)
        elif alert['severity'] == 'warning':
            send_slack_notification(alert)
```

### Daily Performance Summary

```python
def generate_daily_summary():
    """Generate daily performance summary"""
    analyzer = StrategyPerformanceAnalyzer()
    analyzer.load_historical_data()
    analyzer.calculate_regimes()
    
    # Get recent trades
    recent_trades = get_trades_last_n_days(7)
    
    # Analyze performance
    report = analyzer.analyze_performance_periods(window_days=7)
    streaks = analyzer.analyze_streaks()
    
    return {
        'period_performance': report,
        'current_streak': streaks.get('current_streak'),
        'key_metrics': calculate_key_metrics(recent_trades)
    }
```

## Key Insights You Can Uncover

### 1. Temporal Performance Patterns
- **Best Months**: Which months consistently outperform?
- **Quarterly Trends**: Are there Q1 vs Q4 differences?
- **Day of Week**: Does performance vary by trading day?

### 2. Streak Analysis
- **Average Streak Length**: How long do winning/losing streaks typically last?
- **Streak Triggers**: What market conditions start streaks?
- **Recovery Patterns**: How quickly do you recover from losing streaks?

### 3. Performance Periods
- **Optimal Windows**: When does your strategy perform best?
- **Consistency Metrics**: How stable is performance over time?
- **Regime Dependencies**: Which market regimes drive performance?

### 4. Risk Management
- **Drawdown Patterns**: When do最大drawdowns occur?
- **Volatility Impact**: How does volatility affect streaks?
- **Position Sizing**: Optimal sizing during different streak phases?

## Customization Examples

### Custom Performance Windows
```python
# Analyze weekly performance
weekly_performance = analyzer.analyze_performance_periods(window_days=7)

# Analyze quarterly performance  
quarterly_performance = analyzer.analyze_performance_periods(window_days=90)
```

### Custom Alert Thresholds
```python
# Custom alert configuration
custom_config = {
    "alerts": [
        {
            "streak_type": "winning",
            "min_length": 10,  # Alert on 10+ winning trades
            "message_template": "EXCEPTIONAL: {length} trade winning streak!",
            "severity": "critical"
        }
    ]
}

monitor = StreakMonitor(config_file='custom_config.json')
```

### Custom Visualizations
```python
# Add custom chart to dashboard
def plot_custom_metric(ax, trades):
    # Your custom visualization
    ax.plot(trades['entry_date'], trades['custom_metric'])
    ax.set_title('Custom Performance Metric')

dashboard._plot_custom_metric = plot_custom_metric
```

## Running the Analysis

### Quick Start
```bash
# Run comprehensive analysis
cd scripts/analysis
python strategy_performance_periods.py

# Run streak monitor demo
python streak_monitor.py

# Generate dashboards
python performance_dashboard.py
```

### Integration with Existing Systems
```python
# Add to your daily pipeline
from scripts.analysis.strategy_performance_periods import main as performance_main

def daily_analysis():
    analyzer, report = performance_main()
    
    # Save results
    with open('daily_performance_report.json', 'w') as f:
        json.dump(report, f, default=str)
```

## Output Files

- `performance_dashboard.png` - Main performance dashboard
- `streak_dashboard.png` - Streak-specific dashboard  
- `streak_data_*.json` - Exported streak data
- `daily_performance_report.json` - Daily analysis results

## Troubleshooting

### Common Issues

1. **No Trade Data**: Ensure historical data is loaded and strategy simulation runs
2. **Missing Regime Data**: Check regime calculation logic
3. **Empty Visualizations**: Verify data preprocessing completed successfully

### Performance Optimization

- Use `window_days` parameter to adjust analysis granularity
- Limit `max_streak_history` for memory efficiency
- Cache results for frequently accessed metrics

## Temporal Correlation Analysis

### 4. Temporal Correlation Analyzer (`temporal_correlation_analyzer.py`)

**Purpose**: Correlates strategy performance with external time-based indicators and events.

**Key Features**:
- **Market Sentiment Analysis**: Correlate performance with news headlines and sentiment
- **Economic Event Impact**: Analyze performance around FOMC, CPI, jobs reports, etc.
- **Market Indicator Correlations**: VIX, S&P 500, Treasury yields, Dollar Index
- **Sector Rotation Analysis**: Identify sector performance correlations
- **Seasonal Pattern Detection**: Monthly, quarterly, day-of-week patterns
- **Volatility Regime Analysis**: Performance across different volatility environments

**External Data Sources**:
- Market headlines and sentiment scoring
- Economic calendar events (FOMC, CPI, GDP, employment)
- Market indicators (VIX, SPX, TNX, DXY)
- Sector ETF performance data
- Historical volatility regimes

**Usage**:
```python
from scripts.analysis.temporal_correlation_analyzer import TemporalCorrelationAnalyzer

correlation_analyzer = TemporalCorrelationAnalyzer(analyzer)
report = correlation_analyzer.generate_comprehensive_correlation_report()

# Analyze specific correlations
sentiment_analysis = correlation_analyzer.analyze_sentiment_correlation(performance_periods)
economic_impact = correlation_analyzer.analyze_economic_event_impact(performance_periods)
sector_correlations = correlation_analyzer.analyze_sector_rotation_impact(performance_periods)
seasonal_patterns = correlation_analyzer.analyze_seasonal_patterns(performance_periods)
volatility_analysis = correlation_analyzer.analyze_volatility_regime_transitions(performance_periods)
```

### 5. Correlation Dashboard (`correlation_dashboard.py`)

**Purpose**: Visual dashboard for temporal correlation analysis.

**Key Visualizations**:
- **20-panel comprehensive dashboard** covering all correlation aspects
- **Sentiment correlation scatter plots**
- **Economic event impact comparisons**
- **Market indicator correlation rankings**
- **Sector correlation heatmaps**
- **Seasonal performance patterns**
- **Volatility regime performance**
- **Correlation strength rankings**

**Usage**:
```python
from scripts.analysis.correlation_dashboard import CorrelationDashboard

dashboard = CorrelationDashboard(correlation_analyzer)
dashboard.create_comprehensive_correlation_dashboard('correlation_dashboard.png')
```

### 6. Insights Engine (`insights_engine.py`)

**Purpose**: Transform correlation data into actionable trading insights.

**Key Features**:
- **Automated insight generation** from correlation analysis
- **Actionable recommendations** with confidence scores
- **Risk management insights** based on correlation patterns
- **Strategy optimization suggestions**
- **Timing opportunity identification**
- **Executive summary generation**

**Insight Types**:
- **Timing Insights**: When to increase/decrease exposure
- **Risk Management**: Volatility transitions, sector concentration
- **Strategy Optimization**: Signal enhancement, sector filters
- **Market Conditions**: Economic environment specialization
- **Seasonal Patterns**: Monthly/quarterly adjustments

**Usage**:
```python
from scripts.analysis.insights_engine import InsightsEngine

insights_engine = InsightsEngine(correlation_analyzer)
insights = insights_engine.generate_all_insights()

# Get specific insight types
timing_insights = insights_engine.get_insights_by_type(InsightType.TIMING)
risk_insights = insights_engine.get_insights_by_type(InsightType.RISK_MANAGEMENT)
high_priority = insights_engine.get_high_priority_insights()

# Generate executive summary
summary = insights_engine.generate_executive_summary()
insights_engine.print_insights_summary()
```

## Comprehensive Analysis Workflow

### Complete Analysis Pipeline
```python
# 1. Base performance analysis
analyzer = StrategyPerformanceAnalyzer()
analyzer.load_historical_data()
analyzer.calculate_regimes()
analyzer.simulate_bear_expansion_strategy()

# 2. Temporal correlation analysis
correlation_analyzer = TemporalCorrelationAnalyzer(analyzer)
correlation_report = correlation_analyzer.generate_comprehensive_correlation_report()

# 3. Insights generation
insights_engine = InsightsEngine(correlation_analyzer)
insights = insights_engine.generate_all_insights()

# 4. Visualization
performance_dashboard = PerformanceDashboard(analyzer)
correlation_dashboard = CorrelationDashboard(correlation_analyzer)

# 5. Export results
performance_dashboard.create_performance_overview('performance.png')
correlation_dashboard.create_comprehensive_correlation_dashboard('correlations.png')
insights_engine.export_insights('insights.json')
```

## Key Insights You Can Uncover

### 1. Market Timing Opportunities
- **Sentiment-based timing**: Increase exposure during positive/negative news periods
- **Economic event positioning**: Optimize around FOMC and major data releases
- **Volatility regime scaling**: Adjust position sizes based on VIX levels
- **Sector rotation timing**: Align with correlated sector movements

### 2. Risk Management Enhancements
- **Volatility transition risks**: Reduce exposure during VIX spikes
- **Sector concentration monitoring**: Diversify when correlations are too strong
- **Economic surprise protection**: Hedge against unexpected data
- **Seasonal risk adjustments**: Reduce risk in historically weak periods

### 3. Strategy Optimization Opportunities
- **Signal enhancement**: Add correlated indicators as confirmation
- **Sector filter integration**: Use sector health as entry filter
- **Seasonal position sizing**: Scale by historical monthly performance
- **Regime-specific parameters**: Different settings for volatility regimes

### 4. Market Condition Specialization
- **Economic environment focus**: Excel during specific event types
- **News sentiment adaptation**: Adjust strategy based on headline sentiment
- **Volatility specialization**: Optimize for high/low volatility periods
- **Sector momentum integration**: Leverage correlated sector movements

## Real-world Integration Examples

### Trading System Integration
```python
# Real-time correlation monitoring
class EnhancedTradingSystem:
    def __init__(self):
        self.correlation_analyzer = TemporalCorrelationAnalyzer()
        self.insights_engine = InsightsEngine(self.correlation_analyzer)
        
    def on_market_open(self):
        # Check today's market conditions
        sentiment = self.get_market_sentiment()
        vix_level = self.get_vix_level()
        economic_events = self.get_today_events()
        
        # Apply insights
        insights = self.insights_engine.get_relevant_insights(
            sentiment, vix_level, economic_events
        )
        
        # Adjust strategy parameters
        self.apply_insights(insights)
```

### Risk Management Integration
```python
# Correlation-based risk management
class CorrelationRiskManager:
    def __init__(self):
        self.correlation_analyzer = TemporalCorrelationAnalyzer()
        
    def calculate_position_size(self, base_size, market_conditions):
        # Adjust based on volatility regime
        vix_correlation = self.correlation_analyzer.get_vix_correlation()
        if vix_correlation < -0.5 and market_conditions['vix'] > 25:
            return base_size * 0.5  # Reduce size in high volatility
        
        # Adjust based on sector concentration
        sector_risk = self.correlation_analyzer.get_sector_concentration_risk()
        if sector_risk > 0.7:
            return base_size * 0.8  # Reduce size if sector concentrated
        
        return base_size
```

## Advanced Analysis Features

### 1. Multi-factor Correlation Matrix
- Cross-correlation between all external factors
- Factor importance ranking
- Redundancy detection
- Optimal factor selection

### 2. Predictive Timing Model
- Leading indicator identification
- Recurring pattern detection
- Probability-based timing scores
- Confidence interval estimation

### 3. Adaptive Strategy Parameters
- Dynamic parameter adjustment based on correlations
- Regime-specific optimization
- Real-time factor weighting
- Performance attribution analysis

### 4. Portfolio Integration
- Multi-strategy correlation analysis
- Portfolio-level timing optimization
- Cross-strategy diversification benefits
- Risk-adjusted performance enhancement

## Data Sources and APIs

### Market Data
- **Yahoo Finance**: Free market data (VIX, SPX, sector ETFs)
- **Alpha Vantage**: Economic calendar and indicators
- **News APIs**: Headline sentiment analysis
- **Federal Reserve**: FOMC announcements and minutes

### Economic Calendar
- **Investing.com**: Comprehensive economic calendar
- **Forex Factory**: High-impact event tracking
- **Bloomberg Economic Calendar**: Professional-grade data
- **Custom scrapers**: Exchange-specific event data

### Sentiment Analysis
- **News API**: Real-time headline feeds
- **Twitter API**: Social media sentiment
- **Reddit API**: Retail sentiment tracking
- **Custom NLP**: Domain-specific sentiment models

## Performance Optimization

### Computational Efficiency
- **Vectorized operations**: Use pandas/numpy for speed
- **Caching strategies**: Cache expensive API calls
- **Parallel processing**: Analyze multiple factors simultaneously
- **Incremental updates**: Update correlations without full recalculation

### Memory Management
- **Data sampling**: Use representative time periods
- **Compression**: Store correlation matrices efficiently
- **Lazy loading**: Load data only when needed
- **Cleanup**: Remove unused intermediate results

## Future Enhancements

- **Real-time Dashboard**: Live updating charts with streaming data
- **Multi-Strategy Comparison**: Compare different strategies side-by-side
- **Predictive Analytics**: Predict streak continuation probability using ML
- **Integration with Brokers**: Direct trade data feeds and execution
- **Mobile Alerts**: SMS/push notifications for critical correlations
- **Machine Learning Integration**: Advanced pattern recognition and prediction
- **Alternative Data**: Satellite imagery, credit card data, web scraping
- **Global Markets**: International correlations and cross-asset analysis

## Support

For questions or issues, check the source code documentation or create an issue in the repository.
