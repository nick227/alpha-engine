# Temporal Correlation Analysis Documentation

## Overview

The Temporal Correlation Analysis framework extends Alpha Engine's performance analysis capabilities by correlating strategy performance with external time-based indicators and events. This system transforms raw performance data into actionable insights for timing, risk management, and strategy optimization.

## Architecture

```
Temporal Correlation Analysis Framework
    |
    |-- Strategy Performance Periods Analyzer
    |-- Temporal Correlation Analyzer
    |-- Insights Engine
    |-- Correlation Dashboard
    |-- Streak Monitor
    `-- Performance Dashboard
```

## Core Components

### 1. Strategy Performance Periods Analyzer (`strategy_performance_periods.py`)

**Purpose**: Identifies optimal performance periods and streaks in strategy execution.

**Key Capabilities**:
- Rolling window performance analysis (customizable periods)
- Winning/losing streak detection and analysis
- Monthly/quarterly/seasonal pattern identification
- Performance consistency metrics
- Best/worst period identification with statistical significance

**Core Methods**:
```python
# Analyze performance over rolling windows
performance_periods = analyzer.analyze_performance_periods(window_days=30)

# Detect and analyze streaks
streaks = analyzer.analyze_streaks()

# Monthly pattern analysis
monthly_patterns = analyzer.analyze_monthly_patterns()

# Comprehensive report generation
report = analyzer.generate_performance_report()
```

**Output Metrics**:
- Best performing period (date range, P&L, win rate)
- Longest winning/losing streaks (length, P&L, duration)
- Monthly performance breakdown (avg P&L, win rate, trade count)
- Performance consistency (percentage of positive periods)

### 2. Temporal Correlation Analyzer (`temporal_correlation_analyzer.py`)

**Purpose**: Correlates strategy performance with external time-based indicators and market events.

**Data Sources Integrated**:
- **Market Headlines**: News sentiment analysis and impact scoring
- **Economic Calendar**: FOMC, CPI, GDP, employment reports
- **Market Indicators**: VIX, S&P 500, Treasury yields, Dollar Index
- **Sector ETFs**: 10 sector performance correlation analysis
- **Volatility Data**: Historical VIX levels and regime transitions

**Core Analysis Modules**:

#### Sentiment Correlation Analysis
```python
# Correlate performance with market sentiment
sentiment_analysis = analyzer.analyze_sentiment_correlation(performance_periods)
# Returns: correlation coefficient, sentiment performance breakdown
```

**Key Metrics**:
- Sentiment-performance correlation coefficient
- Performance by sentiment type (positive/negative/neutral)
- Headline category impact analysis
- Average sentiment score vs performance scatter

#### Economic Event Impact Analysis
```python
# Analyze impact of economic events
economic_impact = analyzer.analyze_economic_event_impact(performance_periods)
# Returns: high vs low impact event performance, category breakdown
```

**Key Metrics**:
- Performance during high-impact vs low-impact events
- Category-specific performance (monetary_policy, inflation, growth, employment)
- Event timing impact (pre/post event performance)
- Surprise factor analysis (actual vs forecast deviations)

#### Market Indicator Correlations
```python
# Correlate with market indicators
indicator_correlations = analyzer.analyze_market_indicator_correlations(performance_periods)
# Returns: correlation matrix for VIX, SPX, TNX, DXY
```

**Key Metrics**:
- Performance correlation with each indicator
- Cross-indicator correlation analysis
- Leading/lagging indicator identification
- Regime-specific correlation analysis

#### Sector Rotation Analysis
```python
# Analyze sector performance correlations
sector_analysis = analyzer.analyze_sector_rotation_impact(performance_periods)
# Returns: sector correlation rankings, concentration risk metrics
```

**Key Metrics**:
- Sector-performance correlation coefficients
- Best/worst correlated sectors
- Sector concentration risk assessment
- Rotation pattern identification

#### Seasonal Pattern Analysis
```python
# Detect seasonal patterns
seasonal_patterns = analyzer.analyze_seasonal_patterns(performance_periods)
# Returns: monthly, quarterly, day-of-week performance patterns
```

**Key Metrics**:
- Monthly performance averages and standard deviations
- Quarterly performance trends
- Day-of-week performance patterns
- Week-of-year heatmap analysis

#### Volatility Regime Analysis
```python
# Analyze volatility regime transitions
volatility_analysis = analyzer.analyze_volatility_regime_transitions(performance_periods)
# Returns: regime-specific performance, transition risk metrics
```

**Key Metrics**:
- Performance by volatility regime (low/normal/high)
- Volatility transition risk assessment
- VIX-performance correlation
- Regime transition timing analysis

### 3. Insights Engine (`insights_engine.py`)

**Purpose**: Transforms correlation analysis into actionable trading insights and recommendations.

**Insight Categories**:

#### Timing Insights
- **Market Sentiment Timing**: "Increase exposure during positive sentiment periods (r=0.73)"
- **Economic Event Positioning**: "Scale positions around FOMC meetings"
- **Volatility Regime Scaling**: "2x exposure when VIX < 20, 0.5x when VIX > 30"
- **Sector Rotation Timing**: "Align with Technology sector momentum"

#### Risk Management Insights
- **Volatility Transition Risk**: "Reduce exposure during VIX spikes"
- **Sector Concentration**: "Diversify when sector correlation > 0.7"
- **Economic Surprise Protection**: "Hedge against unexpected CPI deviations"
- **Seasonal Risk Adjustment**: "Reduce risk in historically weak periods"

#### Strategy Optimization Insights
- **Signal Enhancement**: "Add VIX as entry confirmation signal"
- **Sector Filter Integration**: "Use Technology sector health as filter"
- **Seasonal Position Sizing**: "Scale by historical monthly performance"
- **Regime-Specific Parameters**: "Different settings for volatility regimes"

#### Market Condition Insights
- **Economic Environment Focus**: "Excel during monetary policy events"
- **News Sentiment Adaptation**: "Adjust based on headline sentiment"
- **Volatility Specialization**: "Optimize for high volatility periods"
- **Sector Momentum Integration**: "Leverage correlated sector movements"

**Insight Scoring System**:
- **Confidence**: 0-1 scale based on correlation strength and sample size
- **Impact**: Low/Medium/High based on performance differential
- **Priority**: 1-10 scale (lower = higher priority)
- **Actionable Steps**: Specific implementation recommendations

### 4. Correlation Dashboard (`correlation_dashboard.py`)

**Purpose**: Comprehensive visualization of temporal correlation analysis.

**Dashboard Panels (20 total)**:

#### Performance Correlation Visualizations
1. **Sentiment Correlation Scatter**: Performance vs sentiment score with trend line
2. **Economic Event Impact**: With/without high-impact events comparison
3. **Market Indicator Rankings**: Horizontal bar chart of correlations
4. **Sector Correlation Heatmap**: Color-coded correlation matrix

#### Temporal Pattern Visualizations
5. **Monthly Performance Patterns**: Bar chart with best/worst month highlighting
6. **Quarterly Performance**: Quarterly comparison with value labels
7. **Day-of-Week Analysis**: Weekday performance patterns
8. **Volatility Regime Performance**: Performance by VIX regime

#### Event and News Analysis
9. **Headline Category Impact**: Performance by news sentiment category
10. **Economic Category Performance**: Performance by event type
11. **Performance Timeline with Events**: Timeline showing key events
12. **Correlation Matrix**: All factor correlations ranked

#### Advanced Analytics
13. **VIX vs Performance Scatter**: Volatility-performance relationship
14. **Sector Performance Comparison**: Sector change vs correlation
15. **Seasonal Heatmap**: Week-of-year performance heatmap
16. **Key Insights Summary**: Top insights with confidence scores

#### Risk and Summary
17. **Performance by Regime**: Distribution across volatility regimes
18. **Event Impact Timeline**: Historical event impact visualization
19. **Correlation Strength Rankings**: Top 10 absolute correlations
20. **Performance Summary Statistics**: Key metrics and overview

### 5. Streak Monitor (`streak_monitor.py`)

**Purpose**: Real-time streak detection and alerting for live trading systems.

**Real-time Capabilities**:
- **Live Streak Tracking**: Monitor winning/losing streaks as trades complete
- **Configurable Alerts**: Customizable alerts for streak lengths and P&L thresholds
- **Streak History**: Maintain historical streak database
- **Export Functionality**: Export streak data for analysis

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

**Integration Example**:
```python
# Real-time streak monitoring
monitor = StreakMonitor()

def on_trade_complete(trade_data):
    alerts = monitor.add_trade(trade_data)
    for alert in alerts:
        if alert['severity'] == 'critical':
            send_email_alert(alert)
```

## Implementation Guide

### Quick Start

```python
# Complete analysis pipeline
from scripts.analysis.strategy_performance_periods import StrategyPerformanceAnalyzer
from scripts.analysis.temporal_correlation_analyzer import TemporalCorrelationAnalyzer
from scripts.analysis.insights_engine import InsightsEngine
from scripts.analysis.correlation_dashboard import CorrelationDashboard

# 1. Base performance analysis
analyzer = StrategyPerformanceAnalyzer()
analyzer.load_historical_data()
analyzer.calculate_regimes()
analyzer.simulate_bear_expansion_strategy()

# 2. Correlation analysis
correlation_analyzer = TemporalCorrelationAnalyzer(analyzer)
report = correlation_analyzer.generate_comprehensive_correlation_report()

# 3. Insights generation
insights_engine = InsightsEngine(correlation_analyzer)
insights = insights_engine.generate_all_insights()

# 4. Visualization
dashboard = CorrelationDashboard(correlation_analyzer)
dashboard.create_comprehensive_correlation_dashboard('correlations.png')

# 5. Export results
insights_engine.export_insights('insights.json')
```

### Live Trading Integration

```python
class EnhancedTradingSystem:
    def __init__(self):
        self.correlation_analyzer = TemporalCorrelationAnalyzer()
        self.insights_engine = InsightsEngine(self.correlation_analyzer)
        self.streak_monitor = StreakMonitor()
        
    def on_market_open(self):
        # Check current market conditions
        market_conditions = self.get_market_conditions()
        
        # Get relevant insights
        insights = self.insights_engine.get_relevant_insights(market_conditions)
        
        # Apply insights to strategy parameters
        self.apply_insights(insights)
        
    def on_trade_complete(self, trade_data):
        # Monitor streaks
        alerts = self.streak_monitor.add_trade(trade_data)
        self.handle_streak_alerts(alerts)
        
    def get_market_conditions(self):
        return {
            'sentiment': self.get_market_sentiment(),
            'vix_level': self.get_vix_level(),
            'economic_events': self.get_today_events(),
            'sector_performance': self.get_sector_performance()
        }
```

### Risk Management Integration

```python
class CorrelationRiskManager:
    def __init__(self):
        self.correlation_analyzer = TemporalCorrelationAnalyzer()
        
    def calculate_position_size(self, base_size, market_conditions):
        adjusted_size = base_size
        
        # Volatility regime adjustment
        vix_correlation = self.correlation_analyzer.get_vix_correlation()
        if vix_correlation < -0.5 and market_conditions['vix'] > 25:
            adjusted_size *= 0.5
            
        # Sector concentration adjustment
        sector_risk = self.correlation_analyzer.get_sector_concentration_risk()
        if sector_risk > 0.7:
            adjusted_size *= 0.8
            
        # Economic event adjustment
        if market_conditions['high_impact_events']:
            adjusted_size *= 0.7
            
        return adjusted_size
```

## Data Sources and APIs

### Market Data Integration

```python
# Yahoo Finance (free)
import yfinance as yf

# VIX Data
vix = yf.download('^VIX', start=start_date, end=end_date)

# Sector ETFs
sector_etfs = {
    'Technology': 'XLK',
    'Financials': 'XLF',
    'Healthcare': 'XLV',
    # ... other sectors
}

# Economic Calendar (sample structure)
economic_events = [
    {
        'date': datetime(2024, 1, 12),
        'event': 'CPI Data Release',
        'category': 'inflation',
        'impact': 'high',
        'actual': 3.4,
        'forecast': 3.5
    }
]
```

### News Sentiment Integration

```python
# News API integration (example)
def fetch_headlines(start_date, end_date):
    # In production, connect to news API
    return [
        {
            'date': datetime(2024, 1, 15),
            'headline': 'Tech Stocks Rally on AI Optimism',
            'sentiment': 'positive',
            'category': 'sector_tech',
            'impact_score': 0.8
        }
    ]
```

## Performance Metrics and KPIs

### Correlation Strength Interpretation
- **0.7+**: Strong correlation - high confidence insights
- **0.5-0.7**: Moderate correlation - medium confidence
- **0.3-0.5**: Weak correlation - low confidence
- **<0.3**: No significant correlation

### Statistical Significance
- **Sample Size**: Minimum 20 periods for correlation analysis
- **P-value**: < 0.05 for statistical significance
- **Confidence Interval**: 95% CI for correlation coefficients

### Performance Benchmarks
- **Consistency**: >60% positive periods considered consistent
- **Streak Quality**: >3 trades with positive P&L considered meaningful
- **Seasonal Significance**: >$2,000 monthly difference considered significant

## Advanced Features

### Multi-Factor Analysis
```python
# Combine multiple correlation factors
def calculate_composite_score(market_conditions):
    weights = {
        'sentiment': 0.3,
        'vix_level': 0.25,
        'sector_momentum': 0.25,
        'economic_events': 0.2
    }
    
    score = 0
    for factor, weight in weights.items():
        factor_score = get_factor_score(factor, market_conditions[factor])
        score += factor_score * weight
        
    return score
```

### Predictive Timing Model
```python
# Predict optimal timing windows
def predict_optimal_window(historical_correlations, current_conditions):
    # Use historical patterns to predict future performance
    similar_periods = find_similar_historical_periods(current_conditions)
    
    if similar_periods:
        avg_performance = np.mean([p['performance'] for p in similar_periods])
        confidence = len(similar_periods) / total_historical_periods
        
        return {
            'expected_performance': avg_performance,
            'confidence': confidence,
            'recommended_action': get_recommended_action(avg_performance, confidence)
        }
```

### Adaptive Strategy Parameters
```python
# Dynamic parameter adjustment
def adjust_strategy_parameters(correlation_insights):
    base_params = get_default_parameters()
    adjusted_params = base_params.copy()
    
    # Volatility-based adjustment
    if correlation_insights['vix_correlation'] < -0.5:
        adjusted_params['position_size'] *= 0.6
        adjusted_params['stop_loss_multiplier'] *= 1.2
        
    # Seasonal adjustment
    current_month = datetime.now().month
    if current_month in correlation_insights['weak_months']:
        adjusted_params['position_size'] *= 0.8
        
    return adjusted_params
```

## Troubleshooting and Best Practices

### Common Issues

1. **Insufficient Data**: Minimum 6 months of trading data required
2. **API Rate Limits**: Implement caching for external data calls
3. **Correlation vs Causation**: Focus on statistically significant correlations
4. **Overfitting**: Use out-of-sample testing for validation

### Performance Optimization

```python
# Caching strategy for API calls
@lru_cache(maxsize=128)
def get_market_data(ticker, start_date, end_date):
    return yf.download(ticker, start=start_date, end=end_date)

# Vectorized operations for efficiency
def calculate_correlations_vectorized(performance_df, indicators_df):
    return performance_df.corrwith(indicators_df, axis=0)
```

### Validation Framework

```python
# Out-of-sample testing
def validate_correlations(train_data, test_data, correlation_threshold=0.5):
    # Calculate correlations on training data
    train_correlations = calculate_correlations(train_data)
    
    # Validate on test data
    test_correlations = calculate_correlations(test_data)
    
    # Check stability
    stable_correlations = {}
    for factor in train_correlations:
        if abs(train_correlations[factor]) > correlation_threshold:
            correlation_diff = abs(train_correlations[factor] - test_correlations[factor])
            if correlation_diff < 0.2:  # Within 20% difference
                stable_correlations[factor] = train_correlations[factor]
    
    return stable_correlations
```

## Future Enhancements

### Machine Learning Integration
- **Pattern Recognition**: ML models for complex correlation patterns
- **Predictive Analytics**: Time series forecasting for optimal periods
- **Anomaly Detection**: Identify unusual market conditions

### Alternative Data Sources
- **Satellite Imagery**: Economic activity indicators
- **Credit Card Data**: Consumer spending patterns
- **Web Scraping**: Alternative sentiment indicators

### Real-time Features
- **Streaming Data**: Real-time correlation updates
- **Live Dashboard**: Dynamic visualization updates
- **Mobile Alerts**: Push notifications for critical insights

### Multi-Strategy Analysis
- **Cross-Strategy Correlations**: Portfolio-level optimization
- **Strategy Diversification**: Identify uncorrelated strategies
- **Risk Parity**: Correlation-based position sizing

## Conclusion

The Temporal Correlation Analysis framework transforms Alpha Engine from a reactive system to a proactive, insight-driven trading platform. By correlating performance with external time-based indicators, traders can:

1. **Anticipate optimal trading periods** using sentiment and economic indicators
2. **Manage risk proactively** through volatility and sector correlation monitoring
3. **Optimize strategy parameters** based on market conditions and seasonal patterns
4. **Generate actionable insights** with confidence scoring and prioritization

This comprehensive approach provides a significant competitive advantage by turning historical performance patterns into predictive trading intelligence.
