# Temporal Correlation Analysis - Quick Reference Guide

## New Features Overview

The temporal correlation analysis adds **6 new components** to Alpha Engine's analysis toolkit:

| Component | Purpose | Key Output |
|------------|---------|------------|
| **Strategy Performance Periods Analyzer** | Identify optimal performance windows & streaks | Best/worst periods, streak analysis |
| **Temporal Correlation Analyzer** | Correlate performance with external factors | Sentiment, economic, sector correlations |
| **Insights Engine** | Generate actionable recommendations | Trading insights with confidence scores |
| **Correlation Dashboard** | Visualize all correlation data | 20-panel comprehensive dashboard |
| **Streak Monitor** | Real-time streak tracking & alerts | Live streak monitoring system |
| **Performance Dashboard** | Enhanced performance visualizations | 12-panel performance dashboard |

## Quick Start Commands

```bash
# Run complete temporal analysis
cd scripts/analysis
python temporal_correlation_analyzer.py

# Generate insights
python insights_engine.py

# Create dashboards
python correlation_dashboard.py
python performance_dashboard.py

# Test streak monitoring
python streak_monitor.py
```

## Key Insights You Can Now Generate

### Timing Opportunities
- **"Increase exposure 2x during positive sentiment periods (r=0.73)"**
- **"Reduce positions 24h before FOMC announcements"**
- **"Scale strategy when VIX < 20, reduce when VIX > 30"**

### Risk Management
- **"Volatility transition risk detected - implement VIX-based stops"**
- **"Sector concentration alert - 78% correlation with Technology"**
- **"Economic surprise protection needed for CPI deviations"**

### Strategy Optimization
- **"Add VIX as entry confirmation signal"**
- **"Implement Technology sector health filter"**
- **"Seasonal position sizing: +30% in Q4, -20% in Q2"**

## External Data Sources Integrated

### Market Indicators
- **VIX** (Volatility Index)
- **S&P 500** (Market trend)
- **10-Year Treasury** (Interest rates)
- **Dollar Index** (Currency strength)

### Economic Events
- **FOMC Meetings** (Monetary policy)
- **CPI Data** (Inflation)
- **GDP Reports** (Growth)
- **Jobs Reports** (Employment)

### Market Sentiment
- **News Headlines** (Market mood)
- **Sentiment Scoring** (Positive/negative/neutral)
- **Impact Assessment** (High/medium/low impact)
- **Category Analysis** (Tech, financial, economic)

### Sector Data
- **10 Sector ETFs** (XLK, XLF, XLV, etc.)
- **Sector Correlations** (Performance alignment)
- **Rotation Patterns** (Sector flow analysis)
- **Concentration Risk** (Overexposure detection)

## Integration Examples

### Live Trading System
```python
# Real-time correlation monitoring
class EnhancedTradingSystem:
    def __init__(self):
        self.correlation_analyzer = TemporalCorrelationAnalyzer()
        self.insights_engine = InsightsEngine(self.correlation_analyzer)
        
    def on_market_open(self):
        market_conditions = self.get_market_conditions()
        insights = self.insights_engine.get_relevant_insights(market_conditions)
        self.apply_insights(insights)
```

### Risk Management
```python
# Correlation-based position sizing
def calculate_position_size(base_size, market_conditions):
    # Volatility adjustment
    if vix_correlation < -0.5 and market_conditions['vix'] > 25:
        base_size *= 0.5
    
    # Sector concentration adjustment
    if sector_concentration > 0.7:
        base_size *= 0.8
    
    return base_size
```

## Key Metrics & Interpretations

### Correlation Strength
- **0.7+**: Strong correlation - high confidence insights
- **0.5-0.7**: Moderate correlation - medium confidence  
- **0.3-0.5**: Weak correlation - low confidence
- **<0.3**: No significant correlation

### Performance Benchmarks
- **Consistency**: >60% positive periods = consistent strategy
- **Streak Quality**: >3 trades with positive P&L = meaningful streak
- **Seasonal Significance**: >$2,000 monthly difference = significant pattern

### Statistical Significance
- **Minimum Data**: 20 periods for correlation analysis
- **P-value**: < 0.05 for statistical significance
- **Sample Size**: Larger samples = higher confidence

## Dashboard Panels Summary

### Performance Dashboard (12 panels)
1. Cumulative P&L timeline
2. Trade P&L distribution
3. Rolling win rate
4. Monthly performance heatmap
5. Streak analysis
6. Performance periods
7. P&L by regime
8. Holding period analysis
9. Risk metrics table
10. Position in range analysis
11. Recent trends
12. Key statistics

### Correlation Dashboard (20 panels)
1. Sentiment correlation scatter
2. Economic event impact
3. Market indicator rankings
4. Sector correlation heatmap
5. Monthly performance patterns
6. Quarterly performance
7. Day-of-week analysis
8. Volatility regime performance
9. Headline category impact
10. Economic category performance
11. Performance timeline with events
12. Correlation matrix
13. VIX vs performance scatter
14. Sector performance comparison
15. Seasonal heatmap
16. Key insights summary
17. Performance by regime
18. Event impact timeline
19. Correlation strength rankings
20. Performance summary statistics

## Alert Configuration

### Streak Monitor Alerts
```json
{
  "alerts": [
    {
      "streak_type": "winning",
      "min_length": 3,
      "message_template": "Winning streak of {length} trades! P&L: ${pnl:,.0f}",
      "severity": "info"
    },
    {
      "streak_type": "losing", 
      "min_length": 3,
      "message_template": "Losing streak of {length} trades. P&L: ${pnl:,.0f}",
      "severity": "warning"
    },
    {
      "streak_type": "winning",
      "min_length": 8,
      "message_template": "OUTSTANDING: {length} trade winning streak!",
      "severity": "critical"
    }
  ]
}
```

## Output Files Generated

### Analysis Outputs
- `performance_dashboard.png` - 12-panel performance visualization
- `correlation_dashboard.png` - 20-panel correlation analysis
- `insights_YYYYMMDD_HHMMSS.json` - Detailed insights export
- `streak_data_YYYYMMDD_HHMMSS.json` - Streak history export

### Report Contents
- **Executive Summary**: Top insights and recommendations
- **Detailed Analysis**: All correlation metrics and patterns
- **Actionable Steps**: Specific implementation recommendations
- **Risk Factors**: Identified risks and mitigation strategies

## Common Use Cases

### 1. Strategy Optimization
```python
# Find best performing time periods
report = analyzer.analyze_performance_periods(window_days=30)
best_period = report['best_period']
print(f"Best period: {best_period['start_date']} - P&L: ${best_period['total_pnl']:,.0f}")

# Get optimization insights
insights = insights_engine.get_insights_by_type(InsightType.STRATEGY_OPTIMIZATION)
for insight in insights:
    print(f"Optimization: {insight.title}")
    print(f"Action: {insight.actionable_steps[0]}")
```

### 2. Risk Management
```python
# Identify volatility transition risk
vol_analysis = correlation_analyzer.analyze_volatility_regime_transitions()
if vol_analysis['transition_analysis']['transition_periods']['avg_performance'] < -1000:
    print("WARNING: High volatility transition risk detected")
    
# Check sector concentration
sector_analysis = correlation_analyzer.analyze_sector_rotation_impact()
best_sector = sector_analysis['best_correlated_sector']
if abs(best_sector['correlation']) > 0.8:
    print(f"WARNING: High sector concentration in {best_sector['name']}")
```

### 3. Market Timing
```python
# Get timing insights
timing_insights = insights_engine.get_insights_by_type(InsightType.TIMING)
for insight in timing_insights:
    if insight.confidence > 0.7:
        print(f"TIMING: {insight.title}")
        print(f"Confidence: {insight.confidence:.1%}")
        print(f"Action: {insight.actionable_steps[0]}")
```

## Performance Optimization Tips

### Data Management
- **Cache API calls** to avoid rate limits
- **Use vectorized operations** for correlation calculations
- **Sample data** for faster initial analysis
- **Incremental updates** for real-time systems

### Memory Efficiency
- **Limit history** to relevant time periods
- **Compress correlation matrices** for storage
- **Clean up intermediate results**
- **Use generators** for large datasets

### Computational Speed
- **Parallel processing** for multiple factors
- **Pre-calculate common metrics**
- **Use efficient data structures** (numpy arrays)
- **Avoid redundant calculations**

## Troubleshooting

### Common Issues
1. **"No trade data available"** - Run strategy simulation first
2. **"Insufficient data for correlation"** - Need minimum 20 periods
3. **"API rate limit exceeded"** - Implement caching
4. **"Empty visualizations"** - Check data preprocessing

### Validation Checks
```python
# Validate correlation analysis
def validate_analysis(correlation_report):
    if 'error' in correlation_report:
        print(f"Analysis error: {correlation_report['error']}")
        return False
    
    metadata = correlation_report.get('analysis_metadata', {})
    if metadata.get('periods_analyzed', 0) < 20:
        print("WARNING: Insufficient data for reliable correlations")
        return False
    
    return True
```

## Advanced Features

### Multi-Factor Scoring
```python
# Combine multiple correlation factors
def calculate_timing_score(market_conditions):
    factors = {
        'sentiment': get_sentiment_score(market_conditions),
        'vix': get_vix_score(market_conditions),
        'economic': get_economic_score(market_conditions),
        'sector': get_sector_score(market_conditions)
    }
    
    # Weighted composite score
    weights = {'sentiment': 0.3, 'vix': 0.3, 'economic': 0.2, 'sector': 0.2}
    return sum(factors[k] * weights[k] for k in factors)
```

### Predictive Timing
```python
# Predict optimal trading windows
def predict_optimal_windows(historical_patterns, current_conditions):
    similar_periods = find_similar_periods(current_conditions)
    if similar_periods:
        future_performance = estimate_future_performance(similar_periods)
        return {
            'optimal_windows': future_performance['best_periods'],
            'confidence': future_performance['confidence'],
            'expected_return': future_performance['avg_return']
        }
```

## Integration Checklist

### Pre-Implementation
- [ ] Verify sufficient historical data (6+ months)
- [ ] Set up external data sources (Yahoo Finance, news APIs)
- [ ] Configure alert thresholds and preferences
- [ ] Test with sample data

### Production Deployment
- [ ] Implement caching for API calls
- [ ] Set up real-time data feeds
- [ ] Configure monitoring and alerts
- [ ] Validate correlation stability

### Ongoing Maintenance
- [ ] Update correlation models monthly
- [ ] Monitor insight accuracy
- [ ] Adjust parameters based on performance
- [ ] Review and refine alert configurations

## Support Resources

### Documentation
- `temporal-correlation-analysis.md` - Complete technical documentation
- `README_performance_analysis.md` - Usage examples and integration
- Code comments and docstrings in each module

### Code Examples
- `scripts/analysis/temporal_correlation_analyzer.py` - Full analysis pipeline
- `scripts/analysis/insights_engine.py` - Insight generation examples
- `scripts/analysis/correlation_dashboard.py` - Visualization examples

### Troubleshooting
- Check data source connectivity
- Validate correlation sample sizes
- Review statistical significance
- Monitor system performance

---

**Quick Tip**: Start with the basic performance analysis, then gradually add correlation factors as you validate their predictive power. Focus on the highest confidence insights first for maximum impact.
