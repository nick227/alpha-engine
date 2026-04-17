"""
Temporal Correlation Analyzer

Correlates strategy performance periods with external time-based indicators:
- Market events and headlines
- Economic calendar events  
- Seasonal patterns
- Market sentiment indicators
- Cross-asset correlations
- Volatility regime transitions
- Sector rotation patterns
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import requests
import json
from collections import defaultdict
import yfinance as yf

from .strategy_performance_periods import StrategyPerformanceAnalyzer


class TemporalCorrelationAnalyzer:
    """
    Analyzes temporal correlations between strategy performance and external indicators.
    """
    
    def __init__(self, analyzer: StrategyPerformanceAnalyzer = None):
        self.analyzer = analyzer or StrategyPerformanceAnalyzer()
        self.external_data = {}
        self.correlations = {}
        
    def fetch_market_headlines(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Fetch market headlines for correlation analysis.
        Note: In production, this would connect to a news API service.
        """
        
        # Sample headlines data structure - in production this would be from API
        sample_headlines = [
            {
                'date': datetime(2024, 1, 2),
                'headline': 'Fed Signals Rate Cut Pause Amid Strong Economic Data',
                'sentiment': 'neutral',
                'category': 'monetary_policy',
                'impact_score': 0.7
            },
            {
                'date': datetime(2024, 1, 15),
                'headline': 'Tech Stocks Rally on AI Optimism',
                'sentiment': 'positive',
                'category': 'sector_tech',
                'impact_score': 0.8
            },
            {
                'date': datetime(2024, 2, 5),
                'headline': 'Inflation Concerns Spark Market Sell-off',
                'sentiment': 'negative',
                'category': 'economic',
                'impact_score': 0.9
            },
            {
                'date': datetime(2024, 3, 10),
                'headline': 'Banking Sector Concerns Rise',
                'sentiment': 'negative',
                'category': 'financial',
                'impact_score': 0.8
            },
            {
                'date': datetime(2024, 4, 15),
                'headline': 'Earnings Season Exceeds Expectations',
                'sentiment': 'positive',
                'category': 'earnings',
                'impact_score': 0.6
            }
        ]
        
        # Filter by date range
        filtered_headlines = [
            h for h in sample_headlines 
            if start_date <= h['date'] <= end_date
        ]
        
        return pd.DataFrame(filtered_headlines)
    
    def fetch_economic_calendar(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Fetch economic calendar events.
        """
        
        # Sample economic events
        sample_events = [
            {
                'date': datetime(2024, 1, 3),
                'event': 'FOMC Meeting Minutes',
                'category': 'monetary_policy',
                'impact': 'high',
                'actual': None,
                'forecast': None,
                'previous': None
            },
            {
                'date': datetime(2024, 1, 12),
                'event': 'CPI Data Release',
                'category': 'inflation',
                'impact': 'high',
                'actual': 3.4,
                'forecast': 3.5,
                'previous': 3.7
            },
            {
                'date': datetime(2024, 1, 26),
                'event': 'GDP Growth Report',
                'category': 'growth',
                'impact': 'high',
                'actual': 2.9,
                'forecast': 2.8,
                'previous': 3.1
            },
            {
                'date': datetime(2024, 2, 2),
                'event': 'Jobs Report',
                'category': 'employment',
                'impact': 'high',
                'actual': 353000,
                'forecast': 185000,
                'previous': 333000
            },
            {
                'date': datetime(2024, 3, 20),
                'event': 'Fed Rate Decision',
                'category': 'monetary_policy',
                'impact': 'high',
                'actual': 5.25,
                'forecast': 5.25,
                'previous': 5.25
            }
        ]
        
        filtered_events = [
            e for e in sample_events 
            if start_date <= e['date'] <= end_date
        ]
        
        return pd.DataFrame(filtered_events)
    
    def fetch_market_indicators(self, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        """
        Fetch market indicators for correlation analysis.
        """
        
        indicators = {}
        
        # VIX data (fear index)
        try:
            vix = yf.download('^VIX', start=start_date, end=end_date)
            indicators['VIX'] = vix[['Close']].rename(columns={'Close': 'VIX'})
        except:
            # Generate sample VIX data if API fails
            dates = pd.date_range(start_date, end_date)
            indicators['VIX'] = pd.DataFrame({
                'VIX': np.random.normal(20, 5, len(dates))
            }, index=dates)
        
        # S&P 500 data
        try:
            sp500 = yf.download('^GSPC', start=start_date, end=end_date)
            indicators['SP500'] = sp500[['Close']].rename(columns={'Close': 'SP500'})
        except:
            dates = pd.date_range(start_date, end_date)
            indicators['SP500'] = pd.DataFrame({
                'SP500': 4500 + np.cumsum(np.random.normal(0, 50, len(dates)))
            }, index=dates)
        
        # 10-Year Treasury Yield
        try:
            dxy = yf.download('^TNX', start=start_date, end=end_date)
            indicators['TNX'] = dxy[['Close']].rename(columns={'Close': 'TNX'})
        except:
            dates = pd.date_range(start_date, end_date)
            indicators['TNX'] = pd.DataFrame({
                'TNX': 4.0 + np.random.normal(0, 0.2, len(dates))
            }, index=dates)
        
        # Dollar Index
        try:
            dxy = yf.download('DX-Y.NYB', start=start_date, end=end_date)
            indicators['DXY'] = dxy[['Close']].rename(columns={'Close': 'DXY'})
        except:
            dates = pd.date_range(start_date, end_date)
            indicators['DXY'] = pd.DataFrame({
                'DXY': 100 + np.random.normal(0, 1, len(dates))
            }, index=dates)
        
        return indicators
    
    def calculate_sector_performance(self, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        """
        Calculate sector performance data.
        """
        
        sector_etfs = {
            'Technology': 'XLK',
            'Financials': 'XLF', 
            'Healthcare': 'XLV',
            'Energy': 'XLE',
            'Consumer': 'XLY',
            'Industrial': 'XLI',
            'Materials': 'XLB',
            'Utilities': 'XLU',
            'Real Estate': 'XLRE',
            'Communications': 'XLC'
        }
        
        sector_data = {}
        
        for sector, ticker in sector_etfs.items():
            try:
                data = yf.download(ticker, start=start_date, end=end_date)
                sector_data[sector] = data[['Close']].rename(columns={'Close': sector})
            except:
                # Generate sample data if API fails
                dates = pd.date_range(start_date, end_date)
                sector_data[sector] = pd.DataFrame({
                    sector: 100 + np.cumsum(np.random.normal(0, 2, len(dates)))
                }, index=dates)
        
        return sector_data
    
    def analyze_sentiment_correlation(self, performance_periods: pd.DataFrame) -> Dict[str, Any]:
        """
        Correlate performance periods with market sentiment.
        """
        
        if len(performance_periods) == 0:
            return {}
        
        # Get headlines for the analysis period
        start_date = performance_periods['start_date'].min()
        end_date = performance_periods['end_date'].max()
        
        headlines = self.fetch_market_headlines(start_date, end_date)
        
        if len(headlines) == 0:
            return {'error': 'No headlines data available'}
        
        # Analyze sentiment by performance period
        sentiment_analysis = []
        
        for _, period in performance_periods.iterrows():
            period_start = period['start_date']
            period_end = period['end_date']
            
            # Get headlines in this period
            period_headlines = headlines[
                (headlines['date'] >= period_start) & 
                (headlines['date'] <= period_end)
            ]
            
            if len(period_headlines) > 0:
                avg_sentiment = period_headlines['impact_score'].mean()
                sentiment_distribution = period_headlines['sentiment'].value_counts().to_dict()
                headline_categories = period_headlines['category'].value_counts().to_dict()
                
                sentiment_analysis.append({
                    'period_start': period_start,
                    'period_end': period_end,
                    'performance': period['total_pnl'],
                    'win_rate': period['win_rate'],
                    'trade_count': period['trade_count'],
                    'headline_count': len(period_headlines),
                    'avg_sentiment_score': avg_sentiment,
                    'sentiment_distribution': sentiment_distribution,
                    'headline_categories': headline_categories
                })
        
        # Calculate correlations
        if len(sentiment_analysis) > 1:
            sentiment_df = pd.DataFrame(sentiment_analysis)
            
            correlation = sentiment_df['performance'].corr(sentiment_df['avg_sentiment_score'])
            win_rate_correlation = sentiment_df['win_rate'].corr(sentiment_df['avg_sentiment_score'])
            
            # Analyze by sentiment type
            sentiment_performance = {}
            for sentiment in ['positive', 'negative', 'neutral']:
                sentiment_periods = sentiment_df[
                    sentiment_df['sentiment_distribution'].apply(
                        lambda x: x.get(sentiment, 0) > 0
                    )
                ]
                
                if len(sentiment_periods) > 0:
                    sentiment_performance[sentiment] = {
                        'avg_performance': sentiment_periods['performance'].mean(),
                        'avg_win_rate': sentiment_periods['win_rate'].mean(),
                        'period_count': len(sentiment_periods)
                    }
            
            return {
                'correlation_with_sentiment': correlation,
                'correlation_with_win_rate': win_rate_correlation,
                'sentiment_performance': sentiment_performance,
                'detailed_analysis': sentiment_analysis
            }
        
        return {'error': 'Insufficient data for correlation analysis'}
    
    def analyze_economic_event_impact(self, performance_periods: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze impact of economic events on performance.
        """
        
        if len(performance_periods) == 0:
            return {}
        
        # Get economic events
        start_date = performance_periods['start_date'].min()
        end_date = performance_periods['end_date'].max()
        
        events = self.fetch_economic_calendar(start_date, end_date)
        
        if len(events) == 0:
            return {'error': 'No economic events data available'}
        
        # Analyze event impact
        event_analysis = []
        
        for _, period in performance_periods.iterrows():
            period_start = period['start_date']
            period_end = period['end_date']
            
            # Get events in this period
            period_events = events[
                (events['date'] >= period_start) & 
                (events['date'] <= period_end)
            ]
            
            if len(period_events) > 0:
                high_impact_events = period_events[period_events['impact'] == 'high']
                event_categories = period_events['category'].value_counts().to_dict()
                
                event_analysis.append({
                    'period_start': period_start,
                    'period_end': period_end,
                    'performance': period['total_pnl'],
                    'win_rate': period['win_rate'],
                    'trade_count': period['trade_count'],
                    'event_count': len(period_events),
                    'high_impact_count': len(high_impact_events),
                    'event_categories': event_categories
                })
        
        # Analyze by event category
        if len(event_analysis) > 0:
            event_df = pd.DataFrame(event_analysis)
            
            category_performance = {}
            for category in events['category'].unique():
                category_periods = event_df[
                    event_df['event_categories'].apply(
                        lambda x: x.get(category, 0) > 0
                    )
                ]
                
                if len(category_periods) > 0:
                    category_performance[category] = {
                        'avg_performance': category_periods['performance'].mean(),
                        'avg_win_rate': category_periods['win_rate'].mean(),
                        'period_count': len(category_periods)
                    }
            
            # High impact events analysis
            high_impact_periods = event_df[event_df['high_impact_count'] > 0]
            no_high_impact_periods = event_df[event_df['high_impact_count'] == 0]
            
            return {
                'category_performance': category_performance,
                'high_impact_performance': {
                    'with_high_impact': {
                        'avg_performance': high_impact_periods['performance'].mean() if len(high_impact_periods) > 0 else 0,
                        'avg_win_rate': high_impact_periods['win_rate'].mean() if len(high_impact_periods) > 0 else 0,
                        'period_count': len(high_impact_periods)
                    },
                    'without_high_impact': {
                        'avg_performance': no_high_impact_periods['performance'].mean() if len(no_high_impact_periods) > 0 else 0,
                        'avg_win_rate': no_high_impact_periods['win_rate'].mean() if len(no_high_impact_periods) > 0 else 0,
                        'period_count': len(no_high_impact_periods)
                    }
                },
                'detailed_analysis': event_analysis
            }
        
        return {'error': 'Insufficient data for event analysis'}
    
    def analyze_market_indicator_correlation(self, performance_periods: pd.DataFrame) -> Dict[str, Any]:
        """
        Correlate performance with market indicators.
        """
        
        if len(performance_periods) == 0:
            return {}
        
        # Get market indicators
        start_date = performance_periods['start_date'].min()
        end_date = performance_periods['end_date'].max()
        
        indicators = self.fetch_market_indicators(start_date, end_date)
        
        indicator_correlations = {}
        
        for indicator_name, indicator_data in indicators.items():
            # Calculate indicator changes for each performance period
            indicator_changes = []
            
            for _, period in performance_periods.iterrows():
                period_start = period['start_date']
                period_end = period['end_date']
                
                # Get indicator values at period boundaries
                try:
                    start_value = indicator_data.loc[indicator_data.index >= period_start].iloc[0][indicator_name]
                    end_value = indicator_data.loc[indicator_data.index <= period_end].iloc[-1][indicator_name]
                    
                    # Calculate percentage change
                    if start_value != 0:
                        change_pct = (end_value - start_value) / start_value * 100
                    else:
                        change_pct = 0
                    
                    indicator_changes.append({
                        'performance': period['total_pnl'],
                        'win_rate': period['win_rate'],
                        'indicator_change': change_pct
                    })
                except:
                    continue
            
            if len(indicator_changes) > 1:
                changes_df = pd.DataFrame(indicator_changes)
                
                correlation = changes_df['performance'].corr(changes_df['indicator_change'])
                win_rate_correlation = changes_df['win_rate'].corr(changes_df['indicator_change'])
                
                indicator_correlations[indicator_name] = {
                    'performance_correlation': correlation,
                    'win_rate_correlation': win_rate_correlation,
                    'sample_size': len(changes_df)
                }
        
        return indicator_correlations
    
    def analyze_sector_rotation_impact(self, performance_periods: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze sector rotation impact on strategy performance.
        """
        
        if len(performance_periods) == 0:
            return {}
        
        # Get sector data
        start_date = performance_periods['start_date'].min()
        end_date = performance_periods['end_date'].max()
        
        sector_data = self.calculate_sector_performance(start_date, end_date)
        
        sector_analysis = {}
        
        for sector, data in sector_data.items():
            sector_changes = []
            
            for _, period in performance_periods.iterrows():
                period_start = period['start_date']
                period_end = period['end_date']
                
                try:
                    start_value = data.loc[data.index >= period_start].iloc[0][sector]
                    end_value = data.loc[data.index <= period_end].iloc[-1][sector]
                    
                    if start_value != 0:
                        change_pct = (end_value - start_value) / start_value * 100
                    else:
                        change_pct = 0
                    
                    sector_changes.append({
                        'performance': period['total_pnl'],
                        'win_rate': period['win_rate'],
                        'sector_change': change_pct
                    })
                except:
                    continue
            
            if len(sector_changes) > 1:
                changes_df = pd.DataFrame(sector_changes)
                
                correlation = changes_df['performance'].corr(changes_df['sector_change'])
                win_rate_correlation = changes_df['win_rate'].corr(changes_df['sector_change'])
                
                sector_analysis[sector] = {
                    'performance_correlation': correlation,
                    'win_rate_correlation': win_rate_correlation,
                    'avg_sector_change': changes_df['sector_change'].mean(),
                    'sample_size': len(changes_df)
                }
        
        # Find best and worst correlated sectors
        if sector_analysis:
            best_correlated = max(sector_analysis.items(), key=lambda x: abs(x[1]['performance_correlation']))
            worst_correlated = min(sector_analysis.items(), key=lambda x: abs(x[1]['performance_correlation']))
            
            return {
                'sector_correlations': sector_analysis,
                'best_correlated_sector': {
                    'name': best_correlated[0],
                    'correlation': best_correlated[1]['performance_correlation']
                },
                'worst_correlated_sector': {
                    'name': worst_correlated[0], 
                    'correlation': worst_correlated[1]['performance_correlation']
                }
            }
        
        return {'error': 'Insufficient sector data for analysis'}
    
    def analyze_seasonal_patterns(self, performance_periods: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze seasonal patterns in performance.
        """
        
        if len(performance_periods) == 0:
            return {}
        
        # Add time-based features
        periods_df = performance_periods.copy()
        periods_df['start_date'] = pd.to_datetime(periods_df['start_date'])
        periods_df['month'] = periods_df['start_date'].dt.month
        periods_df['quarter'] = periods_df['start_date'].dt.quarter
        periods_df['day_of_week'] = periods_df['start_date'].dt.dayofweek
        periods_df['week_of_year'] = periods_df['start_date'].dt.isocalendar().week
        
        # Monthly analysis
        monthly_stats = periods_df.groupby('month').agg({
            'total_pnl': ['mean', 'std', 'count'],
            'win_rate': ['mean', 'std'],
            'trade_count': ['mean', 'std']
        }).round(2)
        
        # Quarterly analysis
        quarterly_stats = periods_df.groupby('quarter').agg({
            'total_pnl': ['mean', 'std', 'count'],
            'win_rate': ['mean', 'std'],
            'trade_count': ['mean', 'std']
        }).round(2)
        
        # Day of week analysis
        dow_stats = periods_df.groupby('day_of_week').agg({
            'total_pnl': ['mean', 'std', 'count'],
            'win_rate': ['mean', 'std'],
            'trade_count': ['mean', 'std']
        }).round(2)
        
        # Week of year analysis (to identify specific week patterns)
        weekly_stats = periods_df.groupby('week_of_year').agg({
            'total_pnl': ['mean', 'count'],
            'win_rate': 'mean'
        }).round(2)
        
        # Find patterns
        best_month = monthly_stats.loc[monthly_stats[('total_pnl', 'mean')].idxmax()]
        worst_month = monthly_stats.loc[monthly_stats[('total_pnl', 'mean')].idxmin()]
        
        best_quarter = quarterly_stats.loc[quarterly_stats[('total_pnl', 'mean')].idxmax()]
        worst_quarter = quarterly_stats.loc[quarterly_stats[('total_pnl', 'mean')].idxmin()]
        
        return {
            'monthly_analysis': monthly_stats.to_dict(),
            'quarterly_analysis': quarterly_stats.to_dict(),
            'day_of_week_analysis': dow_stats.to_dict(),
            'weekly_analysis': weekly_stats.to_dict(),
            'best_month': {
                'month': int(best_month.name),
                'avg_performance': float(best_month[('total_pnl', 'mean')]),
                'win_rate': float(best_month[('win_rate', 'mean')])
            },
            'worst_month': {
                'month': int(worst_month.name),
                'avg_performance': float(worst_month[('total_pnl', 'mean')]),
                'win_rate': float(worst_month[('win_rate', 'mean')])
            },
            'best_quarter': {
                'quarter': int(best_quarter.name),
                'avg_performance': float(best_quarter[('total_pnl', 'mean')]),
                'win_rate': float(best_quarter[('win_rate', 'mean')])
            },
            'worst_quarter': {
                'quarter': int(worst_quarter.name),
                'avg_performance': float(worst_quarter[('total_pnl', 'mean')]),
                'win_rate': float(worst_quarter[('win_rate', 'mean')])
            }
        }
    
    def analyze_volatility_regime_transitions(self, performance_periods: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze performance during volatility regime transitions.
        """
        
        if len(performance_periods) == 0:
            return {}
        
        # Get VIX data for volatility analysis
        start_date = performance_periods['start_date'].min()
        end_date = performance_periods['end_date'].max()
        
        indicators = self.fetch_market_indicators(start_date, end_date)
        
        if 'VIX' not in indicators:
            return {'error': 'VIX data not available'}
        
        vix_data = indicators['VIX']
        
        # Define volatility regimes
        vix_percentiles = vix_data['VIX'].quantile([0.2, 0.8])
        
        regime_analysis = []
        
        for _, period in performance_periods.iterrows():
            period_start = period['start_date']
            period_end = period['end_date']
            
            try:
                # Get VIX values for the period
                period_vix = vix_data.loc[
                    (vix_data.index >= period_start) & 
                    (vix_data.index <= period_end)
                ]['VIX']
                
                if len(period_vix) > 0:
                    avg_vix = period_vix.mean()
                    vix_volatility = period_vix.std()
                    
                    # Determine regime
                    if avg_vix <= vix_percentiles.iloc[0]:
                        regime = 'low_volatility'
                    elif avg_vix >= vix_percentiles.iloc[1]:
                        regime = 'high_volatility'
                    else:
                        regime = 'normal_volatility'
                    
                    # Check for regime transition (high volatility in period)
                    regime_transition = (period_vix.max() - period_vix.min()) > vix_volatility * 2
                    
                    regime_analysis.append({
                        'period_start': period_start,
                        'period_end': period_end,
                        'performance': period['total_pnl'],
                        'win_rate': period['win_rate'],
                        'avg_vix': avg_vix,
                        'vix_volatility': vix_volatility,
                        'volatility_regime': regime,
                        'regime_transition': regime_transition
                    })
            except:
                continue
        
        if len(regime_analysis) > 0:
            regime_df = pd.DataFrame(regime_analysis)
            
            # Analyze by regime
            regime_performance = {}
            for regime in ['low_volatility', 'normal_volatility', 'high_volatility']:
                regime_periods = regime_df[regime_df['volatility_regime'] == regime]
                
                if len(regime_periods) > 0:
                    regime_performance[regime] = {
                        'avg_performance': regime_periods['performance'].mean(),
                        'avg_win_rate': regime_periods['win_rate'].mean(),
                        'avg_vix': regime_periods['avg_vix'].mean(),
                        'period_count': len(regime_periods)
                    }
            
            # Transition analysis
            transition_periods = regime_df[regime_df['regime_transition']]
            stable_periods = regime_df[~regime_df['regime_transition']]
            
            return {
                'regime_performance': regime_performance,
                'transition_analysis': {
                    'transition_periods': {
                        'avg_performance': transition_periods['performance'].mean() if len(transition_periods) > 0 else 0,
                        'avg_win_rate': transition_periods['win_rate'].mean() if len(transition_periods) > 0 else 0,
                        'period_count': len(transition_periods)
                    },
                    'stable_periods': {
                        'avg_performance': stable_periods['performance'].mean() if len(stable_periods) > 0 else 0,
                        'avg_win_rate': stable_periods['win_rate'].mean() if len(stable_periods) > 0 else 0,
                        'period_count': len(stable_periods)
                    }
                },
                'vix_correlation': regime_df['performance'].corr(regime_df['avg_vix'])
            }
        
        return {'error': 'Insufficient data for volatility analysis'}
    
    def generate_comprehensive_correlation_report(self, performance_periods: pd.DataFrame = None) -> Dict[str, Any]:
        """
        Generate comprehensive correlation report combining all analyses.
        """
        
        if performance_periods is None:
            # Get performance periods from analyzer
            if not hasattr(self.analyzer, 'performance_periods') or not self.analyzer.performance_periods:
                print("No performance periods available. Running analysis first...")
                self.analyzer.analyze_performance_periods()
            
            performance_periods = self.analyzer.performance_periods.get('performance_periods', pd.DataFrame())
        
        if len(performance_periods) == 0:
            return {'error': 'No performance periods data available'}
        
        print("Generating comprehensive temporal correlation analysis...")
        
        # Run all correlation analyses
        sentiment_analysis = self.analyze_sentiment_correlation(performance_periods)
        economic_analysis = self.analyze_economic_event_impact(performance_periods)
        indicator_analysis = self.analyze_market_indicator_correlation(performance_periods)
        sector_analysis = self.analyze_sector_rotation_impact(performance_periods)
        seasonal_analysis = self.analyze_seasonal_patterns(performance_periods)
        volatility_analysis = self.analyze_volatility_regime_transitions(performance_periods)
        
        # Compile comprehensive report
        comprehensive_report = {
            'analysis_metadata': {
                'periods_analyzed': len(performance_periods),
                'date_range': {
                    'start': performance_periods['start_date'].min().isoformat(),
                    'end': performance_periods['end_date'].max().isoformat()
                },
                'total_pnl': performance_periods['total_pnl'].sum(),
                'avg_win_rate': performance_periods['win_rate'].mean()
            },
            'sentiment_correlation': sentiment_analysis,
            'economic_event_impact': economic_analysis,
            'market_indicator_correlation': indicator_analysis,
            'sector_rotation_impact': sector_analysis,
            'seasonal_patterns': seasonal_analysis,
            'volatility_regime_analysis': volatility_analysis
        }
        
        # Generate key insights
        insights = self._generate_key_insights(comprehensive_report)
        comprehensive_report['key_insights'] = insights
        
        return comprehensive_report
    
    def _generate_key_insights(self, report: Dict[str, Any]) -> List[str]:
        """
        Generate key insights from the correlation analysis.
        """
        
        insights = []
        
        # Sentiment insights
        sentiment_corr = report.get('sentiment_correlation', {}).get('correlation_with_sentiment')
        if sentiment_corr is not None:
            if abs(sentiment_corr) > 0.5:
                insights.append(f"Strong {'positive' if sentiment_corr > 0 else 'negative'} correlation with market sentiment (r={sentiment_corr:.2f})")
        
        # Economic event insights
        high_impact = report.get('economic_event_impact', {}).get('high_impact_performance', {})
        if high_impact:
            with_events = high_impact.get('with_high_impact', {}).get('avg_performance', 0)
            without_events = high_impact.get('without_high_impact', {}).get('avg_performance', 0)
            
            if abs(with_events - without_events) > 1000:
                better = "better" if with_events > without_events else "worse"
                insights.append(f"Strategy performs {better} during high-impact economic events (${abs(with_events - without_events):,.0f} difference)")
        
        # Sector insights
        sector_analysis = report.get('sector_rotation_impact', {})
        best_sector = sector_analysis.get('best_correlated_sector', {})
        if best_sector and abs(best_sector.get('correlation', 0)) > 0.5:
            insights.append(f"Strong correlation with {best_sector['name']} sector (r={best_sector['correlation']:.2f})")
        
        # Seasonal insights
        seasonal = report.get('seasonal_patterns', {})
        best_month = seasonal.get('best_month', {})
        worst_month = seasonal.get('worst_month', {})
        
        if best_month and worst_month:
            performance_diff = best_month['avg_performance'] - worst_month['avg_performance']
            if abs(performance_diff) > 2000:
                insights.append(f"Significant seasonal pattern: {best_month['avg_performance']:.0f} in best month vs {worst_month['avg_performance']:.0f} in worst month")
        
        # Volatility insights
        vol_analysis = report.get('volatility_regime_analysis', {})
        regime_performance = vol_analysis.get('regime_performance', {})
        
        if regime_performance:
            high_vol = regime_performance.get('high_volatility', {}).get('avg_performance', 0)
            low_vol = regime_performance.get('low_volatility', {}).get('avg_performance', 0)
            
            if abs(high_vol - low_vol) > 1500:
                better_regime = "high" if high_vol > low_vol else "low"
                insights.append(f"Strategy performs better in {better_regime} volatility regimes")
        
        return insights


def main():
    """Main function to run temporal correlation analysis."""
    
    print("Temporal Correlation Analyzer")
    print("=" * 50)
    
    # Initialize analyzer
    analyzer = StrategyPerformanceAnalyzer()
    
    # Load data and run analysis
    if not analyzer.load_historical_data():
        return
    
    if not analyzer.calculate_regimes():
        return
    
    analyzer.simulate_bear_expansion_strategy()
    
    # Initialize correlation analyzer
    correlation_analyzer = TemporalCorrelationAnalyzer(analyzer)
    
    # Generate comprehensive report
    report = correlation_analyzer.generate_comprehensive_correlation_report()
    
    # Print key insights
    print("\nKEY INSIGHTS:")
    print("=" * 30)
    
    insights = report.get('key_insights', [])
    for i, insight in enumerate(insights, 1):
        print(f"{i}. {insight}")
    
    # Print summary statistics
    metadata = report.get('analysis_metadata', {})
    print(f"\nANALYSIS SUMMARY:")
    print(f"  Periods Analyzed: {metadata.get('periods_analyzed', 0)}")
    print(f"  Date Range: {metadata.get('date_range', {}).get('start', 'N/A')} to {metadata.get('date_range', {}).get('end', 'N/A')}")
    print(f"  Total P&L: ${metadata.get('total_pnl', 0):,.0f}")
    print(f"  Average Win Rate: {metadata.get('avg_win_rate', 0):.1%}")
    
    return correlation_analyzer, report


if __name__ == "__main__":
    analyzer, report = main()
