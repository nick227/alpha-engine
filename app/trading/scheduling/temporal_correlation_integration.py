"""
Temporal Correlation Integration for Scheduling System

Integration layer between temporal correlation analysis and trade scheduling.
Provides insights and recommendations for all scheduling levels.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

# Import temporal correlation components
from scripts.analysis.temporal_correlation_analyzer import TemporalCorrelationAnalyzer
from scripts.analysis.insights_engine import InsightsEngine

logger = logging.getLogger(__name__)


class SchedulingTemporalAnalyzer:
    """Temporal correlation analyzer specialized for scheduling decisions"""
    
    def __init__(self):
        self.temporal_analyzer = TemporalCorrelationAnalyzer()
        self.insights_engine = InsightsEngine(self.temporal_analyzer)
        self.cache = {}
        self.cache_ttl = timedelta(hours=1)
    
    def get_period_insights(self, period_type: str, date: datetime) -> Dict[str, Any]:
        """Get temporal insights for specific time period"""
        
        cache_key = f"{period_type}_{date.strftime('%Y-%m-%d')}"
        
        # Check cache
        if cache_key in self.cache:
            cached_data, timestamp = self.cache[cache_key]
            if datetime.now() - timestamp < self.cache_ttl:
                return cached_data
        
        try:
            # Get base temporal analysis
            temporal_data = self.temporal_analyzer.analyze_period(period_type, date)
            
            # Generate scheduling-specific insights
            insights = self._generate_scheduling_insights(temporal_data, period_type, date)
            
            # Cache results
            self.cache[cache_key] = (insights, datetime.now())
            
            return insights
            
        except Exception as e:
            logger.error(f"Error getting period insights for {period_type}: {e}")
            return self._get_default_insights(period_type, date)
    
    def _generate_scheduling_insights(self, temporal_data: Dict, period_type: str, date: datetime) -> Dict[str, Any]:
        """Generate scheduling-specific insights from temporal data"""
        
        base_insights = {
            'period_type': period_type,
            'date': date,
            'sentiment_score': 0.0,
            'economic_events': [],
            'volatility_regime': 'normal',
            'seasonal_multiplier': 1.0,
            'sector_rotation': {},
            'historical_performance': {},
            'confidence_level': 0.5,
            'recommendations': []
        }
        
        # Extract sentiment insights
        if 'sentiment_analysis' in temporal_data:
            sentiment_data = temporal_data['sentiment_analysis']
            base_insights['sentiment_score'] = sentiment_data.get('overall_sentiment', 0.0)
            
            # Add sentiment-based recommendations
            if base_insights['sentiment_score'] > 0.7:
                base_insights['recommendations'].append("High positive sentiment - consider increased exposure")
            elif base_insights['sentiment_score'] < -0.3:
                base_insights['recommendations'].append("Negative sentiment - consider reduced exposure")
        
        # Extract economic event insights
        if 'economic_events' in temporal_data:
            events = temporal_data['economic_events']
            high_impact_events = [e for e in events if e.get('impact') == 'high']
            
            base_insights['economic_events'] = events
            base_insights['high_impact_count'] = len(high_impact_events)
            
            if high_impact_events:
                base_insights['recommendations'].append(
                    f"{len(high_impact_events)} high-impact events - adjust positioning"
                )
        
        # Extract volatility regime insights
        if 'volatility_analysis' in temporal_data:
            vol_data = temporal_data['volatility_analysis']
            vix_percentile = vol_data.get('vix_percentile', 0.5)
            
            if vix_percentile > 0.8:
                base_insights['volatility_regime'] = 'high'
                base_insights['recommendations'].append("High volatility regime - reduce risk exposure")
            elif vix_percentile < 0.2:
                base_insights['volatility_regime'] = 'low'
                base_insights['recommendations'].append("Low volatility regime - consider increased exposure")
            elif vol_data.get('vix_trend') == 'expanding':
                base_insights['volatility_regime'] = 'expansion'
                base_insights['recommendations'].append("Volatility expansion - breakout opportunities")
        
        # Extract seasonal patterns
        if 'seasonal_patterns' in temporal_data:
            seasonal_data = temporal_data['seasonal_patterns']
            
            # Monthly seasonal multiplier
            month = date.month
            if month in seasonal_data.get('monthly_performance', {}):
                monthly_perf = seasonal_data['monthly_performance'][month]
                base_insights['seasonal_multiplier'] = monthly_perf.get('budget_multiplier', 1.0)
                
                if base_insights['seasonal_multiplier'] > 1.1:
                    base_insights['recommendations'].append(
                        f"Strong seasonal pattern for month {month} - increase allocation"
                    )
                elif base_insights['seasonal_multiplier'] < 0.9:
                    base_insights['recommendations'].append(
                        f"Weak seasonal pattern for month {month} - reduce allocation"
                    )
            
            # Quarterly patterns
            quarter = (month - 1) // 3 + 1
            if quarter in seasonal_data.get('quarterly_performance', {}):
                quarterly_perf = seasonal_data['quarterly_performance'][quarter]
                base_insights['quarterly_multiplier'] = quarterly_perf.get('budget_multiplier', 1.0)
        
        # Extract sector rotation insights
        if 'sector_analysis' in temporal_data:
            sector_data = temporal_data['sector_analysis']
            base_insights['sector_rotation'] = sector_data.get('correlations', {})
            
            # Find strongest and weakest sectors
            correlations = sector_data.get('correlations', {})
            if correlations:
                sorted_sectors = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
                if sorted_sectors:
                    strongest = sorted_sectors[0]
                    weakest = sorted_sectors[-1]
                    
                    base_insights['recommendations'].append(
                        f"Strongest sector: {strongest[0]} (corr: {strongest[1]:.2f})"
                    )
                    base_insights['recommendations'].append(
                        f"Weakest sector: {weakest[0]} (corr: {weakest[1]:.2f})"
                    )
        
        # Extract historical performance patterns
        if 'historical_periods' in temporal_data:
            hist_data = temporal_data['historical_periods']
            base_insights['historical_performance'] = hist_data.get('performance_periods', {})
            
            # Calculate confidence based on pattern consistency
            performance_periods = hist_data.get('performance_periods', {})
            if performance_periods:
                consistency_score = self._calculate_pattern_consistency(performance_periods)
                base_insights['confidence_level'] = consistency_score
        
        return base_insights
    
    def _calculate_pattern_consistency(self, performance_periods: Dict) -> float:
        """Calculate consistency score for historical patterns"""
        if not performance_periods:
            return 0.5
        
        # Simple consistency calculation based on variance
        returns = list(performance_periods.values())
        if len(returns) < 2:
            return 0.5
        
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
        
        # Convert variance to consistency score (lower variance = higher consistency)
        consistency = 1.0 / (1.0 + variance)
        
        return min(max(consistency, 0.0), 1.0)
    
    def _get_default_insights(self, period_type: str, date: datetime) -> Dict[str, Any]:
        """Get default insights when analysis fails"""
        return {
            'period_type': period_type,
            'date': date,
            'sentiment_score': 0.0,
            'economic_events': [],
            'volatility_regime': 'normal',
            'seasonal_multiplier': 1.0,
            'sector_rotation': {},
            'historical_performance': {},
            'confidence_level': 0.3,  # Low confidence due to missing data
            'recommendations': ['Using default parameters - temporal analysis unavailable']
        }


class SchedulingInsightsEngine:
    """Enhanced insights engine for scheduling decisions"""
    
    def __init__(self, temporal_analyzer: SchedulingTemporalAnalyzer):
        self.temporal_analyzer = temporal_analyzer
        self.insight_cache = {}
        self.cache_ttl = timedelta(minutes=30)
    
    def get_scheduling_recommendations(self, period_type: str, date: datetime, 
                                  context: Dict[str, Any]) -> Dict[str, Any]:
        """Get comprehensive scheduling recommendations"""
        
        cache_key = f"recs_{period_type}_{date.strftime('%Y-%m-%d')}"
        
        # Check cache
        if cache_key in self.insight_cache:
            cached_data, timestamp = self.insight_cache[cache_key]
            if datetime.now() - timestamp < self.cache_ttl:
                return cached_data
        
        try:
            # Get base temporal insights
            insights = self.temporal_analyzer.get_period_insights(period_type, date)
            
            # Generate scheduling-specific recommendations
            recommendations = self._generate_scheduling_recommendations(insights, context)
            
            # Calculate action scores
            action_scores = self._calculate_action_scores(insights, context)
            
            # Determine optimal actions
            optimal_actions = self._determine_optimal_actions(action_scores)
            
            result = {
                'insights': insights,
                'recommendations': recommendations,
                'action_scores': action_scores,
                'optimal_actions': optimal_actions,
                'confidence': insights['confidence_level'],
                'generated_at': datetime.now()
            }
            
            # Cache results
            self.insight_cache[cache_key] = (result, datetime.now())
            
            return result
            
        except Exception as e:
            logger.error(f"Error generating scheduling recommendations: {e}")
            return self._get_default_recommendations(period_type, date)
    
    def _generate_scheduling_recommendations(self, insights: Dict, context: Dict) -> List[Dict]:
        """Generate specific scheduling recommendations"""
        recommendations = []
        
        # Sentiment-based recommendations
        sentiment = insights.get('sentiment_score', 0.0)
        if sentiment > 0.7:
            recommendations.append({
                'type': 'allocation',
                'action': 'increase',
                'reason': 'High positive sentiment',
                'confidence': 0.8,
                'impact': 'medium'
            })
        elif sentiment < -0.3:
            recommendations.append({
                'type': 'allocation',
                'action': 'decrease',
                'reason': 'Negative sentiment',
                'confidence': 0.7,
                'impact': 'medium'
            })
        
        # Economic event recommendations
        high_impact_count = insights.get('high_impact_count', 0)
        if high_impact_count > 0:
            recommendations.append({
                'type': 'timing',
                'action': 'delay_execution',
                'reason': f'{high_impact_count} high-impact events',
                'confidence': 0.9,
                'impact': 'high'
            })
        
        # Volatility regime recommendations
        vol_regime = insights.get('volatility_regime', 'normal')
        if vol_regime == 'high':
            recommendations.append({
                'type': 'risk',
                'action': 'reduce_exposure',
                'reason': 'High volatility regime',
                'confidence': 0.8,
                'impact': 'high'
            })
        elif vol_regime == 'low':
            recommendations.append({
                'type': 'allocation',
                'action': 'increase',
                'reason': 'Low volatility regime',
                'confidence': 0.7,
                'impact': 'medium'
            })
        
        # Seasonal recommendations
        seasonal_mult = insights.get('seasonal_multiplier', 1.0)
        if seasonal_mult > 1.1:
            recommendations.append({
                'type': 'allocation',
                'action': 'increase',
                'reason': 'Strong seasonal pattern',
                'confidence': 0.6,
                'impact': 'medium'
            })
        elif seasonal_mult < 0.9:
            recommendations.append({
                'type': 'allocation',
                'action': 'decrease',
                'reason': 'Weak seasonal pattern',
                'confidence': 0.6,
                'impact': 'medium'
            })
        
        return recommendations
    
    def _calculate_action_scores(self, insights: Dict, context: Dict) -> Dict[str, float]:
        """Calculate scores for different scheduling actions"""
        scores = {}
        
        # Base sentiment score
        sentiment = insights.get('sentiment_score', 0.0)
        scores['increase_allocation'] = max(0, sentiment * 0.4)
        scores['decrease_allocation'] = max(0, -sentiment * 0.4)
        
        # Volatility adjustment
        vol_regime = insights.get('volatility_regime', 'normal')
        if vol_regime == 'high':
            scores['reduce_risk'] = 0.8
            scores['delay_execution'] = 0.6
        elif vol_regime == 'low':
            scores['increase_allocation'] += 0.3
            scores['immediate_execution'] = 0.5
        
        # Economic event adjustment
        high_impact_count = insights.get('high_impact_count', 0)
        if high_impact_count > 0:
            scores['delay_execution'] += 0.3 * high_impact_count
            scores['reduce_risk'] += 0.2 * high_impact_count
        
        # Seasonal adjustment
        seasonal_mult = insights.get('seasonal_multiplier', 1.0)
        if seasonal_mult > 1.1:
            scores['increase_allocation'] += 0.2 * (seasonal_mult - 1.0)
        elif seasonal_mult < 0.9:
            scores['decrease_allocation'] += 0.2 * (1.0 - seasonal_mult)
        
        # Context-based adjustments
        current_drawdown = context.get('current_drawdown', 0.0)
        if current_drawdown < -0.1:  # 10% drawdown
            scores['reduce_risk'] += 0.5
            scores['conservative_mode'] = 0.7
        
        return scores
    
    def _determine_optimal_actions(self, action_scores: Dict[str, float]) -> List[Dict]:
        """Determine optimal actions based on scores"""
        sorted_actions = sorted(action_scores.items(), key=lambda x: x[1], reverse=True)
        
        optimal_actions = []
        for action, score in sorted_actions:
            if score > 0.3:  # Minimum threshold
                optimal_actions.append({
                    'action': action,
                    'score': score,
                    'priority': 'high' if score > 0.7 else 'medium' if score > 0.5 else 'low'
                })
        
        return optimal_actions[:3]  # Top 3 actions
    
    def _get_default_recommendations(self, period_type: str, date: datetime) -> Dict[str, Any]:
        """Get default recommendations when analysis fails"""
        return {
            'insights': {
                'period_type': period_type,
                'date': date,
                'confidence_level': 0.3
            },
            'recommendations': [{
                'type': 'caution',
                'action': 'use_defaults',
                'reason': 'Temporal analysis unavailable',
                'confidence': 0.3,
                'impact': 'low'
            }],
            'action_scores': {},
            'optimal_actions': [],
            'confidence': 0.3,
            'generated_at': datetime.now()
        }
