"""
Flexibility Manager

Manage flexibility modes and automatic adjustments across the scheduling system.
Provides centralized control of operational modes and their impacts.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

from .temporal_scheduler import (
    FlexibilityMode,
    StrategicMode,
    ControlMode,
    ExecutionMode
)

logger = logging.getLogger(__name__)


class FlexibilityManager:
    """Manage flexibility modes and automatic adjustments"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Flexibility mode definitions
        self.flexibility_modes = {
            FlexibilityMode.STRICT: {
                'auto_adjust': False, 
                'manual_override': 'limited',
                'description': 'Minimal temporal adjustments, strict adherence to base parameters',
                'risk_adjustment': 0.9,
                'return_adjustment': 0.95
            },
            FlexibilityMode.ADAPTIVE: {
                'auto_adjust': True, 
                'manual_override': 'full',
                'description': 'Balanced temporal adjustments with market adaptation',
                'risk_adjustment': 1.0,
                'return_adjustment': 1.0
            },
            FlexibilityMode.OPPORTUNISTIC: {
                'auto_adjust': True, 
                'manual_override': 'full', 
                'risk_multiplier': 1.2,
                'description': 'Aggressive temporal adjustments for opportunity capture',
                'risk_adjustment': 1.3,
                'return_adjustment': 1.2
            },
            FlexibilityMode.CONSERVATIVE: {
                'auto_adjust': True, 
                'manual_override': 'full',
                'risk_multiplier': 0.7,
                'description': 'Risk-focused temporal adjustments with capital preservation',
                'risk_adjustment': 0.7,
                'return_adjustment': 0.8
            }
        }
        
        self.current_mode = FlexibilityMode(config.get('global_flexibility_mode', 'adaptive'))
        self.mode_switch_history = []
        self.impact_assessments = {}
        
        # Mode switching rules
        self.mode_switching_rules = {
            'performance_triggers': {
                'drawdown_threshold': 0.08,      # Switch to conservative at 8% drawdown
                'profit_threshold': 0.12,          # Switch to aggressive at 12% profit
                'volatility_spike': 0.30,           # Switch to conservative on VIX spike
                'consistency_threshold': 0.6           # Switch modes if consistency < 60%
            },
            'time_based_triggers': {
                'quarter_end': True,                    # Review mode at quarter end
                'monthly_review': True,                   # Review mode monthly
                'performance_review_period': 30,           # Days between performance reviews
                'auto_optimization': False               # Enable automatic mode optimization
            },
            'market_condition_triggers': {
                'volatility_regime_change': True,        # Switch on volatility regime change
                'sentiment_extreme': True,               # Switch on extreme sentiment
                'economic_event_density': True,          # Switch on high event density
                'market_stress_threshold': 0.8            # VIX percentile for stress mode
            }
        }
        
        # Initialize mode performance tracking
        self.mode_performance = {}
        self._initialize_mode_performance()
    
    def _initialize_mode_performance(self):
        """Initialize performance tracking for all modes"""
        
        for mode in FlexibilityMode:
            self.mode_performance[mode.value] = {
                'total_return': 0.0,
                'risk_adjusted_return': 0.0,
                'max_drawdown': 0.0,
                'win_rate': 0.0,
                'sharpe_ratio': 0.0,
                'consistency_score': 0.5,
                'days_active': 0,
                'switches_in': 0,
                'switches_out': 0,
                'last_updated': datetime.now()
            }
    
    def switch_flexibility_mode(self, new_mode: FlexibilityMode, reason: str, 
                              impact_assessment: Optional[Dict] = None) -> Dict[str, Any]:
        """Switch flexibility mode with full tracking"""
        
        old_mode = self.current_mode
        
        # Assess impact if not provided
        if impact_assessment is None:
            impact_assessment = self.assess_mode_switch_impact(new_mode)
        
        # Create switch record
        switch_record = {
            'timestamp': datetime.now(),
            'previous_mode': old_mode.value,
            'new_mode': new_mode.value,
            'reason': reason,
            'impact_assessment': impact_assessment,
            'automated': reason.startswith('AUTO:')  # Track automatic vs manual switches
        }
        
        # Update mode performance tracking
        self._update_mode_performance_on_switch(old_mode, new_mode)
        
        # Apply new mode
        self.current_mode = new_mode
        
        # Record switch
        self.mode_switch_history.append(switch_record)
        
        logger.info(f"Flexibility mode switched: {old_mode.value} -> {new_mode.value} - {reason}")
        
        return switch_record
    
    def assess_mode_switch_impact(self, new_mode: FlexibilityMode) -> Dict[str, Any]:
        """Assess impact of switching flexibility modes"""
        
        current_parameters = self.flexibility_modes[self.current_mode]
        new_parameters = self.flexibility_modes[new_mode]
        
        impact = {
            'risk_change': new_parameters.get('risk_adjustment', 1.0) - current_parameters.get('risk_adjustment', 1.0),
            'return_change': new_parameters.get('return_adjustment', 1.0) - current_parameters.get('return_adjustment', 1.0),
            'automation_change': new_parameters.get('auto_adjust') != current_parameters.get('auto_adjust'),
            'override_level_change': new_parameters.get('manual_override') != current_parameters.get('manual_override'),
            'complexity_change': self._calculate_complexity_change(new_mode),
            'expected_performance_impact': self._estimate_performance_impact(new_mode),
            'risk_level': self._calculate_risk_level(new_mode),
            'adaptability_score': self._calculate_adaptability_score(new_mode)
        }
        
        # Overall impact assessment
        impact['overall_impact'] = self._calculate_overall_impact(impact)
        
        return impact
    
    def _calculate_complexity_change(self, new_mode: FlexibilityMode) -> str:
        """Calculate complexity change of mode switch"""
        
        complexity_levels = {
            FlexibilityMode.STRICT: 1,
            FlexibilityMode.ADAPTIVE: 2,
            FlexibilityMode.OPPORTUNISTIC: 3,
            FlexibilityMode.CONSERVATIVE: 2
        }
        
        current_complexity = complexity_levels[self.current_mode]
        new_complexity = complexity_levels[new_mode]
        
        if new_complexity > current_complexity:
            return 'increase'
        elif new_complexity < current_complexity:
            return 'decrease'
        else:
            return 'no_change'
    
    def _estimate_performance_impact(self, new_mode: FlexibilityMode) -> float:
        """Estimate performance impact of new mode"""
        
        # Base performance expectations for each mode
        performance_expectations = {
            FlexibilityMode.STRICT: {'expected_return': 0.08, 'expected_risk': 0.06},
            FlexibilityMode.ADAPTIVE: {'expected_return': 0.12, 'expected_risk': 0.10},
            FlexibilityMode.OPPORTUNISTIC: {'expected_return': 0.18, 'expected_risk': 0.15},
            FlexibilityMode.CONSERVATIVE: {'expected_return': 0.09, 'expected_risk': 0.05}
        }
        
        current_perf = performance_expectations[self.current_mode]
        new_perf = performance_expectations[new_mode]
        
        # Calculate risk-adjusted return difference
        current_risk_adj = current_perf['expected_return'] / current_perf['expected_risk']
        new_risk_adj = new_perf['expected_return'] / new_perf['expected_risk']
        
        return new_risk_adj - current_risk_adj
    
    def _calculate_risk_level(self, mode: FlexibilityMode) -> str:
        """Calculate risk level for mode"""
        
        risk_adjustments = {
            FlexibilityMode.STRICT: 0.9,
            FlexibilityMode.ADAPTIVE: 1.0,
            FlexibilityMode.OPPORTUNISTIC: 1.3,
            FlexibilityMode.CONSERVATIVE: 0.7
        }
        
        risk_level = risk_adjustments[mode]
        
        if risk_level <= 0.8:
            return 'low'
        elif risk_level <= 1.1:
            return 'medium'
        else:
            return 'high'
    
    def _calculate_adaptability_score(self, mode: FlexibilityMode) -> float:
        """Calculate adaptability score for mode"""
        
        mode_params = self.flexibility_modes[mode]
        
        # Score based on auto-adjustment and override capabilities
        auto_score = 1.0 if mode_params.get('auto_adjust') else 0.3
        override_score = {
            'full': 1.0,
            'limited': 0.5,
            'none': 0.1
        }.get(mode_params.get('manual_override', 'none'), 0.1)
        
        return (auto_score + override_score) / 2
    
    def _calculate_overall_impact(self, impact: Dict[str, Any]) -> str:
        """Calculate overall impact assessment"""
        
        # Weight different impact factors
        risk_impact = abs(impact['risk_change'])
        return_impact = abs(impact['return_change'])
        complexity_impact = 1 if impact['complexity_change'] != 'no_change' else 0
        
        # Calculate overall impact score
        overall_score = (risk_impact * 0.4 + return_impact * 0.4 + complexity_impact * 0.2)
        
        if overall_score > 0.3:
            return 'high'
        elif overall_score > 0.1:
            return 'medium'
        else:
            return 'low'
    
    def _update_mode_performance_on_switch(self, old_mode: FlexibilityMode, new_mode: FlexibilityMode):
        """Update performance tracking when switching modes"""
        
        # Update old mode
        if old_mode in self.mode_performance:
            old_perf = self.mode_performance[old_mode.value]
            old_perf['switches_out'] += 1
            old_perf['last_updated'] = datetime.now()
        
        # Update new mode
        if new_mode in self.mode_performance:
            new_perf = self.mode_performance[new_mode.value]
            new_perf['switches_in'] += 1
            new_perf['last_updated'] = datetime.now()
    
    def update_mode_performance(self, mode: FlexibilityMode, performance_data: Dict[str, float]):
        """Update performance data for specific mode"""
        
        if mode.value not in self.mode_performance:
            self._initialize_mode_performance()
        
        mode_perf = self.mode_performance[mode.value]
        
        # Update performance metrics
        for key, value in performance_data.items():
            if key in mode_perf:
                # Use exponential moving average for smooth updates
                alpha = 0.1  # Smoothing factor
                old_value = mode_perf[key]
                mode_perf[key] = alpha * value + (1 - alpha) * old_value
        
        mode_perf['days_active'] += 1
        mode_perf['last_updated'] = datetime.now()
    
    def get_mode_switch_recommendations(self) -> List[Dict[str, Any]]:
        """Get recommendations for mode switches"""
        
        recommendations = []
        
        # Performance-based recommendations
        current_perf = self.mode_performance.get(self.current_mode.value, {})
        
        if current_perf.get('sharpe_ratio', 0) < 0.5:  # Low Sharpe ratio
            recommendations.append({
                'type': 'performance_based',
                'recommended_mode': FlexibilityMode.OPPORTUNISTIC.value,
                'reason': f"Low Sharpe ratio ({current_perf.get('sharpe_ratio', 0):.2f}) suggests more aggressive approach",
                'priority': 'medium',
                'confidence': 0.7,
                'timestamp': datetime.now()
            })
        
        if current_perf.get('max_drawdown', 0) > 0.15:  # High drawdown
            recommendations.append({
                'type': 'risk_based',
                'recommended_mode': FlexibilityMode.CONSERVATIVE.value,
                'reason': f"High drawdown ({current_perf.get('max_drawdown', 0):.1%}) suggests more conservative approach",
                'priority': 'high',
                'confidence': 0.8,
                'timestamp': datetime.now()
            })
        
        # Consistency-based recommendations
        if current_perf.get('consistency_score', 0.5) < 0.4:  # Low consistency
            recommendations.append({
                'type': 'consistency_based',
                'recommended_mode': FlexibilityMode.ADAPTIVE.value,
                'reason': f"Low consistency score ({current_perf.get('consistency_score', 0.5):.2f}) suggests adaptive approach",
                'priority': 'medium',
                'confidence': 0.6,
                'timestamp': datetime.now()
            })
        
        # Market condition-based recommendations
        market_conditions = self._get_current_market_conditions()
        
        if market_conditions.get('volatility_percentile', 0.5) > 0.8:  # High volatility
            recommendations.append({
                'type': 'market_condition',
                'recommended_mode': FlexibilityMode.CONSERVATIVE.value,
                'reason': f"High volatility regime (VIX {market_conditions.get('volatility_percentile', 0.5):.1f} percentile) suggests conservative mode",
                'priority': 'high',
                'confidence': 0.9,
                'timestamp': datetime.now()
            })
        
        if market_conditions.get('market_sentiment', 0) > 0.7:  # High positive sentiment
            recommendations.append({
                'type': 'market_condition',
                'recommended_mode': FlexibilityMode.OPPORTUNISTIC.value,
                'reason': f"Positive sentiment ({market_conditions.get('market_sentiment', 0):.2f}) suggests opportunistic approach",
                'priority': 'medium',
                'confidence': 0.7,
                'timestamp': datetime.now()
            })
        
        # Sort recommendations by priority and confidence
        recommendations.sort(key=lambda x: (x['priority'], x['confidence']), reverse=True)
        
        return recommendations[:3]  # Top 3 recommendations
    
    def _get_current_market_conditions(self) -> Dict[str, Any]:
        """Get current market conditions for mode recommendations"""
        
        # This would integrate with real-time market data
        # For now, return mock data
        return {
            'volatility_percentile': 0.6,
            'market_sentiment': 0.2,
            'volatility_regime': 'normal',
            'economic_event_density': 0.3,
            'trend_strength': 0.4
        }
    
    def get_mode_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for all modes"""
        
        summary = {}
        
        for mode, performance in self.mode_performance.items():
            # Calculate mode efficiency
            total_switches = performance['switches_in'] + performance['switches_out']
            switch_rate = total_switches / max(performance['days_active'], 1)
            
            # Calculate performance stability
            stability_score = 1.0 - abs(performance.get('max_drawdown', 0))
            
            summary[mode] = {
                'total_return': performance['total_return'],
                'risk_adjusted_return': performance['risk_adjusted_return'],
                'sharpe_ratio': performance['sharpe_ratio'],
                'max_drawdown': performance['max_drawdown'],
                'win_rate': performance['win_rate'],
                'consistency_score': performance['consistency_score'],
                'days_active': performance['days_active'],
                'switch_rate': switch_rate,
                'stability_score': stability_score,
                'overall_score': self._calculate_overall_mode_score(performance)
            }
        
        return summary
    
    def _calculate_overall_mode_score(self, performance: Dict[str, float]) -> float:
        """Calculate overall score for mode performance"""
        
        # Weight different performance metrics
        return_score = min(max(performance.get('total_return', 0) / 0.2, 0), 1)  # Normalize to 20% annual
        risk_adj_score = min(max(performance.get('risk_adjusted_return', 0) / 0.15, 0), 1)  # Normalize to 15% annual
        sharpe_score = min(max(performance.get('sharpe_ratio', 0) / 1.0, 0), 1)  # Normalize to 1.0 Sharpe
        consistency_score = performance.get('consistency_score', 0.5)
        stability_score = 1.0 - abs(performance.get('max_drawdown', 0))
        
        # Weighted combination
        overall_score = (
            return_score * 0.25 +
            risk_adj_score * 0.25 +
            sharpe_score * 0.25 +
            consistency_score * 0.15 +
            stability_score * 0.10
        )
        
        return overall_score
    
    def get_optimal_mode_for_conditions(self, market_conditions: Dict[str, Any]) -> FlexibilityMode:
        """Get optimal flexibility mode for given market conditions"""
        
        # Score each mode for current conditions
        mode_scores = {}
        
        for mode in FlexibilityMode:
            score = self._score_mode_for_conditions(mode, market_conditions)
            mode_scores[mode] = score
        
        # Select mode with highest score
        optimal_mode = max(mode_scores, key=mode_scores.get)
        
        return optimal_mode
    
    def _score_mode_for_conditions(self, mode: FlexibilityMode, conditions: Dict[str, Any]) -> float:
        """Score a mode for specific market conditions"""
        
        mode_params = self.flexibility_modes[mode]
        performance = self.mode_performance.get(mode.value, {})
        
        score = 0.0
        
        # Volatility alignment
        volatility = conditions.get('volatility_percentile', 0.5)
        if mode == FlexibilityMode.CONSERVATIVE and volatility > 0.7:
            score += 0.3  # Conservative mode good in high volatility
        elif mode == FlexibilityMode.OPPORTUNISTIC and 0.3 < volatility < 0.7:
            score += 0.3  # Opportunistic mode good in moderate volatility
        elif mode == FlexibilityMode.ADAPTIVE:
            score += 0.2  # Adaptive mode always decent
        
        # Sentiment alignment
        sentiment = conditions.get('market_sentiment', 0)
        if mode == FlexibilityMode.OPPORTUNISTIC and sentiment > 0.5:
            score += 0.3  # Opportunistic mode good in positive sentiment
        elif mode == FlexibilityMode.CONSERVATIVE and sentiment < -0.3:
            score += 0.3  # Conservative mode good in negative sentiment
        
        # Historical performance
        overall_score = performance.get('overall_score', 0.5)
        score += overall_score * 0.4
        
        return score
    
    def get_mode_switch_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent mode switch history"""
        
        return sorted(self.mode_switch_history, key=lambda x: x['timestamp'], reverse=True)[:limit]
    
    def get_current_mode_parameters(self) -> Dict[str, Any]:
        """Get parameters for current flexibility mode"""
        
        return self.flexibility_modes[self.current_mode].copy()
    
    def should_auto_switch_mode(self) -> Optional[Tuple[FlexibilityMode, str]]:
        """Check if automatic mode switch is recommended"""
        
        # Check performance triggers
        current_perf = self.mode_performance.get(self.current_mode.value, {})
        market_conditions = self._get_current_market_conditions()
        
        # Drawdown trigger
        if current_perf.get('max_drawdown', 0) > self.mode_switching_rules['performance_triggers']['drawdown_threshold']:
            return (FlexibilityMode.CONSERVATIVE, "AUTO: High drawdown detected")
        
        # Profit trigger
        if current_perf.get('total_return', 0) > self.mode_switching_rules['performance_triggers']['profit_threshold']:
            return (FlexibilityMode.OPPORTUNISTIC, "AUTO: High profit achieved")
        
        # Volatility spike trigger
        if market_conditions.get('volatility_percentile', 0.5) > self.mode_switching_rules['performance_triggers']['volatility_spike']:
            return (FlexibilityMode.CONSERVATIVE, "AUTO: Volatility spike detected")
        
        # Extreme sentiment trigger
        sentiment = market_conditions.get('market_sentiment', 0)
        if sentiment > 0.8 or sentiment < -0.8:
            if sentiment > 0.8:
                return (FlexibilityMode.OPPORTUNISTIC, "AUTO: Extreme positive sentiment")
            else:
                return (FlexibilityMode.CONSERVATIVE, "AUTO: Extreme negative sentiment")
        
        return None
    
    def get_flexibility_report(self) -> Dict[str, Any]:
        """Get comprehensive flexibility report"""
        
        return {
            'current_mode': self.current_mode.value,
            'current_parameters': self.get_current_mode_parameters(),
            'mode_performance': self.get_mode_performance_summary(),
            'switch_history': self.get_mode_switch_history(),
            'recommendations': self.get_mode_switch_recommendations(),
            'auto_switch_recommendation': self.should_auto_switch_mode(),
            'mode_efficiency': self._calculate_mode_efficiency(),
            'last_updated': datetime.now()
        }
    
    def _calculate_mode_efficiency(self) -> Dict[str, float]:
        """Calculate efficiency metrics for current mode"""
        
        current_perf = self.mode_performance.get(self.current_mode.value, {})
        
        # Calculate efficiency metrics
        return_efficiency = current_perf.get('risk_adjusted_return', 0) / max(current_perf.get('max_drawdown', 0.01), 0.01)
        consistency_efficiency = current_perf.get('consistency_score', 0.5)
        switch_efficiency = 1.0 / (1.0 + current_perf.get('switches_in', 0) + current_perf.get('switches_out', 0))
        
        return {
            'return_efficiency': return_efficiency,
            'consistency_efficiency': consistency_efficiency,
            'switch_efficiency': switch_efficiency,
            'overall_efficiency': (return_efficiency + consistency_efficiency + switch_efficiency) / 3
        }
