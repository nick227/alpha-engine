"""
Weekly Trade Scheduler

Weekly scheduling with advanced signal prioritization and temporal intelligence.
Handles signal ranking, capital allocation, and opportunity cost optimization.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

from .temporal_scheduler import (
    TemporalScheduler,
    FlexibilityMode,
    SchedulingDecision,
    ManualOverride
)
from .temporal_correlation_integration import SchedulingTemporalAnalyzer, SchedulingInsightsEngine

logger = logging.getLogger(__name__)


class WeeklyScheduler(TemporalScheduler):
    """Weekly scheduling with advanced signal prioritization"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Prioritization methods
        self.prioritization_methods = {
            'risk_adjusted_return': {
                'weight_return': 0.4, 
                'weight_risk': 0.6,
                'description': 'Prioritize risk-adjusted returns'
            },
            'temporal_alignment': {
                'weight_temporal': 0.5, 
                'weight_technical': 0.5,
                'description': 'Prioritize temporal alignment with technical factors'
            },
            'opportunity_cost': {
                'weight_alpha': 0.3, 
                'weight_cost': 0.7,
                'description': 'Prioritize high alpha with low opportunity cost'
            },
            'multi_objective': {
                'return': 0.3, 
                'risk': 0.3, 
                'temporal': 0.2, 
                'diversification': 0.2,
                'description': 'Balanced multi-objective optimization'
            }
        }
        
        self.current_method = config.get('prioritization_method', 'temporal_alignment')
        self.weekly_schedules: Dict[int, Dict[str, Any]] = {}
        
        # Weekly patterns
        self.weekly_patterns = {
            1: {'strength': 1.0, 'volatility': 1.0, 'preferred_strategies': ['balanced']},  # Monday
            2: {'strength': 1.1, 'volatility': 1.1, 'preferred_strategies': ['momentum']},  # Tuesday
            3: {'strength': 1.1, 'volatility': 1.0, 'preferred_strategies': ['momentum']},  # Wednesday
            4: {'strength': 1.1, 'volatility': 1.1, 'preferred_strategies': ['momentum']},  # Thursday
            5: {'strength': 0.9, 'volatility': 1.2, 'preferred_strategies': ['risk_management']},  # Friday
        }
        
        # Initialize temporal components
        self.temporal_analyzer = SchedulingTemporalAnalyzer()
        self.insights_engine = SchedulingInsightsEngine(self.temporal_analyzer)
    
    def create_schedule(self, week: int, year: int, monthly_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Create weekly schedule with advanced prioritization"""
        
        logger.info(f"Creating weekly schedule for week {week} of {year}")
        
        # Calculate weekly budget
        weekly_budget = monthly_plan['final_allocation'] / 4.3  # Average weeks per month
        
        # Get weekly temporal insights
        week_start_date = self._get_week_start_date(week, year)
        temporal_insights = self.get_temporal_insights('weekly', week_start_date)
        
        # Base weekly plan
        base_plan = {
            'week': week,
            'year': year,
            'total_budget': weekly_budget,
            'prioritization_method': self.current_method,
            'temporal_adjustments': {},
            'signal_queue': [],
            'allocated_signals': [],
            'rejected_signals': [],
            'flexibility_options': self.generate_weekly_flexibility_options()
        }
        
        # Apply flexibility mode with temporal insights
        decision = self.apply_flexibility_mode(base_plan, temporal_insights.__dict__)
        
        # Add weekly-specific temporal adjustments
        weekly_adjustments = self._get_weekly_temporal_adjustments(week, temporal_insights)
        decision.final_decision['temporal_adjustments'].update(weekly_adjustments)
        
        # Store weekly plan
        self.weekly_schedules[week] = decision.final_decision
        
        logger.info(f"Weekly plan created for week {week}: "
                   f"${weekly_budget:,.0f} budget, "
                   f"{self.current_method} prioritization")
        
        return {
            'plan': decision.final_decision,
            'decision': decision,
            'temporal_insights': temporal_insights.__dict__
        }
    
    def prioritize_signals(self, signals: List[Dict], weekly_plan: Dict) -> List[Dict]:
        """Prioritize signals using selected method"""
        
        method = self.prioritization_methods[self.current_method]
        logger.info(f"Prioritizing {len(signals)} signals using {self.current_method} method")
        
        # Calculate scores for each signal
        scored_signals = []
        for signal in signals:
            score = self.calculate_signal_score(signal, method)
            
            scored_signal = signal.copy()
            scored_signal.update({
                'prioritization_score': score,
                'score_components': self.get_score_components(signal, method),
                'execution_priority': self.calculate_execution_priority(score, signal),
                'allocation_rank': None,  # Will be set after sorting
                'temporal_alignment': self.calculate_temporal_alignment(signal)
            })
            scored_signals.append(scored_signal)
        
        # Sort by score
        scored_signals.sort(key=lambda x: x['prioritization_score'], reverse=True)
        
        # Assign ranks and allocation
        remaining_budget = weekly_plan['total_budget']
        for i, signal in enumerate(scored_signals):
            signal['allocation_rank'] = i + 1
            signal['allocated_budget'] = self.calculate_signal_allocation(signal, remaining_budget, i)
            remaining_budget -= signal['allocated_budget']
        
        # Separate allocated and rejected signals
        allocated_signals = [s for s in scored_signals if s['allocated_budget'] > 0]
        rejected_signals = [s for s in scored_signals if s['allocated_budget'] <= 0]
        
        # Update weekly plan
        weekly_plan['allocated_signals'] = allocated_signals
        weekly_plan['rejected_signals'] = rejected_signals
        
        logger.info(f"Signal prioritization complete: {len(allocated_signals)} allocated, {len(rejected_signals)} rejected")
        
        return allocated_signals
    
    def calculate_signal_score(self, signal: Dict, method: Dict) -> float:
        """Calculate signal score using selected method"""
        
        if method == self.prioritization_methods['risk_adjusted_return']:
            return self._calculate_risk_adjusted_score(signal, method)
        elif method == self.prioritization_methods['temporal_alignment']:
            return self._calculate_temporal_alignment_score(signal, method)
        elif method == self.prioritization_methods['opportunity_cost']:
            return self._calculate_opportunity_cost_score(signal, method)
        elif method == self.prioritization_methods['multi_objective']:
            return self._calculate_multi_objective_score(signal, method)
        
        return 0.0
    
    def _calculate_risk_adjusted_score(self, signal: Dict, method: Dict) -> float:
        """Calculate risk-adjusted return score"""
        
        expected_return = signal.get('expected_return', 0.1)
        risk = max(signal.get('risk', 0.05), 0.01)  # Avoid division by zero
        
        return (
            (expected_return * method['weight_return']) / risk * method['weight_risk']
        )
    
    def _calculate_temporal_alignment_score(self, signal: Dict, method: Dict) -> float:
        """Calculate temporal alignment score"""
        
        temporal_score = self.calculate_temporal_alignment(signal)
        technical_score = self.calculate_technical_score(signal)
        
        return (
            temporal_score * method['weight_temporal'] + 
            technical_score * method['weight_technical']
        )
    
    def _calculate_opportunity_cost_score(self, signal: Dict, method: Dict) -> float:
        """Calculate opportunity cost score"""
        
        alpha_score = signal.get('alpha_score', 0.5)
        opportunity_cost = max(signal.get('opportunity_cost', 0.02), 0.001)
        
        alpha_component = alpha_score * method['weight_alpha']
        cost_component = (1 / opportunity_cost) * method['weight_cost']
        
        return alpha_component + cost_component
    
    def _calculate_multi_objective_score(self, signal: Dict, method: Dict) -> float:
        """Calculate multi-objective score"""
        
        # Normalize components to 0-1 range
        return_score = min(max(signal.get('expected_return', 0.1) / 0.3, 0), 1)
        risk_score = min(max((1 / max(signal.get('risk', 0.05), 0.01)) / 20, 0), 1)
        temporal_score = self.calculate_temporal_alignment(signal)
        diversification_score = signal.get('diversification_score', 0.5)
        
        return (
            return_score * method['return'] +
            risk_score * method['risk'] +
            temporal_score * method['temporal'] +
            diversification_score * method['diversification']
        )
    
    def calculate_temporal_alignment(self, signal: Dict) -> float:
        """Calculate temporal alignment score for signal"""
        
        alignment_score = 0.5  # Base score
        
        # Strategy alignment with current conditions
        strategy = signal.get('strategy', 'unknown')
        current_volatility = signal.get('current_volatility_regime', 'normal')
        current_sentiment = signal.get('current_sentiment', 0.0)
        
        # Volatility alignment
        if strategy == 'volatility_breakout' and current_volatility == 'expansion':
            alignment_score += 0.3
        elif strategy == 'sniper_coil' and current_volatility == 'high':
            alignment_score += 0.3
        elif strategy == 'silent_compounder' and current_volatility == 'low':
            alignment_score += 0.3
        
        # Sentiment alignment
        if strategy in ['silent_compounder', 'volatility_breakout'] and current_sentiment > 0.5:
            alignment_score += 0.2
        elif strategy in ['sniper_coil', 'realness_repricer'] and current_sentiment < -0.3:
            alignment_score += 0.2
        
        # Economic event alignment
        high_impact_events = signal.get('upcoming_events', 0)
        if strategy == 'temporal_correlation' and high_impact_events > 0:
            alignment_score += 0.2
        
        return min(max(alignment_score, 0), 1)
    
    def calculate_technical_score(self, signal: Dict) -> float:
        """Calculate technical score for signal"""
        
        # Technical factors
        confidence = signal.get('confidence', 0.5)
        consensus_score = signal.get('consensus_score', 0.5)
        alpha_score = signal.get('alpha_score', 0.5)
        
        # Volume and price action
        volume_ratio = signal.get('volume_ratio', 1.0)
        price_momentum = signal.get('price_momentum', 0.0)
        
        # Combine technical factors
        technical_score = (
            confidence * 0.3 +
            consensus_score * 0.3 +
            alpha_score * 0.2 +
            min(max(volume_ratio / 2, 0), 1) * 0.1 +
            min(max((price_momentum + 0.1) / 0.2, 0), 1) * 0.1
        )
        
        return min(max(technical_score, 0), 1)
    
    def calculate_execution_priority(self, score: float, signal: Dict) -> str:
        """Calculate execution priority based on score and signal characteristics"""
        
        if score > 0.8:
            return 'immediate'
        elif score > 0.6:
            return 'high'
        elif score > 0.4:
            return 'medium'
        else:
            return 'low'
    
    def calculate_signal_allocation(self, signal: Dict, remaining_budget: float, rank: int) -> float:
        """Calculate allocation for individual signal"""
        
        # Base allocation calculation
        base_allocation = remaining_budget * 0.2  # Max 20% of remaining budget per signal
        
        # Adjust based on rank (top signals get more)
        rank_multiplier = max(1.0 - (rank * 0.1), 0.3)
        adjusted_allocation = base_allocation * rank_multiplier
        
        # Adjust based on signal strength
        strength_multiplier = signal.get('confidence', 0.5)
        final_allocation = adjusted_allocation * strength_multiplier
        
        # Ensure minimum allocation
        min_allocation = remaining_budget * 0.05  # Min 5% of remaining
        
        return max(min(final_allocation, min_allocation), 0)
    
    def get_score_components(self, signal: Dict, method: Dict) -> Dict[str, float]:
        """Get detailed score components for analysis"""
        
        components = {
            'expected_return': signal.get('expected_return', 0.1),
            'risk': signal.get('risk', 0.05),
            'alpha_score': signal.get('alpha_score', 0.5),
            'confidence': signal.get('confidence', 0.5),
            'consensus_score': signal.get('consensus_score', 0.5),
            'temporal_alignment': self.calculate_temporal_alignment(signal),
            'technical_score': self.calculate_technical_score(signal),
            'opportunity_cost': signal.get('opportunity_cost', 0.02),
            'diversification_score': signal.get('diversification_score', 0.5)
        }
        
        return components
    
    def generate_weekly_flexibility_options(self) -> Dict[str, Any]:
        """Generate flexibility options for weekly scheduling"""
        
        return {
            'prioritization_methods': {
                method: params.copy() 
                for method, params in self.prioritization_methods.items()
            },
            'allocation_strategies': {
                'equal_weight': {'description': 'Equal allocation to top N signals'},
                'score_weighted': {'description': 'Allocation proportional to scores'},
                'risk_adjusted': {'description': 'Adjust allocation based on risk'},
                'opportunity_cost': {'description': 'Consider opportunity cost in allocation'}
            },
            'signal_filters': {
                'min_confidence': 0.3,
                'min_consensus': 0.2,
                'max_risk': 0.15,
                'min_alpha': 0.1,
                'max_signals_per_week': 10
            },
            'execution_options': {
                'immediate_execution': {'delay_minutes': 0, 'batch_size': 1},
                'staggered_execution': {'delay_minutes': 30, 'batch_size': 3},
                'optimal_timing': {'use_temporal_timing': True, 'delay_minutes': 15}
            }
        }
    
    def _get_weekly_temporal_adjustments(self, week: int, insights) -> Dict[str, Any]:
        """Get weekly-specific temporal adjustments"""
        
        adjustments = {}
        
        # Day of week patterns
        day_of_week = (week - 1) % 7 + 1  # Simple approximation
        if day_of_week in self.weekly_patterns:
            patterns = self.weekly_patterns[day_of_week]
            adjustments['day_of_week'] = patterns
        
        # Economic event adjustments
        high_impact_events = [e for e in insights.economic_events if e.get('impact') == 'high']
        if high_impact_events:
            adjustments['economic_events'] = {
                'high_impact_count': len(high_impact_events),
                'risk_multiplier': 0.9,
                'execution_delay': 60  # minutes
            }
        
        # Volatility regime adjustments
        vol_adjustments = {
            'low': {'signal_multiplier': 1.2, 'risk_reduction': 0.9},
            'normal': {'signal_multiplier': 1.0, 'risk_reduction': 1.0},
            'high': {'signal_multiplier': 0.7, 'risk_reduction': 1.3},
            'expansion': {'signal_multiplier': 1.1, 'risk_reduction': 1.1}
        }
        
        if insights.volatility_regime in vol_adjustments:
            adjustments['volatility'] = vol_adjustments[insights.volatility_regime]
        
        return adjustments
    
    def _get_week_start_date(self, week: int, year: int) -> datetime:
        """Get start date for given week number"""
        
        # Simple calculation - first day of year + (week-1) weeks
        start_date = datetime(year, 1, 1) + timedelta(weeks=week-1)
        return start_date
    
    def switch_prioritization_method(self, new_method: str, reason: str):
        """Switch prioritization method"""
        
        old_method = self.current_method
        if new_method in self.prioritization_methods:
            self.current_method = new_method
            logger.info(f"Prioritization method switched: {old_method} -> {new_method} - {reason}")
        else:
            logger.error(f"Invalid prioritization method: {new_method}")
    
    def get_weekly_schedule(self, week: int) -> Optional[Dict[str, Any]]:
        """Get specific weekly schedule"""
        return self.weekly_schedules.get(week)
    
    def apply_manual_override(self, override_data: Dict[str, Any]):
        """Apply manual override to weekly scheduler"""
        
        override = ManualOverride(
            timestamp=datetime.now(),
            level='weekly',
            override_data=override_data,
            reason=override_data.get('reason', 'Manual override'),
            approved_by=override_data.get('approved_by', 'System'),
            impact_assessment=self._assess_override_impact(override_data),
            previous_state={'current_method': self.current_method}
        )
        
        self.record_manual_override(override)
        
        # Apply override
        if 'prioritization_method' in override_data:
            new_method = override_data['prioritization_method']
            self.switch_prioritization_method(new_method, override_data.get('reason', 'Manual override'))
        
        if 'signal_override' in override_data:
            # Apply specific signal overrides
            signal_overrides = override_data['signal_override']
            for week, plan in self.weekly_schedules.items():
                for signal in plan.get('allocated_signals', []):
                    if signal['id'] in signal_overrides:
                        signal.update(signal_overrides[signal['id']])
    
    def _assess_override_impact(self, override_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess impact of manual override"""
        
        impact = {
            'method_change': False,
            'signal_change': False,
            'overall_impact': 'medium'
        }
        
        if 'prioritization_method' in override_data:
            new_method = override_data['prioritization_method']
            if new_method != self.current_method:
                impact['method_change'] = True
                impact['overall_impact'] = 'high'
        
        if 'signal_override' in override_data:
            impact['signal_change'] = True
            impact['overall_impact'] = 'medium'
        
        return impact
