"""
Yearly Trade Scheduler

Strategic yearly planning with maximum flexibility and temporal intelligence.
Handles capital allocation, strategy weights, and risk budgeting at yearly level.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

from .temporal_scheduler import (
    TemporalScheduler, 
    StrategicMode, 
    FlexibilityMode,
    SchedulingDecision,
    ManualOverride
)
from .temporal_correlation_integration import SchedulingTemporalAnalyzer, SchedulingInsightsEngine

logger = logging.getLogger(__name__)


class YearlyScheduler(TemporalScheduler):
    """Year-level strategic planning with maximum flexibility"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Strategic mode definitions
        self.strategic_modes = {
            StrategicMode.CONSERVATIVE: {
                'risk_budget': 0.15, 
                'max_drawdown': 0.08,
                'volatility_cap': 0.25,
                'max_positions': 15,
                'expected_return': 0.12
            },
            StrategicMode.BALANCED: {
                'risk_budget': 0.20, 
                'max_drawdown': 0.12,
                'volatility_cap': 0.35,
                'max_positions': 20,
                'expected_return': 0.18
            },
            StrategicMode.AGGRESSIVE: {
                'risk_budget': 0.25, 
                'max_drawdown': 0.15,
                'volatility_cap': 0.45,
                'max_positions': 25,
                'expected_return': 0.25
            },
            StrategicMode.OPPORTUNISTIC: {
                'risk_budget': 0.30, 
                'max_drawdown': 0.20,
                'volatility_cap': 0.60,
                'max_positions': 30,
                'expected_return': 0.35
            }
        }
        
        self.current_mode = StrategicMode(config.get('yearly_mode', 'balanced'))
        self.current_plan: Optional[Dict[str, Any]] = None
        self.quarterly_plans: Dict[int, Dict[str, Any]] = {}
        
        # Base strategy weights
        self.base_strategy_weights = {
            'volatility_breakout': 0.25,
            'sniper_coil': 0.15,
            'silent_compounder': 0.20,
            'realness_repricer': 0.15,
            'narrative_lag': 0.10,
            'temporal_correlation': 0.15
        }
        
        # Initialize temporal components
        self.temporal_analyzer = SchedulingTemporalAnalyzer()
        self.insights_engine = SchedulingInsightsEngine(self.temporal_analyzer)
    
    def create_schedule(self, year: int, manual_overrides: Optional[Dict] = None) -> Dict[str, Any]:
        """Create comprehensive yearly trading plan"""
        
        logger.info(f"Creating yearly schedule for {year} with mode {self.current_mode}")
        
        # Base parameters
        base_plan = {
            'year': year,
            'total_capital': self.config.get('annual_capital', 1000000),
            'strategic_mode': self.current_mode.value,
            'quarterly_allocations': {},
            'strategy_weights': self.base_strategy_weights.copy(),
            'risk_parameters': self.strategic_modes[self.current_mode].copy(),
            'temporal_adjustments': {},
            'flexibility_options': self.generate_flexibility_options()
        }
        
        # Apply manual overrides if provided
        if manual_overrides:
            self._apply_manual_overrides_to_plan(base_plan, manual_overrides)
        
        # Get temporal insights for the year
        temporal_insights = self.get_temporal_insights('yearly', datetime(year, 1, 1))
        
        # Apply flexibility mode with temporal insights
        decision = self.apply_flexibility_mode(base_plan, temporal_insights.__dict__)
        
        # Calculate quarterly allocations
        for quarter in range(1, 5):
            quarterly_allocation = self.calculate_quarterly_allocation(
                quarter, decision.final_decision, temporal_insights
            )
            decision.final_decision['quarterly_allocations'][quarter] = quarterly_allocation
        
        # Store current plan
        self.current_plan = decision.final_decision
        
        logger.info(f"Yearly plan created for {year}: "
                   f"${decision.final_decision['total_capital']:,.0f} capital, "
                   f"{self.current_mode.value} mode")
        
        return {
            'plan': decision.final_decision,
            'decision': decision,
            'temporal_insights': temporal_insights.__dict__
        }
    
    def calculate_quarterly_allocation(self, quarter: int, plan: Dict, insights) -> Dict[str, Any]:
        """Calculate quarterly allocation with all flexibility options"""
        
        base_allocation = plan['total_capital'] / 4
        
        # Get quarterly-specific temporal insights
        quarter_date = datetime(plan['year'], (quarter - 1) * 3 + 1, 1)
        quarterly_insights = self.get_temporal_insights('quarterly', quarter_date)
        
        # Temporal adjustments
        seasonal_multiplier = self._get_seasonal_multiplier(quarter, quarterly_insights)
        economic_event_adjustment = self._get_economic_event_adjustment(quarter, quarterly_insights)
        volatility_adjustment = self._get_volatility_adjustment(quarter, quarterly_insights)
        
        # Flexibility mode adjustments
        if self.current_mode == StrategicMode.OPPORTUNISTIC:
            # Increase allocation in strong periods
            if seasonal_multiplier > 1.2:
                seasonal_multiplier *= 1.2
            if economic_event_adjustment > 1.1:
                economic_event_adjustment *= 1.1
                
        elif self.current_mode == StrategicMode.CONSERVATIVE:
            # Reduce allocation in uncertain periods
            if volatility_adjustment < 0.9:
                volatility_adjustment *= 0.8
        
        # Calculate final allocation
        final_allocation = base_allocation * seasonal_multiplier * economic_event_adjustment * volatility_adjustment
        
        # Calculate strategy weights for quarter
        quarterly_strategy_weights = self.calculate_quarterly_strategy_weights(quarter, quarterly_insights)
        
        allocation = {
            'quarter': quarter,
            'base_allocation': base_allocation,
            'adjusted_allocation': final_allocation,
            'adjustments': {
                'seasonal': seasonal_multiplier,
                'economic_events': economic_event_adjustment,
                'volatility': volatility_adjustment,
                'total_adjustment': seasonal_multiplier * economic_event_adjustment * volatility_adjustment
            },
            'strategy_weights': quarterly_strategy_weights,
            'risk_parameters': self._adjust_risk_parameters_for_quarter(quarter, quarterly_insights),
            'temporal_insights': quarterly_insights.__dict__,
            'recommendations': quarterly_insights.recommendations
        }
        
        # Store quarterly plan
        self.quarterly_plans[quarter] = allocation
        
        return allocation
    
    def _get_seasonal_multiplier(self, quarter: int, insights) -> float:
        """Get seasonal multiplier for quarter"""
        # Historical quarterly performance patterns
        seasonal_patterns = {
            1: 1.1,  # Q1 - slightly strong
            2: 0.9,  # Q2 - weak
            3: 1.0,  # Q3 - neutral
            4: 1.2   # Q4 - strong
        }
        
        base_multiplier = seasonal_patterns.get(quarter, 1.0)
        
        # Adjust with temporal insights
        if hasattr(insights, 'seasonal_multiplier'):
            return insights.seasonal_multiplier
        
        return base_multiplier
    
    def _get_economic_event_adjustment(self, quarter: int, insights) -> float:
        """Get economic event adjustment for quarter"""
        # Count high-impact events in quarter
        high_impact_count = len([e for e in insights.economic_events if e.get('impact') == 'high'])
        
        # Economic event density adjustment
        if high_impact_count == 0:
            return 1.0
        elif high_impact_count <= 2:
            return 1.05  # Slight increase for opportunity
        elif high_impact_count <= 5:
            return 0.95  # Slight decrease for risk
        else:
            return 0.85  # Significant decrease for high event density
    
    def _get_volatility_adjustment(self, quarter: int, insights) -> float:
        """Get volatility adjustment for quarter"""
        vol_regime = insights.volatility_regime
        
        if vol_regime == 'low':
            return 1.1  # Increase allocation in low vol
        elif vol_regime == 'normal':
            return 1.0
        elif vol_regime == 'high':
            return 0.8  # Decrease allocation in high vol
        elif vol_regime == 'expansion':
            return 1.2  # Increase for breakout opportunities
        else:
            return 1.0
    
    def calculate_quarterly_strategy_weights(self, quarter: int, insights) -> Dict[str, float]:
        """Calculate strategy weights for specific quarter"""
        
        base_weights = self.base_strategy_weights.copy()
        
        # Adjust based on volatility regime
        if insights.volatility_regime == 'expansion':
            base_weights['volatility_breakout'] *= 1.5
            base_weights['sniper_coil'] *= 1.2
            base_weights['silent_compounder'] *= 0.7
        elif insights.volatility_regime == 'high':
            base_weights['sniper_coil'] *= 1.3
            base_weights['temporal_correlation'] *= 1.2
            base_weights['volatility_breakout'] *= 0.8
        elif insights.volatility_regime == 'low':
            base_weights['silent_compounder'] *= 1.3
            base_weights['realness_repricer'] *= 1.1
            base_weights['sniper_coil'] *= 0.6
        
        # Adjust based on sentiment
        if insights.sentiment_score > 0.5:
            base_weights['silent_compounder'] *= 1.2
            base_weights['volatility_breakout'] *= 1.1
        elif insights.sentiment_score < -0.3:
            base_weights['sniper_coil'] *= 1.3
            base_weights['realness_repricer'] *= 1.2
        
        # Normalize weights
        total_weight = sum(base_weights.values())
        normalized_weights = {k: v / total_weight for k, v in base_weights.items()}
        
        return normalized_weights
    
    def _adjust_risk_parameters_for_quarter(self, quarter: int, insights) -> Dict[str, Any]:
        """Adjust risk parameters based on quarterly conditions"""
        
        base_risk = self.strategic_modes[self.current_mode].copy()
        
        # Volatility-based adjustments
        if insights.volatility_regime == 'high':
            base_risk['risk_budget'] *= 0.7
            base_risk['max_drawdown'] *= 0.8
        elif insights.volatility_regime == 'low':
            base_risk['risk_budget'] *= 1.1
            base_risk['max_drawdown'] *= 1.1
        
        # Economic event adjustments
        high_impact_count = len([e for e in insights.economic_events if e.get('impact') == 'high'])
        if high_impact_count > 3:
            base_risk['risk_budget'] *= 0.8
            base_risk['max_drawdown'] *= 0.9
        
        # Sentiment adjustments
        if insights.sentiment_score < -0.5:
            base_risk['risk_budget'] *= 0.85
            base_risk['max_drawdown'] *= 0.85
        
        return base_risk
    
    def generate_flexibility_options(self) -> Dict[str, Any]:
        """Generate comprehensive flexibility options for yearly planning"""
        
        return {
            'strategic_modes': {
                mode.value: params.copy() 
                for mode, params in self.strategic_modes.items()
            },
            'allocation_options': {
                'conservative': {'multiplier': 0.8, 'risk_reduction': 0.2},
                'standard': {'multiplier': 1.0, 'risk_reduction': 0.0},
                'aggressive': {'multiplier': 1.3, 'risk_increase': 0.3}
            },
            'strategy_override_options': {
                'volatility_focus': {
                    'volatility_breakout': 0.4,
                    'sniper_coil': 0.3,
                    'temporal_correlation': 0.3
                },
                'value_focus': {
                    'realness_repricer': 0.4,
                    'narrative_lag': 0.3,
                    'silent_compounder': 0.3
                },
                'temporal_focus': {
                    'temporal_correlation': 0.5,
                    'volatility_breakout': 0.3,
                    'silent_compounder': 0.2
                }
            },
            'risk_management_options': {
                'fixed_risk': {'use_fixed_risk': True, 'risk_per_trade': 0.02},
                'adaptive_risk': {'use_adaptive_risk': True, 'volatility_adjustment': True},
                'portfolio_risk': {'use_portfolio_risk': True, 'max_portfolio_heat': 0.15}
            }
        }
    
    def _apply_manual_overrides_to_plan(self, plan: Dict, overrides: Dict):
        """Apply manual overrides to yearly plan"""
        
        if 'total_capital' in overrides:
            plan['total_capital'] = overrides['total_capital']
            logger.info(f"Manual override: Total capital set to ${overrides['total_capital']:,.0f}")
        
        if 'strategic_mode' in overrides:
            try:
                self.current_mode = StrategicMode(overrides['strategic_mode'])
                plan['strategic_mode'] = self.current_mode.value
                plan['risk_parameters'] = self.strategic_modes[self.current_mode].copy()
                logger.info(f"Manual override: Strategic mode set to {self.current_mode.value}")
            except ValueError:
                logger.error(f"Invalid strategic mode: {overrides['strategic_mode']}")
        
        if 'strategy_weights' in overrides:
            plan['strategy_weights'].update(overrides['strategy_weights'])
            # Normalize weights
            total_weight = sum(plan['strategy_weights'].values())
            plan['strategy_weights'] = {
                k: v / total_weight for k, v in plan['strategy_weights'].items()
            }
            logger.info(f"Manual override: Strategy weights updated")
    
    def switch_strategic_mode(self, new_mode: StrategicMode, reason: str):
        """Switch strategic mode with full tracking"""
        
        old_mode = self.current_mode
        self.current_mode = new_mode
        
        # Record mode switch
        logger.info(f"Strategic mode switched: {old_mode.value} -> {new_mode.value} - {reason}")
        
        # Recalculate current plan if exists
        if self.current_plan:
            self.create_schedule(self.current_plan['year'])
    
    def get_current_plan(self) -> Optional[Dict[str, Any]]:
        """Get current yearly plan"""
        return self.current_plan
    
    def get_quarterly_plan(self, quarter: int) -> Optional[Dict[str, Any]]:
        """Get specific quarterly plan"""
        return self.quarterly_plans.get(quarter)
    
    def apply_manual_override(self, override_data: Dict[str, Any]):
        """Apply manual override to yearly scheduler"""
        
        override = ManualOverride(
            timestamp=datetime.now(),
            level='yearly',
            override_data=override_data,
            reason=override_data.get('reason', 'Manual override'),
            approved_by=override_data.get('approved_by', 'System'),
            impact_assessment=self._assess_override_impact(override_data),
            previous_state=self.current_plan.copy() if self.current_plan else {}
        )
        
        self.record_manual_override(override)
        
        # Apply override to current plan
        if self.current_plan:
            self._apply_manual_overrides_to_plan(self.current_plan, override_data)
            # Recalculate quarterly allocations
            for quarter in range(1, 5):
                quarterly_allocation = self.calculate_quarterly_allocation(
                    quarter, self.current_plan, 
                    self.get_temporal_insights('quarterly', 
                        datetime(self.current_plan['year'], (quarter - 1) * 3 + 1, 1))
                )
                self.current_plan['quarterly_allocations'][quarter] = quarterly_allocation
    
    def _assess_override_impact(self, override_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess impact of manual override"""
        
        impact = {
            'capital_change': 0,
            'risk_change': 0,
            'strategy_change': False,
            'overall_impact': 'medium'
        }
        
        if 'total_capital' in override_data and self.current_plan:
            old_capital = self.current_plan.get('total_capital', 0)
            new_capital = override_data['total_capital']
            impact['capital_change'] = (new_capital - old_capital) / old_capital
            
            if abs(impact['capital_change']) > 0.2:
                impact['overall_impact'] = 'high'
            elif abs(impact['capital_change']) < 0.05:
                impact['overall_impact'] = 'low'
        
        if 'strategic_mode' in override_data:
            impact['strategy_change'] = True
            impact['overall_impact'] = 'high'
        
        return impact
