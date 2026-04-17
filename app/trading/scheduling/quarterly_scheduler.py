"""
Quarterly Trade Scheduler

Quarter-level scheduling with dynamic mode switching and temporal intelligence.
Handles strategy selection, risk parameters, and economic event preparation.
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


class QuarterlyScheduler(TemporalScheduler):
    """Quarter-level scheduling with dynamic mode switching"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Mode switching configuration
        self.mode_switching_enabled = config.get('mode_switching', True)
        self.performance_tracking = {}
        self.mode_switch_thresholds = {
            'drawdown_trigger': 0.10,  # Switch to conservative at 10% drawdown
            'profit_trigger': 0.15,     # Switch to aggressive at 15% profit
            'volatility_trigger': 0.25,   # Switch based on volatility regime
            'event_density_trigger': 0.7   # Switch based on economic event density
        }
        
        # Quarterly strategic modes
        self.quarterly_modes = {
            'conservative': {
                'risk_multiplier': 0.7,
                'position_sizing': 'conservative',
                'strategy_focus': 'defensive',
                'max_positions': 12
            },
            'balanced': {
                'risk_multiplier': 1.0,
                'position_sizing': 'standard',
                'strategy_focus': 'diversified',
                'max_positions': 18
            },
            'aggressive': {
                'risk_multiplier': 1.3,
                'position_sizing': 'aggressive',
                'strategy_focus': 'opportunistic',
                'max_positions': 25
            },
            'defensive': {
                'risk_multiplier': 0.6,
                'position_sizing': 'very_conservative',
                'strategy_focus': 'capital_preservation',
                'max_positions': 10
            },
            'cautious': {
                'risk_multiplier': 0.8,
                'position_sizing': 'conservative',
                'strategy_focus': 'risk_aware',
                'max_positions': 15
            }
        }
        
        self.current_mode = config.get('default_quarterly_mode', 'balanced')
        self.quarterly_plans: Dict[int, Dict[str, Any]] = {}
        
        # Initialize temporal components
        self.temporal_analyzer = SchedulingTemporalAnalyzer()
        self.insights_engine = SchedulingInsightsEngine(self.temporal_analyzer)
    
    def create_schedule(self, quarter: int, year: int, yearly_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Create quarterly plan with dynamic adjustments"""
        
        logger.info(f"Creating quarterly schedule for Q{quarter} {year}")
        
        # Select mode dynamically
        market_conditions = self._get_current_market_conditions()
        selected_mode = self.select_quarterly_mode(quarter, market_conditions)
        
        # Base quarterly plan
        base_plan = {
            'quarter': quarter,
            'year': year,
            'selected_mode': selected_mode,
            'base_allocation': yearly_plan['quarterly_allocations'][quarter]['adjusted_allocation'],
            'strategy_weights': yearly_plan['quarterly_allocations'][quarter]['strategy_weights'].copy(),
            'risk_parameters': yearly_plan['quarterly_allocations'][quarter]['risk_parameters'].copy(),
            'temporal_adjustments': {},
            'flexibility_options': self.generate_flexibility_options(quarter, selected_mode),
            'economic_events': [],
            'mode_switch_history': []
        }
        
        # Get quarterly temporal insights
        quarter_start_date = datetime(year, (quarter - 1) * 3 + 1, 1)
        temporal_insights = self.get_temporal_insights('quarterly', quarter_start_date)
        
        # Apply flexibility mode with temporal insights
        decision = self.apply_flexibility_mode(base_plan, temporal_insights.__dict__)
        
        # Add quarterly-specific adjustments
        quarterly_adjustments = self._get_quarterly_temporal_adjustments(quarter, temporal_insights)
        decision.final_decision['temporal_adjustments'].update(quarterly_adjustments)
        
        # Calculate dynamic strategy weights
        dynamic_strategy_weights = self.calculate_dynamic_strategy_weights(quarter, selected_mode, temporal_insights)
        decision.final_decision['strategy_weights'] = dynamic_strategy_weights
        
        # Adjust risk parameters for mode
        mode_risk_params = self.get_mode_risk_parameters(selected_mode)
        decision.final_decision['risk_parameters'].update(mode_risk_params)
        
        # Store quarterly plan
        self.quarterly_plans[quarter] = decision.final_decision
        
        logger.info(f"Quarterly plan created for Q{quarter}: "
                   f"{selected_mode} mode, "
                   f"${decision.final_decision['base_allocation']:,.0f} allocation")
        
        return {
            'plan': decision.final_decision,
            'decision': decision,
            'temporal_insights': temporal_insights.__dict__
        }
    
    def select_quarterly_mode(self, quarter: int, market_conditions: Dict) -> str:
        """Dynamically select strategy mode for quarter"""
        
        if not self.mode_switching_enabled:
            return self.current_mode
        
        # Check performance-based triggers
        current_performance = self.performance_tracking.get('current_quarter_performance', 0)
        
        if current_performance < -self.mode_switch_thresholds['drawdown_trigger']:
            return 'conservative'
        elif current_performance > self.mode_switch_thresholds['profit_trigger']:
            return 'aggressive'
        
        # Check volatility-based triggers
        vix_percentile = market_conditions.get('vix_percentile', 0.5)
        if vix_percentile > self.mode_switch_thresholds['volatility_trigger']:
            return 'defensive'  # New mode for high volatility
        
        # Check economic event density
        event_density = market_conditions.get('economic_event_density', 0)
        if event_density > self.mode_switch_thresholds['event_density_trigger']:
            return 'cautious'  # High event density
        
        # Check market sentiment
        sentiment = market_conditions.get('market_sentiment', 0.0)
        if sentiment < -0.5:
            return 'defensive'
        elif sentiment > 0.7:
            return 'aggressive'
        
        return self.current_mode
    
    def calculate_dynamic_strategy_weights(self, quarter: int, mode: str, insights) -> Dict[str, float]:
        """Calculate dynamic strategy weights based on mode and conditions"""
        
        # Base weights for different modes
        mode_weights = {
            'conservative': {
                'realness_repricer': 0.30,
                'narrative_lag': 0.25,
                'silent_compounder': 0.20,
                'temporal_correlation': 0.15,
                'volatility_breakout': 0.05,
                'sniper_coil': 0.05
            },
            'balanced': {
                'volatility_breakout': 0.25,
                'silent_compounder': 0.20,
                'realness_repricer': 0.15,
                'temporal_correlation': 0.15,
                'narrative_lag': 0.15,
                'sniper_coil': 0.10
            },
            'aggressive': {
                'volatility_breakout': 0.35,
                'sniper_coil': 0.25,
                'temporal_correlation': 0.20,
                'silent_compounder': 0.10,
                'narrative_lag': 0.05,
                'realness_repricer': 0.05
            },
            'defensive': {
                'realness_repricer': 0.35,
                'narrative_lag': 0.25,
                'temporal_correlation': 0.20,
                'silent_compounder': 0.15,
                'volatility_breakout': 0.03,
                'sniper_coil': 0.02
            },
            'cautious': {
                'temporal_correlation': 0.30,
                'realness_repricer': 0.25,
                'silent_compounder': 0.20,
                'narrative_lag': 0.15,
                'volatility_breakout': 0.05,
                'sniper_coil': 0.05
            }
        }
        
        base_weights = mode_weights.get(mode, mode_weights['balanced']).copy()
        
        # Adjust based on volatility regime
        if insights.volatility_regime == 'expansion':
            base_weights['volatility_breakout'] *= 2.0
            base_weights['sniper_coil'] *= 1.5
            base_weights['temporal_correlation'] *= 1.3
            base_weights['silent_compounder'] *= 0.5
        elif insights.volatility_regime == 'high':
            base_weights['sniper_coil'] *= 1.8
            base_weights['temporal_correlation'] *= 1.4
            base_weights['volatility_breakout'] *= 0.7
        elif insights.volatility_regime == 'low':
            base_weights['silent_compounder'] *= 1.6
            base_weights['realness_repricer'] *= 1.3
            base_weights['sniper_coil'] *= 0.4
        
        # Adjust based on sentiment
        if insights.sentiment_score > 0.5:
            base_weights['silent_compounder'] *= 1.4
            base_weights['volatility_breakout'] *= 1.2
            base_weights['realness_repricer'] *= 0.8
        elif insights.sentiment_score < -0.3:
            base_weights['sniper_coil'] *= 1.5
            base_weights['realness_repricer'] *= 1.3
            base_weights['temporal_correlation'] *= 1.2
            base_weights['silent_compounder'] *= 0.7
        
        # Adjust based on economic events
        high_impact_count = len([e for e in insights.economic_events if e.get('impact') == 'high'])
        if high_impact_count > 0:
            base_weights['temporal_correlation'] *= (1 + 0.2 * high_impact_count)
            base_weights['volatility_breakout'] *= 0.8
        
        # Normalize weights
        total_weight = sum(base_weights.values())
        normalized_weights = {k: v / total_weight for k, v in base_weights.items()}
        
        return normalized_weights
    
    def get_mode_risk_parameters(self, mode: str) -> Dict[str, Any]:
        """Get risk parameters for specific mode"""
        return self.quarterly_modes.get(mode, self.quarterly_modes['balanced']).copy()
    
    def _get_quarterly_temporal_adjustments(self, quarter: int, insights) -> Dict[str, Any]:
        """Get quarterly-specific temporal adjustments"""
        
        adjustments = {}
        
        # Seasonal adjustments
        seasonal_patterns = {
            1: {'multiplier': 1.05, 'risk_adjustment': 1.0},   # Q1
            2: {'multiplier': 0.95, 'risk_adjustment': 1.1},   # Q2 (weak)
            3: {'multiplier': 1.0, 'risk_adjustment': 1.0},    # Q3
            4: {'multiplier': 1.15, 'risk_adjustment': 0.9}    # Q4 (strong)
        }
        
        if quarter in seasonal_patterns:
            adjustments['seasonal'] = seasonal_patterns[quarter]
        
        # Economic event adjustments
        high_impact_events = [e for e in insights.economic_events if e.get('impact') == 'high']
        if high_impact_events:
            adjustments['economic_events'] = {
                'high_impact_count': len(high_impact_events),
                'risk_multiplier': 0.9,  # Reduce risk during high event density
                'position_size_multiplier': 0.8
            }
        
        # Volatility regime adjustments
        vol_adjustments = {
            'low': {'position_multiplier': 1.2, 'stop_multiplier': 1.0},
            'normal': {'position_multiplier': 1.0, 'stop_multiplier': 1.1},
            'high': {'position_multiplier': 0.7, 'stop_multiplier': 1.3},
            'expansion': {'position_multiplier': 1.1, 'stop_multiplier': 1.2}
        }
        
        if insights.volatility_regime in vol_adjustments:
            adjustments['volatility'] = vol_adjustments[insights.volatility_regime]
        
        return adjustments
    
    def generate_flexibility_options(self, quarter: int, mode: str) -> Dict[str, Any]:
        """Generate flexibility options for quarter"""
        
        return {
            'mode_options': {
                'conservative': {'risk_reduction': 0.3, 'expected_return_reduction': 0.4},
                'balanced': {'risk_adjustment': 0.0, 'expected_return_adjustment': 0.0},
                'aggressive': {'risk_increase': 0.3, 'expected_return_increase': 0.4},
                'defensive': {'risk_reduction': 0.4, 'expected_return_reduction': 0.5},
                'cautious': {'risk_reduction': 0.2, 'expected_return_reduction': 0.2}
            },
            'strategy_focus_options': {
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
                },
                'balanced_focus': {
                    'volatility_breakout': 0.25,
                    'silent_compounder': 0.20,
                    'realness_repricer': 0.15,
                    'temporal_correlation': 0.15,
                    'narrative_lag': 0.15,
                    'sniper_coil': 0.10
                }
            },
            'risk_management_options': {
                'position_sizing': ['conservative', 'standard', 'aggressive', 'very_conservative'],
                'stop_loss_types': ['fixed', 'atr_based', 'volatility_based', 'trailing'],
                'portfolio_heat_limits': [0.08, 0.12, 0.15, 0.20]
            }
        }
    
    def _get_current_market_conditions(self) -> Dict[str, Any]:
        """Get current market conditions for mode selection"""
        
        # This would integrate with real-time market data
        # For now, return mock data
        return {
            'vix_percentile': 0.6,
            'market_sentiment': 0.2,
            'economic_event_density': 0.4,
            'volatility_regime': 'normal'
        }
    
    def update_performance_tracking(self, performance_data: Dict[str, float]):
        """Update performance tracking for mode switching"""
        self.performance_tracking.update(performance_data)
        
        # Check if mode switch is needed
        if 'current_quarter_performance' in performance_data:
            current_performance = performance_data['current_quarter_performance']
            
            # Trigger mode switch if thresholds are crossed
            if self.mode_switching_enabled:
                market_conditions = self._get_current_market_conditions()
                new_mode = self.select_quarterly_mode(1, market_conditions)  # Use quarter 1 for check
                
                if new_mode != self.current_mode:
                    self.switch_quarterly_mode(new_mode, f"Performance trigger: {current_performance:.2%}")
    
    def switch_quarterly_mode(self, new_mode: str, reason: str):
        """Switch quarterly mode with full tracking"""
        
        old_mode = self.current_mode
        self.current_mode = new_mode
        
        # Record mode switch
        switch_record = {
            'timestamp': datetime.now(),
            'previous_mode': old_mode,
            'new_mode': new_mode,
            'reason': reason
        }
        
        logger.info(f"Quarterly mode switched: {old_mode} -> {new_mode} - {reason}")
        
        # Update existing quarterly plans if any
        for quarter, plan in self.quarterly_plans.items():
            plan['mode_switch_history'].append(switch_record)
    
    def get_quarterly_plan(self, quarter: int) -> Optional[Dict[str, Any]]:
        """Get specific quarterly plan"""
        return self.quarterly_plans.get(quarter)
    
    def apply_manual_override(self, override_data: Dict[str, Any]):
        """Apply manual override to quarterly scheduler"""
        
        override = ManualOverride(
            timestamp=datetime.now(),
            level='quarterly',
            override_data=override_data,
            reason=override_data.get('reason', 'Manual override'),
            approved_by=override_data.get('approved_by', 'System'),
            impact_assessment=self._assess_override_impact(override_data),
            previous_state={'current_mode': self.current_mode}
        )
        
        self.record_manual_override(override)
        
        # Apply override
        if 'mode' in override_data:
            self.switch_quarterly_mode(override_data['mode'], override_data.get('reason', 'Manual override'))
        
        if 'performance_tracking' in override_data:
            self.update_performance_tracking(override_data['performance_tracking'])
    
    def _assess_override_impact(self, override_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess impact of manual override"""
        
        impact = {
            'mode_change': False,
            'performance_change': False,
            'overall_impact': 'medium'
        }
        
        if 'mode' in override_data and override_data['mode'] != self.current_mode:
            impact['mode_change'] = True
            impact['overall_impact'] = 'high'
        
        if 'performance_tracking' in override_data:
            impact['performance_change'] = True
            impact['overall_impact'] = 'medium'
        
        return impact
