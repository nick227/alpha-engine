"""
Monthly Trade Scheduler

Monthly scheduling with granular control options and temporal intelligence.
Handles signal budgeting, risk limits, and execution constraints.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

from .temporal_scheduler import (
    TemporalScheduler,
    ControlMode,
    FlexibilityMode,
    SchedulingDecision,
    ManualOverride
)
from .temporal_correlation_integration import SchedulingTemporalAnalyzer, SchedulingInsightsEngine

logger = logging.getLogger(__name__)


class MonthlyScheduler(TemporalScheduler):
    """Monthly scheduling with granular control options"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Control modes for monthly scheduling
        self.control_modes = {
            ControlMode.FULL_AUTOMATIC: {
                'manual_override': False, 
                'auto_adjust': True,
                'approval_required': False
            },
            ControlMode.SEMI_AUTOMATIC: {
                'manual_override': True, 
                'auto_adjust': True,
                'approval_required': False
            },
            ControlMode.MANUAL_APPROVAL: {
                'manual_override': True, 
                'auto_adjust': False,
                'approval_required': True
            },
            ControlMode.MONITOR_ONLY: {
                'manual_override': True, 
                'auto_adjust': False,
                'approval_required': True
            }
        }
        
        self.current_control_mode = ControlMode(config.get('monthly_control_mode', 'semi_automatic'))
        self.monthly_plans: Dict[int, Dict[str, Any]] = {}
        
        # Monthly patterns and parameters
        self.monthly_patterns = {
            1: {"budget_multiplier": 1.1, "risk_multiplier": 1.0, "max_positions": 18},   # January
            2: {"budget_multiplier": 0.9, "risk_multiplier": 1.2, "max_positions": 15},   # February
            3: {"budget_multiplier": 1.0, "risk_multiplier": 1.0, "max_positions": 17},   # March
            4: {"budget_multiplier": 0.8, "risk_multiplier": 1.3, "max_positions": 12},   # April (weak)
            5: {"budget_multiplier": 0.8, "risk_multiplier": 1.3, "max_positions": 12},   # May (weak)
            6: {"budget_multiplier": 1.0, "risk_multiplier": 1.0, "max_positions": 16},   # June
            7: {"budget_multiplier": 1.0, "risk_multiplier": 1.0, "max_positions": 16},   # July
            8: {"budget_multiplier": 1.0, "risk_multiplier": 1.0, "max_positions": 16},   # August
            9: {"budget_multiplier": 1.0, "risk_multiplier": 1.0, "max_positions": 16},   # September
            10: {"budget_multiplier": 1.0, "risk_multiplier": 1.0, "max_positions": 16},  # October
            11: {"budget_multiplier": 1.2, "risk_multiplier": 0.9, "max_positions": 20},  # November (strong)
            12: {"budget_multiplier": 1.3, "risk_multiplier": 0.8, "max_positions": 22}   # December (strong)
        }
        
        # Initialize temporal components
        self.temporal_analyzer = SchedulingTemporalAnalyzer()
        self.insights_engine = SchedulingInsightsEngine(self.temporal_analyzer)
    
    def create_schedule(self, month: int, year: int, quarterly_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Create monthly plan with granular control options"""
        
        logger.info(f"Creating monthly schedule for {year}-{month:02d} with {self.current_control_mode.value} mode")
        
        base_monthly_allocation = quarterly_plan['base_allocation'] / 3
        
        # Get monthly temporal insights
        month_date = datetime(year, month, 1)
        temporal_insights = self.get_temporal_insights('monthly', month_date)
        
        # Base monthly plan
        base_plan = {
            'month': month,
            'year': year,
            'base_allocation': base_monthly_allocation,
            'control_mode': self.current_control_mode.value,
            'temporal_adjustments': {},
            'strategy_preferences': self.get_monthly_strategy_preferences(month, temporal_insights),
            'risk_limits': self.calculate_monthly_risk_limits(month, temporal_insights),
            'execution_constraints': self.get_execution_constraints(month),
            'flexibility_options': self.generate_monthly_flexibility_options(),
            'approval_status': 'pending' if self.current_control_mode == ControlMode.MANUAL_APPROVAL else 'approved'
        }
        
        # Apply flexibility mode with temporal insights
        decision = self.apply_flexibility_mode(base_plan, temporal_insights.__dict__)
        
        # Apply control mode logic
        if self.current_control_mode == ControlMode.FULL_AUTOMATIC:
            decision.final_decision['final_allocation'] = self.apply_automatic_adjustments(decision.final_decision)
        elif self.current_control_mode == ControlMode.MANUAL_APPROVAL:
            decision.final_decision['final_allocation'] = base_monthly_allocation  # Wait for approval
            decision.final_decision['approval_status'] = 'pending'
        elif self.current_control_mode == ControlMode.MONITOR_ONLY:
            decision.final_decision['final_allocation'] = 0  # No trading
            decision.final_decision['approval_status'] = 'monitor_only'
        else:  # SEMI_AUTOMATIC
            decision.final_decision['final_allocation'] = self.apply_semi_automatic_adjustments(decision.final_decision)
        
        # Add monthly-specific temporal adjustments
        monthly_adjustments = self._get_monthly_temporal_adjustments(month, temporal_insights)
        decision.final_decision['temporal_adjustments'].update(monthly_adjustments)
        
        # Store monthly plan
        self.monthly_plans[month] = decision.final_decision
        
        logger.info(f"Monthly plan created for {year}-{month:02d}: "
                   f"${decision.final_decision['final_allocation']:,.0f} allocation, "
                   f"{decision.final_decision['approval_status']} status")
        
        return {
            'plan': decision.final_decision,
            'decision': decision,
            'temporal_insights': temporal_insights.__dict__
        }
    
    def apply_automatic_adjustments(self, plan: Dict[str, Any]) -> float:
        """Apply automatic adjustments to monthly allocation"""
        
        base_allocation = plan['base_allocation']
        monthly_patterns = self.monthly_patterns[plan['month']]
        
        # Apply seasonal multiplier
        seasonal_adjusted = base_allocation * monthly_patterns['budget_multiplier']
        
        # Apply temporal adjustments
        temporal_multiplier = 1.0
        
        if 'temporal_sentiment' in plan.get('temporal_adjustments', {}):
            sentiment_adj = plan['temporal_adjustments']['temporal_sentiment']
            temporal_multiplier *= sentiment_adj
        
        if 'temporal_volatility' in plan.get('temporal_adjustments', {}):
            vol_adj = plan['temporal_adjustments']['temporal_volatility']
            temporal_multiplier *= vol_adj
        
        final_allocation = seasonal_adjusted * temporal_multiplier
        
        # Apply risk limits
        max_allocation = base_allocation * 1.5  # Max 50% increase
        min_allocation = base_allocation * 0.3  # Min 70% reduction
        
        return max(min(final_allocation, min_allocation), max_allocation)
    
    def apply_semi_automatic_adjustments(self, plan: Dict[str, Any]) -> float:
        """Apply semi-automatic adjustments with manual override capability"""
        
        # Start with automatic adjustments
        auto_allocation = self.apply_automatic_adjustments(plan)
        
        # Check for manual overrides
        manual_overrides = plan.get('manual_overrides', {})
        
        if 'allocation_multiplier' in manual_overrides:
            auto_allocation *= manual_overrides['allocation_multiplier']
        
        if 'fixed_allocation' in manual_overrides:
            auto_allocation = manual_overrides['fixed_allocation']
        
        return auto_allocation
    
    def get_monthly_strategy_preferences(self, month: int, insights) -> Dict[str, float]:
        """Get strategy preferences for specific month"""
        
        # Base monthly preferences
        monthly_preferences = {
            1: {'volatility_breakout': 0.25, 'silent_compounder': 0.25, 'temporal_correlation': 0.20},  # January
            2: {'realness_repricer': 0.30, 'narrative_lag': 0.25, 'temporal_correlation': 0.20},  # February
            3: {'volatility_breakout': 0.20, 'silent_compounder': 0.20, 'balanced': 0.60},  # March
            4: {'realness_repricer': 0.35, 'narrative_lag': 0.30, 'temporal_correlation': 0.20},  # April (weak)
            5: {'realness_repricer': 0.35, 'narrative_lag': 0.30, 'temporal_correlation': 0.20},  # May (weak)
            6: {'volatility_breakout': 0.25, 'silent_compounder': 0.20, 'balanced': 0.55},  # June
            7: {'volatility_breakout': 0.25, 'silent_compounder': 0.20, 'balanced': 0.55},  # July
            8: {'volatility_breakout': 0.25, 'silent_compounder': 0.20, 'balanced': 0.55},  # August
            9: {'volatility_breakout': 0.25, 'silent_compounder': 0.20, 'balanced': 0.55},  # September
            10: {'volatility_breakout': 0.25, 'silent_compounder': 0.20, 'balanced': 0.55},  # October
            11: {'silent_compounder': 0.30, 'volatility_breakout': 0.25, 'temporal_correlation': 0.25},  # November (strong)
            12: {'silent_compounder': 0.30, 'volatility_breakout': 0.25, 'temporal_correlation': 0.25}   # December (strong)
        }
        
        base_preferences = monthly_preferences.get(month, {
            'volatility_breakout': 0.20, 'silent_compounder': 0.20, 'balanced': 0.60
        }).copy()
        
        # Adjust based on temporal insights
        if insights.volatility_regime == 'expansion':
            base_preferences['volatility_breakout'] *= 1.5
            base_preferences['temporal_correlation'] *= 1.3
        elif insights.volatility_regime == 'high':
            base_preferences['temporal_correlation'] *= 1.4
            base_preferences['realness_repricer'] *= 1.2
        
        if insights.sentiment_score > 0.5:
            base_preferences['silent_compounder'] *= 1.3
        elif insights.sentiment_score < -0.3:
            base_preferences['realness_repricer'] *= 1.4
            base_preferences['temporal_correlation'] *= 1.2
        
        # Normalize preferences
        total_weight = sum(base_preferences.values())
        normalized_preferences = {k: v / total_weight for k, v in base_preferences.items()}
        
        return normalized_preferences
    
    def calculate_monthly_risk_limits(self, month: int, insights) -> Dict[str, Any]:
        """Calculate monthly risk limits"""
        
        monthly_patterns = self.monthly_patterns[month]
        
        base_limits = {
            'max_positions': monthly_patterns['max_positions'],
            'max_position_size': 0.05,  # 5% of allocation per position
            'max_portfolio_heat': 0.15,
            'max_daily_loss': 0.03,
            'max_drawdown': 0.12
        }
        
        # Adjust based on volatility regime
        if insights.volatility_regime == 'high':
            base_limits['max_portfolio_heat'] *= 0.7
            base_limits['max_daily_loss'] *= 0.7
            base_limits['max_drawdown'] *= 0.8
        elif insights.volatility_regime == 'low':
            base_limits['max_portfolio_heat'] *= 1.1
            base_limits['max_daily_loss'] *= 1.1
        
        # Adjust based on economic events
        high_impact_count = len([e for e in insights.economic_events if e.get('impact') == 'high'])
        if high_impact_count > 2:
            base_limits['max_portfolio_heat'] *= 0.8
            base_limits['max_daily_loss'] *= 0.8
        
        # Apply risk multiplier
        risk_multiplier = monthly_patterns['risk_multiplier']
        for key in ['max_portfolio_heat', 'max_daily_loss', 'max_drawdown']:
            base_limits[key] *= risk_multiplier
        
        return base_limits
    
    def get_execution_constraints(self, month: int) -> Dict[str, Any]:
        """Get execution constraints for month"""
        
        monthly_patterns = self.monthly_patterns[month]
        
        constraints = {
            'max_trades_per_day': 5,
            'max_trades_per_week': 15,
            'min_time_between_trades': 300,  # 5 minutes
            'execution_windows': ['market_open', 'mid_day', 'market_close'],
            'blackout_periods': [],  # Specific times to avoid trading
            'position_holding_period_min': 1,  # days
            'position_holding_period_max': 30  # days
        }
        
        # Adjust based on month strength
        if monthly_patterns['budget_multiplier'] > 1.1:  # Strong month
            constraints['max_trades_per_day'] = 8
            constraints['max_trades_per_week'] = 25
        elif monthly_patterns['budget_multiplier'] < 0.9:  # Weak month
            constraints['max_trades_per_day'] = 3
            constraints['max_trades_per_week'] = 10
        
        return constraints
    
    def generate_monthly_flexibility_options(self) -> Dict[str, Any]:
        """Generate comprehensive flexibility options for month"""
        
        return {
            'allocation_options': {
                'conservative': {'multiplier': 0.7, 'max_positions': 12},
                'standard': {'multiplier': 1.0, 'max_positions': 16},
                'aggressive': {'multiplier': 1.3, 'max_positions': 22}
            },
            'risk_management_options': {
                'static_stops': {
                    'use_fixed_stops': True, 
                    'atr_multiplier': 1.5,
                    'stop_loss_pct': 0.02
                },
                'dynamic_stops': {
                    'use_volatility_stops': True, 
                    'vix_adjustment': True,
                    'min_stop_pct': 0.015,
                    'max_stop_pct': 0.04
                },
                'trailing_stops': {
                    'use_trailing': True, 
                    'trail_percent': 0.02,
                    'activation_profit': 0.01
                }
            },
            'execution_options': {
                'immediate_execution': {
                    'delay_minutes': 0,
                    'batch_size': 1,
                    'market_impact': 'high'
                },
                'staggered_execution': {
                    'delay_minutes': 15,
                    'batch_size': 3,
                    'market_impact': 'medium'
                },
                'optimal_timing': {
                    'use_vix_timing': True,
                    'use_sentiment_timing': True,
                    'use_economic_timing': True,
                    'market_impact': 'low'
                }
            },
            'override_options': {
                'manual_allocation': {
                    'allow_manual': True,
                    'max_override': 0.5,
                    'requires_approval': False
                },
                'emergency_stop': {
                    'allow_emergency_stop': True,
                    'trigger_conditions': ['vix_spike', 'drawdown', 'system_error'],
                    'auto_activate': True
                },
                'strategy_override': {
                    'allow_manual_strategy_selection': True,
                    'max_strategy_override': 0.3,
                    'requires_rationale': True
                }
            }
        }
    
    def _get_monthly_temporal_adjustments(self, month: int, insights) -> Dict[str, Any]:
        """Get monthly-specific temporal adjustments"""
        
        adjustments = {}
        
        # Monthly pattern adjustments
        monthly_patterns = self.monthly_patterns[month]
        adjustments['seasonal_multiplier'] = monthly_patterns['budget_multiplier']
        adjustments['risk_multiplier'] = monthly_patterns['risk_multiplier']
        
        # Sentiment-based adjustments
        if insights.sentiment_score > 0.7:
            adjustments['sentiment_adjustment'] = 1.2
        elif insights.sentiment_score < -0.3:
            adjustments['sentiment_adjustment'] = 0.8
        
        # Volatility regime adjustments
        vol_adjustments = {
            'low': {'position_multiplier': 1.2, 'stop_multiplier': 0.9},
            'normal': {'position_multiplier': 1.0, 'stop_multiplier': 1.0},
            'high': {'position_multiplier': 0.7, 'stop_multiplier': 1.3},
            'expansion': {'position_multiplier': 1.1, 'stop_multiplier': 1.2}
        }
        
        if insights.volatility_regime in vol_adjustments:
            adjustments['volatility_adjustment'] = vol_adjustments[insights.volatility_regime]
        
        # Economic event adjustments
        high_impact_events = [e for e in insights.economic_events if e.get('impact') == 'high']
        if high_impact_events:
            adjustments['economic_events'] = {
                'high_impact_count': len(high_impact_events),
                'position_size_reduction': 0.8,
                'risk_increase': 1.2,
                'execution_delay': 30  # minutes
            }
        
        return adjustments
    
    def approve_monthly_plan(self, month: int, approved_by: str, allocation_override: Optional[float] = None):
        """Approve monthly plan (for MANUAL_APPROVAL mode)"""
        
        if month in self.monthly_plans:
            plan = self.monthly_plans[month]
            plan['approval_status'] = 'approved'
            plan['approved_by'] = approved_by
            plan['approved_at'] = datetime.now()
            
            if allocation_override:
                plan['final_allocation'] = allocation_override
                plan['allocation_override'] = allocation_override
            
            logger.info(f"Monthly plan approved for {plan['year']}-{month:02d} by {approved_by}")
    
    def switch_control_mode(self, new_mode: ControlMode, reason: str):
        """Switch control mode with full tracking"""
        
        old_mode = self.current_control_mode
        self.current_control_mode = new_mode
        
        logger.info(f"Monthly control mode switched: {old_mode.value} -> {new_mode.value} - {reason}")
    
    def get_monthly_plan(self, month: int) -> Optional[Dict[str, Any]]:
        """Get specific monthly plan"""
        return self.monthly_plans.get(month)
    
    def apply_manual_override(self, override_data: Dict[str, Any]):
        """Apply manual override to monthly scheduler"""
        
        override = ManualOverride(
            timestamp=datetime.now(),
            level='monthly',
            override_data=override_data,
            reason=override_data.get('reason', 'Manual override'),
            approved_by=override_data.get('approved_by', 'System'),
            impact_assessment=self._assess_override_impact(override_data),
            previous_state={'current_control_mode': self.current_control_mode}
        )
        
        self.record_manual_override(override)
        
        # Apply override
        if 'control_mode' in override_data:
            try:
                new_mode = ControlMode(override_data['control_mode'])
                self.switch_control_mode(new_mode, override_data.get('reason', 'Manual override'))
            except ValueError:
                logger.error(f"Invalid control mode: {override_data['control_mode']}")
        
        if 'approve_month' in override_data:
            month = override_data['approve_month']
            allocation = override_data.get('allocation_override')
            self.approve_monthly_plan(month, override_data.get('approved_by', 'System'), allocation)
    
    def _assess_override_impact(self, override_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess impact of manual override"""
        
        impact = {
            'control_mode_change': False,
            'allocation_change': False,
            'approval_change': False,
            'overall_impact': 'medium'
        }
        
        if 'control_mode' in override_data:
            new_mode = override_data['control_mode']
            if new_mode != self.current_control_mode.value:
                impact['control_mode_change'] = True
                impact['overall_impact'] = 'high'
        
        if 'approve_month' in override_data:
            impact['approval_change'] = True
            impact['overall_impact'] = 'high'
        
        if 'allocation_override' in override_data:
            impact['allocation_change'] = True
            impact['overall_impact'] = 'medium'
        
        return impact
