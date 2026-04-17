"""
Scheduling Dashboard

Comprehensive management interface for trade scheduling system.
Provides real-time oversight, control, and performance tracking.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

from .temporal_scheduler import (
    TemporalScheduler,
    FlexibilityMode,
    StrategicMode,
    ControlMode,
    ExecutionMode,
    SchedulingMetrics
)
from .yearly_scheduler import YearlyScheduler
from .quarterly_scheduler import QuarterlyScheduler
from .monthly_scheduler import MonthlyScheduler
from .weekly_scheduler import WeeklyScheduler
from .daily_scheduler import DailyScheduler

logger = logging.getLogger(__name__)


class SchedulingDashboard:
    """Comprehensive management interface for trade scheduling"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Initialize all schedulers
        self.schedulers = {
            'yearly': YearlyScheduler(config.get('yearly', {})),
            'quarterly': QuarterlyScheduler(config.get('quarterly', {})),
            'monthly': MonthlyScheduler(config.get('monthly', {})),
            'weekly': WeeklyScheduler(config.get('weekly', {})),
            'daily': DailyScheduler(config.get('daily', {}))
        }
        
        # Current plans and schedules
        self.current_plans = {}
        self.performance_metrics = SchedulingMetrics()
        
        # Dashboard state
        self.dashboard_state = {
            'last_update': datetime.now(),
            'active_overrides': [],
            'pending_approvals': [],
            'alert_conditions': [],
            'system_health': 'operational'
        }
        
        # Initialize with current date
        self._initialize_current_plans()
    
    def _initialize_current_plans(self):
        """Initialize current plans with default values"""
        
        current_date = datetime.now()
        current_year = current_date.year
        current_quarter = (current_date.month - 1) // 3 + 1
        current_month = current_date.month
        current_week = self._get_week_number(current_date)
        
        # Create default plans
        try:
            yearly_result = self.schedulers['yearly'].create_schedule(current_year)
            self.current_plans['yearly'] = yearly_result['plan']
            
            quarterly_result = self.schedulers['quarterly'].create_schedule(
                current_quarter, current_year, yearly_result['plan']
            )
            self.current_plans['quarterly'] = quarterly_result['plan']
            
            monthly_result = self.schedulers['monthly'].create_schedule(
                current_month, current_year, quarterly_result['plan']
            )
            self.current_plans['monthly'] = monthly_result['plan']
            
            weekly_result = self.schedulers['weekly'].create_schedule(
                current_week, current_year, monthly_result['plan']
            )
            self.current_plans['weekly'] = weekly_result['plan']
            
            daily_result = self.schedulers['daily'].create_schedule(
                current_date, weekly_result['plan'].get('allocated_signals', [])
            )
            self.current_plans['daily'] = daily_result['plan']
            
        except Exception as e:
            logger.error(f"Error initializing current plans: {e}")
            self.dashboard_state['system_health'] = 'error'
    
    def get_scheduling_overview(self) -> Dict[str, Any]:
        """Get complete overview of current scheduling"""
        
        overview = {
            'timestamp': datetime.now(),
            'system_health': self.dashboard_state['system_health'],
            'current_plans': self.current_plans,
            'scheduler_states': self._get_scheduler_states(),
            'temporal_insights': self.get_all_temporal_insights(),
            'performance_vs_plan': self.calculate_performance_vs_plan(),
            'flexibility_status': self.get_flexibility_status(),
            'override_history': self.get_override_history(),
            'pending_actions': self.get_pending_actions(),
            'alerts': self.get_active_alerts(),
            'metrics_summary': self.performance_metrics.get_metrics_summary()
        }
        
        return overview
    
    def _get_scheduler_states(self) -> Dict[str, Any]:
        """Get current states of all schedulers"""
        
        states = {}
        
        for level, scheduler in self.schedulers.items():
            state = {
                'flexibility_mode': scheduler.flexibility_mode.value if hasattr(scheduler, 'flexibility_mode') else None,
                'current_mode': getattr(scheduler, 'current_mode', None),
                'current_control_mode': getattr(scheduler, 'current_control_mode', None),
                'current_execution_mode': getattr(scheduler, 'current_execution_mode', None),
                'current_method': getattr(scheduler, 'current_method', None),
                'decision_count': len(scheduler.decision_history) if hasattr(scheduler, 'decision_history') else 0,
                'override_count': len(scheduler.manual_overrides) if hasattr(scheduler, 'manual_overrides') else 0,
                'last_decision': scheduler.decision_history[-1] if hasattr(scheduler, 'decision_history') and scheduler.decision_history else None
            }
            states[level] = state
        
        return states
    
    def get_all_temporal_insights(self) -> Dict[str, Any]:
        """Get temporal insights from all schedulers"""
        
        insights = {}
        current_date = datetime.now()
        
        # Get insights from each level
        for level, scheduler in self.schedulers.items():
            try:
                if hasattr(scheduler, 'get_temporal_insights'):
                    period_type = level.replace('ly', '')  # yearly -> year, etc.
                    insight = scheduler.get_temporal_insights(period_type, current_date)
                    insights[level] = insight.__dict__ if hasattr(insight, '__dict__') else insight
            except Exception as e:
                logger.error(f"Error getting temporal insights for {level}: {e}")
                insights[level] = {'error': str(e)}
        
        return insights
    
    def calculate_performance_vs_plan(self) -> Dict[str, Any]:
        """Calculate performance vs planned targets"""
        
        performance = {
            'yearly': self._calculate_yearly_performance(),
            'quarterly': self._calculate_quarterly_performance(),
            'monthly': self._calculate_monthly_performance(),
            'weekly': self._calculate_weekly_performance(),
            'daily': self._calculate_daily_performance()
        }
        
        return performance
    
    def _calculate_yearly_performance(self) -> Dict[str, Any]:
        """Calculate yearly performance vs plan"""
        
        if 'yearly' not in self.current_plans:
            return {'status': 'no_plan'}
        
        plan = self.current_plans['yearly']
        
        # Mock performance data - would integrate with actual trading system
        actual_performance = {
            'total_return': 0.08,  # 8% YTD
            'risk_adjusted_return': 0.12,
            'max_drawdown': 0.06,
            'win_rate': 0.65,
            'trades_executed': 145
        }
        
        planned_return = 0.15  # From strategic mode
        performance_ratio = actual_performance['total_return'] / planned_return
        
        return {
            'planned_return': planned_return,
            'actual_return': actual_performance['total_return'],
            'performance_ratio': performance_ratio,
            'status': 'ahead' if performance_ratio > 1.0 else 'behind',
            'variance': performance_ratio - 1.0,
            'details': actual_performance
        }
    
    def _calculate_quarterly_performance(self) -> Dict[str, Any]:
        """Calculate quarterly performance vs plan"""
        
        if 'quarterly' not in self.current_plans:
            return {'status': 'no_plan'}
        
        plan = self.current_plans['quarterly']
        
        # Mock quarterly performance
        actual_performance = {
            'total_return': 0.03,  # 3% QTD
            'risk_adjusted_return': 0.05,
            'max_drawdown': 0.04,
            'win_rate': 0.62,
            'trades_executed': 38
        }
        
        planned_allocation = plan.get('base_allocation', 100000)
        actual_utilization = 85000  # Actual capital used
        
        utilization_ratio = actual_utilization / planned_allocation
        
        return {
            'planned_allocation': planned_allocation,
            'actual_utilization': actual_utilization,
            'utilization_ratio': utilization_ratio,
            'return_details': actual_performance,
            'status': 'on_track' if 0.8 <= utilization_ratio <= 1.0 else 'off_track'
        }
    
    def _calculate_monthly_performance(self) -> Dict[str, Any]:
        """Calculate monthly performance vs plan"""
        
        if 'monthly' not in self.current_plans:
            return {'status': 'no_plan'}
        
        plan = self.current_plans['monthly']
        
        # Mock monthly performance
        actual_performance = {
            'total_return': 0.01,  # 1% MTD
            'risk_adjusted_return': 0.02,
            'max_drawdown': 0.02,
            'win_rate': 0.70,
            'trades_executed': 12
        }
        
        planned_allocation = plan.get('final_allocation', 50000)
        actual_utilization = 45000
        
        return {
            'planned_allocation': planned_allocation,
            'actual_utilization': actual_utilization,
            'utilization_ratio': actual_utilization / planned_allocation,
            'return_details': actual_performance,
            'approval_status': plan.get('approval_status', 'unknown')
        }
    
    def _calculate_weekly_performance(self) -> Dict[str, Any]:
        """Calculate weekly performance vs plan"""
        
        if 'weekly' not in self.current_plans:
            return {'status': 'no_plan'}
        
        plan = self.current_plans['weekly']
        
        # Mock weekly performance
        actual_performance = {
            'total_return': 0.005,  # 0.5% WTD
            'risk_adjusted_return': 0.008,
            'max_drawdown': 0.01,
            'win_rate': 0.75,
            'trades_executed': 4
        }
        
        planned_budget = plan.get('total_budget', 25000)
        allocated_budget = sum(s.get('allocated_budget', 0) for s in plan.get('allocated_signals', []))
        
        return {
            'planned_budget': planned_budget,
            'allocated_budget': allocated_budget,
            'allocation_ratio': allocated_budget / planned_budget,
            'return_details': actual_performance,
            'signals_processed': len(plan.get('allocated_signals', [])),
            'signals_rejected': len(plan.get('rejected_signals', []))
        }
    
    def _calculate_daily_performance(self) -> Dict[str, Any]:
        """Calculate daily performance vs plan"""
        
        if 'daily' not in self.current_plans:
            return {'status': 'no_plan'}
        
        plan = self.current_plans['daily']
        
        # Mock daily performance
        actual_performance = {
            'total_return': 0.002,  # 0.2% daily
            'risk_adjusted_return': 0.003,
            'max_drawdown': 0.005,
            'win_rate': 0.80,
            'trades_executed': 2
        }
        
        scheduled_trades = plan.get('scheduled_trades', [])
        executed_trades = len([t for t in scheduled_trades if t.get('executed', False)])
        
        return {
            'scheduled_trades': len(scheduled_trades),
            'executed_trades': executed_trades,
            'execution_rate': executed_trades / len(scheduled_trades) if scheduled_trades else 0,
            'return_details': actual_performance,
            'execution_mode': plan.get('execution_mode', 'unknown')
        }
    
    def get_flexibility_status(self) -> Dict[str, Any]:
        """Get current flexibility status across all schedulers"""
        
        flexibility_status = {}
        
        for level, scheduler in self.schedulers.items():
            status = {
                'current_mode': None,
                'available_modes': [],
                'switch_count': 0,
                'last_switch': None,
                'override_count': len(scheduler.manual_overrides) if hasattr(scheduler, 'manual_overrides') else 0
            }
            
            # Get current mode and available modes
            if level == 'yearly':
                status['current_mode'] = getattr(scheduler, 'current_mode', None)
                status['available_modes'] = list(StrategicMode)
            elif level == 'quarterly':
                status['current_mode'] = getattr(scheduler, 'current_mode', None)
                status['available_modes'] = ['conservative', 'balanced', 'aggressive', 'defensive', 'cautious']
            elif level == 'monthly':
                status['current_mode'] = getattr(scheduler, 'current_control_mode', None)
                status['available_modes'] = list(ControlMode)
            elif level == 'weekly':
                status['current_mode'] = getattr(scheduler, 'current_method', None)
                status['available_modes'] = list(scheduler.prioritization_methods.keys())
            elif level == 'daily':
                status['current_mode'] = getattr(scheduler, 'current_execution_mode', None)
                status['available_modes'] = list(ExecutionMode)
            
            flexibility_status[level] = status
        
        return flexibility_status
    
    def get_override_history(self) -> List[Dict[str, Any]]:
        """Get consolidated override history from all schedulers"""
        
        all_overrides = []
        
        for level, scheduler in self.schedulers.items():
            if hasattr(scheduler, 'manual_overrides'):
                overrides = scheduler.get_override_history()
                for override in overrides:
                    override_dict = override.__dict__ if hasattr(override, '__dict__') else override
                    override_dict['scheduler_level'] = level
                    all_overrides.append(override_dict)
        
        # Sort by timestamp
        all_overrides.sort(key=lambda x: x.get('timestamp', datetime.min), reverse=True)
        
        return all_overrides[:20]  # Last 20 overrides
    
    def get_pending_actions(self) -> List[Dict[str, Any]]:
        """Get pending actions requiring attention"""
        
        pending_actions = []
        
        # Check for pending approvals
        if 'monthly' in self.current_plans:
            monthly_plan = self.current_plans['monthly']
            if monthly_plan.get('approval_status') == 'pending':
                pending_actions.append({
                    'type': 'approval_required',
                    'level': 'monthly',
                    'description': f"Monthly plan for {monthly_plan.get('month')} needs approval",
                    'priority': 'high',
                    'timestamp': monthly_plan.get('created_at', datetime.now())
                })
        
        # Check for mode switch recommendations
        for level, scheduler in self.schedulers.items():
            if hasattr(scheduler, 'get_mode_switch_recommendations'):
                recommendations = scheduler.get_mode_switch_recommendations()
                for rec in recommendations:
                    pending_actions.append({
                        'type': 'mode_switch_recommendation',
                        'level': level,
                        'description': rec['description'],
                        'priority': rec['priority'],
                        'timestamp': rec['timestamp']
                    })
        
        # Sort by priority and timestamp
        pending_actions.sort(key=lambda x: (x['priority'], x['timestamp']), reverse=True)
        
        return pending_actions
    
    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get active alerts and warnings"""
        
        alerts = []
        
        # Performance alerts
        performance = self.calculate_performance_vs_plan()
        
        # Yearly performance alert
        if 'yearly' in performance and performance['yearly'].get('status') != 'no_plan':
            yearly_perf = performance['yearly']
            if yearly_perf.get('variance', 0) < -0.2:  # More than 20% behind plan
                alerts.append({
                    'type': 'performance_warning',
                    'level': 'yearly',
                    'description': f"Yearly performance {yearly_perf['variance']:.1%} behind plan",
                    'severity': 'high',
                    'timestamp': datetime.now()
                })
        
        # Utilization alerts
        if 'monthly' in performance and performance['monthly'].get('status') != 'no_plan':
            monthly_perf = performance['monthly']
            if monthly_perf.get('utilization_ratio', 1.0) < 0.5:  # Less than 50% utilization
                alerts.append({
                    'type': 'utilization_warning',
                    'level': 'monthly',
                    'description': f"Monthly utilization only {monthly_perf['utilization_ratio']:.1%}",
                    'severity': 'medium',
                    'timestamp': datetime.now()
                })
        
        # Override frequency alerts
        override_history = self.get_override_history()
        recent_overrides = [o for o in override_history if (datetime.now() - o.get('timestamp', datetime.min)).days <= 7]
        
        if len(recent_overrides) > 10:  # More than 10 overrides in 7 days
            alerts.append({
                'type': 'override_frequency',
                'level': 'system',
                'description': f"High override frequency: {len(recent_overrides)} in last 7 days",
                'severity': 'medium',
                'timestamp': datetime.now()
            })
        
        # System health alerts
        if self.dashboard_state['system_health'] != 'operational':
            alerts.append({
                'type': 'system_health',
                'level': 'system',
                'description': f"System health: {self.dashboard_state['system_health']}",
                'severity': 'high',
                'timestamp': datetime.now()
            })
        
        return alerts
    
    def apply_manual_override(self, level: str, override_data: Dict[str, Any]):
        """Apply manual override at any scheduling level"""
        
        override_record = {
            'timestamp': datetime.now(),
            'level': level,
            'override_data': override_data,
            'reason': override_data.get('reason', 'Manual override'),
            'approved_by': override_data.get('approved_by', 'System')
        }
        
        # Apply override to specific scheduler
        if level in self.schedulers:
            try:
                self.schedulers[level].apply_manual_override(override_data)
                
                # Record in dashboard state
                self.dashboard_state['active_overrides'].append(override_record)
                
                logger.info(f"Manual override applied to {level}: {override_data.get('reason')}")
                
                # Update performance metrics
                self.performance_metrics.record_decision(
                    self.schedulers[level].decision_history[-1] if self.schedulers[level].decision_history else None
                )
                
            except Exception as e:
                logger.error(f"Error applying override to {level}: {e}")
                self.dashboard_state['system_health'] = 'error'
        else:
            logger.error(f"Unknown scheduler level: {level}")
    
    def switch_flexibility_mode(self, level: str, new_mode: str, reason: str):
        """Switch flexibility mode for specific scheduler"""
        
        if level in self.schedulers:
            try:
                scheduler = self.schedulers[level]
                
                # Handle different mode types based on level
                if level == 'yearly':
                    from .temporal_scheduler import StrategicMode
                    scheduler.switch_strategic_mode(StrategicMode(new_mode), reason)
                elif level == 'quarterly':
                    scheduler.switch_quarterly_mode(new_mode, reason)
                elif level == 'monthly':
                    from .temporal_scheduler import ControlMode
                    scheduler.switch_control_mode(ControlMode(new_mode), reason)
                elif level == 'daily':
                    from .temporal_scheduler import ExecutionMode
                    scheduler.switch_execution_mode(ExecutionMode(new_mode), reason)
                elif level == 'weekly':
                    scheduler.switch_prioritization_method(new_mode, reason)
                
                # Update performance metrics
                self.performance_metrics.record_mode_switch(
                    getattr(scheduler, 'current_mode', 'old_mode'),
                    new_mode,
                    reason
                )
                
                logger.info(f"Flexibility mode switched for {level}: {new_mode} - {reason}")
                
            except Exception as e:
                logger.error(f"Error switching mode for {level}: {e}")
                self.dashboard_state['system_health'] = 'error'
        else:
            logger.error(f"Unknown scheduler level: {level}")
    
    def approve_monthly_plan(self, month: int, approved_by: str, allocation_override: Optional[float] = None):
        """Approve monthly plan"""
        
        if 'monthly' in self.schedulers:
            try:
                self.schedulers['monthly'].approve_monthly_plan(month, approved_by, allocation_override)
                
                # Update dashboard state
                self.dashboard_state['pending_approvals'] = [
                    pa for pa in self.dashboard_state['pending_approvals'] 
                    if pa.get('month') != month
                ]
                
                logger.info(f"Monthly plan approved for month {month} by {approved_by}")
                
            except Exception as e:
                logger.error(f"Error approving monthly plan: {e}")
                self.dashboard_state['system_health'] = 'error'
    
    def refresh_schedules(self):
        """Refresh all schedules with current data"""
        
        try:
            current_date = datetime.now()
            current_year = current_date.year
            current_quarter = (current_date.month - 1) // 3 + 1
            current_month = current_date.month
            current_week = self._get_week_number(current_date)
            
            # Refresh yearly plan
            yearly_result = self.schedulers['yearly'].create_schedule(current_year)
            self.current_plans['yearly'] = yearly_result['plan']
            
            # Refresh quarterly plan
            quarterly_result = self.schedulers['quarterly'].create_schedule(
                current_quarter, current_year, yearly_result['plan']
            )
            self.current_plans['quarterly'] = quarterly_result['plan']
            
            # Refresh monthly plan
            monthly_result = self.schedulers['monthly'].create_schedule(
                current_month, current_year, quarterly_result['plan']
            )
            self.current_plans['monthly'] = monthly_result['plan']
            
            # Refresh weekly plan
            weekly_result = self.schedulers['weekly'].create_schedule(
                current_week, current_year, monthly_result['plan']
            )
            self.current_plans['weekly'] = weekly_result['plan']
            
            # Refresh daily plan
            daily_result = self.schedulers['daily'].create_schedule(
                current_date, weekly_result['plan'].get('allocated_signals', [])
            )
            self.current_plans['daily'] = daily_result['plan']
            
            # Update dashboard state
            self.dashboard_state['last_update'] = current_date
            self.dashboard_state['system_health'] = 'operational'
            
            logger.info("All schedules refreshed successfully")
            
        except Exception as e:
            logger.error(f"Error refreshing schedules: {e}")
            self.dashboard_state['system_health'] = 'error'
    
    def _get_week_number(self, date: datetime) -> int:
        """Get ISO week number for date"""
        return date.isocalendar()[1]
    
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """Get concise dashboard summary"""
        
        overview = self.get_scheduling_overview()
        
        summary = {
            'system_health': overview['system_health'],
            'last_update': overview['timestamp'],
            'key_metrics': {
                'total_overrides': len(overview['override_history']),
                'pending_approvals': len(overview['pending_actions']),
                'active_alerts': len(overview['alerts']),
                'performance_ratio': overview['performance_vs_plan'].get('yearly', {}).get('performance_ratio', 0)
            },
            'current_modes': {
                level: state.get('current_mode', 'unknown')
                for level, state in overview['scheduler_states'].items()
            },
            'top_alerts': overview['alerts'][:3]  # Top 3 alerts
        }
        
        return summary
