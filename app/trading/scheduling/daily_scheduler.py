"""
Daily Trade Scheduler

Daily execution with micro-timing optimization and temporal intelligence.
Handles entry timing, order scheduling, and intraday execution.
"""

from __future__ import annotations
from datetime import datetime, timedelta, time
from typing import Dict, List, Any, Optional, Tuple
import logging

from .temporal_scheduler import (
    TemporalScheduler,
    ExecutionMode,
    FlexibilityMode,
    SchedulingDecision,
    ManualOverride
)
from .temporal_correlation_integration import SchedulingTemporalAnalyzer, SchedulingInsightsEngine

logger = logging.getLogger(__name__)


class DailyScheduler(TemporalScheduler):
    """Daily execution with micro-timing optimization"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Execution modes
        self.execution_modes = {
            ExecutionMode.IMMEDIATE: {
                'execution_delay': 0, 
                'batch_size': 1,
                'market_impact': 'high',
                'timing_optimization': False
            },
            ExecutionMode.STAGGERED: {
                'execution_delay': 5, 
                'batch_size': 3,
                'market_impact': 'medium',
                'timing_optimization': False
            },
            ExecutionMode.OPTIMAL_TIMING: {
                'use_vix_timing': True, 
                'use_sentiment_timing': True,
                'use_economic_timing': True,
                'market_impact': 'low',
                'timing_optimization': True
            },
            ExecutionMode.MARKET_ADAPTIVE: {
                'adjust_to_volume': True, 
                'adjust_to_volatility': True,
                'adjust_to_sentiment': True,
                'market_impact': 'low',
                'timing_optimization': True
            }
        }
        
        self.current_execution_mode = ExecutionMode(config.get('daily_execution_mode', 'optimal_timing'))
        self.daily_schedules: Dict[datetime, Dict[str, Any]] = {}
        
        # Execution windows configuration
        self.execution_windows = {
            'pre_market': {
                'start': time(9, 0),    # 9:00 AM
                'end': time(9, 30),      # 9:30 AM
                'characteristics': {'liquidity': 'low', 'volatility': 'medium'},
                'preferred_for': ['economic_event_sensitive', 'gap_trading']
            },
            'market_open': {
                'start': time(9, 30),    # 9:30 AM
                'end': time(10, 30),    # 10:30 AM
                'characteristics': {'liquidity': 'high', 'volatility': 'high'},
                'preferred_for': ['momentum_strategies', 'breakout_strategies']
            },
            'mid_day': {
                'start': time(10, 30),   # 10:30 AM
                'end': time(15, 0),     # 3:00 PM
                'characteristics': {'liquidity': 'medium', 'volatility': 'medium'},
                'preferred_for': ['mean_reversion', 'value_strategies']
            },
            'close': {
                'start': time(15, 0),    # 3:00 PM
                'end': time(16, 0),     # 4:00 PM
                'characteristics': {'liquidity': 'high', 'volatility': 'high'},
                'preferred_for': ['end_of_day_positioning', 'risk_management']
            }
        }
        
        # Initialize temporal components
        self.temporal_analyzer = SchedulingTemporalAnalyzer()
        self.insights_engine = SchedulingInsightsEngine(self.temporal_analyzer)
    
    def create_schedule(self, date: datetime, weekly_signals: List[Dict]) -> Dict[str, Any]:
        """Create optimal daily execution schedule"""
        
        logger.info(f"Creating daily execution schedule for {date.strftime('%Y-%m-%d')}")
        
        # Filter signals for execution today
        todays_signals = self._filter_signals_for_execution(weekly_signals, date)
        
        # Get daily temporal insights
        temporal_insights = self.get_temporal_insights('daily', date)
        
        # Base daily plan
        base_plan = {
            'date': date,
            'execution_mode': self.current_execution_mode.value,
            'scheduled_trades': [],
            'execution_windows': self.generate_execution_windows(date),
            'temporal_adjustments': {},
            'risk_management': self.get_daily_risk_parameters(date),
            'contingency_plans': self.generate_contingency_plans(date),
            'market_conditions': self._get_current_market_conditions()
        }
        
        # Apply flexibility mode with temporal insights
        decision = self.apply_flexibility_mode(base_plan, temporal_insights.__dict__)
        
        # Add daily-specific temporal adjustments
        daily_adjustments = self._get_daily_temporal_adjustments(date, temporal_insights)
        decision.final_decision['temporal_adjustments'].update(daily_adjustments)
        
        # Schedule each signal
        for signal in todays_signals:
            if self._should_execute_today(signal, date, temporal_insights):
                scheduled_trade = self.schedule_signal_execution(signal, date, decision.final_decision)
                decision.final_decision['scheduled_trades'].append(scheduled_trade)
        
        # Store daily schedule
        self.daily_schedules[date] = decision.final_decision
        
        logger.info(f"Daily schedule created for {date.strftime('%Y-%m-%d')}: "
                   f"{len(decision.final_decision['scheduled_trades'])} trades scheduled")
        
        return {
            'plan': decision.final_decision,
            'decision': decision,
            'temporal_insights': temporal_insights.__dict__
        }
    
    def schedule_signal_execution(self, signal: Dict, date: datetime, daily_plan: Dict) -> Dict[str, Any]:
        """Schedule signal execution with optimal timing"""
        
        # Determine optimal execution window
        execution_window = self.get_optimal_execution_window(signal, daily_plan)
        
        # Calculate execution time
        execution_time = self._calculate_execution_time(signal, execution_window, daily_plan)
        
        # Determine order type
        order_type = self.select_order_type(signal, daily_plan)
        
        # Calculate position size with temporal adjustments
        position_size = self._calculate_temporal_adjusted_position_size(signal, daily_plan)
        
        scheduled_trade = {
            'signal_id': signal['id'],
            'symbol': signal['symbol'],
            'direction': signal['direction'],
            'execution_time': execution_time,
            'execution_window': execution_window,
            'order_type': order_type,
            'position_size': position_size,
            'original_position_size': signal.get('position_size', position_size),
            'temporal_adjustments': self._get_signal_temporal_adjustments(signal, daily_plan),
            'execution_priority': signal.get('execution_priority', 'medium'),
            'contingency_plans': self._generate_signal_contingency_plans(signal),
            'risk_parameters': self._get_signal_risk_parameters(signal, daily_plan)
        }
        
        return scheduled_trade
    
    def get_optimal_execution_window(self, signal: Dict, daily_plan: Dict) -> Dict[str, Any]:
        """Get optimal execution window for signal"""
        
        strategy = signal.get('strategy', 'unknown')
        market_conditions = daily_plan.get('market_conditions', {})
        
        # Base window selection
        optimal_window = 'mid_day'  # Default
        
        # Strategy-based window selection
        if strategy in ['volatility_breakout', 'sniper_coil']:
            optimal_window = 'market_open'
        elif strategy in ['realness_repricer', 'narrative_lag']:
            optimal_window = 'mid_day'
        elif strategy == 'silent_compounder':
            optimal_window = 'close'
        
        # Temporal adjustments
        temporal_adjustments = daily_plan.get('temporal_adjustments', {})
        
        # Economic event adjustments
        if 'economic_events' in temporal_adjustments:
            high_impact_count = temporal_adjustments['economic_events'].get('high_impact_count', 0)
            if high_impact_count > 0:
                # Delay execution around high-impact events
                optimal_window = 'close'
        
        # Volatility adjustments
        if 'volatility' in temporal_adjustments:
            vol_regime = market_conditions.get('volatility_regime', 'normal')
            if vol_regime == 'high':
                optimal_window = 'close'  # Wait for volatility to settle
            elif vol_regime == 'expansion':
                optimal_window = 'market_open'  # Execute early in expansion
        
        # Sentiment adjustments
        if 'sentiment' in temporal_adjustments:
            sentiment = market_conditions.get('market_sentiment', 0.0)
            if sentiment < -0.5:
                optimal_window = 'close'  # Wait for sentiment to improve
        
        window_config = self.execution_windows[optimal_window].copy()
        window_config['name'] = optimal_window
        
        return window_config
    
    def _calculate_execution_time(self, signal: Dict, execution_window: Dict, daily_plan: Dict) -> datetime:
        """Calculate optimal execution time within window"""
        
        execution_mode = self.current_execution_mode
        base_date = daily_plan['date']
        
        if execution_mode == ExecutionMode.IMMEDIATE:
            # Execute at window start
            return datetime.combine(base_date, execution_window['start'])
        
        elif execution_mode == ExecutionMode.STAGGERED:
            # Stagger execution within window
            delay_minutes = self.execution_modes[execution_mode]['execution_delay']
            execution_time = datetime.combine(base_date, execution_window['start']) + timedelta(minutes=delay_minutes)
            return execution_time
        
        elif execution_mode == ExecutionMode.OPTIMAL_TIMING:
            # Use temporal optimization
            return self._calculate_optimal_timing(signal, execution_window, base_date)
        
        elif execution_mode == ExecutionMode.MARKET_ADAPTIVE:
            # Adapt to market conditions
            return self._calculate_adaptive_timing(signal, execution_window, base_date, daily_plan)
        
        # Default to window start
        return datetime.combine(base_date, execution_window['start'])
    
    def _calculate_optimal_timing(self, signal: Dict, execution_window: Dict, base_date: datetime) -> datetime:
        """Calculate optimal timing using temporal insights"""
        
        window_start = datetime.combine(base_date, execution_window['start'])
        window_end = datetime.combine(base_date, execution_window['end'])
        window_duration = window_end - window_start
        
        # Strategy-specific timing
        strategy = signal.get('strategy', 'unknown')
        
        if strategy == 'volatility_breakout':
            # Execute early in window for breakout strategies
            optimal_offset = window_duration * 0.1  # 10% into window
        elif strategy == 'sniper_coil':
            # Execute mid-window for contrarian strategies
            optimal_offset = window_duration * 0.5  # 50% into window
        elif strategy in ['realness_repricer', 'narrative_lag']:
            # Execute later in window for value strategies
            optimal_offset = window_duration * 0.7  # 70% into window
        else:
            # Default to middle of window
            optimal_offset = window_duration * 0.5
        
        return window_start + optimal_offset
    
    def _calculate_adaptive_timing(self, signal: Dict, execution_window: Dict, base_date: datetime, daily_plan: Dict) -> datetime:
        """Calculate adaptive timing based on market conditions"""
        
        window_start = datetime.combine(base_date, execution_window['start'])
        window_end = datetime.combine(base_date, execution_window['end'])
        
        market_conditions = daily_plan.get('market_conditions', {})
        
        # Volume-based timing
        if market_conditions.get('volume_trend') == 'increasing':
            # Execute when volume is building
            optimal_offset = timedelta(minutes=15)
        elif market_conditions.get('volume_trend') == 'decreasing':
            # Execute early before volume dries up
            optimal_offset = timedelta(minutes=5)
        else:
            optimal_offset = timedelta(minutes=10)
        
        # Volatility-based timing
        if market_conditions.get('volatility_trend') == 'increasing':
            # Execute before volatility spikes
            optimal_offset = min(optimal_offset, timedelta(minutes=5))
        elif market_conditions.get('volatility_trend') == 'decreasing':
            # Execute when volatility is stable
            optimal_offset = timedelta(minutes=20)
        
        return window_start + optimal_offset
    
    def select_order_type(self, signal: Dict, daily_plan: Dict) -> str:
        """Select optimal order type for signal"""
        
        strategy = signal.get('strategy', 'unknown')
        market_conditions = daily_plan.get('market_conditions', {})
        volatility_regime = market_conditions.get('volatility_regime', 'normal')
        
        # Base order type selection
        if strategy in ['volatility_breakout', 'sniper_coil']:
            base_order_type = 'market'  # Immediate execution for momentum
        elif strategy in ['realness_repricer', 'narrative_lag']:
            base_order_type = 'limit'   # Control entry price for value
        else:
            base_order_type = 'market'
        
        # Adjust based on volatility
        if volatility_regime == 'high':
            # Use limit orders in high volatility to control slippage
            if base_order_type == 'market':
                base_order_type = 'limit'
        elif volatility_regime == 'low':
            # Can use market orders in low volatility
            if base_order_type == 'limit':
                base_order_type = 'market'
        
        # Adjust based on execution mode
        if self.current_execution_mode in [ExecutionMode.OPTIMAL_TIMING, ExecutionMode.MARKET_ADAPTIVE]:
            # Prefer limit orders for better timing control
            if base_order_type == 'market':
                base_order_type = 'limit'
        
        return base_order_type
    
    def _calculate_temporal_adjusted_position_size(self, signal: Dict, daily_plan: Dict) -> float:
        """Calculate position size with temporal adjustments"""
        
        base_position_size = signal.get('position_size', 0.02)  # 2% default
        temporal_adjustments = daily_plan.get('temporal_adjustments', {})
        
        adjusted_size = base_position_size
        
        # Volatility adjustments
        if 'volatility' in temporal_adjustments:
            vol_adj = temporal_adjustments['volatility']
            if 'position_multiplier' in vol_adj:
                adjusted_size *= vol_adj['position_multiplier']
        
        # Economic event adjustments
        if 'economic_events' in temporal_adjustments:
            event_adj = temporal_adjustments['economic_events']
            if 'position_size_reduction' in event_adj:
                adjusted_size *= event_adj['position_size_reduction']
        
        # Sentiment adjustments
        if 'sentiment' in temporal_adjustments:
            sent_adj = temporal_adjustments['sentiment']
            if 'position_multiplier' in sent_adj:
                adjusted_size *= sent_adj['position_multiplier']
        
        # Apply limits
        max_position_size = base_position_size * 1.5  # Max 50% increase
        min_position_size = base_position_size * 0.3  # Min 70% reduction
        
        return max(min(adjusted_size, min_position_size), max_position_size)
    
    def generate_execution_windows(self, date: datetime) -> List[Dict[str, Any]]:
        """Generate optimal execution windows for day"""
        
        windows = []
        
        for window_name, window_config in self.execution_windows.items():
            window = window_config.copy()
            
            # Add date-specific adjustments
            window['start_datetime'] = datetime.combine(date, window['start'])
            window['end_datetime'] = datetime.combine(date, window['end'])
            window['duration_minutes'] = (
                window['end_datetime'] - window['start_datetime']
            ).total_seconds() / 60
            
            # Adjust for day of week
            day_of_week = date.weekday()
            if day_of_week == 0:  # Monday
                window['liquidity_adjustment'] = 0.9  # Lower liquidity
            elif day_of_week == 4:  # Friday
                window['volatility_adjustment'] = 1.2  # Higher volatility
            
            windows.append(window)
        
        return windows
    
    def _get_daily_temporal_adjustments(self, date: datetime, insights) -> Dict[str, Any]:
        """Get daily-specific temporal adjustments"""
        
        adjustments = {}
        
        # Day of week adjustments
        day_of_week = date.weekday()
        dow_patterns = {
            0: {'multiplier': 0.9, 'risk_adjustment': 1.1},  # Monday
            1: {'multiplier': 1.1, 'risk_adjustment': 1.0},  # Tuesday
            2: {'multiplier': 1.1, 'risk_adjustment': 1.0},  # Wednesday
            3: {'multiplier': 1.1, 'risk_adjustment': 1.0},  # Thursday
            4: {'multiplier': 0.8, 'risk_adjustment': 1.2},  # Friday
        }
        
        if day_of_week in dow_patterns:
            adjustments['day_of_week'] = dow_patterns[day_of_week]
        
        # Economic event adjustments
        high_impact_events = [e for e in insights.economic_events if e.get('impact') == 'high']
        if high_impact_events:
            adjustments['economic_events'] = {
                'high_impact_count': len(high_impact_events),
                'execution_delay': 30,  # minutes
                'position_size_multiplier': 0.8,
                'risk_multiplier': 1.2
            }
        
        # Volatility regime adjustments
        vol_adjustments = {
            'low': {'execution_timing': 'normal', 'order_type_preference': 'market'},
            'normal': {'execution_timing': 'normal', 'order_type_preference': 'market'},
            'high': {'execution_timing': 'delayed', 'order_type_preference': 'limit'},
            'expansion': {'execution_timing': 'immediate', 'order_type_preference': 'market'}
        }
        
        if insights.volatility_regime in vol_adjustments:
            adjustments['volatility'] = vol_adjustments[insights.volatility_regime]
        
        return adjustments
    
    def _filter_signals_for_execution(self, weekly_signals: List[Dict], date: datetime) -> List[Dict]:
        """Filter signals for execution on specific date"""
        
        todays_signals = []
        
        for signal in weekly_signals:
            # Check if signal should execute today
            execution_date = signal.get('execution_date', date)
            
            if execution_date.date() == date.date():
                # Additional filtering
                if self._passes_daily_filters(signal, date):
                    todays_signals.append(signal)
        
        return todays_signals
    
    def _passes_daily_filters(self, signal: Dict, date: datetime) -> bool:
        """Check if signal passes daily execution filters"""
        
        # Day of week filter
        day_of_week = date.weekday()
        restricted_days = signal.get('restricted_days', [])
        if day_of_week in restricted_days:
            return False
        
        # Time window filter
        execution_windows = signal.get('execution_windows', ['market_open', 'mid_day', 'close'])
        current_time = date.time()
        
        # Check if current time is in any allowed window
        for window_name in execution_windows:
            if window_name in self.execution_windows:
                window = self.execution_windows[window_name]
                if window['start'] <= current_time <= window['end']:
                    return True
        
        return False
    
    def _should_execute_today(self, signal: Dict, date: datetime, insights) -> bool:
        """Determine if signal should execute today"""
        
        # Check economic event constraints
        high_impact_events = [e for e in insights.economic_events if e.get('impact') == 'high']
        if high_impact_events and signal.get('economic_event_sensitive', False):
            # Delay execution around high-impact events
            return False
        
        # Check volatility constraints
        if insights.volatility_regime == 'high' and signal.get('volatility_sensitive', False):
            # Reduce execution in high volatility
            return False
        
        # Check sentiment constraints
        if insights.sentiment_score < -0.5 and signal.get('sentiment_sensitive', False):
            # Reduce execution in very negative sentiment
            return False
        
        return True
    
    def get_daily_risk_parameters(self, date: datetime) -> Dict[str, Any]:
        """Get daily risk parameters"""
        
        day_of_week = date.weekday()
        
        base_risk_params = {
            'max_position_size': 0.03,  # 3% max per position
            'max_daily_risk': 0.06,  # 6% max daily risk
            'max_portfolio_heat': 0.12,
            'stop_loss_multiplier': 1.5,
            'trailing_stop_activation': 0.02
        }
        
        # Day of week adjustments
        if day_of_week == 0:  # Monday
            base_risk_params['max_daily_risk'] *= 0.8
        elif day_of_week == 4:  # Friday
            base_risk_params['max_daily_risk'] *= 0.9
            base_risk_params['stop_loss_multiplier'] *= 1.2
        
        return base_risk_params
    
    def generate_contingency_plans(self, date: datetime) -> Dict[str, Any]:
        """Generate contingency plans for the day"""
        
        return {
            'vix_spike': {
                'trigger': 'vix > 35',
                'action': 'reduce_positions_by_50%',
                'execution': 'immediate'
            },
            'market_crash': {
                'trigger': 'spx_down_5%_in_1h',
                'action': 'emergency_stop_all',
                'execution': 'immediate'
            },
            'liquidity_crisis': {
                'trigger': 'volume_down_80%_for_30min',
                'action': 'pause_new_trades',
                'execution': 'immediate'
            },
            'technical_failure': {
                'trigger': 'system_error_or_data_delay',
                'action': 'switch_to_manual',
                'execution': 'immediate'
            }
        }
    
    def _get_signal_temporal_adjustments(self, signal: Dict, daily_plan: Dict) -> Dict[str, Any]:
        """Get temporal adjustments for specific signal"""
        
        adjustments = {}
        temporal_adjustments = daily_plan.get('temporal_adjustments', {})
        
        # Volatility adjustments
        if 'volatility' in temporal_adjustments:
            adjustments['volatility'] = temporal_adjustments['volatility']
        
        # Economic event adjustments
        if 'economic_events' in temporal_adjustments:
            adjustments['economic_events'] = temporal_adjustments['economic_events']
        
        # Sentiment adjustments
        if 'sentiment' in temporal_adjustments:
            adjustments['sentiment'] = temporal_adjustments['sentiment']
        
        return adjustments
    
    def _get_signal_risk_parameters(self, signal: Dict, daily_plan: Dict) -> Dict[str, Any]:
        """Get risk parameters for specific signal"""
        
        base_risk = daily_plan.get('risk_management', {})
        signal_risk = base_risk.copy()
        
        # Strategy-specific adjustments
        strategy = signal.get('strategy', 'unknown')
        if strategy == 'sniper_coil':
            signal_risk['stop_loss_multiplier'] *= 0.8  # Tighter stops
        elif strategy == 'volatility_breakout':
            signal_risk['stop_loss_multiplier'] *= 1.3  # Wider stops
        elif strategy == 'silent_compounder':
            signal_risk['trailing_stop_activation'] *= 0.5  # Earlier trailing
        
        return signal_risk
    
    def _generate_signal_contingency_plans(self, signal: Dict) -> List[Dict[str, Any]]:
        """Generate contingency plans for specific signal"""
        
        return [
            {
                'condition': 'entry_failure',
                'action': 'retry_with_limit_order',
                'delay_minutes': 15
            },
            {
                'condition': 'adverse_movement',
                'action': 'tighten_stop_loss',
                'adjustment_factor': 0.8
            },
            {
                'condition': 'missed_opportunity',
                'action': 'increase_position_size',
                'max_increase': 0.5
            }
        ]
    
    def _get_current_market_conditions(self) -> Dict[str, Any]:
        """Get current market conditions"""
        
        # This would integrate with real-time market data
        # For now, return mock data
        return {
            'vix_percentile': 0.6,
            'market_sentiment': 0.2,
            'volatility_regime': 'normal',
            'volume_trend': 'stable',
            'volatility_trend': 'stable'
        }
    
    def switch_execution_mode(self, new_mode: ExecutionMode, reason: str):
        """Switch execution mode"""
        
        old_mode = self.current_execution_mode
        self.current_execution_mode = new_mode
        
        logger.info(f"Daily execution mode switched: {old_mode.value} -> {new_mode.value} - {reason}")
    
    def get_daily_schedule(self, date: datetime) -> Optional[Dict[str, Any]]:
        """Get specific daily schedule"""
        return self.daily_schedules.get(date)
    
    def apply_manual_override(self, override_data: Dict[str, Any]):
        """Apply manual override to daily scheduler"""
        
        override = ManualOverride(
            timestamp=datetime.now(),
            level='daily',
            override_data=override_data,
            reason=override_data.get('reason', 'Manual override'),
            approved_by=override_data.get('approved_by', 'System'),
            impact_assessment=self._assess_override_impact(override_data),
            previous_state={'current_execution_mode': self.current_execution_mode}
        )
        
        self.record_manual_override(override)
        
        # Apply override
        if 'execution_mode' in override_data:
            try:
                new_mode = ExecutionMode(override_data['execution_mode'])
                self.switch_execution_mode(new_mode, override_data.get('reason', 'Manual override'))
            except ValueError:
                logger.error(f"Invalid execution mode: {override_data['execution_mode']}")
        
        if 'signal_override' in override_data:
            # Apply specific signal overrides for today
            today = datetime.now().date()
            for date, schedule in self.daily_schedules.items():
                if date.date() == today:
                    signal_overrides = override_data['signal_override']
                    for trade in schedule.get('scheduled_trades', []):
                        if trade['signal_id'] in signal_overrides:
                            trade.update(signal_overrides[trade['signal_id']])
    
    def _assess_override_impact(self, override_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess impact of manual override"""
        
        impact = {
            'execution_mode_change': False,
            'signal_change': False,
            'overall_impact': 'medium'
        }
        
        if 'execution_mode' in override_data:
            new_mode = override_data['execution_mode']
            if new_mode != self.current_execution_mode.value:
                impact['execution_mode_change'] = True
                impact['overall_impact'] = 'high'
        
        if 'signal_override' in override_data:
            impact['signal_change'] = True
            impact['overall_impact'] = 'medium'
        
        return impact
