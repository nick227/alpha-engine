"""
Scheduling System Integration

Integration layer for the hierarchical trade scheduling system.
Connects scheduling components with existing trading infrastructure.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

from .scheduling_dashboard import SchedulingDashboard
from .flexibility_manager import FlexibilityManager
from .temporal_scheduler import SchedulingMetrics

# Import existing trading components
from app.trading.paper_trader import PaperTrader
from app.trading.trade_lifecycle import TradeLifecycleManager

logger = logging.getLogger(__name__)


class SchedulingSystemIntegration:
    """Integration layer for scheduling system with existing trading infrastructure"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Initialize scheduling components
        self.dashboard = SchedulingDashboard(config.get('dashboard', {}))
        self.flexibility_manager = FlexibilityManager(config.get('flexibility', {}))
        self.metrics = SchedulingMetrics()
        
        # Trading system connections
        self.paper_trader: Optional[PaperTrader] = None
        self.trade_lifecycle: Optional[TradeLifecycleManager] = None
        
        # Integration state
        self.integration_state = {
            'paper_trader_connected': False,
            'trade_lifecycle_connected': False,
            'scheduling_active': False,
            'last_sync': None,
            'sync_errors': []
        }
        
        # Configuration
        self.integration_config = {
            'auto_refresh_interval': 300,  # 5 minutes
            'sync_tolerance_seconds': 60,
            'enable_auto_execution': True,
            'enable_risk_checks': True,
            'enable_performance_sync': True
        }
    
    def connect_to_trading_system(self, paper_trader: PaperTrader, 
                               trade_lifecycle: TradeLifecycleManager):
        """Connect scheduling system to existing trading infrastructure"""
        
        try:
            self.paper_trader = paper_trader
            self.trade_lifecycle = trade_lifecycle
            
            # Update integration state
            self.integration_state['paper_trader_connected'] = True
            self.integration_state['trade_lifecycle_connected'] = True
            self.integration_state['last_sync'] = datetime.now()
            
            logger.info("Successfully connected to trading system")
            
            # Start scheduling if configured
            if self.integration_config['enable_auto_execution']:
                self.start_scheduling()
                
        except Exception as e:
            logger.error(f"Error connecting to trading system: {e}")
            self.integration_state['sync_errors'].append({
                'timestamp': datetime.now(),
                'error': str(e),
                'component': 'connection'
            })
    
    def start_scheduling(self):
        """Start the scheduling system"""
        
        if not self._is_ready_for_scheduling():
            logger.error("Scheduling system not ready for operation")
            return False
        
        try:
            # Initialize current schedules
            self.dashboard.refresh_schedules()
            
            # Activate scheduling
            self.integration_state['scheduling_active'] = True
            
            logger.info("Scheduling system started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error starting scheduling: {e}")
            self.integration_state['sync_errors'].append({
                'timestamp': datetime.now(),
                'error': str(e),
                'component': 'startup'
            })
            return False
    
    def stop_scheduling(self):
        """Stop the scheduling system"""
        
        self.integration_state['scheduling_active'] = False
        logger.info("Scheduling system stopped")
    
    def process_daily_trades(self) -> Dict[str, Any]:
        """Process daily trades through scheduling system"""
        
        if not self.integration_state['scheduling_active']:
            return {'status': 'inactive', 'trades': []}
        
        try:
            # Get current daily schedule
            current_date = datetime.now()
            overview = self.dashboard.get_scheduling_overview()
            daily_schedule = overview['current_plans'].get('daily', {})
            
            if not daily_schedule:
                return {'status': 'no_schedule', 'trades': []}
            
            scheduled_trades = daily_schedule.get('scheduled_trades', [])
            
            # Process each scheduled trade
            processed_trades = []
            for trade in scheduled_trades:
                processed_trade = self._process_scheduled_trade(trade)
                if processed_trade:
                    processed_trades.append(processed_trade)
            
            # Sync with trading system
            if self.integration_config['enable_auto_execution']:
                self._execute_trades_with_paper_trader(processed_trades)
            
            return {
                'status': 'success',
                'date': current_date,
                'scheduled_count': len(scheduled_trades),
                'processed_count': len(processed_trades),
                'trades': processed_trades
            }
            
        except Exception as e:
            logger.error(f"Error processing daily trades: {e}")
            self.integration_state['sync_errors'].append({
                'timestamp': datetime.now(),
                'error': str(e),
                'component': 'daily_processing'
            })
            return {'status': 'error', 'error': str(e)}
    
    def _process_scheduled_trade(self, scheduled_trade: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process individual scheduled trade"""
        
        try:
            # Validate trade data
            if not self._validate_scheduled_trade(scheduled_trade):
                logger.warning(f"Invalid scheduled trade: {scheduled_trade.get('signal_id', 'unknown')}")
                return None
            
            # Apply final risk checks
            if self.integration_config['enable_risk_checks']:
                risk_check = self._perform_risk_check(scheduled_trade)
                if not risk_check['passed']:
                    logger.warning(f"Trade failed risk check: {risk_check['reason']}")
                    return None
                
                scheduled_trade['risk_check_result'] = risk_check
            
            # Add execution metadata
            processed_trade = scheduled_trade.copy()
            processed_trade.update({
                'processed_at': datetime.now(),
                'execution_status': 'pending',
                'integration_metadata': {
                    'scheduling_system': True,
                    'processed_by': 'scheduling_integration',
                    'temporal_adjustments': scheduled_trade.get('temporal_adjustments', {})
                }
            })
            
            return processed_trade
            
        except Exception as e:
            logger.error(f"Error processing scheduled trade {scheduled_trade.get('signal_id', 'unknown')}: {e}")
            return None
    
    def _validate_scheduled_trade(self, trade: Dict[str, Any]) -> bool:
        """Validate scheduled trade data"""
        
        required_fields = ['signal_id', 'symbol', 'direction', 'position_size', 'execution_time']
        
        for field in required_fields:
            if field not in trade:
                logger.error(f"Missing required field: {field}")
                return False
        
        # Validate data types and ranges
        if trade['position_size'] <= 0:
            logger.error(f"Invalid position size: {trade['position_size']}")
            return False
        
        if trade['direction'] not in ['long', 'short']:
            logger.error(f"Invalid direction: {trade['direction']}")
            return False
        
        # Validate execution time
        execution_time = trade.get('execution_time')
        if isinstance(execution_time, datetime):
            if execution_time < datetime.now() - timedelta(hours=1):  # Too old
                logger.error(f"Execution time too old: {execution_time}")
                return False
        
        return True
    
    def _perform_risk_check(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """Perform risk check on scheduled trade"""
        
        risk_check = {
            'passed': True,
            'reason': '',
            'warnings': [],
            'risk_score': 0.0
        }
        
        # Position size check
        max_position_size = self.config.get('max_position_size', 0.05)  # 5% default
        if trade['position_size'] > max_position_size:
            risk_check['passed'] = False
            risk_check['reason'] = f"Position size {trade['position_size']:.3f} exceeds maximum {max_position_size:.3f}"
            return risk_check
        
        # Portfolio heat check (if portfolio data available)
        if self.paper_trader:
            try:
                current_portfolio = self.paper_trader.portfolio
                current_heat = self._calculate_portfolio_heat(current_portfolio, trade)
                
                max_portfolio_heat = self.config.get('max_portfolio_heat', 0.15)
                if current_heat > max_portfolio_heat:
                    risk_check['warnings'].append(f"Portfolio heat {current_heat:.2f} exceeds maximum {max_portfolio_heat:.2f}")
                    risk_check['risk_score'] += 0.3
                
            except Exception as e:
                logger.warning(f"Error calculating portfolio heat: {e}")
        
        # Temporal risk check
        temporal_adjustments = trade.get('temporal_adjustments', {})
        if 'volatility' in temporal_adjustments:
            vol_adj = temporal_adjustments['volatility']
            if vol_adj.get('position_multiplier', 1.0) < 0.5:  # Significant reduction
                risk_check['warnings'].append("Significant position size reduction due to volatility")
                risk_check['risk_score'] += 0.2
        
        # Economic event risk check
        if 'economic_events' in temporal_adjustments:
            event_adj = temporal_adjustments['economic_events']
            if event_adj.get('high_impact_count', 0) > 2:
                risk_check['warnings'].append(f"High economic event density: {event_adj['high_impact_count']} events")
                risk_check['risk_score'] += 0.3
        
        # Final risk assessment
        if risk_check['risk_score'] > 0.5:
            risk_check['passed'] = False
            risk_check['reason'] = f"Risk score {risk_check['risk_score']:.2f} exceeds threshold"
        
        return risk_check
    
    def _calculate_portfolio_heat(self, portfolio: Dict[str, Any], new_trade: Dict[str, Any]) -> float:
        """Calculate portfolio heat with new trade"""
        
        # Simple portfolio heat calculation
        total_risk = 0.0
        
        # Sum risk from existing positions
        for position in portfolio.get('positions', []):
            position_risk = abs(position.get('quantity', 0) * position.get('entry_price', 0))
            total_risk += position_risk
        
        # Add risk from new trade
        new_trade_risk = new_trade['position_size'] * new_trade.get('current_price', 100)  # Mock price
        total_risk += new_trade_risk
        
        # Calculate heat as percentage of notional portfolio value
        portfolio_value = self.config.get('portfolio_value', 1000000)  # $1M default
        heat = total_risk / portfolio_value
        
        return heat
    
    def _execute_trades_with_paper_trader(self, trades: List[Dict[str, Any]]):
        """Execute trades through paper trader"""
        
        if not self.paper_trader:
            logger.error("Paper trader not connected")
            return
        
        for trade in trades:
            try:
                # Convert scheduled trade to paper trader format
                signal_data = self._convert_to_paper_trader_format(trade)
                
                # Execute through paper trader
                result = self.paper_trader.process_signal(
                    ticker=trade['symbol'],
                    strategy_id=trade.get('strategy_id', 'scheduling_system'),
                    direction='long' if trade['direction'] == 'long' else 'short',
                    confidence=trade.get('confidence', 0.5),
                    consensus_score=trade.get('consensus_score', 0.5),
                    alpha_score=trade.get('alpha_score', 0.5),
                    feature_snapshot=trade.get('feature_snapshot', {}),
                    entry_price=trade.get('entry_price', 0),
                    regime=trade.get('regime', 'unknown')
                )
                
                # Update trade status
                trade['execution_result'] = result
                trade['executed_at'] = datetime.now()
                
                logger.info(f"Executed trade {trade['signal_id']}: {result.get('status', 'unknown')}")
                
            except Exception as e:
                logger.error(f"Error executing trade {trade['signal_id']}: {e}")
                trade['execution_error'] = str(e)
    
    def _convert_to_paper_trader_format(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """Convert scheduled trade to paper trader signal format"""
        
        return {
            'id': trade['signal_id'],
            'symbol': trade['symbol'],
            'direction': trade['direction'],
            'confidence': trade.get('confidence', 0.5),
            'consensus_score': trade.get('consensus_score', 0.5),
            'alpha_score': trade.get('alpha_score', 0.5),
            'feature_snapshot': trade.get('feature_snapshot', {}),
            'entry_price': trade.get('entry_price', 0),
            'regime': trade.get('regime', 'unknown'),
            'position_size': trade['position_size'],
            'scheduling_metadata': {
                'execution_window': trade.get('execution_window'),
                'temporal_adjustments': trade.get('temporal_adjustments', {}),
                'order_type': trade.get('order_type', 'market')
            }
        }
    
    def sync_performance_data(self) -> Dict[str, Any]:
        """Sync performance data between scheduling and trading systems"""
        
        if not self.integration_config['enable_performance_sync']:
            return {'status': 'disabled'}
        
        try:
            # Get performance data from paper trader
            if self.paper_trader:
                trading_performance = self._extract_trading_performance()
                
                # Update scheduling system performance metrics
                self._update_scheduling_performance(trading_performance)
                
                self.integration_state['last_sync'] = datetime.now()
                
                return {
                    'status': 'success',
                    'sync_time': datetime.now(),
                    'trading_performance': trading_performance
                }
            
            return {'status': 'no_trading_data'}
            
        except Exception as e:
            logger.error(f"Error syncing performance data: {e}")
            self.integration_state['sync_errors'].append({
                'timestamp': datetime.now(),
                'error': str(e),
                'component': 'performance_sync'
            })
            return {'status': 'error', 'error': str(e)}
    
    def _extract_trading_performance(self) -> Dict[str, Any]:
        """Extract performance data from trading system"""
        
        if not self.paper_trader:
            return {}
        
        # Mock performance extraction - would integrate with actual paper trader
        return {
            'daily_return': 0.002,  # 0.2% daily
            'weekly_return': 0.008,  # 0.8% weekly
            'monthly_return': 0.015,  # 1.5% monthly
            'total_trades': 156,
            'winning_trades': 102,
            'losing_trades': 54,
            'win_rate': 0.654,
            'average_win': 0.025,
            'average_loss': -0.018,
            'profit_factor': 1.42,
            'max_drawdown': 0.08,
            'sharpe_ratio': 0.89,
            'current_positions': 8,
            'portfolio_heat': 0.12
        }
    
    def _update_scheduling_performance(self, trading_performance: Dict[str, Any]):
        """Update scheduling system performance metrics"""
        
        # Update flexibility manager performance
        current_mode = self.flexibility_manager.current_mode
        mode_performance_data = {
            'total_return': trading_performance.get('monthly_return', 0),
            'risk_adjusted_return': trading_performance.get('sharpe_ratio', 0) * 0.1,  # Rough conversion
            'max_drawdown': trading_performance.get('max_drawdown', 0),
            'win_rate': trading_performance.get('win_rate', 0),
            'sharpe_ratio': trading_performance.get('sharpe_ratio', 0),
            'consistency_score': 1.0 - abs(trading_performance.get('max_drawdown', 0))
        }
        
        self.flexibility_manager.update_mode_performance(current_mode, mode_performance_data)
        
        # Record decision in metrics
        self.metrics.record_decision(None)  # Generic decision record
    
    def get_integration_status(self) -> Dict[str, Any]:
        """Get comprehensive integration status"""
        
        return {
            'integration_state': self.integration_state,
            'scheduling_status': self.dashboard.get_scheduling_overview(),
            'flexibility_status': self.flexibility_manager.get_flexibility_report(),
            'connection_status': {
                'paper_trader': self.integration_state['paper_trader_connected'],
                'trade_lifecycle': self.integration_state['trade_lifecycle_connected']
            },
            'performance_sync': {
                'last_sync': self.integration_state['last_sync'],
                'sync_errors': len(self.integration_state['sync_errors']),
                'recent_errors': self.integration_state['sync_errors'][-5:]
            },
            'configuration': self.integration_config,
            'system_health': self._calculate_system_health()
        }
    
    def _calculate_system_health(self) -> str:
        """Calculate overall system health"""
        
        health_score = 100
        
        # Check connections
        if not self.integration_state['paper_trader_connected']:
            health_score -= 30
        if not self.integration_state['trade_lifecycle_connected']:
            health_score -= 30
        
        # Check scheduling status
        if not self.integration_state['scheduling_active']:
            health_score -= 20
        
        # Check sync errors
        recent_errors = len([e for e in self.integration_state['sync_errors'] 
                           if (datetime.now() - e['timestamp']).total_seconds() < 3600])  # Last hour
        health_score -= recent_errors * 5
        
        # Check last sync
        if self.integration_state['last_sync']:
            sync_age = (datetime.now() - self.integration_state['last_sync']).total_seconds()
            if sync_age > 600:  # More than 10 minutes
                health_score -= 10
        
        # Determine health status
        if health_score >= 90:
            return 'excellent'
        elif health_score >= 70:
            return 'good'
        elif health_score >= 50:
            return 'fair'
        elif health_score >= 30:
            return 'poor'
        else:
            return 'critical'
    
    def _is_ready_for_scheduling(self) -> bool:
        """Check if system is ready for scheduling"""
        
        return (
            self.integration_state['paper_trader_connected'] and
            self.integration_state['trade_lifecycle_connected'] and
            len(self.integration_state['sync_errors']) < 5  # Not too many errors
        )
    
    def handle_manual_override(self, override_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle manual override from external interface"""
        
        try:
            # Route override to appropriate component
            level = override_data.get('level', 'unknown')
            
            if level == 'flexibility':
                # Handle flexibility mode override
                from .temporal_scheduler import FlexibilityMode
                new_mode = FlexibilityMode(override_data.get('mode', 'adaptive'))
                switch_record = self.flexibility_manager.switch_flexibility_mode(
                    new_mode, override_data.get('reason', 'Manual override')
                )
                return {'status': 'success', 'switch_record': switch_record}
            
            elif level in ['yearly', 'quarterly', 'monthly', 'weekly', 'daily']:
                # Handle scheduler-specific override
                self.dashboard.apply_manual_override(level, override_data)
                return {'status': 'success', 'message': f'Override applied to {level} scheduler'}
            
            else:
                return {'status': 'error', 'message': f'Unknown override level: {level}'}
                
        except Exception as e:
            logger.error(f"Error handling manual override: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get comprehensive system metrics"""
        
        return {
            'scheduling_metrics': self.metrics.get_metrics_summary(),
            'integration_metrics': {
                'uptime_percentage': self._calculate_uptime(),
                'error_rate': self._calculate_error_rate(),
                'sync_success_rate': self._calculate_sync_success_rate(),
                'performance_correlation': self._calculate_performance_correlation()
            },
            'flexibility_metrics': self.flexibility_manager.get_flexibility_report(),
            'dashboard_metrics': self.dashboard.get_dashboard_summary()
        }
    
    def _calculate_uptime(self) -> float:
        """Calculate system uptime percentage"""
        
        # Mock uptime calculation
        return 0.987  # 98.7% uptime
    
    def _calculate_error_rate(self) -> float:
        """Calculate system error rate"""
        
        total_operations = len(self.integration_state['sync_errors']) + 1000  # Mock total ops
        error_count = len(self.integration_state['sync_errors'])
        
        return error_count / total_operations if total_operations > 0 else 0
    
    def _calculate_sync_success_rate(self) -> float:
        """Calculate sync success rate"""
        
        total_syncs = len(self.integration_state['sync_errors']) + 50  # Mock total syncs
        successful_syncs = total_syncs - len(self.integration_state['sync_errors'])
        
        return successful_syncs / total_syncs if total_syncs > 0 else 1.0
    
    def _calculate_performance_correlation(self) -> float:
        """Calculate correlation between scheduled and actual performance"""
        
        # Mock correlation calculation
        return 0.73  # 73% correlation
