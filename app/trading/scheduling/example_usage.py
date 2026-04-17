"""
Trade Scheduling System - Example Usage

Demonstrates how to use the hierarchical trade scheduling system.
Shows integration with existing trading infrastructure.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, Any
import logging

# Import scheduling components
from app.trading.scheduling import (
    SchedulingSystemIntegration,
    SchedulingDashboard,
    FlexibilityManager,
    YearlyScheduler,
    QuarterlyScheduler,
    MonthlyScheduler,
    WeeklyScheduler,
    DailyScheduler,
    FlexibilityMode,
    StrategicMode,
    ControlMode,
    ExecutionMode
)

# Import existing trading components
from app.trading.paper_trader import PaperTrader
from app.trading.trade_lifecycle import TradeLifecycleManager

logger = logging.getLogger(__name__)


def example_basic_usage():
    """Basic usage example of the scheduling system"""
    
    print("=== Basic Scheduling System Usage ===")
    
    # 1. Initialize scheduling system
    config = {
        'yearly': {
            'yearly_mode': 'balanced',
            'annual_capital': 1000000
        },
        'quarterly': {
            'mode_switching': True,
            'default_quarterly_mode': 'balanced'
        },
        'monthly': {
            'monthly_control_mode': 'semi_automatic'
        },
        'weekly': {
            'prioritization_method': 'temporal_alignment'
        },
        'daily': {
            'daily_execution_mode': 'optimal_timing'
        },
        'dashboard': {},
        'flexibility': {
            'global_flexibility_mode': 'adaptive'
        }
    }
    
    # 2. Create integration system
    scheduling_system = SchedulingSystemIntegration(config)
    
    # 3. Connect to existing trading system (mock for example)
    # In real usage, you would connect to actual PaperTrader and TradeLifecycleManager
    print("Note: In real usage, connect to actual PaperTrader and TradeLifecycleManager")
    
    # 4. Start scheduling system
    if scheduling_system.start_scheduling():
        print("✅ Scheduling system started successfully")
    else:
        print("❌ Failed to start scheduling system")
        return
    
    # 5. Get current overview
    overview = scheduling_system.dashboard.get_scheduling_overview()
    print(f"\n📊 Current Scheduling Overview:")
    print(f"   System Health: {overview['system_health']}")
    print(f"   Current Plans: {list(overview['current_plans'].keys())}")
    
    # 6. Process daily trades
    daily_result = scheduling_system.process_daily_trades()
    print(f"\n📅 Daily Trade Processing:")
    print(f"   Status: {daily_result['status']}")
    if daily_result['status'] == 'success':
        print(f"   Scheduled: {daily_result['scheduled_count']}")
        print(f"   Processed: {daily_result['processed_count']}")
    
    # 7. Get integration status
    integration_status = scheduling_system.get_integration_status()
    print(f"\n🔗 Integration Status:")
    print(f"   Paper Trader Connected: {integration_status['connection_status']['paper_trader']}")
    print(f"   Scheduling Active: {integration_status['integration_state']['scheduling_active']}")
    print(f"   System Health: {integration_status['system_health']}")


def example_manual_overrides():
    """Example of manual overrides"""
    
    print("\n=== Manual Override Examples ===")
    
    # Initialize scheduling system
    config = {
        'yearly': {'annual_capital': 1000000},
        'flexibility': {'global_flexibility_mode': 'adaptive'}
    }
    scheduling_system = SchedulingSystemIntegration(config)
    
    # Example 1: Override yearly capital allocation
    print("\n1. Yearly Capital Override:")
    override_result = scheduling_system.handle_manual_override({
        'level': 'yearly',
        'mode': 'aggressive',
        'reason': 'Market opportunity detected',
        'approved_by': 'trader_manager'
    })
    print(f"   Result: {override_result['status']}")
    
    # Example 2: Override monthly control mode
    print("\n2. Monthly Control Mode Override:")
    override_result = scheduling_system.handle_manual_override({
        'level': 'monthly',
        'control_mode': 'manual_approval',
        'reason': 'Increased market volatility',
        'approved_by': 'risk_manager'
    })
    print(f"   Result: {override_result['status']}")
    
    # Example 3: Approve monthly plan
    print("\n3. Monthly Plan Approval:")
    scheduling_system.dashboard.approve_monthly_plan(
        month=datetime.now().month,
        approved_by='portfolio_manager',
        allocation_override=75000  # $75K instead of planned
    )
    print("   Monthly plan approved with allocation override")
    
    # Example 4: Switch flexibility mode
    print("\n4. Flexibility Mode Switch:")
    switch_record = scheduling_system.flexibility_manager.switch_flexibility_mode(
        FlexibilityMode.OPPORTUNISTIC,
        'Strong bullish sentiment detected',
        {
            'expected_performance_impact': 0.15,
            'risk_level': 'high'
        }
    )
    print(f"   Switched to: {switch_record['new_mode']}")
    print(f"   Reason: {switch_record['reason']}")


def example_flexibility_management():
    """Example of flexibility management"""
    
    print("\n=== Flexibility Management Examples ===")
    
    # Initialize flexibility manager
    config = {'global_flexibility_mode': 'adaptive'}
    flexibility_manager = FlexibilityManager(config)
    
    # Example 1: Get current mode parameters
    print("\n1. Current Mode Parameters:")
    current_params = flexibility_manager.get_current_mode_parameters()
    print(f"   Mode: {flexibility_manager.current_mode.value}")
    print(f"   Auto Adjust: {current_params['auto_adjust']}")
    print(f"   Manual Override: {current_params['manual_override']}")
    print(f"   Description: {current_params['description']}")
    
    # Example 2: Get mode performance summary
    print("\n2. Mode Performance Summary:")
    performance_summary = flexibility_manager.get_mode_performance_summary()
    for mode, performance in performance_summary.items():
        print(f"   {mode}:")
        print(f"     Overall Score: {performance['overall_score']:.3f}")
        print(f"     Return: {performance['total_return']:.3f}")
        print(f"     Sharpe: {performance['sharpe_ratio']:.3f}")
    
    # Example 3: Get mode switch recommendations
    print("\n3. Mode Switch Recommendations:")
    recommendations = flexibility_manager.get_mode_switch_recommendations()
    for i, rec in enumerate(recommendations, 1):
        print(f"   {i}. Recommended: {rec['recommended_mode']}")
        print(f"      Reason: {rec['reason']}")
        print(f"      Priority: {rec['priority']}")
        print(f"      Confidence: {rec['confidence']:.2f}")
    
    # Example 4: Check for automatic mode switch
    print("\n4. Automatic Mode Switch Check:")
    auto_switch = flexibility_manager.should_auto_switch_mode()
    if auto_switch:
        print(f"   Recommendation: Switch to {auto_switch[0].value}")
        print(f"   Reason: {auto_switch[1]}")
    else:
        print("   No automatic switch recommended")


def example_scheduler_usage():
    """Example of using individual schedulers"""
    
    print("\n=== Individual Scheduler Usage ===")
    
    # Example 1: Yearly Scheduler
    print("\n1. Yearly Scheduler:")
    yearly_config = {
        'yearly_mode': 'balanced',
        'annual_capital': 2000000
    }
    yearly_scheduler = YearlyScheduler(yearly_config)
    
    yearly_result = yearly_scheduler.create_schedule(2024)
    yearly_plan = yearly_result['plan']
    print(f"   Year: {yearly_plan['year']}")
    print(f"   Total Capital: ${yearly_plan['total_capital']:,.0f}")
    print(f"   Strategic Mode: {yearly_plan['strategic_mode']}")
    print(f"   Quarterly Allocations: {len(yearly_plan['quarterly_allocations'])}")
    
    # Example 2: Quarterly Scheduler
    print("\n2. Quarterly Scheduler:")
    quarterly_config = {
        'mode_switching': True,
        'default_quarterly_mode': 'balanced'
    }
    quarterly_scheduler = QuarterlyScheduler(quarterly_config)
    
    quarterly_result = quarterly_scheduler.create_schedule(2, 2024, yearly_plan)
    quarterly_plan = quarterly_result['plan']
    print(f"   Quarter: Q{quarterly_plan['quarter']}")
    print(f"   Selected Mode: {quarterly_plan['selected_mode']}")
    print(f"   Base Allocation: ${quarterly_plan['base_allocation']:,.0f}")
    print(f"   Strategy Weights: {list(quarterly_plan['strategy_weights'].keys())}")
    
    # Example 3: Monthly Scheduler
    print("\n3. Monthly Scheduler:")
    monthly_config = {
        'monthly_control_mode': 'semi_automatic'
    }
    monthly_scheduler = MonthlyScheduler(monthly_config)
    
    monthly_result = monthly_scheduler.create_schedule(11, 2024, quarterly_plan)
    monthly_plan = monthly_result['plan']
    print(f"   Month: {monthly_plan['month']}")
    print(f"   Control Mode: {monthly_plan['control_mode']}")
    print(f"   Final Allocation: ${monthly_plan['final_allocation']:,.0f}")
    print(f"   Approval Status: {monthly_plan['approval_status']}")
    
    # Example 4: Weekly Scheduler
    print("\n4. Weekly Scheduler:")
    weekly_config = {
        'prioritization_method': 'temporal_alignment'
    }
    weekly_scheduler = WeeklyScheduler(weekly_config)
    
    # Mock signals for demonstration
    mock_signals = [
        {
            'id': 'signal_1',
            'symbol': 'AAPL',
            'direction': 'long',
            'strategy': 'volatility_breakout',
            'confidence': 0.8,
            'expected_return': 0.15,
            'risk': 0.05,
            'alpha_score': 0.7,
            'consensus_score': 0.75
        },
        {
            'id': 'signal_2',
            'symbol': 'MSFT',
            'direction': 'long',
            'strategy': 'silent_compounder',
            'confidence': 0.6,
            'expected_return': 0.08,
            'risk': 0.03,
            'alpha_score': 0.5,
            'consensus_score': 0.6
        }
    ]
    
    weekly_result = weekly_scheduler.create_schedule(15, 2024, monthly_plan)
    weekly_plan = weekly_result['plan']
    print(f"   Week: {weekly_plan['week']}")
    print(f"   Total Budget: ${weekly_plan['total_budget']:,.0f}")
    print(f"   Prioritization Method: {weekly_plan['prioritization_method']}")
    
    # Prioritize signals
    allocated_signals = weekly_scheduler.prioritize_signals(mock_signals, weekly_plan)
    print(f"   Allocated Signals: {len(allocated_signals)}")
    for i, signal in enumerate(allocated_signals, 1):
        print(f"     {i}. {signal['symbol']} - Score: {signal['prioritization_score']:.3f} - "
              f"Budget: ${signal['allocated_budget']:,.0f}")
    
    # Example 5: Daily Scheduler
    print("\n5. Daily Scheduler:")
    daily_config = {
        'daily_execution_mode': 'optimal_timing'
    }
    daily_scheduler = DailyScheduler(daily_config)
    
    daily_result = daily_scheduler.create_schedule(datetime.now(), allocated_signals)
    daily_plan = daily_result['plan']
    print(f"   Date: {daily_plan['date'].strftime('%Y-%m-%d')}")
    print(f"   Execution Mode: {daily_plan['execution_mode']}")
    print(f"   Scheduled Trades: {len(daily_plan['scheduled_trades'])}")
    
    for trade in daily_plan['scheduled_trades']:
        print(f"     {trade['symbol']} - {trade['execution_time'].strftime('%H:%M')} - "
              f"{trade['order_type']} - {trade['position_size']:.3f}")


def example_dashboard_usage():
    """Example of using the scheduling dashboard"""
    
    print("\n=== Dashboard Usage Examples ===")
    
    # Initialize dashboard
    dashboard_config = {}
    dashboard = SchedulingDashboard(dashboard_config)
    
    # Example 1: Get scheduling overview
    print("\n1. Scheduling Overview:")
    overview = dashboard.get_scheduling_overview()
    print(f"   System Health: {overview['system_health']}")
    print(f"   Pending Actions: {len(overview['pending_actions'])}")
    print(f"   Active Alerts: {len(overview['alerts'])}")
    
    # Example 2: Get flexibility status
    print("\n2. Flexibility Status:")
    flexibility_status = overview['flexibility_status']
    for level, status in flexibility_status.items():
        print(f"   {level}: {status['current_mode'] or 'N/A'}")
    
    # Example 3: Get performance vs plan
    print("\n3. Performance vs Plan:")
    performance = overview['performance_vs_plan']
    for level, perf in performance.items():
        if perf.get('status') != 'no_plan':
            print(f"   {level}: {perf.get('status', 'unknown')}")
            if 'performance_ratio' in perf:
                print(f"     Performance Ratio: {perf['performance_ratio']:.2f}")
    
    # Example 4: Get override history
    print("\n4. Override History:")
    overrides = overview['override_history']
    for i, override in enumerate(overrides[:5], 1):  # Last 5 overrides
        print(f"   {i}. {override['timestamp'].strftime('%Y-%m-%d %H:%M')} - "
              f"{override['level']} - {override['reason']}")
    
    # Example 5: Get dashboard summary
    print("\n5. Dashboard Summary:")
    summary = dashboard.get_dashboard_summary()
    print(f"   System Health: {summary['system_health']}")
    print(f"   Total Overrides: {summary['key_metrics']['total_overrides']}")
    print(f"   Pending Approvals: {summary['key_metrics']['pending_approvals']}")
    print(f"   Active Alerts: {summary['key_metrics']['active_alerts']}")


def example_integration_with_trading_system():
    """Example of full integration with trading system"""
    
    print("\n=== Full Integration Example ===")
    
    # This would be the actual integration in production
    print("Note: This example shows how to integrate with the actual trading system")
    
    integration_code = '''
# Production Integration Example
from app.trading.scheduling import SchedulingSystemIntegration
from app.trading.paper_trader import PaperTrader
from app.trading.trade_lifecycle import TradeLifecycleManager

# 1. Initialize components
config = {
    'yearly': {'annual_capital': 1000000, 'yearly_mode': 'balanced'},
    'quarterly': {'mode_switching': True},
    'monthly': {'monthly_control_mode': 'semi_automatic'},
    'weekly': {'prioritization_method': 'temporal_alignment'},
    'daily': {'daily_execution_mode': 'optimal_timing'},
    'dashboard': {},
    'flexibility': {'global_flexibility_mode': 'adaptive'}
}

# 2. Create scheduling system
scheduling_system = SchedulingSystemIntegration(config)

# 3. Connect to existing trading infrastructure
paper_trader = PaperTrader(config)  # Your existing paper trader
trade_lifecycle = TradeLifecycleManager(config)  # Your existing trade lifecycle

scheduling_system.connect_to_trading_system(paper_trader, trade_lifecycle)

# 4. Start scheduling
scheduling_system.start_scheduling()

# 5. Main trading loop
while True:
    try:
        # Process daily trades
        daily_result = scheduling_system.process_daily_trades()
        
        if daily_result['status'] == 'success':
            print(f"Processed {daily_result['processed_count']} trades for {daily_result['date']}")
        
        # Sync performance data
        sync_result = scheduling_system.sync_performance_data()
        
        # Get system status
        status = scheduling_system.get_integration_status()
        
        if status['system_health'] == 'critical':
            print("System health critical - checking for issues")
            # Handle system issues
            break
        
        # Wait for next cycle
        import time
        time.sleep(300)  # 5 minutes
        
    except KeyboardInterrupt:
        print("Shutting down scheduling system...")
        scheduling_system.stop_scheduling()
        break
    except Exception as e:
        print(f"Error in main loop: {e}")
        # Continue with error handling
'''
    
    print("Integration Code:")
    print(integration_code)


def main():
    """Main function to run all examples"""
    
    print("🚀 Trade Scheduling System - Usage Examples")
    print("=" * 60)
    
    try:
        # Run all examples
        example_basic_usage()
        example_manual_overrides()
        example_flexibility_management()
        example_scheduler_usage()
        example_dashboard_usage()
        example_integration_with_trading_system()
        
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        logger.error(f"Error in examples: {e}")


if __name__ == "__main__":
    main()
