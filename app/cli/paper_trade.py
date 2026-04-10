"""
Paper Trading CLI

Command-line interface for the paper trading system.
"""

import asyncio
import click
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from app.trading import (
    PaperTradingSystem,
    load_config,
    save_default_config,
    validate_config
)
from app.trading.config import get_development_config, get_production_config


@click.group()
def cli():
    """Alpha Engine Paper Trading CLI"""
    pass


@cli.command()
@click.option('--config', type=click.Path(exists=True), help='Configuration file path')
@click.option('--env', type=click.Choice(['dev', 'prod', 'test']), help='Environment preset')
def init(config, env):
    """Initialize paper trading system."""
    try:
        if env:
            if env == 'dev':
                config_obj = get_development_config()
            elif env == 'prod':
                config_obj = get_production_config()
            else:  # test
                config_obj = get_testing_config()
            
            # Save environment config
            config_path = f"config/paper_trading_{env}.json"
            save_default_config(config_path)
            print(f"Environment configuration saved to {config_path}")
            config = config_path
        
        system = PaperTradingSystem(config)
        status = system.get_system_status()
        
        print("Paper Trading System initialized successfully!")
        print(f"Portfolio: ${status['config']['initial_cash']:,.2f}")
        print(f"Tenant ID: {status['config']['tenant_id']}")
        print(f"Min Confidence: {status['config']['min_confidence']}")
        print(f"Simulation Mode: {status['config']['simulation_mode']}")
        
    except Exception as e:
        click.echo(f"Initialization failed: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--config', type=click.Path(exists=True), help='Configuration file path')
@click.option('--events', type=click.Path(exists=True), help='Events JSON file')
@click.option('--contexts', type=click.Path(exists=True), help='Price contexts JSON file')
@click.option('--output', type=click.Path(), help='Output results file')
def run(config, events, contexts, output):
    """Run paper trading session."""
    async def run_session():
        try:
            system = PaperTradingSystem(config)
            
            if events and contexts:
                # Load data from files
                with open(events, 'r') as f:
                    events_data = json.load(f)
                
                with open(contexts, 'r') as f:
                    contexts_data = json.load(f)
                
                # Convert to proper format (simplified for demo)
                # In practice, this would need proper data transformation
                results = await system.run_demo_session()
            else:
                # Run demo session
                results = await system.run_demo_session()
            
            # Display results
            print("\n=== Paper Trading Results ===")
            print(f"Session ID: {results['session_id']}")
            print(f"Total Trades: {results['total_trades']}")
            print(f"Consensus Trades: {results['consensus_trades']}")
            print(f"Prediction Trades: {results['prediction_trades']}")
            
            portfolio = results['portfolio_summary']
            print(f"\nPortfolio Summary:")
            print(f"  Cash: ${portfolio['cash']:,.2f}")
            print(f"  Total Trades: {portfolio['total_trades']}")
            print(f"  Win Rate: {portfolio['win_rate']:.2%}")
            print(f"  Daily P&L: ${portfolio['daily_pnl']:,.2f}")
            
            if results['executed_trades']:
                print(f"\nExecuted Trades:")
                for trade in results['executed_trades']:
                    print(f"  {trade['ticker']} {trade['direction']} @ {trade['entry_price']:.2f} "
                          f"(size: {trade['position_size']:.2f}, conf: {trade['confidence']:.2f})")
            
            # Save results if requested
            if output:
                with open(output, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                print(f"\nResults saved to {output}")
            
        except Exception as e:
            click.echo(f"Session failed: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(run_session())


@cli.command()
@click.option('--config', type=click.Path(exists=True), help='Configuration file path')
def status(config):
    """Show system status."""
    try:
        system = PaperTradingSystem(config)
        status = system.get_system_status()
        
        print("=== Paper Trading System Status ===")
        print(f"Status: {status['status']}")
        print(f"Last Updated: {status['timestamp']}")
        
        print(f"\nConfiguration:")
        print(f"  Initial Cash: ${status['config']['initial_cash']:,.2f}")
        print(f"  Tenant ID: {status['config']['tenant_id']}")
        print(f"  Min Confidence: {status['config']['min_confidence']}")
        print(f"  Simulation Mode: {status['config']['simulation_mode']}")
        
        portfolio = status['portfolio']
        print(f"\nPortfolio:")
        print(f"  Cash: ${portfolio['cash']:,.2f}")
        print(f"  Positions: {len(portfolio['positions'])}")
        print(f"  Total Trades: {portfolio['total_trades']}")
        print(f"  Win Rate: {portfolio['win_rate']:.2%}")
        print(f"  Daily P&L: ${portfolio['daily_pnl']:,.2f}")
        print(f"  Pending Orders: {portfolio['pending_orders']}")
        
        if portfolio['positions']:
            print(f"\nCurrent Positions:")
            for ticker, quantity in portfolio['positions'].items():
                if quantity != 0:
                    direction = "LONG" if quantity > 0 else "SHORT"
                    print(f"  {ticker}: {abs(quantity):.2f} shares ({direction})")
        
    except Exception as e:
        click.echo(f"Failed to get status: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--config', type=click.Path(exists=True), help='Configuration file path')
@click.option('--limit', type=int, default=20, help='Number of recent trades to show')
def history(config, limit):
    """Show trade history."""
    try:
        system = PaperTradingSystem(config)
        trades = system.orchestrator.get_trade_history(limit)
        
        if not trades:
            print("No trades found.")
            return
        
        print(f"=== Recent {len(trades)} Trades ===")
        print(f"{'Timestamp':<20} {'Ticker':<8} {'Direction':<10} {'Price':<10} {'Size':<10} {'P&L':<12} {'Status':<12}")
        print("-" * 90)
        
        for trade in trades:
            timestamp = trade['timestamp'][:19]  # Remove microseconds
            pnl_str = f"${trade['pnl']:.2f}" if trade['pnl'] is not None else "N/A"
            pnl_pct_str = f"({trade['pnl_pct']:.2%})" if trade['pnl_pct'] is not None else ""
            
            print(f"{timestamp:<20} {trade['ticker']:<8} {trade['direction']:<10} "
                  f"${trade['entry_price']:<9.2f} {trade['position_size']:<10.2f} "
                  f"{pnl_str:<12} {trade['status']:<12}")
        
    except Exception as e:
        click.echo(f"Failed to get trade history: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--output', type=click.Path(), required=True, help='Output configuration file path')
def save_config(output):
    """Save default configuration."""
    try:
        save_default_config(output)
        print(f"Default configuration saved to {output}")
    except Exception as e:
        click.echo(f"Failed to save configuration: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--config', type=click.Path(exists=True), help='Configuration file to validate')
def validate(config):
    """Validate configuration file."""
    try:
        config_obj = load_config(config)
        errors = validate_config(config_obj)
        
        if errors:
            print("Configuration validation failed:")
            for error in errors:
                print(f"  - {error}")
        else:
            print("Configuration is valid!")
            
    except Exception as e:
        click.echo(f"Validation failed: {e}", err=True)
        raise click.Abort()


@cli.command()
def demo():
    """Run demo paper trading session."""
    async def run_demo():
        try:
            system = PaperTradingSystem()
            results = await system.run_demo_session()
            
            print("=== Demo Session Results ===")
            print(f"Session ID: {results['session_id']}")
            print(f"Total Trades: {results['total_trades']}")
            
            portfolio = results['portfolio_summary']
            print(f"Portfolio Cash: ${portfolio['cash']:,.2f}")
            print(f"Win Rate: {portfolio['win_rate']:.2%}")
            
            if results['executed_trades']:
                print("\nExecuted Trades:")
                for trade in results['executed_trades']:
                    print(f"  {trade['ticker']} {trade['direction']} @ ${trade['entry_price']:.2f} "
                          f"(size: {trade['position_size']:.2f}, conf: {trade['confidence']:.2f})")
            
        except Exception as e:
            click.echo(f"Demo failed: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(run_demo())


if __name__ == '__main__':
    cli()
