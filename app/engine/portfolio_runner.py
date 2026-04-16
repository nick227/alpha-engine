import sys
import os
import pandas as pd
import json
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Add experiments directory to Python path
experiments_dir = Path(__file__).parent.parent.parent / 'experiments'
sys.path.append(str(experiments_dir))

# Now try to import modules
try:
    from app.engine.vectorbt_adapter import VectorbtAdapter
    from app.engine.portfolio_adapter import PortfolioAdapter
    from experiments.strategies.baseline_momentum import BaselineMomentum
    from experiments.strategies.mean_reversion_fixed import MeanReversionStrategy
    
    import_success = True
except ImportError as e:
    print(f"Import error: {e}")
    import_success = False

def load_price_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Load price data for the given date range."""
    adapter = VectorbtAdapter(start_date, end_date)
    return adapter._load_prices()

def run_individual_strategy(
    strategy_config: dict,
    pricing: pd.DataFrame,
    start_date: str,
    end_date: str
) -> dict:
    """Run simulation for a single strategy."""
    # Initialize strategy
    strategy_class = strategy_config['class']
    strategy_name = strategy_config['name']
    params = strategy_config.get('params', {})
    
    strategy = strategy_class(**params)
    
    # Generate signals
    raw_signals = strategy.get_signals(pricing)
    
    # Convert to vectorbt format
    signals_df = VectorbtAdapter._convert_signals_to_vectorbt_format(raw_signals)
    
    # Create adapter with strategy signals
    strategy_adapter = VectorbtAdapter(start_date, end_date)
    strategy_adapter.signals = signals_df
    
    # Run simulation
    results = strategy_adapter.run_vectorbt_simulation()
    
    return {
        'name': strategy_name,
        'config': strategy_config,
        'signals': signals_df,
        'results': results
    }

def run_portfolio_analysis():
    """Run portfolio analysis and comparison."""
    if not import_success:
        print("Failed to import required modules")
        return None
        
    # Define time period
    start_date = "2023-01-01"
    end_date = "2024-01-01"
    
    # Load pricing data
    try:
        pricing = load_price_data(start_date, end_date)
    except Exception as e:
        print(f"Error loading price data: {e}")
        return None
    
    # Define strategies
    strategies = [
        {
            'name': 'baseline_momentum',
            'class': BaselineMomentum,
            'params': {'lookback': 20, 'threshold': 0.02}
        },
        {
            'name': 'short_term_mean_reversion',
            'class': MeanReversionStrategy,
            'params': {'lookback': 5, 'threshold': 0.015}
        }
    ]
    
    # Strategy 1: Baseline Momentum
    print("Running Baseline Momentum Strategy...")
    momentum_result = run_individual_strategy(
        strategies[0], pricing, start_date, end_date
    )
    
    # Strategy 2: Mean Reversion
    print("Running Mean Reversion Strategy...")
    mean_reversion_result = run_individual_strategy(
        strategies[1], pricing, start_date, end_date
    )
    
    # Portfolio: Both strategies
    print("Running Portfolio Simulation...")
    try:
        portfolio_adapter = PortfolioAdapter(start_date, end_date, strategies)
        portfolio_comparison = portfolio_adapter.run_portfolio_simulation()
        correlation_analysis = portfolio_adapter.run_correlation_analysis()
    except Exception as e:
        print(f"Error running portfolio simulation: {e}")
        return None
    
    # Build results
    results = {
        'metadata': {
            'run_timestamp': datetime.now().isoformat(),
            'period': {
                'start': start_date,
                'end': end_date
            }
        },
        'individual_strategies': {
            'momentum': {
                'config': strategies[0],
                'results': momentum_result['results']
            },
            'mean_reversion': {
                'config': strategies[1],
                'results': mean_reversion_result['results']
            }
        },
        'portfolio': {
            'config': {
                'strategies': [s['name'] for s in strategies],
                'combination': 'equal_weight'
            },
            'results': portfolio_comparison,
            'correlation': correlation_analysis
        }
    }
    
    return results

def format_number(n: float, precision: int = 2) -> str:
    """Format number with thousand separators."""
    return f"{n:,.{precision}f}"

def generate_dashboard(results: dict) -> str:
    """Generate text dashboard from analysis results."""
    if results is None:
        return "Portfolio analysis failed during execution"
        
    # Extract metrics
    try:
        momentum = results['individual_strategies']['momentum']['results']
        mean_rev = results['individual_strategies']['mean_reversion']['results']
        portfolio = results['portfolio']['results']
    except KeyError as e:
        return f"Missing data in results structure: {e}"
    
    # Format dashboard
    dashboard = f"""
Portfolio Analysis Dashboard
{(datetime.now().strftime('%Y-%m-%d %H:%M'))}

Time Period: {results['metadata']['period']['start']} → {results['metadata']['period']['end']}

STRATEGY COMPOSITION
──────────────────────────────────────────────────
1. Baseline Momentum (lookback: {results['individual_strategies']['momentum']['config']['params']['lookback']}, threshold: {results['individual_strategies']['momentum']['config']['params']['threshold']})
2. Mean Reversion (lookback: {results['individual_strategies']['mean_reversion']['config']['params']['lookback']}, threshold: {results['individual_strategies']['mean_reversion']['config']['params']['threshold']})

STRATEGY CORRELATION
──────────────────────────────────────────────────
Average correlation: {format_number(results['portfolio']['correlation']['average_correlation'] * 100, 1)}%
{'⚠️' if results['portfolio']['correlation']['average_correlation'] > 0.3 else '✅'} Diversification {'weak' if results['portfolio']['correlation']['average_correlation'] > 0.3 else 'strong'}

PERFORMANCE COMPARISON
──────────────────────────────────────────────────────────────────────
               | Momentum | Mean Rev | Portfolio | Change (Momentum → Portfolio)
───────────────|──────────|──────────|───────────|────────────────────────────
Return         | {format_number(momentum['total_return'] * 100)}%   | {format_number(mean_rev['total_return'] * 100)}%   | {format_number(portfolio['total_return'] * 100)}%   | {"↑" if portfolio["total_return"] > momentum["total_return"] else "↓"}{format_number((portfolio["total_return"] - momentum["total_return"]) * 100, 1)}%
Sharpe         | {format_number(momentum['sharpe_ratio'], 2)}   | {format_number(mean_rev['sharpe_ratio'], 2)}   | {format_number(portfolio['sharpe_ratio'], 2)}   | {"↑" if portfolio["sharpe_ratio"] > momentum["sharpe_ratio"] else "↓"}{format_number((portfolio["sharpe_ratio"] - momentum["sharpe_ratio"]), 2)}
Max Drawdown   | {format_number(momentum['max_drawdown'] * 100, 1)}%   | {format_number(mean_rev['max_drawdown'] * 100, 1)}%   | {format_number(portfolio['max_drawdown'] * 100, 1)}%   | {"↑" if portfolio["max_drawdown"] > momentum["max_drawdown"] else "↓"}{format_number((portfolio["max_drawdown"] - momentum["max_drawdown"]) * 100, 1)}%
Win Rate       | {format_number(momentum['win_rate'] * 100)}%   | {format_number(mean_rev['win_rate'] * 100)}%   | {format_number(portfolio['win_rate'] * 100)}%   | {"↑" if portfolio["win_rate"] > momentum["win_rate"] else "↓"}{format_number((portfolio["win_rate"] - momentum["win_rate"]) * 100, 1)}%
Total Trades   | {format_number(momentum['total_trades'])}    | {format_number(mean_rev['total_trades'])}    | {format_number(portfolio['total_trades'])}    | {"↑" if portfolio["total_trades"] > momentum["total_trades"] else "↓"}{format_number(portfolio["total_trades"] - momentum["total_trades"])}

CONCLUSION
──────────────────────────────────────────────────
{'✅ Diversification improves strategy robustness' if portfolio['sharpe_ratio'] > momentum['sharpe_ratio'] and portfolio['max_drawdown'] < momentum['max_drawdown'] else '⚠️ Portfolio has mixed performance'}
"""

    return dashboard

if __name__ == "__main__":
    print("Starting portfolio analysis...")
    print(f"Python path: {sys.path}")
    
    results = run_portfolio_analysis()
    if results is not None:
        dashboard = generate_dashboard(results)
        print(dashboard)
        
        # Save results
        try:
            with open("portfolio_analysis_results.json", "w") as f:
                json.dump(results, f, indent=2)
            print("\nResults saved to 'portfolio_analysis_results.json'")
        except Exception as e:
            print(f"Error saving results: {e}")
    else:
        print("Portfolio analysis failed to complete")
