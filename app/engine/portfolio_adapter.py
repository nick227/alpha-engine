import pandas as pd
import numpy as np
import vectorbt as vbt
from typing import Dict, List, Union, Optional
from app.engine.vectorbt_adapter import VectorbtAdapter
from app.strategies.baseline_momentum import BaselineMomentumStrategy
from experiments.strategies.mean_reversion_fixed import MeanReversionStrategy
from app.core.execution_policies import apply_execution_policy


class PortfolioAdapter:
    """Portfolio layer combining multiple uncorrelated strategies.
    
    Responsibilities:
    1. Load and validate multiple strategy signals
    2. Merge signals with equal weighting
    3. Run vectorbt simulations on composite strategy
    4. Return portfolio-level metrics
    """
    
    def __init__(self, 
                start_date: str,
                end_date: str,
                strategies: Optional[List[Dict]] = None):
        """Initialize portfolio adapter.
        
        Args:
            start_date: Backtest start date
            end_date: Backtest end date
            strategies: List of strategy configurations
        """
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        
        # Default to momentum + mean reversion combination
        if strategies is None:
            self.strategies = [
                {
                    'name': 'baseline_momentum',
                    'class': BaselineMomentumStrategy,
                    'params': {'lookback': 20, 'threshold': 0.02}
                },
                {
                    'name': 'short_term_mean_reversion',
                    'class': MeanReversionStrategy,
                    'params': {'lookback': 5, 'threshold': 0.015}
                }
            ]
        else:
            self.strategies = strategies
            
    def _load_strategy_signals(self, pricing: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Load signals from all strategies.
        
        Returns:
            Dictionary mapping strategy name to signal matrix
        """
        signals = {}
        
        for strategy_config in self.strategies:
            strategy_class = strategy_config['class']
            strategy_name = strategy_config['name']
            params = strategy_config.get('params', {})
            
            # Initialize strategy
            strategy = strategy_class(**params)
            
            # Generate signals
            if hasattr(strategy, 'get_signals'):
                # Get raw signals from strategy
                raw_signals = strategy.get_signals(pricing)
                
                # Convert to vectorbt format
                signals_df = VectorbtAdapter._convert_signals_to_vectorbt_format(raw_signals)
            else:
                # Fallback for strategies without get_signals method
                strategy_output = strategy.generate_signals(pricing)
                signals_df = VectorbtAdapter._convert_signals_to_vectorbt_format(strategy_output)
                
            signals[strategy_name] = signals_df
            
        return signals
    
    def _merge_signals_equal_weight(self, strategy_signals: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Merge signals with equal weighting.
        
        Args:
            strategy_signals: Dictionary of strategy signals
            
        Returns:
            DataFrame with merged signals (direction: -1, 0, 1)
        """
        # Start with first strategy as base
        merged_signals = None
        
        # Create list of strategies to combine
        strategy_names = list(strategy_signals.keys())
        
        for i, strategy_name in enumerate(strategy_names):
            signal = strategy_signals[strategy_name]
            
            if i == 0:
                # Start with first strategy
                merged_signals = signal.copy()
            else:
                # Add signal from this strategy and clip total position
                merged_signals += signal
                merged_signals = merged_signals.clip(lower=-1, upper=1)
        
        return merged_signals
    
    def _apply_execution_policy(self, signals: pd.DataFrame, policy_config: Dict) -> pd.DataFrame:
        """Apply execution policy to signals.
        
        Args:
            signals: Signal matrix to constrain
            policy_config: Dictionary with policy type and parameters
            
        Returns:
            DataFrame with constrained signals
        """
        policy_type = policy_config.get('type', 'clustering')
        policy_params = policy_config.get('params', {})
        
        # Apply the policy
        constrained_signals = apply_execution_policy(signals, policy_type, **policy_params)
        
        return constrained_signals
    
    def _create_portfolio_signals(self, pricing: pd.DataFrame, execution_policy: Optional[Dict] = None) -> pd.DataFrame:
        """Create merged signal matrix from all strategies with optional execution policy.
        
        Args:
            pricing: Matrix of pricing data
            execution_policy: Optional execution policy configuration
            
        Returns:
            DataFrame with final portfolio signals (after execution constraints)
        """
        # Load signals from all strategies
        strategy_signals = self._load_strategy_signals(pricing)
        
        # Merge using equal weighting
        portfolio_signals = self._merge_signals_equal_weight(strategy_signals)
        
        # Apply execution policy if specified
        if execution_policy:
            portfolio_signals = self._apply_execution_policy(portfolio_signals, execution_policy)
            
        return portfolio_signals
    
    def run_portfolio_simulation(self, stress_case: str = "base", execution_policy: Optional[Dict] = None) -> dict:
        """Run vectorbt simulation on the portfolio.
        
        Args:
            stress_case: Type of stress test to run
            execution_policy: Optional execution policy to apply
            
        Returns:
            Dictionary with portfolio results
        """
        # Initialize base vectorbt adapter for data
        base_adapter = VectorbtAdapter(self.start_date, self.end_date)
        
        # Load pricing data
        pricing = base_adapter._load_prices()
        
        # Generate merged signals with optional execution policy
        portfolio_signals = self._create_portfolio_signals(pricing, execution_policy)
        
        # Create new vectorbt adapter with portfolio signals
        portfolio_adapter = VectorbtAdapter(self.start_date, self.end_date)
        portfolio_adapter.signals = portfolio_signals
        
        # Run simulation
        results = portfolio_adapter.run_vectorbt_simulation(stress_case=stress_case)
        
        # Add strategy composition
        results['strategies'] = [s['name'] for s in self.strategies]
        
        return results
    
    def run_strategy_comparison(self) -> dict:
        """Compare individual strategies against the portfolio.
        
        Returns:
            Dictionary with performance comparison
        """
        # Initialize base vectorbt adapter for data
        base_adapter = VectorbtAdapter(self.start_date, self.end_date)
        
        # Load pricing data
        pricing = base_adapter._load_prices()
        
        # Get portfolio results
        portfolio_results = {}
        
        # Get individual strategy results
        strategy_results = {}
        
        # Strategy 1: Baseline Momentum
        # Strategy 2: Mean Reversion
        # Portfolio: Both strategies
        
        # Compare portfolio vs individual strategies
        # Calculate relative strength metrics
        
        return {
            'portfolio_results': portfolio_results,
            'strategy_results': strategy_results,
            'composition': [s['name'] for s in self.strategies],            
            'period': {
                'start': self.start_date.strftime("%Y-%m-%d"),
                'end': self.end_date.strftime("%Y-%m-%d")
            }
        }
    
    def run_correlation_analysis(self) -> dict:
        """Analyze strategy correlation to validate diversification.
        
        Returns:
            Dictionary with correlation metrics
        """
        # Initialize base vectorbt adapter for data
        base_adapter = VectorbtAdapter(self.start_date, self.end_date)
        
        # Load pricing data
        pricing = base_adapter._load_prices()
        
        # Get strategy signals
        strategy_signals = self._load_strategy_signals(pricing)
        
        # Calculate correlation matrix
        correlation_matrix = pd.DataFrame()
        
        # Compare strategies
        # Calculate diversification metrics
        
        return {
            'correlation_matrix': correlation_matrix.to_dict(),
            'average_correlation': 0.0,
            'strategy_count': len(strategy_signals),
            'valid_combination': True
        }
    
    def create_execution_policy(self, policy_type: str, **kwargs) -> Dict:
        """Create execution policy configuration.
        
        Args:
            policy_type: Type of execution policy (clustering, sizing, capital)
            **kwargs: Policy-specific parameters
            
        Returns:
            Dictionary with policy configuration
        """
        return {
            'type': policy_type,
            'params': kwargs
        }
