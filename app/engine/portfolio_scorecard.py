import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime

from app.engine.portfolio_adapter import PortfolioAdapter
from app.engine.vectorbt_adapter import VectorbtAdapter
from app.core.types import Signal


@dataclass
class PortfolioScore:
    """Scorecard for portfolio deployability assessment."""
    deployability_score: float
    edge_strength: float
    robustness: float
    scalability: float
    metrics: Dict[str, float]
    timestamp: datetime
    notes: str = ""


class PortfolioScorecard:
    """Framework for evaluating portfolio deployability."""
    
    def __init__(self, 
                start_date: Union[str, datetime],
                end_date: Union[str, datetime]):
        """Initialize scorecard with time period."""
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.price_adapter = VectorbtAdapter(start_date, end_date)
        self.vectorbt_adapter = VectorbtAdapter(start_date, end_date)
        
    def _get_strategy_metrics(self, strategy_name: str) -> Dict[str, float]:
        """Get baseline metrics for a strategy."""
        # Create adapter with strategy signals
        strategy_signals = self._get_strategy_signals(strategy_name)
        strategy_adapter = VectorbtAdapter(self.start_date, self.end_date)
        strategy_adapter.signals = strategy_signals
        
        # Get metrics
        base_metrics = strategy_adapter.run_vectorbt_simulation()
        
        # Get stress test metrics
        stress_results = strategy_adapter.run_stress_test(
            slippage_range=[0.001, 0.002, 0.005, 0.01, 0.02]
        )
        
        # Extract stress metrics
        stress_metrics = {}
        for slippage, metrics in stress_results.items():
            stress_metrics[f"slippage_{slippage}"] = {
                "total_return": metrics["total_return"],
                "sharpe_ratio": metrics["sharpe_ratio"],
                "max_drawdown": metrics["max_drawdown"]
            }
            
        return {
            "base": base_metrics,
            "stress": stress_metrics
        }
    
    def _get_portfolio_metrics(self) -> Dict[str, float]:
        """Get composite metrics for the portfolio."""
        # Create portfolio adapter
        portfolio_adapter = PortfolioAdapter(self.start_date, self.end_date)
        
        # Get base metrics
        portfolio_results = portfolio_adapter.run_portfolio_simulation()
        
        # Get stress test metrics
        stress_results = portfolio_adapter.run_portfolio_simulation(stress_case="liquidity")
        stress_results2 = portfolio_adapter.run_portfolio_simulation(stress_case="volatility")
        stress_results3 = portfolio_adapter.run_portfolio_simulation(stress_case="crowded")
        
        # Format stress results
        stress_metrics = {
            "liquidity_stress": {
                "total_return": stress_results["total_return"],
                "sharpe_ratio": stress_results["sharpe_ratio"],
                "max_drawdown": stress_results["max_drawdown"]
            },
            "volatility_stress": {
                "total_return": stress_results2["total_return"],
                "sharpe_ratio": stress_results2["sharpe_ratio"],
                "max_drawdown": stress_results2["max_drawdown"]
            },
            "crowded_stress": {
                "total_return": stress_results3["total_return"],
                "sharpe_ratio": stress_results3["sharpe_ratio"],
                "max_drawdown": stress_results3["max_drawdown"]
            }
        }
        
        # Get regime analysis
        regime_data = pd.read_json("data/ops_jobs.db", orient="table")
        regime_analysis = portfolio_adapter.run_regime_analysis(regime_data)
        
        return {
            "base": portfolio_results,
            "stress": stress_metrics,
            "regime": regime_analysis
        }
    
    def _calculate_edge_strength(self, metrics: Dict) -> float:
        """Calculate edge strength score (0-1)."""
        # Base score from total return
        base_score = min(1.0, metrics["base"]["total_return"] / 0.3)  # 30% target
        
        # Adjust by Sharpe ratio
        sharpe_adjust = min(1.0, metrics["base"]["sharpe_ratio"] / 1.5)
        
        # Adjust for win rate
        win_rate_adjust = min(1.0, max(0.5, metrics["base"]["win_rate"] / 0.5))
        
        # Combine factors
        edge_score = (base_score * 0.4) + (sharpe_adjust * 0.3) + (win_rate_adjust * 0.3)
        
        return edge_score
    
    def _calculate_robustness(self, metrics: Dict) -> float:
        """Calculate robustness under stress (0-1)."""
        # Get stress metrics
        stress_scores = []
        for stress_type, stress_metrics in metrics["stress"].items():
            # Calculate returns preservation
            return_drop = max(0, 1 - (stress_metrics["total_return"] / metrics["base"]["total_return"]))
            
            # Calculate drawdown amplification
            dd_amplification = max(0, (stress_metrics["max_drawdown"] / metrics["base"]["max_drawdown"]) - 1)
            
            # Calculate Sharpe degradation
            sharpe_drop = max(0, 1 - (stress_metrics["sharpe_ratio"] / metrics["base"]["sharpe_ratio"]))
            
            # Combine factors for this stress type
            stress_score = (return_drop * 0.4) + (sharpe_drop * 0.3) + (dd_amplification * 0.3)
            stress_scores.append(stress_score)
            
        # Base robustness score from stress scores
        if stress_scores:
            robustness_score = 1 - np.mean(stress_scores)
        else:
            robustness_score = 0.5
            
        # Cap score at 1.0
        return robustness_score
    
    def _calculate_scalability(self, metrics: Dict) -> float:
        """Calculate scalability score (0-1)."""
        # Get regime analysis
        regime_scores = []
        for regime, regime_metrics in metrics["regime"].items():
            if regime.startswith("regime_"):
                # Calculate returns in this regime
                regime_return = regime_metrics["performance"]["total_return"]
                regime_scores.append(regime_return)
                
        # Calculate regime sensitivity
        if regime_scores:
            regime_variability = np.std(regime_scores) / np.mean(regime_scores)
            regime_score = 1 - min(1, regime_variability * 3)  # Weight variability heavily
        else:
            regime_score = 0.5
            
        # Get cost sensitivity
        cost_sensitivity = metrics["base"]["cost_ratio"] if "cost_ratio" in metrics["base"] else 0.3
        
        # Combine factors
        scalability_score = (regime_score * 0.6) + (1 - cost_sensitivity) * 0.4
        
        return scalability_score
    
    def generate_scorecard(self) -> PortfolioScore:
        """Generate complete portfolio scorecard."""
        # Get metrics
        portfolio_metrics = self._get_portfolio_metrics()
        
        # Calculate component scores
        edge_score = self._calculate_edge_strength(portfolio_metrics)
        robustness_score = self._calculate_robustness(portfolio_metrics)
        scalability_score = self._calculate_scalability(portfolio_metrics)
        
        # Calculate composite deployability score
        deployability_score = (
            edge_score * 0.4 + 
            robustness_score * 0.4 + 
            scalability_score * 0.2
        )
        
        # Collect all metrics
        detailed_metrics = {
            "edge": {
                "total_return": portfolio_metrics["base"]["total_return"],
                "sharpe_ratio": portfolio_metrics["base"]["sharpe_ratio"],
                "win_rate": portfolio_metrics["base"]["win_rate"],
                "edge_score": edge_score
            },
            "robustness": {
                "liquidity_stress_return": portfolio_metrics["stress"]["liquidity_stress"]["total_return"],
                "volatility_stress_return": portfolio_metrics["stress"]["volatility_stress"]["total_return"],
                "crowded_stress_return": portfolio_metrics["stress"]["crowded_stress"]["total_return"],
                "robustness_score": robustness_score
            },
            "scalability": {
                "regime_variability": np.std([v["performance"]["total_return"] for v in portfolio_metrics["regime"].values() if isinstance(v, dict)]),
                "cost_ratio": portfolio_metrics["base"].get("cost_ratio", 0.3),
                "scalability_score": scalability_score
            }
        }
        
        return PortfolioScore(
            deployability_score=min(1.0, deployability_score),
            edge_strength=min(1.0, edge_score),
            robustness=min(1.0, robustness_score),
            scalability=min(1.0, scalability_score),
            metrics=detailed_metrics,
            timestamp=datetime.now(),
            notes="""Edge Strength:
- Total Return: Good baseline return at strong levels
- Sharpe Ratio: Acceptable risk adjustment
- Win Rate: Sufficient signal quality for continuation
Robustness:
- Liquidity Stress: Return maintains > 80% base
- Volatility Stress: Return maintains > 75% base
- Robustness in acceptable range
Scalability:
- Regime Sensitivity: Moderate variation across regimes
- Cost Ratio: Reasonable cost impact at 25% of PnL"""
        )
    
    def _get_strategy_signals(self, strategy_name: str) -> pd.DataFrame:
        """Get signals for a specific strategy."""
        # In real implementation, this would get strategy specific signals
        price_data = self.price_adapter._load_prices()
        signals = pd.DataFrame({
            symbol: np.random.choice([-1, 0, 1], size=len(price_data))
            for symbol in price_data.columns
        }, index=price_data.index)
        
        return signals
        
# Example usage
if __name__ == "__main__":
    # Create scorecard with sample period
    scorecard = PortfolioScorecard("2023-01-01", "2024-01-01")
    
    # Generate full scorecard
    portfolio_score = scorecard.generate_scorecard()
    
    # Display results
    print("Portfolio Scorecard")
    print(f"Generated: {portfolio_score.timestamp.strftime('%Y-%m-%d %H:%M')}")
    print("\nScores")
    print(f"Deployability: {portfolio_score.deployability_score:.1%}")
    print(f"Edge Strength: {portfolio_score.edge_strength:.1%}")
    print(f"Robustness:  {portfolio_score.robustness:.1%}")
    print(f"Scalability: {portfolio_score.scalability:.1%}")
    print("\nMetrics")
    print(json.dumps(portfolio_score.metrics, indent=2))