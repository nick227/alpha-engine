import pandas as pd
import numpy as np
import json
from datetime import datetime


class SimplifiedScorecard:
    """Basic scorecard for strategy evaluation without complex dependencies."""
    
    def __init__(self, strategy_returns, benchmark_returns=None):
        """
        Initialize scorecard with return data.
        
        Args:
            strategy_returns: Series of strategy returns
            benchmark_returns: Series of benchmark returns if available
        """
        self.strategy_returns = strategy_returns
        self.benchmark_returns = benchmark_returns or None
    
    def calculate_sharpe(self):
        """Calculate Sharpe ratio with annualization."""
        if len(self.strategy_returns) < 2:
            return 0
            
        mean_return = np.mean(self.strategy_returns)
        return_std = np.std(self.strategy_returns)
        
        if return_std == 0:
            return np.inf if mean_return > 0 else 0
            
        # Annualize Sharpe
        sharpe = (mean_return / return_std) * np.sqrt(252)
        return sharpe
    
    def calculate_drawdown(self):
        """Calculate maximum drawdown."""
        if len(self.strategy_returns) == 0:
            return 0
            
        # Calculate cumulative returns
        cumulative_returns = (1 + self.strategy_returns).cumprod()
        
        # Calculate drawdowns
        rolling_max = cumulative_returns.cummax()
        drawdowns = cumulative_returns / rolling_max - 1
        
        return abs(drawdowns.min())
    
    def calculate_win_rate(self):
        """Calculate win rate (percentage of positive returns)."""
        if len(self.strategy_returns) == 0:
            return 0
            
        return len(self.strategy_returns[self.strategy_returns > 0]) / len(self.strategy_returns)
    
    def run_basic_analysis(self):
        """Run basic performance analysis."""
        sharpe = self.calculate_sharpe()
        max_drawdown = self.calculate_drawdown()
        win_rate = self.calculate_win_rate()
        total_return = (1 + self.strategy_returns).prod() - 1
        
        return {
            "basic_metrics": {
                "total_return": total_return,
                "sharpe_ratio": max(sharpe, 0),  # Ensure non-negative
                "max_drawdown": max_drawdown,
                "win_rate": win_rate,
                "trade_count": len(self.strategy_returns)
            }
        }
    
    def run_stress_tests(self, slippage_levels=[0.001, 0.005, 0.01, 0.02]):
        """Run basic stress tests applying various slippage levels."""
        stress_results = {}
        base_return = (1 + self.strategy_returns).prod() - 1
        base_sharpe = max(self.calculate_sharpe(), 0.001)  # Avoid division by zero
        
        for slippage in slippage_levels:
            # Apply slippage
            adjusted_returns = self.strategy_returns - slippage
            
            # Calculate metrics
            adjusted_sharpe = self.calculate_sharpe(adjusted_returns)
            adjusted_sharpe = max(adjusted_sharpe, 0)
            
            # Calculate drawdown
            adjusted_drawdown = self.calculate_drawdown(adjusted_returns)
            
            stress_results[f"slippage_{slippage}"] = {
                "total_return": float((1 + adjusted_returns).prod() - 1),
                "sharpe_ratio": float(adjusted_sharpe),
                "drawdown_ratio": float(adjusted_drawdown)
            }
            
        return {"stress_tests": stress_results}
    
    def calculate_sharpe(self, returns=None):
        """Calculate Sharpe ratio with annualization."""
        curr_returns = returns if returns is not None else self.strategy_returns
        
        if len(curr_returns) < 2:
            return 0
            
        mean_return = np.mean(curr_returns)
        return_std = np.std(curr_returns)
        
        if return_std == 0:
            return np.inf if mean_return > 0 else 0
            
        # Annualize Sharpe
        return float((mean_return / return_std) * np.sqrt(252))
    
    def calculate_drawdown(self, returns=None):
        """Calculate maximum drawdown."""
        curr_returns = returns if returns is not None else self.strategy_returns
        
        if len(curr_returns) == 0:
            return 0
            
        # Calculate cumulative returns
        cumulative_returns = (1 + curr_returns).cumprod()
        
        # Calculate drawdowns
        rolling_max = cumulative_returns.cummax()
        drawdowns = cumulative_returns / rolling_max - 1
        
        return abs(drawdowns.min())
    
    def calculate_leverage_factor(self, returns=None):
        """Calculate leverage factor based on risk and return."""
        curr_returns = returns if returns is not None else self.strategy_returns
        
        if len(curr_returns) == 0:
            return 0
            
        mean_return = np.mean(curr_returns)
        return_std = np.std(curr_returns)
        
        if mean_return <= 0 or return_std == 0:
            return 0
            
        # Calculate optimal leverage using Kelly criterion
        win_rate = self.calculate_win_rate()
        avg_win = curr_returns[curr_returns > 0].mean()
        avg_loss = abs(curr_returns[curr_returns < 0].mean())
        
        if avg_loss == 0:
            return 0
            
        # Sharpe-based Kelly
        sharpe = self.calculate_sharpe(curr_returns)
        sharpe_kelly = sharpe / (return_std / mean_return)
        
        # Win/loss Kelly
        edge = win_rate * avg_win - (1 - win_rate) * avg_loss
        if edge == 0:
            return 0
            
        kelly = edge / avg_win  # Simplified for binary outcomes
            
        # Final leverage is the smaller of the two approaches
        return min(sharpe_kelly, kelly)
    
    def generate_scorecard(self):
        """Generate simplified scorecard with core metrics."""
        # Run analyses
        basic_results = self.run_basic_analysis()
        stress_results = self.run_stress_tests()
        
        # Calculate degradation metrics
        base_metrics = basic_results["basic_metrics"]
        base_return = base_metrics["total_return"]
        base_sharpe = base_metrics["sharpe_ratio"] or 0.001
        base_drawdown = base_metrics["max_drawdown"] or 0.001
        
        # Get worst case degradation
        worst_return_degradation = 0
        worst_sharpe_degradation = 0
        
        for stress_type, metrics in stress_results["stress_tests"].items():
            return_degradation = 1 - (metrics["total_return"] / base_return) if base_return > 0 else 1
            sharpe_degradation = 1 - (metrics["sharpe_ratio"] / base_sharpe) if metrics["sharpe_ratio"] > 0 else 1
            
            worst_return_degradation = max(worst_return_degradation, return_degradation)
            worst_sharpe_degradation = max(worst_sharpe_degradation, sharpe_degradation)
        
        # Calculate deployability score using more realistic thresholds
        basic_score = min(1, (base_sharpe / 1.0) + (base_return / 0.25)) / 2
        
        # Stress score needs to maintain 80% of baseline returns to pass
        stress_score = max(0, 1 - worst_return_degradation * 0.6 - worst_sharpe_degradation * 0.4)
        
        # Composite score with a floor at 0
        composite_score = max(0, (basic_score * 0.4) + (stress_score * 0.6))
        
        # Calculate risk-adjusted leverage
        optimal_leverage = self.calculate_leverage_factor()
        
        return {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "period": {
                    "start": self.strategy_returns.index[0].isoformat(),
                    "end": self.strategy_returns.index[-1].isoformat()
                }
            },
            "scorecard": {
                "composite_score": composite_score,
                "basic_performance_score": basic_score,
                "robustness_score": stress_score,
                "optimal_leverage": optimal_leverage,
                "base_metrics": basic_results["basic_metrics"],
                "stress_results": stress_results,
                "classifications": {
                    "edge_strength": (
                        "strong" if base_return > 0.2 and base_sharpe > 1.5 
                        else "moderate" if base_return > 0.1 and base_sharpe > 1.0 
                        else "weak"
                    ),
                    "robustness": (
                        "strong" if stress_score > 0.7 
                        else "moderate" if stress_score > 0.5 
                        else "weak"
                    ),
                    "deployability": (
                        "deployable" if composite_score > 0.7 
                        else "conditional" if composite_score > 0.5 
                        else "not_ready"
                    ),
                    "leverage": (
                        "high" if optimal_leverage > 1.5 
                        else "normal" if optimal_leverage > 0.5 
                        else "conservative"
                    )
                },
                "risk_profile": self._calculate_risk_profile(
                    self.strategy_returns,
                    base_sharpe,
                    base_drawdown
                )
            }
        }
    
    def _calculate_risk_profile(self, returns, sharpe, drawdown):
        """Calculate risk profile based on multiple factors."""
        risk_factors = {
            "volatility": np.std(returns) * np.sqrt(252),
            "sharpe_threshold": 1.0,
            "drawdown_threshold": 0.15,
            "return_threshold": 0.15,  # 15% annual return
            "volatility_threshold": 0.20  # 20% annual volatility
        }
        
        # Calculate relative risk
        annual_vol = risk_factors["volatility"]
        risk_level = (
            (annual_vol / risk_factors["volatility_threshold"]) * 0.5 +
            (drawdown / risk_factors["drawdown_threshold"]) * 0.3 +
            (1 - sharpe / risk_factors["sharpe_threshold"]) * 0.2
        ) / (0.5 + 0.3 + 0.2)
        
        # Adjust risk to match all factors
        risk_category = "moderate" if 0.4 <= risk_level <= 0.6 else "high" if risk_level > 0.6 else "low"
        
        return {
            "risk_factor": risk_level,
            "risk_category": risk_category,
            "volatility_annualized": annual_vol,
            "risk_rating": (
                "Low Risk" if risk_level < 0.4 else
                "Moderate Risk" if risk_level < 0.6 else
                "High Risk"
            )
        }
    
    @classmethod
    def generate_mock(cls):
        """Generate mock scorecard with more realistic data."""
        # Generate realistic return series
        np.random.seed(42)  # Seed for reproducibility
        
        # Realistic parameters for stronger performance
        days = 252
        base_return = 0.0015  # 0.15% daily mean return
        volatility = 0.008  # 0.8% daily volatility
        
        # Generate daily returns (with some persistence)
        rand_returns = np.random.normal(base_return, volatility, days)
        
        # Add some persistence (AR(1) effect)
        correlated_returns = np.zeros(days)
        correlated_returns[0] = base_return
        for i in range(1, days):
            correlated_returns[i] = 0.6 * correlated_returns[i-1] + 0.4 * rand_returns[i]
            
        # Constrain returns to reasonable bounds
        correlated_returns = np.clip(correlated_returns, -0.04, 0.04)
        
        daily_returns = pd.Series(
            correlated_returns,
            index=pd.date_range(start="2023-01-01", periods=days, freq='D')
        )
        
        # Create scorecard
        scorecard = cls(daily_returns)
        return scorecard.generate_scorecard()


if __name__ == "__main__":
    # Generate mock scorecard
    mock_card = SimplifiedScorecard.generate_mock()
    
    # Print dashboard
    print("Simplified Scorecard")
    print(f"Timestamp: {mock_card['metadata']['timestamp']}")
    
    print("\nBasic Metrics")
    for key, val in mock_card['scorecard']['base_metrics'].items():
        if key == "total_return":
            print(f"Total Return: {val:.1%}")
        elif key == "sharpe_ratio":
            print(f"Sharpe Ratio: {val:.2f}")
        elif key == "max_drawdown":
            print(f"Max Drawdown: {val:.1%}")
        elif key == "win_rate":
            print(f"Win Rate: {val:.1%}")
        else:
            print(f"{key}: {val}")
            
    print("\nClassification")
    for key, val in mock_card['scorecard']['classifications'].items():
        if key in ['risk_factor', 'volatility_annualized']:
            print(f"{key.replace('_', ' ').title()}: {val:.2f}")
        else:
            print(f"{key}: {val}")
    
    print("\nComposite Score")
    print(f"Composite Deployability Score: {mock_card['scorecard']['composite_score']:.1%}")
    print(f"Basic Performance Score: {mock_card['scorecard']['basic_performance_score']:.1%}")
    print(f"Robustness Score: {mock_card['scorecard']['robustness_score']:.1%}")
    print(f"Optimal Leverage: {mock_card['scorecard']['optimal_leverage']:.1f}")
    print(f"Risk Profile: {mock_card['scorecard']['risk_profile']['risk_rating']}")
    
    # Save results
    with open("simplified_scorecard.json", "w") as f:
        json.dump(mock_card, f, indent=2)
    
    print("\nResults saved to 'simplified_scorecard.json'")
