import sys
import os
import json
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Add app directory to Python path
app_dir = Path(__file__).parent.parent
sys.path.append(str(app_dir))

from app.engine.portfolio_scorecard import PortfolioScorecard


def run_portfolio_scorecard():
    """Run portfolio scorecard evaluation."""
    print("Starting portfolio scorecard evaluation...")
    
    # Create scorecard with sample period
    scorecard = PortfolioScorecard("2023-01-01", "2024-01-01")
    
    # Generate full scorecard
    portfolio_score = scorecard.generate_scorecard()
    
    # Format output
    output = {
        "metadata": {
            "run_timestamp": datetime.now().isoformat(),
            "period": {
                "start": "2023-01-01",
                "end": "2024-01-01"
            }
        },
        "scorecard": {
            "deployability_score": portfolio_score.deployability_score,
            "edge_strength": portfolio_score.edge_strength,
            "robustness": portfolio_score.robustness,
            "scalability": portfolio_score.scalability,
            "metrics": portfolio_score.metrics,
            "timestamp": portfolio_score.timestamp.isoformat(),
            "notes": portfolio_score.notes
        }
    }
    
    return output


def format_number(n: float, precision: int = 2) -> str:
    """Format number with thousand separators."""
    return f"{n:,.{precision}f}"


def generate_dashboard(scorecard: dict) -> str:
    """Generate text dashboard from scorecard."""
    # Extract metrics
    try:
        scores = scorecard["scorecard"]
        metrics = scores["metrics"]
    except KeyError as e:
        return f"Missing data in scorecard: {e}"
    
    # Format dashboard
    dashboard = f"""
Portfolio Scorecard
{(datetime.now().strftime('%Y-%m-%d %H:%M'))}

Time Period: {scorecard['metadata']['period']['start']} → {scorecard['metadata']['period']['end']}

DEPLOYABILITY SCORE
───────────────────────────────────────────────
TOTAL SCORE   : {format_number(scores["deployability_score"] * 100)}%
EDGE STRENGTH: {format_number(scores["edge_strength"] * 100)}%
ROBUSTNESS   : {format_number(scores["robustness"] * 100)}%
SCALABILITY  : {format_number(scores["scalability"] * 100)}%

STRATEGIC CLASSIFICATION
───────────────────────────────────────────────
{'✅ READY FOR DEPLOYMENT' if scores["deployability_score"] > 0.7 and scores["edge_strength"] > 0.65 else '⚠️ NEEDS STRENGTHENING'}
{'→' if scores["robustness"] > 0.7 else '⚠️'} Stress degradation {"> 70%" if scores["robustness"] > 0.7 else "needs improvement"}
{'→' if scores["scalability"] > 0.6 else '⚠️'} Regime stability {"> 60%" if scores["scalability"] > 0.6 else "requires diversification"}

EDGE METRICS
───────────────────────────────────────────────
Return:      {format_number(metrics["edge"]["total_return"] * 100)}%
Sharpe:      {format_number(metrics["edge"]["sharpe_ratio"], 2)}
Win Rate:    {format_number(metrics["edge"]["win_rate"] * 100)}%
Edge Score:  {format_number(scores["edge_strength"] * 100)}%

ROBUSTNESS METRICS
───────────────────────────────────────────────
Liquidity Stress Return: {format_number(metrics["robustness"]["liquidity_stress_return"] * 100)}%
Volatility Stress Return: {format_number(metrics["robustness"]["volatility_stress_return"] * 100)}%
Crowded Stress Return: {format_number(metrics["robustness"]["crowded_stress_return"] * 100)}%
Robustness Score: {format_number(scores["robustness"] * 100)}%

SCALABILITY METRICS
───────────────────────────────────────────────
Regime Variability: {format_number(metrics["scalability"]["regime_variability"], 2)}
Cost Ratio: {format_number(metrics["scalability"].get("cost_ratio", 0.3) * 100, 1)}%
Scalability Score: {format_number(scores["scalability"] * 100)}%

ASSESSMENT NOTES
───────────────────────────────────────────────
{scores["notes"]}
"""
    return dashboard

if __name__ == "__main__":
    print("Running portfolio scorecard evaluation...")
    print(f"Python path: {sys.path}")
    
    try:
        # Run scorecard
        results = run_portfolio_scorecard()
        
        # Generate dashboard
        dashboard = generate_dashboard(results)
        print(dashboard)
        
        # Save results
        with open("portfolio_scorecard.json", "w") as f:
            json.dump(results, f, indent=2)
        
        print("Results saved to 'portfolio_scorecard.json'")
        
    except Exception as e:
        print(f"Error running portfolio scorecard: {e}")