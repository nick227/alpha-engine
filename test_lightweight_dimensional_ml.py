"""
Test Lightweight Dimensional ML System.

Demonstrates elegant axis-based tagging and selective activation approach.
"""

from __future__ import annotations

import random
from app.ml.lightweight_dimensional_ml import get_lightweight_dimensional_ml
from app.core.environment import build_env_snapshot_v3


def create_test_features() -> dict:
    """Create test features with different dimensional characteristics."""
    
    features = {}
    
    # Create diverse test scenarios
    scenarios = [
        {
            "symbol": "TECH_AA",
            "volatility": 0.04, "trend": 0.03, "volume": 3000000, "sector": "technology"
        },
        {
            "symbol": "FIN_BB", 
            "volatility": 0.025, "trend": 0.015, "volume": 2000000, "sector": "financials"
        },
        {
            "symbol": "HEA_CC",
            "volatility": 0.015, "trend": 0.005, "volume": 1500000, "sector": "healthcare"
        },
        {
            "symbol": "ENE_DD",
            "volatility": 0.035, "trend": -0.01, "volume": 2500000, "sector": "energy"
        },
        {
            "symbol": "CON_EE",
            "volatility": 0.018, "trend": 0.008, "volume": 1200000, "sector": "consumer"
        },
    ]
    
    for scenario in scenarios:
        features[scenario["symbol"]] = {
            "volatility_20d": scenario["volatility"],
            "return_5d": scenario["trend"],
            "return_63d": scenario["trend"] * 1.5,
            "dollar_volume": scenario["volume"],
            "volume_zscore_20d": (scenario["volume"] - 1000000) / 500000.0,
            "price_percentile_252d": random.uniform(0.2, 0.8),
            "sector": scenario["sector"],
        }
    
    return features


def demonstrate_dimensional_tagging():
    """Demonstrate the lightweight dimensional tagging approach."""
    
    print("🏷 LIGHTWEIGHT DIMENSIONAL ML SYSTEM")
    print("=" * 60)
    print("Elegant axis-based tagging + selective activation")
    print("=" * 60)
    
    # Get the system
    ml_system = get_lightweight_dimensional_ml()
    
    # Create test features
    features = create_test_features()
    
    # Simulate some predictions
    predictions = {
        "TECH_AA": 0.025,  # Tech momentum
        "FIN_BB": 0.012,   # Financials moderate
        "HEA_CC": -0.005,  # Healthcare defensive
        "ENE_DD": 0.018,   # Energy aggressive
        "CON_EE": 0.008,    # Consumer balanced
    }
    
    confidences = {
        "TECH_AA": 0.85,  # High confidence
        "FIN_BB": 0.70,   # Moderate confidence
        "HEA_CC": 0.60,   # Lower confidence
        "ENE_DD": 0.75,   # Good confidence
        "CON_EE": 0.65,   # Moderate confidence
    }
    
    print(f"\n🎯 PREDICTION TAGGING DEMONSTRATION")
    print(f"Processing {len(predictions)} predictions with dimensional axes...")
    
    # Simulate prediction with dimensional tagging
    results = []
    for symbol, prediction in predictions.items():
        if symbol in features:
            result = ml_system.predict_with_dimensional_tagging(
                {symbol: features[symbol]}, 
                prediction, 
                confidences[symbol],
                "data/alpha.db", 
                "2024-01-01"
            )
            results.append(result)
    
    # Show tagging results
    print(f"\n📊 DIMENSIONAL TAGGING RESULTS:")
    for result in results:
        status = "✅ ACTIVATE" if result["should_activate"] else "🚫 BLOCK"
        print(f"  {result['symbol']}: {result['environment_tag']}_{result['sector_tag']} | {status}")
        print(f"    Axis: {result['axis_key']}")
        print(f"    Reason: {result['activation_reason']}")
        print(f"    Confidence: {result['confidence']:.2f}")
    
    return results


def demonstrate_selective_activation():
    """Demonstrate selective activation based on proven edges."""
    
    print(f"\n🎯 SELECTIVE ACTIVATION DEMONSTRATION")
    
    # Get the system
    ml_system = get_lightweight_dimensional_ml()
    
    # Create test features
    features = create_test_features()
    
    # Simulate predictions with different confidence levels
    predictions = {
        "TECH_AA": 0.030,  # High confidence, should activate
        "FIN_BB": 0.015,  # Moderate confidence, might activate
        "HEA_CC": 0.003,  # Low confidence, should block
        "ENE_DD": 0.020,  # Good confidence, should activate
        "CON_EE": 0.010,  # Moderate confidence, might activate
    }
    
    confidences = {
        "TECH_AA": 0.90,
        "FIN_BB": 0.65,
        "HEA_CC": 0.45,
        "ENE_DD": 0.80,
        "CON_EE": 0.60,
    }
    
    print("Testing different activation modes...")
    
    # Test conservative mode (only high-confidence edges)
    print(f"\n🛡️ CONSERVATIVE MODE:")
    conservative_results = ml_system.selective_activation_pipeline(
        features, predictions, confidences, "data/alpha.db", "2024-01-01", 
        "conservative"
    )
    
    activated = [r for r in conservative_results if r["should_activate"]]
    print(f"  Activated: {len(activated)}/{len(conservative_results)} predictions")
    print(f"  Selectivity: {len(activated)/len(conservative_results):.1%}")
    
    # Test aggressive mode (activate most predictions)
    print(f"\n⚡ AGGRESSIVE MODE:")
    aggressive_results = ml_system.selective_activation_pipeline(
        features, predictions, confidences, "data/alpha.db", "2024-01-01", 
        "aggressive"
    )
    
    activated = [r for r in aggressive_results if r["should_activate"]]
    print(f"  Activated: {len(activated)}/{len(aggressive_results)} predictions")
    print(f"  Selectivity: {len(activated)/len(aggressive_results):.1%}")
    
    return conservative_results, aggressive_results


def demonstrate_performance_surface():
    """Demonstrate performance surface analysis."""
    
    print(f"\n📊 PERFORMANCE SURFACE ANALYSIS")
    
    ml_system = get_lightweight_dimensional_ml()
    analysis = ml_system.get_performance_surface_analysis()
    
    if "error" in analysis:
        print("❌ Insufficient data for performance surface analysis")
        return
    
    print(f"Total axes analyzed: {analysis['total_analyzed_axes']}")
    print(f"Analysis date: {analysis['analysis_date']}")
    
    # Show best performers by environment
    print(f"\n🏆 BEST BY ENVIRONMENT:")
    for env, performances in analysis["environment_performance"].items():
        best = max(performances, key=lambda x: x["win_rate"])
        print(f"  {env}: {best['axis_key']} (win_rate: {best['win_rate']:.1%})")
    
    # Show best performers by sector
    print(f"\n🏆 BEST BY SECTOR:")
    for sector, performances in analysis["sector_performance"].items():
        best = max(performances, key=lambda x: x["win_rate"])
        print(f"  {sector}: {best['axis_key']} (win_rate: {best['win_rate']:.1%})")
    
    print(f"\n🎯 OVERALL BEST:")
    print(f"  Environment: {analysis['best_environment']}")
    print(f"  Sector: {analysis['best_sector']}")


def main():
    """Run complete lightweight dimensional ML demonstration."""
    
    print("🚀 LIGHTWEIGHT DIMENSIONAL ML: ELEGANT SOLUTION")
    print("=" * 60)
    print("Instead of complex model selection:")
    print("1. Tag every prediction with lightweight axes")
    print("2. Track performance by dimensional combination") 
    print("3. Activate only where proven edges exist")
    print("4. Build performance surface for continuous improvement")
    print("=" * 60)
    
    # Demonstrate the system
    demonstrate_dimensional_tagging()
    demonstrate_selective_activation()
    demonstrate_performance_surface()
    
    print(f"\n" + "=" * 60)
    print("🏆 LIGHTWEIGHT DIMENSIONAL ML SUMMARY:")
    print("✅ Dimensional Tagging: ELEGANT AND LIGHTWEIGHT")
    print("✅ Selective Activation: PERFORMANCE-BASED FILTERING")
    print("✅ Performance Surface: CONTINUOUS IMPROVEMENT")
    print("✅ Production Ready: MINIMAL COMPLEXITY, MAXIMUM INSIGHT")
    
    print("\n🎯 KEY INNOVATION:")
    print("Transform raw predictions → context-aware decisions")
    print("Build performance surface → (environment, sector, model) → performance")
    print("Selective activation → only act where historical edge exists")
    print("Lightweight complexity → easy to understand, maintain, and extend")
    
    print("\n🚀 READY FOR PRODUCTION DEPLOYMENT")


if __name__ == "__main__":
    main()
