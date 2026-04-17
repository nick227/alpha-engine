"""
Integrate Temporal Correlation Strategy into Discovery System

This script integrates the temporal correlation strategy into the Alpha Engine
discovery system, making it available for backtesting and live trading.
"""

import yaml
import sys
from pathlib import Path
from typing import Dict, List, Any

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from app.discovery.strategies import create_temporal_correlation_strategy
from app.discovery.strategies.bear_expansion_strategy import BearExpansionStrategy
from app.discovery.strategies.registry import STRATEGIES, DEFAULT_STRATEGY_CONFIGS


def load_temporal_config() -> Dict[str, Any]:
    """Load temporal correlation strategy configuration."""
    
    config_path = project_root / "app/discovery/strategies/temporal_correlation_config.yaml"
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"Warning: Config file not found at {config_path}")
        return {}
    except yaml.YAMLError as e:
        print(f"Error parsing config file: {e}")
        return {}


def create_enhanced_temporal_strategy() -> 'TemporalCorrelationStrategy':
    """Create enhanced temporal correlation strategy with full configuration."""
    
    # Load configuration
    config = load_temporal_config()
    
    # Create base strategy
    base_strategy = BearExpansionStrategy()
    
    # Create temporal correlation strategy
    temporal_strategy = create_temporal_correlation_strategy(
        base_strategy=base_strategy,
        config=config
    )
    
    return temporal_strategy


def register_temporal_strategy():
    """Register temporal correlation strategy in discovery system."""
    
    print("Registering Temporal Correlation Strategy in Discovery System...")
    print("=" * 60)
    
    # Load configuration
    config = load_temporal_config()
    
    # Create strategy configuration for registry
    strategy_config = {
        "name": config.get("name", "temporal_correlation_strategy"),
        "description": config.get("description", ""),
        "version": config.get("version", "1.0.0"),
        "base_strategy": config.get("base_strategy", "bear_expansion_strategy"),
        
        # Temporal-specific parameters
        "signal_thresholds": config.get("signal_thresholds", {}),
        "position_sizing": config.get("position_sizing", {}),
        "signal_expiry_hours": config.get("signal_expiry_hours", {}),
        "risk_management": config.get("risk_management", {}),
        
        # Data sources
        "data_sources": config.get("data_sources", {}),
        
        # Historical patterns
        "historical_patterns": config.get("historical_patterns", {}),
        "sector_correlations": config.get("sector_correlations", {}),
        "economic_sensitivities": config.get("economic_sensitivities", {}),
        "volatility_regimes": config.get("volatility_regimes", {}),
        
        # Integration settings
        "integration": config.get("integration", {}),
        
        # Performance tracking
        "performance_tracking": config.get("performance_tracking", {}),
        
        # Factory function
        "factory": "app.discovery.integrate_temporal_strategy:create_enhanced_temporal_strategy"
    }
    
    # Register in STRATEGIES dictionary
    STRATEGIES["temporal_correlation"] = strategy_config
    
    # Add to default configurations
    DEFAULT_STRATEGY_CONFIGS["temporal_correlation"] = {
        "enabled": True,
        "weight": 1.0,
        "max_positions": 2,
        "position_size": 0.02,
        "temporal_adjustments": True,
        "signal_threshold": 0.3
    }
    
    print(f"Registered strategy: {strategy_config['name']}")
    print(f"Description: {strategy_config['description']}")
    print(f"Base strategy: {strategy_config['base_strategy']}")
    print(f"Version: {strategy_config['version']}")
    
    return strategy_config


def test_temporal_strategy():
    """Test the temporal correlation strategy integration."""
    
    print("\nTesting Temporal Correlation Strategy Integration...")
    print("=" * 50)
    
    try:
        # Create strategy
        strategy = create_enhanced_temporal_strategy()
        
        # Sample market data
        sample_market_data = {
            'vix': 22.5,
            'news_sentiment': 0.3,
            'market_momentum': 0.1,
            'date': '2024-01-15',
            'symbol_data': {
                'AAPL': {
                    'close': 150.0,
                    'volume': 1000000,
                    'high': 152.0,
                    'low': 148.0,
                    'open': 149.5
                },
                'MSFT': {
                    'close': 250.0,
                    'volume': 800000,
                    'high': 252.0,
                    'low': 248.0,
                    'open': 249.0
                }
            }
        }
        
        # Analyze with temporal insights
        signals = strategy.analyze(sample_market_data)
        
        print(f"Generated {len(signals)} temporal-adjusted signals")
        
        # Get temporal insights
        insights = strategy.get_temporal_insights()
        print(f"Position multiplier: {insights['position_multiplier']:.2f}")
        print(f"Active signals: {len(insights['active_signals'])}")
        
        if insights['active_signals']:
            print("\nActive Temporal Signals:")
            for signal in insights['active_signals']:
                print(f"  - {signal['type']}: {signal['direction']} (confidence: {signal['confidence']:.2f})")
                print(f"    Rationale: {signal['rationale']}")
        
        if insights['insights']:
            print(f"\nKey Insight: {insights['insights'][0]}")
        
        # Test signal adjustment
        if signals:
            sample_signal = signals[0]
            print(f"\nSample Signal Adjustment:")
            print(f"  Symbol: {sample_signal.symbol}")
            print(f"  Original Strength: {sample_signal.metadata.get('original_strength', 'N/A')}")
            print(f"  Adjusted Strength: {sample_signal.strength:.3f}")
            print(f"  Temporal Factors: {len(sample_signal.metadata.get('temporal_adjustments', {}).get('temporal_signals', []))}")
        
        print("\nIntegration test completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error during integration test: {e}")
        return False


def create_discovery_integration_script():
    """Create script for easy discovery system integration."""
    
    script_content = '''"""
Temporal Correlation Strategy Integration Script

This script demonstrates how to integrate and use the temporal correlation strategy
within the Alpha Engine discovery system.

Usage:
    python -c "from app.discovery.integrate_temporal_strategy import main; main()"
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from app.discovery.integrate_temporal_strategy import (
    register_temporal_strategy, 
    test_temporal_strategy,
    create_enhanced_temporal_strategy
)

def main():
    """Main integration function."""
    
    print("Temporal Correlation Strategy Integration")
    print("=" * 50)
    
    # Register strategy
    strategy_config = register_temporal_strategy()
    
    # Test integration
    success = test_temporal_strategy()
    
    if success:
        print("\n" + "=" * 50)
        print("INTEGRATION COMPLETE!")
        print("=" * 50)
        print("\nThe temporal correlation strategy is now available in the discovery system.")
        print("\nKey features:")
        print("- Market sentiment timing")
        print("- Economic event positioning")
        print("- Volatility regime adaptation")
        print("- Seasonal pattern recognition")
        print("- Sector rotation signals")
        print("- Historical period optimization")
        
        print("\nUsage in discovery system:")
        print("from app.discovery.strategies import create_temporal_correlation_strategy")
        print("strategy = create_temporal_correlation_strategy(base_strategy)")
        
    else:
        print("\nIntegration test failed. Please check the error messages above.")

if __name__ == "__main__":
    main()
'''
    
    script_path = project_root / "app/discovery/temporal_strategy_integration.py"
    
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    print(f"Created integration script: {script_path}")
    return script_path


def update_registry_documentation():
    """Update strategy registry documentation."""
    
    # This would update the main documentation to include the new strategy
    # For now, we'll print the information that should be added
    
    print("\nRegistry Documentation Update:")
    print("=" * 40)
    print("Add the following to the discovery system documentation:")
    print("""
## Temporal Correlation Strategy

The temporal correlation strategy enhances base strategies with time-based insights
derived from comprehensive correlation analysis.

### Key Features:
- **Market Sentiment Timing**: Adjusts exposure based on news sentiment
- **Economic Event Positioning**: Optimizes around FOMC, CPI, and other events
- **Volatility Regime Adaptation**: Scales positions based on VIX levels
- **Seasonal Pattern Recognition**: Leverages historical monthly/quarterly patterns
- **Sector Rotation Signals**: Uses sector performance correlations
- **Historical Period Optimization**: Identifies and leverages optimal time periods

### Configuration:
- Configured via `temporal_correlation_config.yaml`
- Integrates with external data sources (VIX, news, economic calendar)
- Real-time market condition analysis
- Adjustable signal thresholds and position sizing

### Usage:
```python
from app.discovery.strategies import create_temporal_correlation_strategy
from app.discovery.strategies.bear_expansion_strategy import BearExpansionStrategy

base_strategy = BearExpansionStrategy()
temporal_strategy = create_temporal_correlation_strategy(base_strategy)

# Analyze with temporal insights
signals = temporal_strategy.analyze(market_data)

# Get temporal insights
insights = temporal_strategy.get_temporal_insights()
print(f"Position multiplier: {insights['position_multiplier']:.2f}")
```
""")


def main():
    """Main integration function."""
    
    print("Temporal Correlation Strategy - Discovery System Integration")
    print("=" * 65)
    
    # Register strategy
    strategy_config = register_temporal_strategy()
    
    # Test integration
    success = test_temporal_strategy()
    
    # Create integration script
    script_path = create_discovery_integration_script()
    
    # Update documentation
    update_registry_documentation()
    
    if success:
        print("\n" + "=" * 65)
        print("INTEGRATION COMPLETE!")
        print("=" * 65)
        print(f"\nStrategy registered: {strategy_config['name']}")
        print(f"Integration script: {script_path}")
        print("\nThe temporal correlation strategy is now available in the discovery system.")
        
        print("\nNext steps:")
        print("1. Run the integration script: python app/discovery/temporal_strategy_integration.py")
        print("2. Test with backtesting framework")
        print("3. Configure for live trading")
        print("4. Monitor temporal signal performance")
        
        print("\nKey benefits:")
        print("- Enhanced timing through sentiment analysis")
        print("- Economic event optimization")
        print("- Volatility regime adaptation")
        print("- Seasonal pattern leverage")
        print("- Sector rotation integration")
        print("- Historical period optimization")
        
    else:
        print("\nIntegration test failed. Please check the error messages above.")
        return False
    
    return True


if __name__ == "__main__":
    main()
