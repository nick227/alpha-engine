"""
Debug why configs aren't working - step by step validation.
"""

from app.discovery.strategies import silent_compounder, DEFAULT_STRATEGY_CONFIGS
from app.discovery.types import FeatureRow


def create_test_feature_row(volatility: float, return_63d: float) -> FeatureRow:
    """Create test feature row with specific volatility and return."""
    return FeatureRow(
        symbol='TEST',
        as_of_date='2024-01-01',
        close=100.0,
        volume=1000000,
        dollar_volume=100000000,
        avg_dollar_volume_20d=100000000,
        return_1d=0.01,
        return_5d=0.02,
        return_20d=0.03,
        return_63d=return_63d,
        return_252d=0.15,
        volatility_20d=volatility,
        max_drawdown_252d=0.1,
        price_percentile_252d=0.5,
        volume_zscore_20d=1.0,
        dollar_volume_zscore_20d=1.0,
        revenue_ttm=None,
        revenue_growth=None,
        shares_outstanding=None,
        shares_growth=None,
        sector=None,
        industry=None,
        sector_return_63d=None,
        peer_relative_return_63d=None,
        price_bucket=None,
    )


def debug_config_effects():
    """Debug exactly what's happening with configs."""
    print("=== DEBUG: Config Effects ===")
    
    # Test with perfect volatility for both configs
    test_stock = create_test_feature_row(volatility=0.02, return_63d=0.05)
    
    # Default config
    default_config = DEFAULT_STRATEGY_CONFIGS["silent_compounder"]
    print(f"Default config: {default_config}")
    
    # Extreme configs
    strict_config = {"vol_band": 0.01, "threshold": 0.9, "min_vol": 0.005, "max_vol": 0.02}
    loose_config = {"vol_band": 0.04, "threshold": 0.1, "min_vol": 0.02, "max_vol": 0.06}
    
    print(f"\nTest stock volatility: {test_stock.volatility_20d}")
    print(f"Test stock return: {test_stock.return_63d}")
    
    # Test each config
    configs = [
        ("Default", default_config),
        ("Strict", strict_config), 
        ("Loose", loose_config)
    ]
    
    for name, config in configs:
        result = silent_compounder(test_stock, config=config)
        score = result[0] if result[0] is not None else "None"
        reason = result[1] if result[1] else "None"
        print(f"{name} config: score={score}, reason='{reason}'")
    
    # Test volatility ranges
    print(f"\n=== VOLATILITY RANGE TEST ===")
    volatilities = [0.008, 0.012, 0.02, 0.025, 0.035]
    
    for vol in volatilities:
        stock = create_test_feature_row(volatility=vol, return_63d=0.05)
        
        default_result = silent_compounder(stock, config=default_config)
        strict_result = silent_compounder(stock, config=strict_config)
        loose_result = silent_compounder(stock, config=loose_config)
        
        print(f"Vol {vol:.3f}: Default={default_result[0]:.3f if default_result[0] else None}, "
              f"Strict={strict_result[0]:.3f if strict_result[0] else None}, "
              f"Loose={loose_result[0]:.3f if loose_result[0] else None}")


if __name__ == "__main__":
    debug_config_effects()
