"""
Paper Trading Configuration

Configuration management for paper trading system.
Includes risk parameters, qualification layers, and trading settings.
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
import os
import json


@dataclass
class PaperTradingConfig:
    """Complete paper trading configuration."""
    
    # Portfolio settings
    initial_cash: float = 100000.0
    tenant_id: str = "paper_trading"
    
    # Position sizing
    base_position_pct: float = 0.01  # 1% of portfolio per trade
    max_position_pct: float = 0.02   # 2% maximum per trade
    confidence_scaling: bool = True
    
    # Advanced position sizing
    confidence_threshold: float = 0.5
    volatility_cap: float = 0.05
    max_leverage: float = 1.0
    kelly_fraction: float = 0.25
    kelly_confidence_weight: float = 2.0
    volatility_target: float = 0.02
    volatility_lookback: int = 20
    max_risk_per_trade: float = 0.02
    drawdown_scaling: bool = True
    regime_adjustment: bool = True
    
    # Risk management
    max_ticker_exposure: float = 0.10      # 10% max per ticker
    max_sector_exposure: float = 0.20      # 20% max per sector
    max_strategy_exposure: float = 0.15    # 15% max per strategy
    max_daily_loss_pct: float = 0.02       # 2% max daily loss
    max_correlation_exposure: float = 0.15 # 15% max correlated exposure
    
    # Comprehensive risk limits
    max_total_exposure: float = 0.80       # 80% max total exposure
    max_concurrent_trades: int = 10        # Max concurrent positions
    max_trades_per_day: int = 50           # Max trades per day
    max_trades_per_hour: int = 10          # Max trades per hour
    loss_cooldown_minutes: int = 30        # Cooldown after loss
    consecutive_loss_limit: int = 3        # Max consecutive losses
    confidence_floor: float = 0.6          # Minimum confidence after losses
    emergency_halt_loss_pct: float = 0.05  # 5% loss triggers emergency halt
    max_drawdown_pct: float = 0.15          # 15% max drawdown
    
    # Stop loss and targets
    stop_loss_volatility_multiplier: float = 2.0
    reward_risk_ratio: float = 2.0
    trailing_stop_enabled: bool = False
    trailing_stop_pct: float = 0.01
    
    # Signal quality filters
    min_confidence: float = 0.6
    min_consensus: float = 0.5
    min_volume_ratio: float = 0.5
    max_volatility: float = 0.1
    
    # LLM validation
    llm_validation_enabled: bool = False
    llm_min_confidence: float = 0.8
    llm_provider: str = "openai"  # openai, anthropic, local
    llm_model: str = "gpt-4"
    
    # Market hours and execution
    market_hours_only: bool = True
    execution_delay_seconds: float = 0.1
    slippage_bps: float = 5.0  # 5 basis points
    
    # Performance tracking
    performance_update_interval: int = 60  # seconds
    save_trade_history: bool = True
    trade_history_file: str = "data/paper_trades.json"
    
    # Feature integration
    feature_version: str = "v2.0"
    legacy_mode: bool = False
    cross_asset_enabled: bool = True
    
    # Logging and monitoring
    log_level: str = "INFO"
    enable_metrics: bool = True
    metrics_port: int = 8080
    
    # Development settings
    simulation_mode: bool = True
    dry_run: bool = False
    debug_mode: bool = False


def load_config(config_path: Optional[str] = None) -> PaperTradingConfig:
    """
    Load paper trading configuration from file or environment.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        PaperTradingConfig instance
    """
    # Default configuration
    config = PaperTradingConfig()
    
    # Load from file if provided
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            file_config = json.load(f)
        
        # Update config with file values
        for key, value in file_config.items():
            if hasattr(config, key):
                setattr(config, key, value)
    
    # Override with environment variables
    env_overrides = {
        'initial_cash': os.getenv('PAPER_TRADING_INITIAL_CASH'),
        'tenant_id': os.getenv('PAPER_TRADING_TENANT_ID'),
        'min_confidence': os.getenv('PAPER_TRADING_MIN_CONFIDENCE'),
        'llm_validation_enabled': os.getenv('PAPER_TRADING_LLM_ENABLED'),
        'simulation_mode': os.getenv('PAPER_TRADING_SIMULATION_MODE'),
        'debug_mode': os.getenv('PAPER_TRADING_DEBUG_MODE'),
    }
    
    for key, env_value in env_overrides.items():
        if env_value is not None:
            # Convert string to appropriate type
            if key == 'initial_cash':
                setattr(config, key, float(env_value))
            elif key in ['llm_validation_enabled', 'simulation_mode', 'debug_mode']:
                setattr(config, key, env_value.lower() in ['true', '1', 'yes'])
            else:
                setattr(config, key, env_value)
    
    return config


def get_qualification_layers_config(config: PaperTradingConfig) -> Dict[str, Dict[str, Any]]:
    """
    Get qualification layer configurations from main config.
    
    Args:
        config: Paper trading configuration
        
    Returns:
        Dictionary of layer configurations
    """
    return {
        'signal_quality': {
            'min_confidence': config.min_confidence,
            'min_consensus': config.min_consensus,
            'min_volume_ratio': config.min_volume_ratio,
            'max_volatility': config.max_volatility
        },
        'risk_management': {
            'max_position_pct': config.max_position_pct,
            'max_ticker_exposure': config.max_ticker_exposure,
            'max_sector_exposure': config.max_sector_exposure,
            'max_strategy_exposure': config.max_strategy_exposure,
            'max_daily_loss_pct': config.max_daily_loss_pct,
            'max_correlation_exposure': config.max_correlation_exposure
        },
        'llm_validation': {
            'enabled': config.llm_validation_enabled,
            'min_confidence_for_llm': config.llm_min_confidence,
            'provider': config.llm_provider,
            'model': config.llm_model
        }
    }


def get_paper_trader_config(config: PaperTradingConfig) -> Dict[str, Any]:
    """
    Get paper trader specific configuration.
    
    Args:
        config: Paper trading configuration
        
    Returns:
        Paper trader configuration dictionary
    """
    return {
        'initial_cash': config.initial_cash,
        'tenant_id': config.tenant_id,
        'base_position_pct': config.base_position_pct,
        'max_position_pct': config.max_position_pct,
        'max_ticker_exposure': config.max_ticker_exposure,
        'max_sector_exposure': config.max_sector_exposure,
        'max_strategy_exposure': config.max_strategy_exposure,
        'max_daily_loss_pct': config.max_daily_loss_pct,
        'max_correlation_exposure': config.max_correlation_exposure,
        'stop_loss_volatility_multiplier': config.stop_loss_volatility_multiplier,
        'reward_risk_ratio': config.reward_risk_ratio,
        'trailing_stop_enabled': config.trailing_stop_enabled,
        'trailing_stop_pct': config.trailing_stop_pct,
        'execution_delay_seconds': config.execution_delay_seconds,
        'slippage_bps': config.slippage_bps,
        'save_trade_history': config.save_trade_history,
        'trade_history_file': config.trade_history_file,
        'simulation_mode': config.simulation_mode,
        'dry_run': config.dry_run,
        'debug_mode': config.debug_mode,
        'position_sizing': {
            'base_position_pct': config.base_position_pct,
            'max_position_pct': config.max_position_pct,
            'confidence_threshold': config.confidence_threshold,
            'volatility_cap': config.volatility_cap,
            'max_leverage': config.max_leverage,
            'kelly_fraction': config.kelly_fraction,
            'kelly_confidence_weight': config.kelly_confidence_weight,
            'volatility_target': config.volatility_target,
            'volatility_lookback': config.volatility_lookback,
            'max_risk_per_trade': config.max_risk_per_trade,
            'drawdown_scaling': config.drawdown_scaling,
            'regime_adjustment': config.regime_adjustment,
            'max_ticker_exposure': config.max_ticker_exposure,
            'max_sector_exposure': config.max_sector_exposure,
            'max_strategy_exposure': config.max_strategy_exposure,
            'daily_loss_limit_pct': config.max_daily_loss_pct,
            'max_correlation_exposure': config.max_correlation_exposure
        },
        'risk_limits': {
            'max_position_size': config.max_position_pct,
            'max_ticker_exposure': config.max_ticker_exposure,
            'max_sector_exposure': config.max_sector_exposure,
            'max_strategy_exposure': config.max_strategy_exposure,
            'max_total_exposure': config.max_total_exposure,
            'max_daily_loss_pct': config.max_daily_loss_pct,
            'max_drawdown_pct': config.max_drawdown_pct,
            'stop_loss_pct': config.stop_loss_volatility_multiplier * 0.02,  # Approximate
            'trailing_stop_pct': config.trailing_stop_pct,
            'max_concurrent_trades': config.max_concurrent_trades,
            'max_trades_per_day': config.max_trades_per_day,
            'max_trades_per_hour': config.max_trades_per_hour,
            'loss_cooldown_minutes': config.loss_cooldown_minutes,
            'consecutive_loss_limit': config.consecutive_loss_limit,
            'confidence_floor': config.confidence_floor,
            'emergency_halt_loss_pct': config.emergency_halt_loss_pct,
            'position_size_emergency_cap': config.max_position_pct * 0.5
        }
    }


def save_default_config(config_path: str = "config/paper_trading.json") -> None:
    """
    Save default configuration to file.
    
    Args:
        config_path: Path to save configuration
    """
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    
    config = PaperTradingConfig()
    config_dict = {
        'initial_cash': config.initial_cash,
        'tenant_id': config.tenant_id,
        'base_position_pct': config.base_position_pct,
        'max_position_pct': config.max_position_pct,
        'confidence_scaling': config.confidence_scaling,
        'max_ticker_exposure': config.max_ticker_exposure,
        'max_sector_exposure': config.max_sector_exposure,
        'max_strategy_exposure': config.max_strategy_exposure,
        'max_daily_loss_pct': config.max_daily_loss_pct,
        'max_correlation_exposure': config.max_correlation_exposure,
        'stop_loss_volatility_multiplier': config.stop_loss_volatility_multiplier,
        'reward_risk_ratio': config.reward_risk_ratio,
        'trailing_stop_enabled': config.trailing_stop_enabled,
        'trailing_stop_pct': config.trailing_stop_pct,
        'min_confidence': config.min_confidence,
        'min_consensus': config.min_consensus,
        'min_volume_ratio': config.min_volume_ratio,
        'max_volatility': config.max_volatility,
        'llm_validation_enabled': config.llm_validation_enabled,
        'llm_min_confidence': config.llm_min_confidence,
        'llm_provider': config.llm_provider,
        'llm_model': config.llm_model,
        'market_hours_only': config.market_hours_only,
        'execution_delay_seconds': config.execution_delay_seconds,
        'slippage_bps': config.slippage_bps,
        'performance_update_interval': config.performance_update_interval,
        'save_trade_history': config.save_trade_history,
        'trade_history_file': config.trade_history_file,
        'feature_version': config.feature_version,
        'legacy_mode': config.legacy_mode,
        'cross_asset_enabled': config.cross_asset_enabled,
        'log_level': config.log_level,
        'enable_metrics': config.enable_metrics,
        'metrics_port': config.metrics_port,
        'simulation_mode': config.simulation_mode,
        'dry_run': config.dry_run,
        'debug_mode': config.debug_mode
    }
    
    with open(config_path, 'w') as f:
        json.dump(config_dict, f, indent=2)
    
    print(f"Default configuration saved to {config_path}")


# Environment-specific configurations
def get_development_config() -> PaperTradingConfig:
    """Get development configuration."""
    config = PaperTradingConfig()
    config.initial_cash = 10000.0  # Smaller portfolio for testing
    config.min_confidence = 0.5     # Lower threshold for more signals
    config.debug_mode = True
    config.simulation_mode = True
    config.dry_run = True
    return config


def get_production_config() -> PaperTradingConfig:
    """Get production configuration."""
    config = PaperTradingConfig()
    config.initial_cash = 1000000.0  # Larger portfolio
    config.min_confidence = 0.7       # Higher threshold
    config.debug_mode = False
    config.simulation_mode = False
    config.dry_run = False
    config.llm_validation_enabled = True
    return config


def get_testing_config() -> PaperTradingConfig:
    """Get testing configuration."""
    config = PaperTradingConfig()
    config.initial_cash = 1000.0
    config.min_confidence = 0.3      # Very low for testing
    config.execution_delay_seconds = 0.0
    config.debug_mode = True
    config.save_trade_history = False
    return config


# Configuration validation
def validate_config(config: PaperTradingConfig) -> List[str]:
    """
    Validate paper trading configuration.
    
    Args:
        config: Configuration to validate
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # Portfolio validation
    if config.initial_cash <= 0:
        errors.append("initial_cash must be positive")
    
    # Position sizing validation
    if config.base_position_pct <= 0 or config.base_position_pct > 0.1:
        errors.append("base_position_pct must be between 0 and 0.1 (10%)")
    
    if config.max_position_pct <= config.base_position_pct:
        errors.append("max_position_pct must be greater than base_position_pct")
    
    # Risk limits validation
    if config.max_ticker_exposure > 0.5:
        errors.append("max_ticker_exposure should not exceed 50%")
    
    if config.max_daily_loss_pct > 0.1:
        errors.append("max_daily_loss_pct should not exceed 10%")
    
    # Signal quality validation
    if not 0 <= config.min_confidence <= 1:
        errors.append("min_confidence must be between 0 and 1")
    
    if not 0 <= config.min_consensus <= 1:
        errors.append("min_consensus must be between 0 and 1")
    
    # LLM validation
    if config.llm_validation_enabled and config.llm_min_confidence < config.min_confidence:
        errors.append("llm_min_confidence should be >= min_confidence")
    
    return errors
