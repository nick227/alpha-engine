"""
Paper Trading Module

Comprehensive paper trading system for Alpha Engine.
Integrates feature engineering, qualification pipeline, and execution.
"""

from .paper_trader import (
    PaperTrader,
    PortfolioState,
    TradeDirection,
    TradeStatus,
    SignalType,
    QualificationLayer,
    SignalQualityFilter,
    RiskEngineLayer,
    LLMAnalysisLayer
)

from .position_sizing import (
    PositionSizer,
    PositionSizeResult,
    SizingContext,
    PositionSizingMethod
)

# Risk management
from .risk_engine import (
    RiskEngine,
    RiskCheckResult,
    RiskMetrics,
    RiskLimits,
    RiskLevel,
    RiskAction
)

# Trade lifecycle
from app.trading.execution_planner import (
    ExecutionPlanner,
    ExecutionPlan,
    ExecutionPriority,
    ExecutionStrategy,
    PortfolioConstraints,
    Signal
)
from app.trading.execution_simulator import (
    ExecutionSimulator,
    ExecutionResult,
    MarketCondition
)
from .trade_lifecycle import (
    TradeLifecycleManager,
    Trade,
    TradePosition,
    TradeLeg,
    TradeState,
    ExitReason,
    OrderType
)

from .alpha_integration import (
    AlphaEngineIntegration,
    PaperTradingOrchestrator
)

from .config import (
    PaperTradingConfig,
    load_config,
    save_default_config,
    validate_config,
    get_development_config,
    get_production_config,
    get_testing_config,
    get_qualification_layers_config,
    get_paper_trader_config
)

from .main import PaperTradingSystem

__version__ = "1.0.0"
__author__ = "Alpha Engine Team"

__all__ = [
    # Core paper trading
    "PaperTrader",
    "PortfolioState",
    "TradeDirection",
    "TradeStatus",
    "SignalType",
    
    # Qualification layers
    "QualificationLayer",
    "SignalQualityFilter",
    "RiskEngineLayer",
    "LLMValidationLayer",
    
    # Position sizing
    "PositionSizer",
    "PositionSizeResult",
    "SizingContext",
    "PositionSizingMethod",
    
    # Risk management
    "RiskEngine",
    "RiskCheckResult",
    "RiskMetrics",
    "RiskLimits",
    "RiskLevel",
    "RiskAction",
    
    # Trade lifecycle
    "TradeLifecycleManager",
    "Trade",
    "TradePosition",
    "TradeLeg",
    "TradeState",
    "ExitReason",
    "OrderType",
    
    # Execution planning
    "ExecutionPlanner",
    "ExecutionPlan",
    "ExecutionPriority",
    "ExecutionStrategy",
    "PortfolioConstraints",
    "Signal",
    
    # Execution simulation
    "ExecutionSimulator",
    "ExecutionResult",
    "MarketCondition",
    
    # Integration
    "AlphaEngineIntegration",
    "PaperTradingOrchestrator",
    
    # Configuration
    "PaperTradingConfig",
    "load_config",
    "save_default_config",
    "validate_config",
    "get_development_config",
    "get_production_config",
    "get_testing_config",
    "get_qualification_layers_config",
    "get_paper_trader_config",
    
    # Main system
    "PaperTradingSystem"
]
