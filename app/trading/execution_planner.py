"""
Execution Planner

Takes FINAL SIGNALS from Alpha Engine and plans how to execute them.
No re-ranking, no re-selection - just execution planning.

Alpha Engine produces final signals through:
- Performance learning
- Weight engine  
- Champion selection
- Consensus

ExecutionPlanner receives these already-ranked signals and plans:
- Execution timing and sequencing
- Order sizing and splitting
- Market impact minimization
- Portfolio integration
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ExecutionPriority(Enum):
    """Execution priority levels."""
    IMMEDIATE = "immediate"      # Execute now (high conviction, time sensitive)
    HIGH = "high"                # Execute soon (strong signal)
    NORMAL = "normal"            # Standard execution
    BATCH = "batch"              # Batch with other orders (lower urgency)
    CONDITIONAL = "conditional"  # Wait for condition


class ExecutionStrategy(Enum):
    """Execution strategies for orders."""
    MARKET = "market"                    # Immediate market order
    LIMIT = "limit"                      # Limit order at specified price
    TWAP = "twap"                        # Time-weighted average price
    VWAP = "vwap"                        # Volume-weighted average price
    ICEBERG = "iceberg"                  # Hidden large order
    PEGGED = "pegged"                    # Peg to bid/ask/mid
    STOP = "stop"                        # Stop order
    STOP_LIMIT = "stop_limit"            # Stop-limit order
    TRAILING_STOP = "trailing_stop"      # Trailing stop


@dataclass(frozen=True)
class Signal:
    """Immutable Alpha Engine signal.
    
    Prevents accidental mutation to preserve learning purity.
    Signal confidence/direction are frozen at generation time.
    Links to prediction for outcome traceability.
    """
    id: str
    ticker: str
    direction: str  # "long" or "short"
    confidence: float
    alpha_score: float
    consensus_score: float
    strategy_id: str
    entry_price: float
    quantity: float
    regime: str
    feature_snapshot: Dict[str, Any] = field(default_factory=dict, hash=False)
    timestamp: Any = field(default=None)
    prediction_id: str = ""  # Links to Prediction for outcome tracking
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'id': self.id,
            'ticker': self.ticker,
            'direction': self.direction,
            'confidence': self.confidence,
            'alpha_score': self.alpha_score,
            'consensus_score': self.consensus_score,
            'strategy_id': self.strategy_id,
            'entry_price': self.entry_price,
            'quantity': self.quantity,
            'regime': self.regime,
            'features': self.feature_snapshot,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'prediction_id': self.prediction_id
        }


@dataclass
class ExecutionPlan:
    """Complete execution plan for a signal."""
    signal_id: str
    ticker: str
    direction: str  # "long" or "short"
    
    # Execution parameters
    target_quantity: float
    execution_strategy: ExecutionStrategy
    priority: ExecutionPriority
    
    # Timing
    scheduled_time: Optional[datetime] = None
    expire_time: Optional[datetime] = None
    
    # Price limits
    max_entry_price: Optional[float] = None  # Don't pay more than this (long)
    min_entry_price: Optional[float] = None  # Don't accept less than this (short)
    
    # Sizing and splitting
    slice_size: Optional[float] = None      # Size per slice for TWAP/VWAP
    num_slices: int = 1
    slice_interval_seconds: int = 0
    
    # Risk controls
    max_slippage_bps: float = 50.0         # Cancel if slippage exceeds this
    time_limit_seconds: int = 300            # Cancel if not filled within this time
    
    # Metadata
    confidence: float = 0.0
    strategy_id: str = ""
    regime: str = "UNKNOWN"
    feature_snapshot: Dict[str, Any] = field(default_factory=dict)
    
    # Execution tracking
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "planned"  # planned, executing, completed, cancelled, failed


@dataclass
class PortfolioConstraints:
    """Portfolio-level execution constraints."""
    max_concurrent_orders: int = 5
    max_exposure_pct: float = 0.95
    min_cash_buffer: float = 0.05
    sector_concentration_limit: float = 0.25
    max_orders_per_ticker: int = 1


class ExecutionPlanner:
    """
    Plans execution of FINAL SIGNALS from Alpha Engine.
    
    Does NOT re-rank or re-select. Alpha Engine already did that.
    Just plans HOW to execute the already-ranked signals efficiently.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.constraints = PortfolioConstraints(
            max_concurrent_orders=config.get('max_concurrent_orders', 5),
            max_exposure_pct=config.get('max_exposure_pct', 0.95),
            min_cash_buffer=config.get('min_cash_buffer', 0.05),
            sector_concentration_limit=config.get('sector_concentration_limit', 0.25)
        )
        
        # Execution parameters
        self.default_strategy = ExecutionStrategy(
            config.get('default_strategy', 'market')
        )
        self.max_slippage_bps = config.get('max_slippage_bps', 50.0)
        self.time_limit_seconds = config.get('time_limit_seconds', 300)
        
        logger.info("ExecutionPlanner initialized - planning final signal execution")
    
    def plan_execution(
        self,
        signal: Dict[str, Any],
        portfolio_state: Dict[str, Any],
        market_conditions: Optional[Dict[str, Any]] = None
    ) -> ExecutionPlan:
        """
        Create execution plan for a final Alpha Engine signal.
        
        Args:
            signal: Final signal from Alpha Engine (already ranked by weight)
            portfolio_state: Current portfolio state
            market_conditions: Optional market condition data
            
        Returns:
            ExecutionPlan with complete execution strategy
        """
        ticker = signal['ticker']
        confidence = signal.get('confidence', 0.5)
        direction = signal.get('direction', 'long')
        target_quantity = signal.get('quantity', 0.0)
        
        # Determine execution priority based on confidence and urgency
        priority = self._determine_priority(signal, market_conditions)
        
        # Determine execution strategy based on size and urgency
        strategy = self._determine_strategy(
            target_quantity, 
            confidence, 
            priority,
            market_conditions
        )
        
        # Calculate order slicing for large orders
        slice_size, num_slices, interval = self._calculate_slicing(
            target_quantity, 
            strategy,
            market_conditions
        )
        
        # Set price limits
        max_entry, min_entry = self._calculate_price_limits(
            signal.get('entry_price', 0.0),
            direction,
            confidence,
            market_conditions
        )
        
        # Create execution plan
        plan = ExecutionPlan(
            signal_id=signal.get('id', ''),
            ticker=ticker,
            direction=direction,
            target_quantity=target_quantity,
            execution_strategy=strategy,
            priority=priority,
            slice_size=slice_size,
            num_slices=num_slices,
            slice_interval_seconds=interval,
            max_entry_price=max_entry,
            min_entry_price=min_entry,
            max_slippage_bps=self.max_slippage_bps,
            time_limit_seconds=self.time_limit_seconds,
            confidence=confidence,
            strategy_id=signal.get('strategy_id', ''),
            regime=signal.get('regime', 'UNKNOWN'),
            feature_snapshot=signal.get('features', {})
        )
        
        logger.info(
            f"Planned execution for {ticker}: {strategy.value}, "
            f"priority={priority.value}, qty={target_quantity:.2f}"
        )
        
        return plan
    
    def plan_batch_execution(
        self,
        signals: List[Dict[str, Any]],
        portfolio_state: Dict[str, Any],
        market_conditions: Optional[Dict[str, Any]] = None
    ) -> List[ExecutionPlan]:
        """
        Plan execution for multiple signals with portfolio constraints.
        
        Respects:
        - max_concurrent_orders
        - cash availability
        - exposure limits
        """
        plans = []
        
        # Sort by priority (execute high confidence first)
        sorted_signals = sorted(
            signals,
            key=lambda s: s.get('confidence', 0),
            reverse=True
        )
        
        for signal in sorted_signals:
            # Check portfolio constraints
            if not self._can_execute_signal(signal, portfolio_state, plans):
                logger.info(f"Skipping {signal['ticker']} - portfolio constraints")
                continue
            
            plan = self.plan_execution(signal, portfolio_state, market_conditions)
            plans.append(plan)
            
            # Update portfolio state for next iteration
            portfolio_state = self._simulate_portfolio_update(portfolio_state, plan)
        
        return plans
    
    def _determine_priority(
        self,
        signal: Dict[str, Any],
        market_conditions: Optional[Dict[str, Any]]
    ) -> ExecutionPriority:
        """Determine execution priority based on signal characteristics."""
        confidence = signal.get('confidence', 0.5)
        urgency = signal.get('urgency', 'normal')
        
        if urgency == 'immediate' or confidence > 0.9:
            return ExecutionPriority.IMMEDIATE
        elif confidence > 0.75:
            return ExecutionPriority.HIGH
        elif urgency == 'batch' or confidence < 0.6:
            return ExecutionPriority.BATCH
        elif urgency == 'conditional':
            return ExecutionPriority.CONDITIONAL
        else:
            return ExecutionPriority.NORMAL
    
    def _determine_strategy(
        self,
        quantity: float,
        confidence: float,
        priority: ExecutionPriority,
        market_conditions: Optional[Dict[str, Any]]
    ) -> ExecutionStrategy:
        """Determine execution strategy based on size and urgency."""
        # Small orders or high urgency = market order
        if quantity < 1000 or priority in [ExecutionPriority.IMMEDIATE, ExecutionPriority.HIGH]:
            return ExecutionStrategy.MARKET
        
        # Large orders need careful execution
        if quantity > 10000:
            return ExecutionStrategy.TWAP
        
        # Medium orders = VWAP or limit
        if market_conditions and market_conditions.get('volume_profile'):
            return ExecutionStrategy.VWAP
        
        return ExecutionStrategy.LIMIT
    
    def _calculate_slicing(
        self,
        quantity: float,
        strategy: ExecutionStrategy,
        market_conditions: Optional[Dict[str, Any]]
    ) -> Tuple[Optional[float], int, int]:
        """Calculate order slicing parameters."""
        if strategy not in [ExecutionStrategy.TWAP, ExecutionStrategy.VWAP]:
            return None, 1, 0
        
        # Slice large orders
        if quantity <= 5000:
            return None, 1, 0
        
        # Calculate optimal slice size
        avg_daily_volume = market_conditions.get('avg_daily_volume', 1000000) if market_conditions else 1000000
        max_slice_pct = 0.01  # Max 1% of ADV per slice
        
        slice_size = min(quantity * 0.2, avg_daily_volume * max_slice_pct)
        num_slices = int(quantity / slice_size)
        
        # Distribute over time (e.g., 5 minutes for TWAP)
        total_time_seconds = 300
        interval = total_time_seconds // max(num_slices - 1, 1)
        
        return slice_size, num_slices, interval
    
    def _calculate_price_limits(
        self,
        entry_price: float,
        direction: str,
        confidence: float,
        market_conditions: Optional[Dict[str, Any]]
    ) -> Tuple[Optional[float], Optional[float]]:
        """Calculate price limits based on confidence and volatility."""
        if entry_price <= 0:
            return None, None
        
        # Wider limits for lower confidence
        volatility = market_conditions.get('volatility', 0.02) if market_conditions else 0.02
        spread = market_conditions.get('spread_bps', 10) / 10000 if market_conditions else 0.001
        
        # Max acceptable slippage increases with volatility, decreases with confidence
        max_slippage = (1.5 - confidence) * volatility * 3 + spread
        
        if direction == 'long':
            max_entry = entry_price * (1 + max_slippage)
            min_entry = None  # No lower limit for long entries
        else:  # short
            max_entry = None  # No upper limit for short entries
            min_entry = entry_price * (1 - max_slippage)
        
        return max_entry, min_entry
    
    def _can_execute_signal(
        self,
        signal: Dict[str, Any],
        portfolio_state: Dict[str, Any],
        existing_plans: List[ExecutionPlan]
    ) -> bool:
        """Check if signal can be executed given portfolio constraints."""
        # Check concurrent order limit
        active_orders = len(existing_plans)
        if active_orders >= self.constraints.max_concurrent_orders:
            return False
        
        # Check cash availability
        cash = portfolio_state.get('cash', 0)
        position_value = signal.get('quantity', 0) * signal.get('entry_price', 0)
        
        if signal.get('direction') == 'long' and position_value > cash * (1 - self.constraints.min_cash_buffer):
            return False
        
        # Check for duplicate ticker
        ticker = signal['ticker']
        existing_tickers = [p.ticker for p in existing_plans]
        if existing_tickers.count(ticker) >= self.constraints.max_orders_per_ticker:
            return False
        
        return True
    
    def _simulate_portfolio_update(
        self,
        portfolio_state: Dict[str, Any],
        plan: ExecutionPlan
    ) -> Dict[str, Any]:
        """Simulate portfolio update for planning purposes."""
        updated = portfolio_state.copy()
        
        position_value = plan.target_quantity * plan.feature_snapshot.get('entry_price', 0)
        
        if plan.direction == 'long':
            updated['cash'] = updated.get('cash', 0) - position_value
            updated['long_exposure'] = updated.get('long_exposure', 0) + position_value
        else:
            updated['cash'] = updated.get('cash', 0) + position_value
            updated['short_exposure'] = updated.get('short_exposure', 0) + position_value
        
        return updated
    
    def get_plan_summary(self, plans: List[ExecutionPlan]) -> Dict[str, Any]:
        """Get summary of execution plans."""
        if not plans:
            return {'total_plans': 0}
        
        strategies = {}
        priorities = {}
        total_quantity = 0
        
        for plan in plans:
            strategies[plan.execution_strategy.value] = strategies.get(plan.execution_strategy.value, 0) + 1
            priorities[plan.priority.value] = priorities.get(plan.priority.value, 0) + 1
            total_quantity += plan.target_quantity
        
        return {
            'total_plans': len(plans),
            'total_quantity': total_quantity,
            'strategies': strategies,
            'priorities': priorities,
            'avg_confidence': sum(p.confidence for p in plans) / len(plans)
        }
