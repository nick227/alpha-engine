"""
Trade Lifecycle Management

Comprehensive trade lifecycle system for paper trading.
Manages trades from signal generation through complete position lifecycle.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
import uuid
import logging

logger = logging.getLogger(__name__)


class TradeState(Enum):
    """Trade lifecycle states."""
    SIGNAL = "signal"
    PENDING_ENTRY = "pending_entry"
    ENTERED = "entered"
    HOLDING = "holding"
    PARTIAL_EXIT = "partial_exit"
    STOPPED = "stopped"
    CLOSED = "closed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class ExitReason(Enum):
    """Reasons for trade exit."""
    TARGET_REACHED = "target_reached"
    STOP_LOSS = "stop_loss"
    TRAILING_STOP = "trailing_stop"
    TIME_EXIT = "time_exit"
    MANUAL_CLOSE = "manual_close"
    RISK_HALT = "risk_halt"
    EMERGENCY_EXIT = "emergency_exit"
    LIQUIDATION = "liquidation"


class OrderType(Enum):
    """Order types for trade execution."""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


@dataclass
class TradeLeg:
    """Individual leg of a trade (for partial exits)."""
    id: str
    trade_id: str
    leg_type: str  # "entry", "partial_exit", "stop_exit", "final_exit"
    direction: str  # "long" or "short"
    quantity: float
    price: float
    timestamp: datetime
    order_type: OrderType
    reason: Optional[str] = None
    commission: float = 0.0
    fees: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TradePosition:
    """Current position state for a trade."""
    ticker: str
    direction: str  # "long" or "short"
    total_quantity: float
    filled_quantity: float
    remaining_quantity: float
    average_entry_price: float
    current_price: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    last_updated: datetime


@dataclass
class Trade:
    """Complete trade lifecycle management.
    
    Stores immutable signal snapshot to prevent drift and enable
    accurate backtesting/replay. Links to prediction for outcome
    traceability in learning loop.
    """
    # Required fields (no defaults) - must come first
    id: str
    signal_id: str
    strategy_id: str
    ticker: str
    direction: str  # "long" or "short"
    entry_price: float
    target_price: Optional[float]
    stop_loss_price: Optional[float]
    trailing_stop_price: Optional[float]
    quantity: float
    order_type: OrderType
    signal_timestamp: datetime
    entry_timestamp: Optional[datetime]
    exit_timestamp: Optional[datetime]
    duration_minutes: Optional[float]
    state: TradeState
    position: Optional[TradePosition]
    
    # Optional fields (with defaults) - must come after required fields
    prediction_id: str = ""
    signal_snapshot: Dict[str, Any] = field(default_factory=dict)
    legs: List[TradeLeg] = field(default_factory=list)
    state_history: List[Tuple[datetime, TradeState, str]] = field(default_factory=list)
    realized_pnl: float = 0.0
    realized_pnl_pct: float = 0.0
    max_runup: float = 0.0
    max_drawdown: float = 0.0
    exit_reason: Optional[ExitReason] = None
    exit_price: Optional[float] = None
    confidence: float = 0.0
    regime: str = "UNKNOWN"
    feature_snapshot: Dict[str, Any] = field(default_factory=dict)
    risk_metrics: Dict[str, Any] = field(default_factory=dict)
    mode: str = "backtest"  # backtest | paper | live - keeps corpus clean
    analysis: str = ""      # LLM generated analysis/justification
    llm_prediction: str = "" # LLM recommendation (QUALIFIED | CAUTION | REJECT)

    on_entry: Optional[Callable] = None
    on_exit: Optional[Callable] = None
    on_partial_exit: Optional[Callable] = None
    on_stop: Optional[Callable] = None
    
    def __post_init__(self):
        """Initialize trade state."""
        self._add_state_history(TradeState.SIGNAL, "Trade created from signal")


class TradeLifecycleManager:
    """
    Manages complete trade lifecycle from signal to exit.
    
    Handles entry, holding, partial exits, stops, and closing.
    """
    
    def __init__(self, config: Dict[str, Any], provider: Optional[TradingProvider] = None, repository: Optional[AlphaRepository] = None):
        self.config = config
        self.provider = provider
        self.repository = repository
        self.active_trades: Dict[str, Trade] = {}
        self.completed_trades: Dict[str, Trade] = {}
        self.trade_history: List[Trade] = []
        
        # Lifecycle configuration
        self.default_stop_loss_pct = config.get('stop_loss_pct', 0.02)
        self.default_target_pct = config.get('target_pct', 0.04)
        self.trailing_stop_pct = config.get('trailing_stop_pct', 0.015)
        self.max_hold_time_hours = config.get('max_hold_time_hours', 24)
        self.partial_exit_enabled = config.get('partial_exit_enabled', True)
        self.partial_exit_levels = config.get('partial_exit_levels', [0.5, 0.75])
        
        # Monitoring
        self.price_subscribers: Dict[str, List[Callable]] = {}
        
        logger.info(f"Trade lifecycle manager initialized with provider: {getattr(provider, 'name', 'None')}")
    
    def create_trade_from_signal(
        self,
        signal_id: str,
        strategy_id: str,
        ticker: str,
        direction: str,
        entry_price: float,
        quantity: float,
        confidence: float,
        regime: str = "UNKNOWN",
        target_price: Optional[float] = None,
        stop_loss_price: Optional[float] = None,
        feature_snapshot: Optional[Dict[str, Any]] = None,
        order_type: OrderType = OrderType.MARKET,
        prediction_id: str = "",
        mode: str = "backtest",
        analysis: str = "",
        llm_prediction: str = "",
        engine_decision: str = "",
        llm_status: str = "",
        llm_agrees: Optional[int] = None
    ) -> Trade:
        """
        Create a new trade from a trading signal.
        
        Args:
            signal_id: Unique signal identifier
            strategy_id: Strategy that generated the signal
            ticker: Trading symbol
            direction: "long" or "short"
            entry_price: Desired entry price
            quantity: Position size
            confidence: Signal confidence
            regime: Market regime
            target_price: Optional target price
            stop_loss_price: Optional stop loss price
            feature_snapshot: Signal feature data
            order_type: Order type for execution
            
        Returns:
            Created Trade object
        """
        # Generate trade ID
        trade_id = str(uuid.uuid4())
        
        # Calculate default stop loss and target if not provided
        if stop_loss_price is None:
            if direction.lower() == "long":
                stop_loss_price = entry_price * (1 - self.default_stop_loss_pct)
            else:
                stop_loss_price = entry_price * (1 + self.default_stop_loss_pct)
        
        if target_price is None:
            if direction.lower() == "long":
                target_price = entry_price * (1 + self.default_target_pct)
            else:
                target_price = entry_price * (1 - self.default_target_pct)
        
        # Build immutable signal snapshot (prevents drift)
        signal_snapshot = {
            'signal_id': signal_id,
            'strategy_id': strategy_id,
            'ticker': ticker,
            'direction': direction,
            'entry_price': entry_price,
            'quantity': quantity,
            'confidence': confidence,
            'regime': regime,
            'target_price': target_price,
            'stop_loss_price': stop_loss_price,
            'feature_snapshot': feature_snapshot or {},
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'prediction_id': prediction_id,  # For learning loop traceability
            'mode': mode,  # backtest | paper | live - keeps corpus clean
            'engine_decision': engine_decision,
            'llm_status': llm_status,
            'llm_agrees': llm_agrees
        }
        
        # Create trade with signal snapshot
        trade = Trade(
            id=trade_id,
            signal_id=signal_id,
            strategy_id=strategy_id,
            ticker=ticker,
            direction=direction.lower(),
            prediction_id=prediction_id,  # Link to prediction for outcome tracking
            signal_snapshot=signal_snapshot,  # Immutable reference
            entry_price=entry_price,
            target_price=target_price,
            stop_loss_price=stop_loss_price,
            trailing_stop_price=None,
            quantity=quantity,
            order_type=order_type,
            signal_timestamp=datetime.now(timezone.utc),
            entry_timestamp=None,
            exit_timestamp=None,
            duration_minutes=None,
            state=TradeState.SIGNAL,
            position=None,
            legs=[],
            state_history=[],
            realized_pnl=0.0,
            realized_pnl_pct=0.0,
            max_runup=0.0,
            max_drawdown=0.0,
            exit_reason=None,
            exit_price=None,
            confidence=confidence,
            regime=regime,
            feature_snapshot=feature_snapshot or {},
            risk_metrics={},
            mode=mode,
            analysis=analysis,
            llm_prediction=llm_prediction
        )
        
        # Store as active trade
        self.active_trades[trade_id] = trade
        
        logger.info(f"Created trade {trade_id} for {ticker} {direction} @ {entry_price}")
        
        return trade
    
    async def execute_entry(self, trade_id: str, execution_price: float, execution_quantity: Optional[float] = None) -> bool:
        """
        Execute trade entry.
        
        Args:
            trade_id: Trade identifier
            execution_price: Actual execution price
            execution_quantity: Executed quantity (if different from requested)
            
        Returns:
            True if entry successful, False otherwise
        """
        trade = self.active_trades.get(trade_id)
        if not trade:
            logger.error(f"Trade {trade_id} not found for entry")
            return False
        
        if trade.state != TradeState.SIGNAL and trade.state != TradeState.PENDING_ENTRY:
            logger.error(f"Trade {trade_id} in invalid state for entry: {trade.state}")
            return False

        # If we have a provider, submit the order to the real/sim broker
        if self.provider:
            try:
                # Use requested quantity if execution_quantity not provided
                qty = execution_quantity if execution_quantity is not None else trade.quantity
                
                order_result = await self.provider.submit_order(
                    ticker=trade.ticker,
                    quantity=qty,
                    direction=trade.direction,
                    order_type=trade.order_type
                )
                
                # Update with actual fill data from provider if possible
                execution_price = order_result.get("average_fill_price", execution_price)
                filled_quantity = order_result.get("filled_quantity", qty)
            except Exception as e:
                logger.error(f"Provider failed to execute entry for {trade.ticker}: {e}")
                return False
        
        # Use execution quantity or default to requested quantity
        if not self.provider:
            filled_quantity = execution_quantity if execution_quantity is not None else trade.quantity
        
        # Create entry leg
        entry_leg = TradeLeg(
            id=str(uuid.uuid4()),
            trade_id=trade_id,
            leg_type="entry",
            direction=trade.direction,
            quantity=filled_quantity,
            price=execution_price,
            timestamp=datetime.now(timezone.utc),
            order_type=trade.order_type,
            reason="Market entry execution"
        )
        
        # Record entry in repository if available
        if self.repository:
            try:
                self.repository.save_trade({
                    "id": trade.id,
                    "ticker": trade.ticker,
                    "direction": trade.direction,
                    "quantity": filled_quantity,
                    "entry_price": execution_price,
                    "status": "EXECUTED",
                    "mode": trade.mode,
                    "strategy_id": trade.strategy_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "analysis": trade.analysis,
                    "llm_prediction": trade.llm_prediction,
                    "engine_decision": trade.signal_snapshot.get("engine_decision"),
                    "llm_status": trade.signal_snapshot.get("llm_status"),
                    "llm_agrees": trade.signal_snapshot.get("llm_agrees")
                })
                self.repository.upsert_position({
                    "ticker": trade.ticker,
                    "direction": trade.direction,
                    "quantity": filled_quantity,
                    "average_entry_price": execution_price,
                    "mode": trade.mode
                })
            except Exception as e:
                logger.error(f"Failed to persist trade entry to repository: {e}")

        # Update trade
        trade.legs.append(entry_leg)
        trade.entry_timestamp = entry_leg.timestamp
        trade.state = TradeState.ENTERED
        
        # Create position
        trade.position = TradePosition(
            ticker=trade.ticker,
            direction=trade.direction,
            total_quantity=trade.quantity,
            filled_quantity=filled_quantity,
            remaining_quantity=trade.quantity - filled_quantity,
            average_entry_price=execution_price,
            current_price=execution_price,
            unrealized_pnl=0.0,
            unrealized_pnl_pct=0.0,
            last_updated=datetime.now(timezone.utc)
        )
        
        # Calculate initial P&L
        self._update_position_pnl(trade, execution_price)
        
        # Set trailing stop if enabled
        if self.trailing_stop_pct > 0:
            if trade.direction == "long":
                trade.trailing_stop_price = execution_price * (1 - self.trailing_stop_pct)
            else:
                trade.trailing_stop_price = execution_price * (1 + self.trailing_stop_pct)
        
        # Add state history
        trade._add_state_history(TradeState.ENTERED, f"Entry executed @ {execution_price}")
        
        # Trigger callback
        if trade.on_entry:
            try:
                trade.on_entry(trade, entry_leg)
            except Exception as e:
                logger.error(f"Error in entry callback for trade {trade_id}: {e}")
        
        logger.info(f"Trade {trade_id} entered: {filled_quantity} @ {execution_price}")
        
        return True
    
    def update_position(
        self,
        trade_id: str,
        current_price: float,
        timestamp: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Update trade position with current price.
        
        Args:
            trade_id: Trade identifier
            current_price: Current market price
            timestamp: Update timestamp (defaults to now)
            
        Returns:
            Position update information or None if trade not found
        """
        trade = self.active_trades.get(trade_id)
        if not trade or not trade.position or trade.state != TradeState.HOLDING:
            return None
        
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        # Update position
        old_pnl = trade.position.unrealized_pnl
        trade.position.current_price = current_price
        trade.position.last_updated = timestamp
        
        # Calculate unrealized P&L
        self._update_position_pnl(trade, current_price)
        
        # Update max runup and drawdown
        if trade.position.unrealized_pnl > trade.max_runup:
            trade.max_runup = trade.position.unrealized_pnl
        
        if trade.position.unrealized_pnl < trade.max_drawdown:
            trade.max_drawdown = trade.position.unrealized_pnl
        
        # Check for exit conditions
        exit_actions = self._check_exit_conditions(trade, current_price, timestamp)
        
        # Update trailing stop
        if trade.trailing_stop_price is not None:
            self._update_trailing_stop(trade, current_price)
        
        # Add state history if significant change
        pnl_change = abs(trade.position.unrealized_pnl - old_pnl)
        if pnl_change > 0.001:  # Significant P&L change
            trade._add_state_history(
                TradeState.HOLDING, 
                f"Position updated: P&L {trade.position.unrealized_pnl:.4f} ({trade.position.unrealized_pnl_pct:.2%})"
            )
        
        return {
            'trade_id': trade_id,
            'old_pnl': old_pnl,
            'new_pnl': trade.position.unrealized_pnl,
            'pnl_change': trade.position.unrealized_pnl - old_pnl,
            'exit_actions': exit_actions,
            'position': trade.position
        }
    
    def execute_partial_exit(
        self,
        trade_id: str,
        exit_quantity: float,
        exit_price: Optional[float] = None,
        reason: str = "Partial exit"
    ) -> bool:
        """
        Execute partial exit from a trade.
        
        Args:
            trade_id: Trade identifier
            exit_quantity: Quantity to exit
            exit_price: Exit price (defaults to current price)
            reason: Exit reason
            
        Returns:
            True if partial exit successful, False otherwise
        """
        trade = self.active_trades.get(trade_id)
        if not trade or not trade.position:
            logger.error(f"Trade {trade_id} not found or no position for partial exit")
            return False
        
        if trade.state != TradeState.HOLDING:
            logger.error(f"Trade {trade_id} in invalid state for partial exit: {trade.state}")
            return False
        
        if exit_quantity > trade.position.remaining_quantity:
            logger.error(f"Exit quantity {exit_quantity} exceeds remaining {trade.position.remaining_quantity}")
            return False
        
        # Use current price if not provided
        if exit_price is None:
            exit_price = trade.position.current_price
        
        # Create partial exit leg
        exit_leg = TradeLeg(
            id=str(uuid.uuid4()),
            trade_id=trade_id,
            leg_type="partial_exit",
            direction="short" if trade.direction == "long" else "long",  # Opposite direction
            quantity=exit_quantity,
            price=exit_price,
            timestamp=datetime.now(timezone.utc),
            order_type=OrderType.MARKET,
            reason=reason
        )
        
        # Calculate P&L for partial exit
        partial_pnl = self._calculate_leg_pnl(trade, exit_leg)
        
        # Update trade
        trade.legs.append(exit_leg)
        trade.position.remaining_quantity -= exit_quantity
        trade.realized_pnl += partial_pnl
        trade.state = TradeState.PARTIAL_EXIT
        
        # Update position P&L
        self._update_position_pnl(trade, trade.position.current_price)
        
        # Add state history
        trade._add_state_history(
            TradeState.PARTIAL_EXIT,
            f"Partial exit: {exit_quantity} @ {exit_price}, P&L: {partial_pnl:.4f}"
        )
        
        # Trigger callback
        if trade.on_partial_exit:
            try:
                trade.on_partial_exit(trade, exit_leg, partial_pnl)
            except Exception as e:
                logger.error(f"Error in partial exit callback for trade {trade_id}: {e}")
        
        # Check if trade should be closed
        if trade.position.remaining_quantity <= 0:
            self._close_trade(trade_id, ExitReason.TARGET_REACHED, exit_price)
        
        logger.info(f"Trade {trade_id} partial exit: {exit_quantity} @ {exit_price}")
        
        return True
    
    def execute_stop_loss(
        self,
        trade_id: str,
        stop_price: Optional[float] = None,
        reason: str = "Stop loss triggered"
    ) -> bool:
        """
        Execute stop loss exit.
        
        Args:
            trade_id: Trade identifier
            stop_price: Stop price (defaults to current price)
            reason: Stop reason
            
        Returns:
            True if stop loss executed, False otherwise
        """
        trade = self.active_trades.get(trade_id)
        if not trade or not trade.position:
            logger.error(f"Trade {trade_id} not found or no position for stop loss")
            return False
        
        if trade.state not in [TradeState.HOLDING, TradeState.PARTIAL_EXIT]:
            logger.error(f"Trade {trade_id} in invalid state for stop loss: {trade.state}")
            return False
        
        # Use current price if not provided
        if stop_price is None:
            stop_price = trade.position.current_price
        
        # Create stop loss leg
        stop_leg = TradeLeg(
            id=str(uuid.uuid4()),
            trade_id=trade_id,
            leg_type="stop_exit",
            direction="short" if trade.direction == "long" else "long",
            quantity=trade.position.remaining_quantity,
            price=stop_price,
            timestamp=datetime.now(timezone.utc),
            order_type=OrderType.STOP,
            reason=reason
        )
        
        # Calculate P&L for stop loss
        stop_pnl = self._calculate_leg_pnl(trade, stop_leg)
        
        # Update trade
        trade.legs.append(stop_leg)
        trade.realized_pnl += stop_pnl
        trade.exit_reason = ExitReason.STOP_LOSS
        trade.exit_price = stop_price
        
        # Add state history
        trade._add_state_history(
            TradeState.STOPPED,
            f"Stop loss: {trade.position.remaining_quantity} @ {stop_price}, P&L: {stop_pnl:.4f}"
        )
        
        # Close trade
        self._close_trade(trade_id, ExitReason.STOP_LOSS, stop_price)
        
        # Trigger callback
        if trade.on_stop:
            try:
                trade.on_stop(trade, stop_leg, stop_pnl)
            except Exception as e:
                logger.error(f"Error in stop callback for trade {trade_id}: {e}")
        logger.warning(f"Trade {trade_id} stop loss: {trade.position.remaining_quantity} @ {stop_price}")
        
        return True
    
    async def close_trade(
        self,
        trade_id: str,
        close_price: Optional[float] = None,
        reason: ExitReason = ExitReason.MANUAL_CLOSE
    ) -> bool:
        """
        Close a trade completely.
        """
        trade = self.active_trades.get(trade_id)
        if not trade or not trade.position:
            logger.error(f"Trade {trade_id} not found or no position to close")
            return False
        
        if trade.state in [TradeState.CLOSED, TradeState.CANCELLED]:
            logger.error(f"Trade {trade_id} already in terminal state: {trade.state}")
            return False

        # If we have a provider, submit the exit order
        if self.provider:
            try:
                order_result = await self.provider.submit_order(
                    ticker=trade.ticker,
                    quantity=trade.position.remaining_quantity,
                    direction="sell" if trade.direction == "long" else "buy",
                    order_type=OrderType.MARKET
                )
                close_price = order_result.get("average_fill_price", close_price)
            except Exception as e:
                logger.error(f"Provider failed to execute close for {trade.ticker}: {e}")
                return False

        # Use current price if not provided
        if close_price is None:
            close_price = trade.position.current_price
        
        # Create close leg
        close_leg = TradeLeg(
            id=str(uuid.uuid4()),
            trade_id=trade_id,
            leg_type="final_exit",
            direction="short" if trade.direction == "long" else "long",
            quantity=trade.position.remaining_quantity if trade.position else trade.quantity,
            price=close_price,
            timestamp=datetime.now(timezone.utc),
            order_type=OrderType.MARKET,
            reason=str(reason)
        )
        
        # Calculate P&L for close
        close_pnl = self._calculate_leg_pnl(trade, close_leg)
        
        # Update trade
        trade.legs.append(close_leg)
        trade.realized_pnl += close_pnl
        trade.exit_reason = reason
        trade.exit_price = close_price
        
        # Add state history
        trade._add_state_history(
            TradeState.CLOSED,
            f"Trade closed: {close_leg.quantity} @ {close_price}, P&L: {close_pnl:.4f}"
        )
        
        # Close trade
        self._close_trade(trade_id, reason, close_price)
        
        # Trigger callback
        if trade.on_exit:
            try:
                trade.on_exit(trade, close_leg, close_pnl)
            except Exception as e:
                logger.error(f"Error in exit callback for trade {trade_id}: {e}")
        
        logger.info(f"Trade {trade_id} closed: {close_leg.quantity} @ {close_price}, P&L: {close_pnl:.4f}")
        
        return True
    
    def _close_trade(self, trade_id: str, exit_reason: ExitReason, exit_price: float) -> None:
        """Internal method to close trade and move to completed."""
        trade = self.active_trades.get(trade_id)
        if not trade:
            return
        
        # Calculate final metrics
        if trade.entry_timestamp:
            trade.exit_timestamp = datetime.now(timezone.utc)
            trade.duration_minutes = (trade.exit_timestamp - trade.entry_timestamp).total_seconds() / 60
        
        # Calculate final P&L percentage
        if trade.entry_price > 0:
            trade.realized_pnl_pct = trade.realized_pnl / (trade.entry_price * trade.quantity)
        
        # Update state
        trade.state = TradeState.CLOSED
        trade.exit_reason = exit_reason
        trade.exit_price = exit_price
        
        # Move to completed trades
        self.completed_trades[trade_id] = trade
        self.trade_history.append(trade)
        
        # Record close in repository if available
        if self.repository:
            try:
                self.repository.save_trade({
                    "id": trade.id,
                    "ticker": trade.ticker,
                    "direction": trade.direction,
                    "quantity": trade.quantity,
                    "entry_price": trade.entry_price,
                    "exit_price": exit_price,
                    "pnl": trade.realized_pnl,
                    "status": "CLOSED",
                    "mode": trade.mode,
                    "strategy_id": trade.strategy_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "analysis": trade.analysis,
                    "llm_prediction": trade.llm_prediction,
                    "engine_decision": trade.signal_snapshot.get("engine_decision"),
                    "llm_status": trade.signal_snapshot.get("llm_status"),
                    "llm_agrees": trade.signal_snapshot.get("llm_agrees")
                })
            except Exception as e:
                logger.error(f"Failed to persist trade close to repository: {e}")

        if trade_id in self.active_trades:
            del self.active_trades[trade_id]
        
        logger.info(f"Trade {trade_id} completed with P&L: {trade.realized_pnl:.4f}")
    
    def create_outcome_from_trade(self, trade: Trade) -> Optional[Dict[str, Any]]:
        """
        Create a PredictionOutcome-compatible dict from a completed trade.
        
        This enables the learning loop to use actual executed returns
        (with slippage, stops, etc.) instead of theoretical returns.
        
        Args:
            trade: Completed trade with realized_pnl populated
            
        Returns:
            Dict compatible with PredictionOutcome and learner.ingest_pairing()
        """
        if trade.state != TradeState.CLOSED:
            logger.warning(f"Trade {trade.id} not closed, cannot create outcome")
            return None
        
        if not trade.prediction_id:
            logger.warning(f"Trade {trade.id} has no prediction_id, cannot link to learning")
            return None
        
        # Calculate actual return percentage from executed trade
        actual_return_pct = trade.realized_pnl_pct
        
        # Determine if direction was correct based on actual PnL
        direction_correct = trade.realized_pnl > 0
        
        # Map exit reason to outcome exit_reason
        exit_reason_map = {
            ExitReason.TARGET_HIT: "target",
            ExitReason.STOP_LOSS: "stop_loss",
            ExitReason.TRAILING_STOP: "trailing_stop",
            ExitReason.TIMEOUT: "horizon",
            ExitReason.MANUAL_CLOSE: "manual",
        }
        outcome_exit_reason = exit_reason_map.get(trade.exit_reason, "unknown")
        
        outcome = {
            'prediction_id': trade.prediction_id,
            'exit_price': trade.exit_price or trade.entry_price,
            'return_pct': actual_return_pct,
            'direction_correct': direction_correct,
            'max_runup': trade.max_runup,
            'max_drawdown': trade.max_drawdown,
            'evaluated_at': trade.exit_timestamp or datetime.now(timezone.utc),
            'exit_reason': outcome_exit_reason,
            'trade_duration_minutes': trade.duration_minutes,
            'realized_pnl': trade.realized_pnl,
            'strategy_id': trade.strategy_id,
            'ticker': trade.ticker,
            'regime': trade.regime,
            'mode': trade.mode,  # backtest | paper | live - keeps corpus clean
        }
        
        logger.info(f"Created outcome for trade {trade.id}: return={actual_return_pct:.4%}, correct={direction_correct}")
        return outcome
    
    def get_completed_trade_outcomes(self) -> List[Dict[str, Any]]:
        """
        Get outcomes for all completed trades.
        
        Returns:
            List of outcome dicts ready for learner ingestion
        """
        outcomes = []
        for trade in self.completed_trades.values():
            outcome = self.create_outcome_from_trade(trade)
            if outcome:
                outcomes.append(outcome)
        return outcomes
    
    def _update_position_pnl(self, trade: Trade, current_price: float) -> None:
        """Update position P&L based on current price."""
        if not trade.position:
            return
        
        if trade.direction == "long":
            trade.position.unrealized_pnl = (current_price - trade.position.average_entry_price) * trade.position.filled_quantity
        else:  # short
            trade.position.unrealized_pnl = (trade.position.average_entry_price - current_price) * trade.position.filled_quantity
        
        # Calculate percentage
        if trade.position.average_entry_price > 0:
            trade.position.unrealized_pnl_pct = trade.position.unrealized_pnl / (trade.position.average_entry_price * trade.position.filled_quantity)
    
    def _calculate_leg_pnl(self, trade: Trade, leg: TradeLeg) -> float:
        """Calculate P&L for a specific leg."""
        if trade.direction == "long":
            if leg.direction == "long":  # Entry leg
                return 0.0  # No P&L on entry
            else:  # Exit leg
                return (leg.price - trade.position.average_entry_price) * leg.quantity
        else:  # short position
            if leg.direction == "short":  # Entry leg
                return 0.0  # No P&L on entry
            else:  # Exit leg
                return (trade.position.average_entry_price - leg.price) * leg.quantity
    
    def _check_exit_conditions(self, trade: Trade, current_price: float, timestamp: datetime) -> List[Dict[str, Any]]:
        """Check if any exit conditions are met."""
        actions = []
        
        # Target price check
        if trade.target_price:
            if trade.direction == "long" and current_price >= trade.target_price:
                actions.append({
                    'type': 'target_reached',
                    'action': 'close',
                    'reason': f"Target price {trade.target_price} reached"
                })
            elif trade.direction == "short" and current_price <= trade.target_price:
                actions.append({
                    'type': 'target_reached',
                    'action': 'close',
                    'reason': f"Target price {trade.target_price} reached"
                })
        
        # Stop loss check
        if trade.stop_loss_price:
            if trade.direction == "long" and current_price <= trade.stop_loss_price:
                actions.append({
                    'type': 'stop_loss',
                    'action': 'close',
                    'reason': f"Stop loss {trade.stop_loss_price} triggered"
                })
            elif trade.direction == "short" and current_price >= trade.stop_loss_price:
                actions.append({
                    'type': 'stop_loss',
                    'action': 'close',
                    'reason': f"Stop loss {trade.stop_loss_price} triggered"
                })
        
        # Trailing stop check
        if trade.trailing_stop_price:
            if trade.direction == "long" and current_price <= trade.trailing_stop_price:
                actions.append({
                    'type': 'trailing_stop',
                    'action': 'close',
                    'reason': f"Trailing stop {trade.trailing_stop_price} triggered"
                })
            elif trade.direction == "short" and current_price >= trade.trailing_stop_price:
                actions.append({
                    'type': 'trailing_stop',
                    'action': 'close',
                    'reason': f"Trailing stop {trade.trailing_stop_price} triggered"
                })
        
        # Time exit check
        if trade.entry_timestamp:
            hold_time = timestamp - trade.entry_timestamp
            max_hold_time = timedelta(hours=self.max_hold_time_hours)
            if hold_time > max_hold_time:
                actions.append({
                    'type': 'time_exit',
                    'action': 'close',
                    'reason': f"Maximum hold time {self.max_hold_time_hours}h exceeded"
                })
        
        # Partial exit levels (if enabled)
        if self.partial_exit_enabled and trade.position:
            for level in self.partial_exit_levels:
                if trade.direction == "long":
                    target_level_price = trade.entry_price * (1 + level)
                    if current_price >= target_level_price:
                        actions.append({
                            'type': 'partial_exit',
                            'action': 'partial_exit',
                            'level': level,
                            'reason': f"Partial exit level {level} reached at {target_level_price}"
                        })
                else:  # short
                    target_level_price = trade.entry_price * (1 - level)
                    if current_price <= target_level_price:
                        actions.append({
                            'type': 'partial_exit',
                            'action': 'partial_exit',
                            'level': level,
                            'reason': f"Partial exit level {level} reached at {target_level_price}"
                        })
        
        return actions
    
    def _update_trailing_stop(self, trade: Trade, current_price: float) -> None:
        """Update trailing stop price."""
        if not trade.trailing_stop_price:
            return
        
        if trade.direction == "long":
            new_trailing_stop = current_price * (1 - self.trailing_stop_pct)
            if new_trailing_stop > trade.trailing_stop_price:
                trade.trailing_stop_price = new_trailing_stop
        else:  # short
            new_trailing_stop = current_price * (1 + self.trailing_stop_pct)
            if new_trailing_stop < trade.trailing_stop_price:
                trade.trailing_stop_price = new_trailing_stop
    
    def get_active_trades(self) -> List[Trade]:
        """Get all active trades."""
        return list(self.active_trades.values())
    
    def get_trade(self, trade_id: str) -> Optional[Trade]:
        """Get specific trade by ID."""
        return self.active_trades.get(trade_id) or self.completed_trades.get(trade_id)
    
    def get_trade_history(self, limit: int = 100) -> List[Trade]:
        """Get trade history."""
        return self.trade_history[-limit:] if self.trade_history else []
    
    def get_portfolio_positions(self) -> Dict[str, TradePosition]:
        """Get current portfolio positions."""
        positions = {}
        for trade in self.active_trades.values():
            if trade.position and trade.position.remaining_quantity > 0:
                positions[trade.ticker] = trade.position
        return positions
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary."""
        active_trades = self.get_active_trades()
        positions = self.get_portfolio_positions()
        
        total_unrealized_pnl = sum(pos.unrealized_pnl for pos in positions.values())
        total_realized_pnl = sum(trade.realized_pnl for trade in self.completed_trades.values())
        
        return {
            'active_trades': len(active_trades),
            'positions': len(positions),
            'total_unrealized_pnl': total_unrealized_pnl,
            'total_realized_pnl': total_realized_pnl,
            'total_pnl': total_unrealized_pnl + total_realized_pnl,
            'completed_trades': len(self.completed_trades),
            'trade_ids': list(self.active_trades.keys())
        }
    
    def subscribe_to_price_updates(self, ticker: str, callback: Callable) -> None:
        """Subscribe to price updates for a ticker."""
        if ticker not in self.price_subscribers:
            self.price_subscribers[ticker] = []
        self.price_subscribers[ticker].append(callback)
    
    def update_market_prices(self, price_updates: Dict[str, float]) -> None:
        """Update market prices and trigger position updates."""
        for ticker, price in price_updates.items():
            # Notify subscribers
            if ticker in self.price_subscribers:
                for callback in self.price_subscribers[ticker]:
                    try:
                        callback(ticker, price)
                    except Exception as e:
                        logger.error(f"Error in price callback for {ticker}: {e}")
            
            # Update trades for this ticker
            for trade in self.active_trades.values():
                if trade.ticker == ticker and trade.position:
                    self.update_position(trade.id, price)


# Add method to Trade class for state history
def _add_state_history(self, state: TradeState, reason: str) -> None:
    """Add state change to history."""
    self.state_history.append((datetime.now(timezone.utc), state, reason))


# Monkey patch the method to Trade class
Trade._add_state_history = _add_state_history
