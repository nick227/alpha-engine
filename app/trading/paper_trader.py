"""
Paper Trading Engine

Core paper trading execution system with comprehensive qualification pipeline.
Integrates with Alpha Engine features and provides Alpaca simulation.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import uuid
import asyncio
import logging

from app.db.repository import AlphaRepository
from app.trading.base import TradingProvider
from app.trading.position_sizing import PositionSizer, SizingContext
from app.trading.risk_engine import RiskEngine, RiskCheckResult, RiskAction
from app.trading.trade_lifecycle import (
    TradeLifecycleManager, Trade, TradeState, ExitReason, OrderType
)
from app.trading.execution_planner import ExecutionPlanner, ExecutionPlan
from app.trading.execution_simulator import ExecutionSimulator, ExecutionResult
from app.trading.llm_client import LLMClient

logger = logging.getLogger(__name__)


class TradeDirection(Enum):
    LONG = "long"
    SHORT = "short"


class TradeStatus(Enum):
    PENDING = "pending"
    EXECUTED = "executed"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class SignalType(Enum):
    ENTRY = "entry"
    EXIT = "exit"

class LLMDecision(Enum):
    QUALIFIED = "QUALIFIED"
    CAUTION = "CAUTION"
    REJECT = "REJECT"


@dataclass
class Position:
    """Individual position tracking with P&L"""
    ticker: str
    quantity: float  # Positive for long, negative for short
    entry_price: float
    entry_timestamp: datetime
    strategy_id: str
    
    # Current state
    current_price: float = 0.0
    current_value: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    
    # Risk management
    stop_loss: Optional[float] = None
    target_price: Optional[float] = None
    
    # Performance tracking
    trades_count: int = 0
    
    def update_current_price(self, current_price: float):
        """Update position with current market price"""
        self.current_price = current_price
        self.current_value = abs(self.quantity) * current_price
        
        # Calculate unrealized P&L
        if self.quantity > 0:  # Long position
            self.unrealized_pnl = self.quantity * (current_price - self.entry_price)
        else:  # Short position
            self.unrealized_pnl = self.quantity * (self.entry_price - current_price)
    
    def should_exit(self, current_price: float) -> Tuple[bool, str]:
        """Check if position should be exited"""
        if self.stop_loss and current_price <= self.stop_loss:
            return True, "stop_loss"
        if self.target_price and current_price >= self.target_price:
            return True, "target_reached"
        return False, ""
    
    def close_position(self, exit_price: float, exit_timestamp: datetime) -> float:
        """Close position and return realized P&L"""
        if self.quantity > 0:  # Long position
            realized_pnl = self.quantity * (exit_price - self.entry_price)
        else:  # Short position
            realized_pnl = self.quantity * (self.entry_price - exit_price)
        
        self.realized_pnl += realized_pnl
        return realized_pnl


@dataclass
class PortfolioState:
    """Enhanced portfolio state with real P&L tracking"""
    cash: float
    positions: Dict[str, Position]  # ticker -> Position object
    pending_orders: List[str]  # trade IDs
    last_updated: datetime
    initial_capital: float = 100000.0
    peak_value: float = 0.0
    
    # Portfolio metrics
    total_value: float = 0.0
    total_exposure: float = 0.0
    leverage: float = 0.0
    
    # P&L tracking
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    total_pnl: float = 0.0
    daily_pnl: float = 0.0
    daily_pnl_last_reset: Optional[datetime] = None
    
    # Risk metrics
    sector_exposure: Dict[str, float] = field(default_factory=dict)
    strategy_exposure: Dict[str, float] = field(default_factory=dict)
    max_drawdown: float = 0.0
    current_drawdown: float = 0.0
    
    def update_portfolio_metrics(self, current_prices: Dict[str, float]):
        """Real-time portfolio valuation"""
        
        # Update position values and P&L
        total_position_value = 0.0
        total_unrealized_pnl = 0.0
        
        for ticker, position in self.positions.items():
            current_price = current_prices.get(ticker, position.current_price)
            position.update_current_price(current_price)
            
            total_position_value += position.current_value
            total_unrealized_pnl += position.unrealized_pnl
            
        # Update portfolio metrics
        self.total_value = self.cash + total_position_value
        self.unrealized_pnl = total_unrealized_pnl
        self.total_exposure = total_position_value
        self.leverage = total_position_value / self.total_value if self.total_value > 0 else 0.0
        self.total_pnl = self.realized_pnl + self.unrealized_pnl
        
        # Update drawdown
        if self.peak_value <= 0.0:
            self.peak_value = max(self.initial_capital, self.total_value)
        else:
            self.peak_value = max(self.peak_value, self.total_value)

        self.current_drawdown = (self.peak_value - self.total_value) / self.peak_value if self.peak_value > 0 else 0.0
        self.max_drawdown = max(self.max_drawdown, self.current_drawdown)
        
        self.last_updated = datetime.now(timezone.utc)

    def reset_daily_pnl(self, reset_timestamp: Optional[datetime] = None) -> None:
        """Explicitly reset daily P&L at a boundary (e.g., market open)."""
        self.daily_pnl = 0.0
        self.daily_pnl_last_reset = reset_timestamp or datetime.now(timezone.utc)
        self.last_updated = datetime.now(timezone.utc)

    def _apply_realized_pnl(self, pnl: float) -> None:
        self.realized_pnl += pnl
        self.daily_pnl += pnl

    def open_position_from_trade(self, trade: Trade) -> None:
        """Apply an entry fill from a Trade to cash + positions."""
        if not trade.position:
            return

        ticker = trade.ticker
        fill_price = float(trade.position.average_entry_price)
        fill_qty = float(trade.position.filled_quantity)
        signed_qty = fill_qty if trade.direction == "long" else -fill_qty

        # Order-based cash math: buy decreases cash, sell increases cash
        self.cash -= signed_qty * fill_price

        if ticker in self.positions:
            pos = self.positions[ticker]
            new_qty = pos.quantity + signed_qty

            # Same-side add: update weighted-average entry price
            same_side_add = (pos.quantity > 0 and signed_qty > 0) or (pos.quantity < 0 and signed_qty < 0)
            if same_side_add:
                total_cost = (abs(pos.quantity) * pos.entry_price) + (abs(signed_qty) * fill_price)
                pos.entry_price = total_cost / abs(new_qty) if abs(new_qty) > 0 else pos.entry_price
                pos.quantity = new_qty
                pos.trades_count += 1
                pos.update_current_price(fill_price)
            else:
                # Opposite-side fill: reduce or flip position and realize P&L on the closed portion
                closed_qty = min(abs(signed_qty), abs(pos.quantity))
                if closed_qty > 0:
                    if pos.quantity > 0:  # reducing a long via sell
                        realized = (fill_price - pos.entry_price) * closed_qty
                    else:  # reducing a short via buy-to-cover
                        realized = (pos.entry_price - fill_price) * closed_qty
                    self._apply_realized_pnl(realized)

                if abs(new_qty) < 0.00001:
                    del self.positions[ticker]
                else:
                    # If we flipped through zero, the remainder is a new position at the fill price
                    flipped = (pos.quantity > 0 and new_qty < 0) or (pos.quantity < 0 and new_qty > 0)
                    pos.quantity = new_qty
                    if flipped:
                        pos.entry_price = fill_price
                    pos.trades_count += 1
                    pos.update_current_price(fill_price)
        else:
            self.positions[ticker] = Position(
                ticker=ticker,
                quantity=signed_qty,
                entry_price=fill_price,
                entry_timestamp=datetime.now(timezone.utc),
                strategy_id=trade.strategy_id,
            )
            self.positions[ticker].trades_count = 1
            self.positions[ticker].update_current_price(fill_price)

        self.last_updated = datetime.now(timezone.utc)

    def close_position_for_trade(self, trade: Trade, exit_price: float) -> float:
        """Close the portfolio position for a trade's ticker at a given price."""
        ticker = trade.ticker
        if ticker not in self.positions:
            return 0.0

        pos = self.positions[ticker]
        exit_qty = abs(pos.quantity)
        if exit_qty < 0.00001:
            del self.positions[ticker]
            return 0.0

        pnl = 0.0
        if pos.quantity > 0:  # long
            pnl = (exit_price - pos.entry_price) * exit_qty
            self.cash += exit_price * exit_qty
        else:  # short
            pnl = (pos.entry_price - exit_price) * exit_qty
            self.cash -= exit_price * exit_qty

        self._apply_realized_pnl(pnl)
        del self.positions[ticker]
        self.last_updated = datetime.now(timezone.utc)
        return pnl

    def partial_close_for_trade(self, trade: Trade, exit_quantity: float, exit_price: float) -> float:
        """Partially close the portfolio position for a trade's ticker."""
        ticker = trade.ticker
        if ticker not in self.positions:
            return 0.0

        pos = self.positions[ticker]
        exit_qty = float(exit_quantity)
        if exit_qty <= 0:
            return 0.0

        available_qty = abs(pos.quantity)
        exit_qty = min(exit_qty, available_qty)

        pnl = 0.0
        if pos.quantity > 0:  # long
            pnl = (exit_price - pos.entry_price) * exit_qty
            self.cash += exit_price * exit_qty
            pos.quantity -= exit_qty
        else:  # short
            pnl = (pos.entry_price - exit_price) * exit_qty
            self.cash -= exit_price * exit_qty
            pos.quantity += exit_qty

        self._apply_realized_pnl(pnl)

        if abs(pos.quantity) < 0.00001:
            del self.positions[ticker]
        else:
            pos.update_current_price(exit_price)

        self.last_updated = datetime.now(timezone.utc)
        return pnl
    
    def add_position(self, position: Position):
        """Add new position or update existing"""
        self.positions[position.ticker] = position
    
    def close_position(self, ticker: str, exit_price: float, exit_timestamp: datetime) -> float:
        """Close position and return realized P&L"""
        if ticker not in self.positions:
            return 0.0
        
        position = self.positions[ticker]
        realized_pnl = position.close_position(exit_price, exit_timestamp)
        self.realized_pnl += realized_pnl
        
        # Remove position
        del self.positions[ticker]
        
        return realized_pnl
    
    def get_position(self, ticker: str) -> Optional[Position]:
        """Get position by ticker"""
        return self.positions.get(ticker)
    
    def get_position_value(self, ticker: str, price: float) -> float:
        """Get current position value for a ticker."""
        position = self.positions.get(ticker)
        if position:
            position.update_current_price(price)
            return position.current_value
        return 0.0
    
    def get_total_value(self, prices: Dict[str, float]) -> float:
        """Get total portfolio value."""
        self.update_portfolio_metrics(prices)
        return self.total_value
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get comprehensive portfolio summary"""
        return {
            'cash': self.cash,
            'total_value': self.total_value,
            'total_exposure': self.total_exposure,
            'leverage': self.leverage,
            'realized_pnl': self.realized_pnl,
            'unrealized_pnl': self.unrealized_pnl,
            'total_pnl': self.total_pnl,
            'daily_pnl': self.daily_pnl,
            'max_drawdown': self.max_drawdown,
            'current_drawdown': self.current_drawdown,
            'open_positions': len(self.positions),
            'positions': {
                ticker: {
                    'quantity': pos.quantity,
                    'entry_price': pos.entry_price,
                    'current_price': pos.current_price,
                    'unrealized_pnl': pos.unrealized_pnl,
                    'realized_pnl': pos.realized_pnl
                }
                for ticker, pos in self.positions.items()
            }
        }


class QualificationLayer:
    """Base class for trade qualification layers."""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
    
    async def qualify(self, signal_data: Dict[str, Any], context: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Qualify a trade event.
        
        Returns:
            Tuple of (is_qualified, reason, metadata)
        """
        raise NotImplementedError


class SignalQualityFilter(QualificationLayer):
    """Filter trades based on signal quality metrics."""
    
    async def qualify(self, signal_data: Dict[str, Any], context: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        metadata = {}
        
        # Minimum confidence threshold
        min_confidence = self.config.get('min_confidence', 0.6)
        if signal_data['confidence'] < min_confidence:
            return False, f"Confidence {signal_data['confidence']:.3f} below threshold {min_confidence}", metadata
        
        # Minimum consensus score
        min_consensus = self.config.get('min_consensus', 0.5)
        if signal_data['consensus_score'] < min_consensus:
            return False, f"Consensus {signal_data['consensus_score']:.3f} below threshold {min_consensus}", metadata
        
        # Minimum liquidity (check volume)
        features = signal_data['feature_snapshot']
        volume_ratio = features.get('volume_ratio_20', 1.0)
        min_volume_ratio = self.config.get('min_volume_ratio', 0.5)
        if volume_ratio < min_volume_ratio:
            return False, f"Volume ratio {volume_ratio:.2f} below threshold {min_volume_ratio}", metadata
        
        # Volatility check
        volatility = features.get('realized_vol_20', 0.02)
        max_volatility = self.config.get('max_volatility', 0.1)
        if volatility > max_volatility:
            return False, f"Volatility {volatility:.3f} above threshold {max_volatility}", metadata
        
        metadata['quality_score'] = (signal_data['confidence'] + signal_data['consensus_score']) / 2
        return True, "Signal quality acceptable", metadata




class RiskEngineLayer(QualificationLayer):
    """Risk management qualification layer using comprehensive risk engine."""
    
    def __init__(self, name: str, risk_engine: RiskEngine):
        super().__init__(name, {})
        self.risk_engine = risk_engine
    
    async def qualify(self, signal_data: Dict[str, Any], context: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        metadata = {}
        
        # Get portfolio and current prices from context
        portfolio = context.get('portfolio')
        current_prices = context.get('prices', {})
        
        # Create risk context
        risk_context = {
            'portfolio_value': portfolio.get_total_value(current_prices),
            'current_positions': portfolio.positions,
            'current_prices': current_prices,
            'daily_pnl': context.get('daily_pnl', 0.0),
            'trade_count': context.get('trade_count', 0)
        }
        
        # Run comprehensive risk checks
        risk_checks = self.risk_engine.check_trade_risk(
            ticker=signal_data['ticker'],
            direction=signal_data['direction'].value if hasattr(signal_data['direction'], 'value') else signal_data['direction'],
            position_size=signal_data['position_size'],
            entry_price=signal_data['entry_price'],
            confidence=signal_data['confidence'],
            strategy_id=signal_data['strategy_id'],
            portfolio_value=portfolio.get_total_value(current_prices),
            current_positions=portfolio.positions,
            current_exposure={},  # Simplified
            daily_pnl=context.get('daily_pnl', 0.0)
        )
        
        # Decide based on risk checks (reject if any fail)
        passed = all(c.passed for c in risk_checks)
        failed_checks = [c for c in risk_checks if not c.passed]
        reason = failed_checks[0].reason if failed_checks else "Risk engine checks passed"
        
        # Store risk check results
        metadata['risk_checks'] = [c.__dict__ for c in risk_checks]
        
        if not passed:
            return False, reason, metadata
        
        return True, "Risk engine checks passed", metadata


class LLMAnalysisLayer(QualificationLayer):
    """LLM-based signal analysis for narrative justification."""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.enabled = config.get('enabled', False)
        self.min_confidence_for_llm = config.get('min_confidence_for_llm', config.get('min_confidence', 0.0))
        self.mode = config.get("mode", "sidecar")  # sidecar | gatekeeper
        self.client = LLMClient(config)
        self.template_path = "app/trading/prompts/signal_analysis.txt"
    
    @staticmethod
    def _normalize_decision(raw: Any) -> str:
        if raw is None:
            return ""
        decision = str(raw).strip().upper()
        return decision if decision in {d.value for d in LLMDecision} else ""
    
    async def qualify(self, signal_data: Dict[str, Any], context: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        metadata = {}
        
        # Only use LLM for high-conviction trades (or if enabled)
        if not self.enabled:
            metadata['llm_skipped'] = True
            metadata["llm_status"] = "SKIPPED"
            metadata["analysis"] = ""
            metadata["llm_prediction"] = ""
            return True, "LLM validation disabled", metadata

        if signal_data['confidence'] < self.min_confidence_for_llm:
            metadata['llm_skipped'] = True
            metadata["llm_status"] = "SKIPPED"
            metadata["analysis"] = ""
            metadata["llm_prediction"] = ""
            return True, f"Confidence {signal_data['confidence']} below LLM threshold {self.min_confidence_for_llm}", metadata
        
        # Prepare data for prompt
        # We need to format the features into a readable string
        features = signal_data.get('feature_snapshot', {})
        feature_str = "\n".join([f"- {k}: {v}" for k, v in features.items()])
        
        prompt_data = {
            'ticker': signal_data['ticker'],
            'direction': signal_data['direction'].value if hasattr(signal_data['direction'], 'value') else signal_data['direction'],
            'confidence': f"{signal_data['confidence']:.3f}",
            'consensus_score': f"{signal_data['consensus_score']:.3f}",
            'entry_price': f"{signal_data['entry_price']:.2f}",
            'alpha_score': f"{signal_data['alpha_score']:.3f}",
            'regime': signal_data.get('regime', 'UNKNOWN'),
            'features': feature_str
        }
        
        prompt = self.client.format_prompt(self.template_path, prompt_data)
        
        # Call LLM
        logger.info(f"Generating LLM analysis for {signal_data['ticker']}...")
        result = await self.client.validate_signal(prompt)
        
        if not result:
            # Fail-safe: Skip validation if LLM fails
            logger.warning(f"LLM analysis failed for {signal_data['ticker']}. Proceeding without narrative.")
            metadata["llm_status"] = "ERROR"
            metadata["analysis"] = "LLM_ERROR: No response from LLM client."
            metadata["llm_prediction"] = ""
            return True, "LLM analysis failed (Fail-Safe skip)", metadata
         
        analysis = result.get('analysis', result.get('reasoning', "No analysis provided."))
        decision = self._normalize_decision(result.get("decision"))
         
        metadata["analysis"] = str(analysis or "")
        metadata["llm_prediction"] = decision
        metadata["llm_status"] = "OK" if decision else "MALFORMED"

        if self.mode == "gatekeeper" and decision == LLMDecision.REJECT.value:
            return False, "LLM gatekeeper rejected trade", metadata

        return True, "Analysis generated as background layer.", metadata


class PaperTrader:
    """Main paper trading engine."""
    
    def __init__(self, config: Dict[str, Any], provider: Optional[TradingProvider] = None, repository: Optional[AlphaRepository] = None):
        self.config = config
        self.provider = provider
        self.repository = repository
        initial_cash = config.get('initial_cash', 100000.0)
        self.portfolio = PortfolioState(
            cash=initial_cash,
            positions={},
            pending_orders=[],
            last_updated=datetime.now(timezone.utc),
            initial_capital=initial_cash,
            peak_value=initial_cash
        )
        
        # Initialize position sizer
        self.position_sizer = PositionSizer(config)
        
        # Initialize risk engine
        self.risk_engine = RiskEngine(config)
        
        # Initialize trade lifecycle manager with provider and repository
        self.trade_lifecycle = TradeLifecycleManager(config, provider, repository)
        
        # Initialize execution planner and simulator
        self.execution_planner = ExecutionPlanner(config)
        self.execution_simulator = ExecutionSimulator(config, seed=config.get("simulation_seed"))
        
        # Initialize qualification layers
        self.qualification_layers = self._build_qualification_layers()
        
        # Performance tracking
        self.trade_count = 0
        self.win_count = 0
        
        logger.info(f"Paper trader initialized with ${self.portfolio.cash:,.2f}")
    
    def _build_qualification_layers(self) -> List[QualificationLayer]:
        """Build qualification pipeline layers."""
        layers = []
        
        # Signal quality filter
        layers.append(SignalQualityFilter(
            "signal_quality",
            self.config.get('signal_quality', {})
        ))
        
        # Risk engine (comprehensive risk management)
        layers.append(RiskEngineLayer(
            "risk_engine",
            self.risk_engine
        ))
        
        # LLM analysis (non-blocking)
        llm_config = self.config.get('llm_validation', {})
        if llm_config.get('enabled', False):
            layers.append(LLMAnalysisLayer(
                "llm_analysis",
                llm_config
            ))
        
        return layers
    
    async def process_signal(
        self,
        ticker: str,
        strategy_id: str,
        direction: TradeDirection,
        confidence: float,
        consensus_score: float,
        alpha_score: float,
        feature_snapshot: Dict[str, Any],
        entry_price: float,
        regime: str = "UNKNOWN"
    ) -> Optional[Dict[str, Any]]:
        """
        Process a trading signal through qualification pipeline.
        """
        # Create signal data
        signal_data = {
            'id': str(uuid.uuid4()),
            'timestamp': datetime.now(timezone.utc),
            'ticker': ticker,
            'strategy_id': strategy_id,
            'signal_type': SignalType.ENTRY,
            'direction': direction,
            'confidence': confidence,
            'consensus_score': consensus_score,
            'alpha_score': alpha_score,
            'feature_snapshot': feature_snapshot,
            'entry_price': entry_price,
            'regime': regime,
            'tenant_id': self.config.get('tenant_id', 'default'),
            'qualification_layers': [],
            'decision_path': {}
        }
        
        # Calculate position size
        position_size = self._calculate_position_size(signal_data)
        signal_data['position_size'] = position_size
        
        # Run qualification pipeline
        context = {
            'portfolio': self.portfolio,
            'prices': {ticker: entry_price},
            'daily_pnl': self.portfolio.daily_pnl,
            'trade_count': self.trade_count
        }
        
        for layer in self.qualification_layers:
            qualified, reason, metadata = await layer.qualify(signal_data, context)
            
            signal_data['qualification_layers'].append(layer.name)
            signal_data['decision_path'][layer.name] = {
                'qualified': qualified,
                'reason': reason,
                'metadata': metadata
            }
            
            if not qualified:
                logger.info(f"Signal {signal_data['id']} rejected by {layer.name}: {reason}")
                signal_data['status'] = 'REJECTED'
                return signal_data
        
        # Execute trade
        # Extract LLM analysis and prediction if present
        analysis = ""
        llm_prediction = ""
        llm_status = "SKIPPED"
        engine_decision = LLMDecision.QUALIFIED.value
        llm_agrees: Optional[int] = None
        for layer_res in signal_data['decision_path'].values():
            if 'analysis' in layer_res['metadata']:
                analysis = layer_res['metadata']['analysis']
                llm_prediction = layer_res['metadata'].get('llm_prediction', "")
                llm_status = layer_res["metadata"].get("llm_status", llm_status)
                break

        if llm_prediction in {d.value for d in LLMDecision}:
            llm_agrees = 1 if llm_prediction == engine_decision else 0
        
        signal_data['analysis'] = analysis
        signal_data['llm_prediction'] = llm_prediction
        signal_data["llm_status"] = llm_status
        signal_data["engine_decision"] = engine_decision
        signal_data["llm_agrees"] = llm_agrees
        
        # Log LLM comparison consistently (not only CAUTION/REJECT)
        if llm_status in {"OK", "MALFORMED", "ERROR"}:
            logger.info(
                "LLM_COMPARISON: %s engine=%s llm=%s agrees=%s status=%s",
                signal_data["ticker"],
                engine_decision,
                llm_prediction or "(none)",
                llm_agrees,
                llm_status,
            )
            if llm_prediction == LLMDecision.REJECT.value:
                logger.warning(
                    "STRATEGY DISAGREEMENT: Engine is EXECUTING %s but LLM recommends REJECT. Justification: %s...",
                    signal_data["ticker"],
                    (analysis or "")[:100],
                )
            elif llm_prediction == LLMDecision.CAUTION.value:
                logger.info(
                    "STRATEGY CAUTION: LLM suggests CAUTION for %s. Justification: %s...",
                    signal_data["ticker"],
                    (analysis or "")[:100],
                )
        
        await self._execute_trade(signal_data)
        
        return signal_data
    
    def _calculate_position_size(self, signal_data: Dict[str, Any]) -> float:
        """Calculate position size using comprehensive position sizing model."""
        # Extract features from signal data
        features = signal_data['feature_snapshot']
        confidence = signal_data['confidence']
        
        # Get additional features for position sizing
        stability = features.get('stability_score', 0.7)
        volatility = features.get('realized_vol_20', 0.02)
        regime = signal_data['regime']
        
        # Create sizing context
        sizing_context = SizingContext(
            portfolio_value=self.portfolio.get_total_value({signal_data['ticker']: signal_data['entry_price']}),
            available_cash=self.portfolio.cash,
            current_positions=self.portfolio.positions,
            current_exposure={},  # Simplified for now
            sector_exposure={},
            strategy_exposure={},
            daily_pnl=self.portfolio.daily_pnl,
            max_drawdown=self.portfolio.max_drawdown,
            current_drawdown=self.portfolio.current_drawdown,
            volatility_regime=features.get('volatility_regime', 'NORMAL'),
            market_regime=features.get('cross_asset_regime', 'NEUTRAL')
        )
        
        # Calculate position size using comprehensive model
        sizing_result = self.position_sizer.calculate_position_size(
            ticker=signal_data['ticker'],
            direction=signal_data['direction'].value,
            entry_price=signal_data['entry_price'],
            confidence=confidence,
            stability=stability,
            volatility=volatility,
            regime=regime,
            strategy_id=signal_data['strategy_id'],
            context=sizing_context
        )
        
        # Store sizing result in signal data for analysis
        signal_data['decision_path']['position_sizing'] = {
            'method': sizing_result.method_used,
            'size_value': sizing_result.size_value,
            'size_shares': sizing_result.size_shares,
            'risk_amount': sizing_result.risk_amount,
            'risk_pct': sizing_result.risk_pct,
            'adjustments': {
                'confidence': sizing_result.confidence_adj,
                'volatility': sizing_result.volatility_adj,
                'stability': sizing_result.stability_adj,
                'regime': sizing_result.regime_adj,
                'drawdown': sizing_result.drawdown_adj
            },
            'metadata': sizing_result.metadata
        }
        
        return sizing_result.size_shares
    async def _execute_trade(self, signal_data: Dict[str, Any]) -> None:
        """Execute the paper trade using trade lifecycle manager."""
        try:
            # Simulate execution delay
            await asyncio.sleep(0.1)
            
            # Create trade from signal using lifecycle manager
            trade = self.trade_lifecycle.create_trade_from_signal(
                signal_id=signal_data['id'],
                strategy_id=signal_data['strategy_id'],
                ticker=signal_data['ticker'],
                direction=signal_data['direction'].value,
                entry_price=signal_data['entry_price'],
                quantity=signal_data['position_size'],
                confidence=signal_data['confidence'],
                regime=signal_data['regime'],
                target_price=signal_data.get('target_price'),
                stop_loss_price=signal_data.get('stop_loss'),
                feature_snapshot=signal_data['feature_snapshot'],
                order_type=OrderType.MARKET,
                analysis=signal_data.get('analysis', ""),
                llm_prediction=signal_data.get('llm_prediction', ""),
                engine_decision=signal_data.get("engine_decision", ""),
                llm_status=signal_data.get("llm_status", ""),
                llm_agrees=signal_data.get("llm_agrees")
            )
            
            # Execute entry immediately (in real system, would wait for market execution)
            entry_success = await self.trade_lifecycle.execute_entry(
                trade.id,
                signal_data['entry_price'],
                signal_data['position_size']
            )
            
            if entry_success:
                # Update portfolio based on trade position
                self._update_portfolio_from_trade(trade)
                
                # Update signal status
                signal_data['status'] = 'EXECUTED'
                signal_data['execution_price'] = signal_data['entry_price']
                signal_data['execution_timestamp'] = datetime.now(timezone.utc)
                
                # Record trade in risk engine
                self.risk_engine.record_trade(
                    ticker=signal_data['ticker'],
                    pnl=0.0,  # No P&L yet on entry
                    confidence=signal_data['confidence']
                )
                
                # Update trade count
                self.trade_count += 1
                
                logger.info(f"Executed {signal_data['direction'].value} trade: {signal_data['ticker']} @ {signal_data['entry_price']:.2f}, size: {signal_data['position_size']:.2f}")
            else:
                # Entry failed
                signal_data['status'] = 'REJECTED'
                signal_data['validation_flags'] = ["entry_execution_failed"]
                logger.error(f"Trade entry failed for {signal_data['ticker']}")
            
        except Exception as e:
            signal_data['status'] = 'REJECTED'
            signal_data['validation_flags'] = [f"execution_error: {str(e)}"]
            logger.error(f"Trade execution failed: {e}")
    
    def _update_portfolio_from_trade(self, trade: Trade) -> None:
        """Update portfolio based on trade position."""
        self.portfolio.open_position_from_trade(trade)
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get current portfolio summary."""
        return {
            'cash': self.portfolio.cash,
            'positions': dict(self.portfolio.positions),
            'total_trades': self.trade_count,
            'win_rate': self.win_count / max(self.trade_count, 1),
            'daily_pnl': self.portfolio.daily_pnl,
            'pending_orders': len(self.portfolio.pending_orders),
            'last_updated': self.portfolio.last_updated.isoformat()
        }
    
    def get_trade_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trade history from lifecycle manager."""
        # Get completed trades from lifecycle manager
        trades = self.trade_lifecycle.get_trade_history(limit)
        
        # Convert to consistent format
        trade_history = []
        for trade in trades:
            trade_history.append({
                'id': trade.id,
                'timestamp': trade.signal_timestamp.isoformat(),
                'ticker': trade.ticker,
                'strategy_id': trade.strategy_id,
                'direction': trade.direction,
                'status': trade.state.value,
                'confidence': trade.confidence,
                'entry_price': trade.entry_price,
                'position_size': trade.quantity,
                'pnl': trade.realized_pnl,
                'pnl_pct': trade.realized_pnl_pct,
                'exit_reason': trade.exit_reason.value if trade.exit_reason else None,
                'duration_minutes': trade.duration_minutes,
                'max_runup': trade.max_runup,
                'max_drawdown': trade.max_drawdown
            })
        
        return trade_history
    
    async def update_market_prices(self, price_updates: Dict[str, float]) -> None:
        """Update market prices and handle trade lifecycle events."""
        # Update trade lifecycle manager
        self.trade_lifecycle.update_market_prices(price_updates)
        
        active_trades = self.trade_lifecycle.get_active_trades()
        trades_by_ticker: Dict[str, List[Trade]] = {}
        for trade in active_trades:
            trades_by_ticker.setdefault(trade.ticker, []).append(trade)

        # Update risk engine with current prices
        for ticker, price in price_updates.items():
            # Check for any exit conditions
            for trade in trades_by_ticker.get(ticker, []):
                if trade.position:
                    # Check for exit conditions and execute if needed
                    position_update = self.trade_lifecycle.update_position(trade.id, price)
                    if position_update and position_update.get('exit_actions'):
                        for action in position_update['exit_actions']:
                            if action['action'] == 'close':
                                # Execute close
                                await self.trade_lifecycle.close_trade(trade.id, price, ExitReason.TARGET_REACHED)
                                # Update portfolio
                                self._update_portfolio_on_exit(trade, price)
                                # Record in risk engine
                                final_pnl = trade.realized_pnl
                                self.risk_engine.record_trade(ticker, final_pnl, trade.confidence)
                            elif action['action'] == 'partial_exit':
                                # Execute partial exit
                                level = action.get('level', 0.5)
                                partial_quantity = trade.quantity * level
                                self.trade_lifecycle.execute_partial_exit(trade.id, partial_quantity, price, f"Partial exit at level {level}")
                                # Update portfolio
                                self._update_portfolio_on_partial_exit(trade, partial_quantity, price)
                            elif action['action'] in ['stop_loss', 'trailing_stop']:
                                # Execute stop loss
                                await self.trade_lifecycle.execute_stop_loss(trade.id, price, action['reason'])
                                # Update portfolio
                                self._update_portfolio_on_exit(trade, price)
                                # Record in risk engine
                                final_pnl = trade.realized_pnl
                                self.risk_engine.record_trade(ticker, final_pnl, trade.confidence)
    
    def _update_portfolio_on_exit(self, trade: Trade, exit_price: float) -> None:
        """Update portfolio when trade is completely closed."""
        pnl = self.portfolio.close_position_for_trade(trade, exit_price)
        
        if pnl > 0:
            self.win_count += 1
    
    def _update_portfolio_on_partial_exit(self, trade: Trade, exit_quantity: float, exit_price: float) -> None:
        """Update portfolio for partial exit."""
        pnl = self.portfolio.partial_close_for_trade(trade, exit_quantity, exit_price)

    def reset_daily_pnl(self, reset_timestamp: Optional[datetime] = None) -> None:
        """Trigger the daily P&L reset boundary explicitly (e.g., by a scheduler at market open)."""
        self.portfolio.reset_daily_pnl(reset_timestamp)
    
    def get_active_positions(self) -> Dict[str, Any]:
        """Get current active positions from trade lifecycle manager."""
        positions = self.trade_lifecycle.get_portfolio_positions()
        
        # Convert to legacy format
        return {
            ticker: {
                'quantity': pos.filled_quantity,
                'direction': pos.direction,
                'entry_price': pos.average_entry_price,
                'current_price': pos.current_price,
                'unrealized_pnl': pos.unrealized_pnl,
                'unrealized_pnl_pct': pos.unrealized_pnl_pct,
                'remaining_quantity': pos.remaining_quantity
            }
            for ticker, pos in positions.items()
        }
    
    def get_lifecycle_summary(self) -> Dict[str, Any]:
        """Get comprehensive trade lifecycle summary."""
        return self.trade_lifecycle.get_portfolio_summary()
    
    def plan_executions(
        self,
        signals: List[Dict[str, Any]],
        portfolio_state: Optional[Dict[str, Any]] = None,
        market_conditions: Optional[Dict[str, Any]] = None
    ) -> List[ExecutionPlan]:
        """
        Plan execution for final Alpha Engine signals.
        
        Takes already-ranked signals from Alpha Engine and plans
        how to execute them efficiently without re-ranking.
        
        Args:
            signals: Final signals from Alpha Engine (performance-ranked)
            portfolio_state: Current portfolio state
            market_conditions: Optional market conditions for planning
            
        Returns:
            List of execution plans
        """
        if portfolio_state is None:
            portfolio_state = self._get_portfolio_state()
        
        plans = self.execution_planner.plan_batch_execution(
            signals=signals,
            portfolio_state=portfolio_state,
            market_conditions=market_conditions
        )
        
        logger.info(f"Planned execution for {len(plans)} signals")
        
        return plans
    
    def simulate_executions(
        self,
        plans: List[ExecutionPlan],
        current_prices: Dict[str, float],
        market_conditions: Optional[Dict[str, Any]] = None
    ) -> List[ExecutionResult]:
        """
        Simulate realistic market execution for planned orders.
        
        Args:
            plans: Execution plans to simulate
            current_prices: Current market prices
            market_conditions: Optional market conditions
            
        Returns:
            List of execution results with realistic fills
        """
        signals = [self._plan_to_signal(p) for p in plans]
        
        results = self.execution_simulator.simulate_batch_execution(
            signals=signals,
            current_prices=current_prices,
            market_conditions=market_conditions
        )
        
        summary = self.execution_simulator.get_simulation_summary(results)
        logger.info(
            f"Execution simulation: {summary.get('filled', 0)}/{len(results)} filled, "
            f"avg cost {summary.get('avg_cost_bps', 0):.1f}bps"
        )
        
        return results
    
    def _plan_to_signal(self, plan: ExecutionPlan) -> Dict[str, Any]:
        """Convert execution plan to signal format for simulator."""
        return {
            'id': plan.signal_id,
            'ticker': plan.ticker,
            'direction': plan.direction,
            'quantity': plan.target_quantity,
            'entry_price': plan.feature_snapshot.get('entry_price', 0),
            'confidence': plan.confidence,
            'strategy_id': plan.strategy_id,
            'regime': plan.regime,
            'features': plan.feature_snapshot
        }
    
    def _get_portfolio_state(self) -> Dict[str, Any]:
        """Get current portfolio state for execution planning."""
        return {
            'cash': self.portfolio.cash,
            'positions': dict(self.portfolio.positions),
            'exposure': self.portfolio.total_exposure,
            'daily_pnl': self.portfolio.daily_pnl,
            'trade_count': self.trade_count
        }
