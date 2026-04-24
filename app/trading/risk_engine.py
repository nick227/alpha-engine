"""
Risk Management Engine

Comprehensive risk management system for paper trading.
Implements position limits, loss controls, and trading safeguards.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """Risk severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RiskAction(Enum):
    """Risk management actions."""
    ALLOW = "allow"
    REDUCE = "reduce"
    REJECT = "reject"
    CLOSE_ALL = "close_all"
    HALT_TRADING = "halt_trading"


@dataclass
class RiskCheckResult:
    """Result of a risk check."""
    passed: bool
    action: RiskAction
    reason: str
    risk_level: RiskLevel
    current_value: float
    limit_value: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskMetrics:
    """Current risk metrics snapshot."""
    total_exposure: float
    cash_available: float
    daily_pnl: float
    daily_loss_pct: float
    max_drawdown: float
    current_drawdown: float
    open_positions: int
    concurrent_trades: int
    last_trade_time: Optional[datetime]
    consecutive_losses: int
    confidence_floor_active: bool
    trading_halted: bool
    
    # Per-ticker exposure
    ticker_exposure: Dict[str, float] = field(default_factory=dict)
    
    # Per-sector exposure
    sector_exposure: Dict[str, float] = field(default_factory=dict)
    
    # Per-strategy exposure
    strategy_exposure: Dict[str, float] = field(default_factory=dict)


@dataclass
class RiskLimits:
    """Risk management limits and thresholds."""
    # Position limits
    max_position_size: float = 0.02           # 2% max per trade
    max_ticker_exposure: float = 0.10        # 10% max per ticker
    max_sector_exposure: float = 0.20        # 20% max per sector
    max_strategy_exposure: float = 0.15      # 15% max per strategy
    max_total_exposure: float = 0.80         # 80% max total exposure
    
    # Loss limits
    max_daily_loss_pct: float = 0.02         # 2% max daily loss
    max_drawdown_pct: float = 0.15           # 15% max drawdown
    stop_loss_pct: float = 0.02              # 2% stop loss
    trailing_stop_pct: float = 0.015          # 1.5% trailing stop
    
    # Trading limits
    max_concurrent_trades: int = 10          # Max concurrent positions
    max_trades_per_day: int = 50             # Max trades per day
    max_trades_per_hour: int = 10            # Max trades per hour
    
    # Cooldown and recovery
    loss_cooldown_minutes: int = 30          # Cooldown after loss
    consecutive_loss_limit: int = 3          # Max consecutive losses
    confidence_floor: float = 0.6            # Minimum confidence after losses
    
    # Emergency controls
    emergency_halt_loss_pct: float = 0.05    # 5% loss triggers halt
    circuit_breaker_volatility: float = 0.05  # High volatility halt
    position_size_emergency_cap: float = 0.01 # 1% cap in emergencies


class RiskEngine:
    """
    Comprehensive risk management engine.
    
    Enforces trading limits, monitors portfolio risk, and prevents catastrophic losses.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.risk_limits = RiskLimits(**config.get('risk_limits', {}))
        
        # State tracking
        self.daily_trades: List[datetime] = []
        self.hourly_trades: List[datetime] = []
        self.consecutive_losses: int = 0
        self.last_trade_time: Optional[datetime] = None
        self.trading_halted: bool = False
        self.halt_reason: Optional[str] = None
        self.halt_time: Optional[datetime] = None
        
        # Historical tracking
        self.daily_pnl_history: List[float] = []
        self.portfolio_value_history: List[Tuple[datetime, float]] = []
        self.max_portfolio_value: float = 0.0
        
        # Risk metrics cache
        self._last_metrics_update: Optional[datetime] = None
        self._cached_metrics: Optional[RiskMetrics] = None
        
        logger.info("Risk engine initialized with comprehensive limits")
    
    def check_trade_risk(
        self,
        ticker: str,
        direction: str,
        position_size: float,
        entry_price: float,
        confidence: float,
        strategy_id: str,
        sector: Optional[str] = None,
        portfolio_value: float = 100000.0,
        current_positions: Dict[str, float] = None,
        current_exposure: Dict[str, float] = None,
        daily_pnl: float = 0.0
    ) -> List[RiskCheckResult]:
        """
        Perform comprehensive risk check for a potential trade.
        
        Returns:
            List of risk check results
        """
        if current_positions is None:
            current_positions = {}
        if current_exposure is None:
            current_exposure = {}
        
        risk_checks = []
        
        # Check if trading is halted
        if self.trading_halted:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.HALT_TRADING,
                reason=f"Trading halted: {self.halt_reason}",
                risk_level=RiskLevel.CRITICAL,
                current_value=0.0,
                limit_value=0.0
            ))
            return risk_checks
        
        # 1. Position size limit
        position_value = position_size * entry_price
        position_pct = position_value / portfolio_value if portfolio_value > 0 else 0
        
        if position_pct > self.risk_limits.max_position_size:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.REDUCE,
                reason=f"Position size {position_pct:.2%} exceeds limit {self.risk_limits.max_position_size:.2%}",
                risk_level=RiskLevel.HIGH,
                current_value=position_pct,
                limit_value=self.risk_limits.max_position_size,
                metadata={'suggested_size': self.risk_limits.max_position_size * portfolio_value / entry_price}
            ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Position size within limits",
                risk_level=RiskLevel.LOW,
                current_value=position_pct,
                limit_value=self.risk_limits.max_position_size
            ))
        
        # 2. Ticker exposure limit
        current_ticker_exposure = current_exposure.get(ticker, 0)
        proposed_ticker_exposure = current_ticker_exposure + position_value
        ticker_exposure_pct = proposed_ticker_exposure / portfolio_value if portfolio_value > 0 else 0
        
        if ticker_exposure_pct > self.risk_limits.max_ticker_exposure:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.REDUCE,
                reason=f"Ticker exposure {ticker_exposure_pct:.2%} exceeds limit {self.risk_limits.max_ticker_exposure:.2%}",
                risk_level=RiskLevel.HIGH,
                current_value=ticker_exposure_pct,
                limit_value=self.risk_limits.max_ticker_exposure,
                metadata={'available_exposure': (self.risk_limits.max_ticker_exposure * portfolio_value - current_ticker_exposure) / entry_price}
            ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Ticker exposure within limits",
                risk_level=RiskLevel.LOW,
                current_value=ticker_exposure_pct,
                limit_value=self.risk_limits.max_ticker_exposure
            ))
        
        # 3. Sector exposure limit
        if sector:
            current_sector_exposure = 0.0
            for other_ticker, exposure in current_exposure.items():
                # Simplified: assume same sector for now
                # In practice, would have ticker->sector mapping
                if other_ticker == ticker:
                    current_sector_exposure += exposure
            
            proposed_sector_exposure = current_sector_exposure + position_value
            sector_exposure_pct = proposed_sector_exposure / portfolio_value if portfolio_value > 0 else 0
            
            if sector_exposure_pct > self.risk_limits.max_sector_exposure:
                risk_checks.append(RiskCheckResult(
                    passed=False,
                    action=RiskAction.REDUCE,
                    reason=f"Sector exposure {sector_exposure_pct:.2%} exceeds limit {self.risk_limits.max_sector_exposure:.2%}",
                    risk_level=RiskLevel.MEDIUM,
                    current_value=sector_exposure_pct,
                    limit_value=self.risk_limits.max_sector_exposure
                ))
            else:
                risk_checks.append(RiskCheckResult(
                    passed=True,
                    action=RiskAction.ALLOW,
                    reason="Sector exposure within limits",
                    risk_level=RiskLevel.LOW,
                    current_value=sector_exposure_pct,
                    limit_value=self.risk_limits.max_sector_exposure
                ))
        
        # 4. Daily loss limit
        daily_loss_pct = abs(daily_pnl) / portfolio_value if daily_pnl < 0 and portfolio_value > 0 else 0
        
        if daily_loss_pct > self.risk_limits.max_daily_loss_pct:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.HALT_TRADING,
                reason=f"Daily loss {daily_loss_pct:.2%} exceeds limit {self.risk_limits.max_daily_loss_pct:.2%}",
                risk_level=RiskLevel.CRITICAL,
                current_value=daily_loss_pct,
                limit_value=self.risk_limits.max_daily_loss_pct
            ))
        elif daily_loss_pct > self.risk_limits.emergency_halt_loss_pct:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.HALT_TRADING,
                reason=f"Emergency halt triggered by daily loss {daily_loss_pct:.2%}",
                risk_level=RiskLevel.CRITICAL,
                current_value=daily_loss_pct,
                limit_value=self.risk_limits.emergency_halt_loss_pct
            ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Daily loss within limits",
                risk_level=RiskLevel.LOW,
                current_value=daily_loss_pct,
                limit_value=self.risk_limits.max_daily_loss_pct
            ))
        
        # 5. Concurrent trades limit
        open_positions = len([pos for pos in current_positions.values() if pos != 0])
        
        if open_positions >= self.risk_limits.max_concurrent_trades:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.REJECT,
                reason=f"Concurrent trades {open_positions} exceeds limit {self.risk_limits.max_concurrent_trades}",
                risk_level=RiskLevel.MEDIUM,
                current_value=float(open_positions),
                limit_value=float(self.risk_limits.max_concurrent_trades)
            ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Concurrent trades within limits",
                risk_level=RiskLevel.LOW,
                current_value=float(open_positions),
                limit_value=float(self.risk_limits.max_concurrent_trades)
            ))
        
        # 6. Trading frequency limits
        now = datetime.now(timezone.utc)
        
        # Daily trades limit
        trades_today = len([t for t in self.daily_trades if t.date() == now.date()])
        if trades_today >= self.risk_limits.max_trades_per_day:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.REJECT,
                reason=f"Daily trades {trades_today} exceeds limit {self.risk_limits.max_trades_per_day}",
                risk_level=RiskLevel.MEDIUM,
                current_value=float(trades_today),
                limit_value=float(self.risk_limits.max_trades_per_day)
            ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Daily trades within limits",
                risk_level=RiskLevel.LOW,
                current_value=float(trades_today),
                limit_value=float(self.risk_limits.max_trades_per_day)
            ))
        
        # Hourly trades limit
        hour_ago = now - timedelta(hours=1)
        trades_this_hour = len([t for t in self.hourly_trades if t > hour_ago])
        if trades_this_hour >= self.risk_limits.max_trades_per_hour:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.REJECT,
                reason=f"Hourly trades {trades_this_hour} exceeds limit {self.risk_limits.max_trades_per_hour}",
                risk_level=RiskLevel.MEDIUM,
                current_value=float(trades_this_hour),
                limit_value=float(self.risk_limits.max_trades_per_hour)
            ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Hourly trades within limits",
                risk_level=RiskLevel.LOW,
                current_value=float(trades_this_hour),
                limit_value=float(self.risk_limits.max_trades_per_hour)
            ))
        
        # 7. Cooldown after loss
        if self.consecutive_losses >= self.risk_limits.consecutive_loss_limit:
            if self.last_trade_time:
                cooldown_end = self.last_trade_time + timedelta(minutes=self.risk_limits.loss_cooldown_minutes)
                if now < cooldown_end:
                    remaining_minutes = (cooldown_end - now).total_seconds() / 60
                    risk_checks.append(RiskCheckResult(
                        passed=False,
                        action=RiskAction.REJECT,
                        reason=f"Cooldown active: {remaining_minutes:.0f} minutes remaining after {self.consecutive_losses} consecutive losses",
                        risk_level=RiskLevel.MEDIUM,
                        current_value=float(remaining_minutes),
                        limit_value=float(self.risk_limits.loss_cooldown_minutes)
                    ))
                else:
                    risk_checks.append(RiskCheckResult(
                        passed=True,
                        action=RiskAction.ALLOW,
                        reason="Cooldown period expired",
                        risk_level=RiskLevel.LOW,
                        current_value=0.0,
                        limit_value=float(self.risk_limits.loss_cooldown_minutes)
                    ))
            else:
                risk_checks.append(RiskCheckResult(
                    passed=True,
                    action=RiskAction.ALLOW,
                    reason="No previous trade timestamp",
                    risk_level=RiskLevel.LOW,
                    current_value=0.0,
                    limit_value=float(self.risk_limits.loss_cooldown_minutes)
                ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Consecutive losses within limit",
                risk_level=RiskLevel.LOW,
                current_value=float(self.consecutive_losses),
                limit_value=float(self.risk_limits.consecutive_loss_limit)
            ))
        
        # 8. Confidence floor
        confidence_floor_active = self.consecutive_losses >= 2
        if confidence_floor_active and confidence < self.risk_limits.confidence_floor:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.REJECT,
                reason=f"Confidence floor active: confidence {confidence:.2f} below floor {self.risk_limits.confidence_floor:.2f}",
                risk_level=RiskLevel.MEDIUM,
                current_value=confidence,
                limit_value=self.risk_limits.confidence_floor
            ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Confidence above floor",
                risk_level=RiskLevel.LOW,
                current_value=confidence,
                limit_value=self.risk_limits.confidence_floor
            ))
        
        # 9. Total exposure limit
        total_exposure = sum(current_exposure.values()) + position_value
        total_exposure_pct = total_exposure / portfolio_value if portfolio_value > 0 else 0
        
        if total_exposure_pct > self.risk_limits.max_total_exposure:
            risk_checks.append(RiskCheckResult(
                passed=False,
                action=RiskAction.REDUCE,
                reason=f"Total exposure {total_exposure_pct:.2%} exceeds limit {self.risk_limits.max_total_exposure:.2%}",
                risk_level=RiskLevel.HIGH,
                current_value=total_exposure_pct,
                limit_value=self.risk_limits.max_total_exposure
            ))
        else:
            risk_checks.append(RiskCheckResult(
                passed=True,
                action=RiskAction.ALLOW,
                reason="Total exposure within limits",
                risk_level=RiskLevel.LOW,
                current_value=total_exposure_pct,
                limit_value=self.risk_limits.max_total_exposure
            ))
        
        return risk_checks
    
    def record_trade(self, ticker: str, pnl: float, confidence: float) -> None:
        """Record trade outcome for risk tracking."""
        now = datetime.now(timezone.utc)
        
        # Update trade timing
        self.daily_trades.append(now)
        self.hourly_trades.append(now)
        self.last_trade_time = now
        
        # Update consecutive losses
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        
        # Update daily P&L
        self.daily_pnl_history.append(pnl)
        
        # Clean old trade records
        self._cleanup_old_trades()
        
        logger.info(f"Recorded trade: {ticker}, P&L: ${pnl:.2f}, Consecutive losses: {self.consecutive_losses}")
    
    def update_portfolio_metrics(
        self,
        portfolio_value: float,
        cash_available: float,
        positions: Dict[str, float],
        current_prices: Dict[str, float]
    ) -> RiskMetrics:
        """Update and return current risk metrics."""
        now = datetime.now(timezone.utc)
        
        # Calculate total exposure
        total_exposure = 0.0
        ticker_exposure = {}
        
        for ticker, quantity in positions.items():
            if quantity != 0 and ticker in current_prices:
                exposure = abs(quantity * current_prices[ticker])
                ticker_exposure[ticker] = exposure
                total_exposure += exposure
        
        # Calculate daily P&L
        daily_pnl = sum(self.daily_pnl_history)
        daily_loss_pct = abs(daily_pnl) / portfolio_value if daily_pnl < 0 and portfolio_value > 0 else 0
        
        # Update portfolio value history
        self.portfolio_value_history.append((now, portfolio_value))
        self.max_portfolio_value = max(self.max_portfolio_value, portfolio_value)
        
        # Calculate drawdown
        if self.max_portfolio_value > 0:
            current_drawdown = (self.max_portfolio_value - portfolio_value) / self.max_portfolio_value
        else:
            current_drawdown = 0.0
        
        # Count open positions
        open_positions = len([pos for pos in positions.values() if pos != 0])
        
        # Check if confidence floor is active
        confidence_floor_active = self.consecutive_losses >= 2
        
        # Create metrics
        metrics = RiskMetrics(
            total_exposure=total_exposure,
            cash_available=cash_available,
            daily_pnl=daily_pnl,
            daily_loss_pct=daily_loss_pct,
            max_drawdown=0.0,  # Would calculate from history
            current_drawdown=current_drawdown,
            open_positions=open_positions,
            concurrent_trades=open_positions,
            last_trade_time=self.last_trade_time,
            consecutive_losses=self.consecutive_losses,
            confidence_floor_active=confidence_floor_active,
            trading_halted=self.trading_halted,
            ticker_exposure=ticker_exposure,
            sector_exposure={},  # Would calculate from sector mapping
            strategy_exposure={}  # Would calculate from strategy tracking
        )
        
        # Cache metrics
        self._cached_metrics = metrics
        self._last_metrics_update = now
        
        # Check for emergency conditions
        self._check_emergency_conditions(metrics)
        
        return metrics
    
    def _cleanup_old_trades(self) -> None:
        """Clean up old trade records to prevent memory leaks."""
        now = datetime.now(timezone.utc)
        
        # Keep only trades from last 24 hours
        day_ago = now - timedelta(days=1)
        self.daily_trades = [t for t in self.daily_trades if t > day_ago]
        
        # Keep only trades from last hour
        hour_ago = now - timedelta(hours=1)
        self.hourly_trades = [t for t in self.hourly_trades if t > hour_ago]
        
        # Keep only daily P&L from today
        today = now.date()
        if self.daily_pnl_history and len(self.daily_pnl_history) > 100:
            # Simplified: keep last 100 trades
            self.daily_pnl_history = self.daily_pnl_history[-100:]
    
    def _check_emergency_conditions(self, metrics: RiskMetrics) -> None:
        """Check for emergency conditions that require immediate action."""
        # Emergency halt for excessive daily loss
        if metrics.daily_loss_pct > self.risk_limits.emergency_halt_loss_pct:
            self.halt_trading(f"Emergency halt: Daily loss {metrics.daily_loss_pct:.2%} exceeds threshold")
        
        # Emergency halt for excessive drawdown
        if metrics.current_drawdown > self.risk_limits.max_drawdown_pct:
            self.halt_trading(f"Emergency halt: Drawdown {metrics.current_drawdown:.2%} exceeds threshold")
        
        # Check if halt can be lifted
        if self.trading_halted and self.halt_time:
            halt_duration = datetime.now(timezone.utc) - self.halt_time
            if halt_duration > timedelta(hours=1):  # Auto-lift after 1 hour
                self.resume_trading("Halt period expired")
    
    def halt_trading(self, reason: str) -> None:
        """Halt all trading activities."""
        self.trading_halted = True
        self.halt_reason = reason
        self.halt_time = datetime.now(timezone.utc)
        logger.warning(f"Trading halted: {reason}")
    
    def resume_trading(self, reason: str) -> None:
        """Resume trading activities."""
        self.trading_halted = False
        self.halt_reason = None
        self.halt_time = None
        logger.info(f"Trading resumed: {reason}")
    
    def get_risk_summary(self) -> Dict[str, Any]:
        """Get comprehensive risk summary."""
        if not self._cached_metrics:
            return {"status": "No metrics available"}
        
        metrics = self._cached_metrics
        
        return {
            "risk_status": "HALTED" if self.trading_halted else "ACTIVE",
            "halt_reason": self.halt_reason,
            "halt_time": self.halt_time.isoformat() if self.halt_time else None,
            "portfolio_metrics": {
                "total_exposure": metrics.total_exposure,
                "cash_available": metrics.cash_available,
                "daily_pnl": metrics.daily_pnl,
                "daily_loss_pct": metrics.daily_loss_pct,
                "current_drawdown": metrics.current_drawdown,
                "open_positions": metrics.open_positions
            },
            "trading_metrics": {
                "consecutive_losses": metrics.consecutive_losses,
                "confidence_floor_active": metrics.confidence_floor_active,
                "daily_trades": len(self.daily_trades),
                "hourly_trades": len(self.hourly_trades),
                "last_trade_time": metrics.last_trade_time.isoformat() if metrics.last_trade_time else None
            },
            "limits": {
                "max_position_size": self.risk_limits.max_position_size,
                "max_ticker_exposure": self.risk_limits.max_ticker_exposure,
                "max_daily_loss_pct": self.risk_limits.max_daily_loss_pct,
                "max_concurrent_trades": self.risk_limits.max_concurrent_trades,
                "confidence_floor": self.risk_limits.confidence_floor
            },
            "exposure_breakdown": {
                "ticker_exposure": metrics.ticker_exposure,
                "sector_exposure": metrics.sector_exposure,
                "strategy_exposure": metrics.strategy_exposure
            }
        }
    
    def get_risk_alerts(self) -> List[Dict[str, Any]]:
        """Get current risk alerts and warnings."""
        alerts = []
        
        if not self._cached_metrics:
            return alerts
        
        metrics = self._cached_metrics
        
        # High daily loss
        if metrics.daily_loss_pct > self.risk_limits.max_daily_loss_pct * 0.8:
            alerts.append({
                "level": "HIGH",
                "type": "daily_loss",
                "message": f"Daily loss {metrics.daily_loss_pct:.2%} approaching limit {self.risk_limits.max_daily_loss_pct:.2%}",
                "current": metrics.daily_loss_pct,
                "limit": self.risk_limits.max_daily_loss_pct
            })
        
        # High drawdown
        if metrics.current_drawdown > self.risk_limits.max_drawdown_pct * 0.8:
            alerts.append({
                "level": "HIGH",
                "type": "drawdown",
                "message": f"Drawdown {metrics.current_drawdown:.2%} approaching limit {self.risk_limits.max_drawdown_pct:.2%}",
                "current": metrics.current_drawdown,
                "limit": self.risk_limits.max_drawdown_pct
            })
        
        # Consecutive losses
        if metrics.consecutive_losses >= self.risk_limits.consecutive_loss_limit - 1:
            alerts.append({
                "level": "MEDIUM",
                "type": "consecutive_losses",
                "message": f"Consecutive losses {metrics.consecutive_losses} approaching limit {self.risk_limits.consecutive_loss_limit}",
                "current": metrics.consecutive_losses,
                "limit": self.risk_limits.consecutive_loss_limit
            })
        
        # High exposure
        portfolio_value = metrics.total_exposure + metrics.cash_available
        if portfolio_value > 0:
            exposure_pct = metrics.total_exposure / portfolio_value
            if exposure_pct > self.risk_limits.max_total_exposure * 0.9:
                alerts.append({
                    "level": "MEDIUM",
                    "type": "total_exposure",
                    "message": f"Total exposure {exposure_pct:.2%} approaching limit {self.risk_limits.max_total_exposure:.2%}",
                    "current": exposure_pct,
                    "limit": self.risk_limits.max_total_exposure
                })
        
        return alerts
