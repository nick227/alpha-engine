"""
Execution Simulator

Realistic market simulation for paper trading including:
- Slippage (size-based, volatility-based)
- Bid-ask spread
- Execution latency
- Partial fills
- Market impact
- Time-of-day effects

Provides realistic execution without re-ranking signals.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import random
import logging

logger = logging.getLogger(__name__)


class MarketCondition(Enum):
    """Market liquidity conditions."""
    HIGH_LIQUIDITY = "high_liquidity"      # Easy to fill, tight spreads
    NORMAL = "normal"                       # Standard conditions
    LOW_LIQUIDITY = "low_liquidity"         # Wider spreads, slower fills
    HIGH_VOLATILITY = "high_volatility"     # Slippage, partial fills
    STRESSED = "stressed"                 # Market stress, difficult execution


@dataclass
class ExecutionResult:
    """Result of simulated execution."""
    signal_id: str
    ticker: str
    
    # Execution details
    filled_quantity: float
    fill_ratio: float  # 0.0 to 1.0
    
    # Price details
    requested_price: float
    execution_price: float
    
    # Cost breakdown
    spread_cost: float
    slippage_cost: float
    market_impact: float
    total_cost: float
    
    # Metrics in basis points
    spread_bps: float
    slippage_bps: float
    total_cost_bps: float
    
    # Fill details
    partial_fills: List[Dict[str, Any]] = field(default_factory=list)
    
    # Simulation metadata
    latency_ms: float = 0.0
    market_condition: MarketCondition = MarketCondition.NORMAL
    execution_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Status
    status: str = "filled"  # filled, partial, rejected, pending


@dataclass 
class PartialFill:
    """Individual partial fill."""
    quantity: float
    price: float
    timestamp: datetime
    fill_id: str = ""


class ExecutionSimulator:
    """
    Simulates realistic market execution for paper trading.
    
    Does NOT modify signals or re-rank - just simulates what happens
    when you try to execute a signal at the current market.
    """
    
    def __init__(self, config: Dict[str, Any], seed: Optional[int] = None):
        self.config = config
        
        # Latency parameters
        self.base_latency_ms = config.get('base_latency_ms', 50)
        self.latency_volatility_factor = config.get('latency_volatility_factor', 1.0)
        self.latency_size_factor = config.get('latency_size_factor', 0.1)
        
        # Spread parameters
        self.base_spread_bps = config.get('base_spread_bps', 10)
        self.spread_volatility_factor = config.get('spread_volatility_factor', 20)
        
        # Slippage parameters
        self.slippage_base_bps = config.get('slippage_base_bps', 5)
        self.slippage_size_factor = config.get('slippage_size_factor', 1.0)
        self.slippage_volatility_factor = config.get('slippage_volatility_factor', 50)
        
        # Market impact
        self.impact_base_bps = config.get('impact_base_bps', 2)
        self.impact_size_factor = config.get('impact_size_factor', 0.5)
        
        # Fill probability
        self.min_fill_probability = config.get('min_fill_probability', 0.95)
        self.large_order_threshold = config.get('large_order_threshold', 10000)
        
        # Local RNG instance (prevents cross-module randomness bleed)
        # Priority: explicit seed param > config seed > None (non-deterministic)
        self.random_seed = seed if seed is not None else config.get('random_seed')
        if self.random_seed is not None:
            self.rng = random.Random(self.random_seed)
            logger.info(f"ExecutionSimulator initialized with deterministic seed={self.random_seed}")
        else:
            self.rng = random.Random()  # Non-deterministic
            logger.info("ExecutionSimulator initialized with non-deterministic mode")
        
        logger.info("ExecutionSimulator ready for realistic market simulation")
    
    def reset_seed(self, seed: int) -> None:
        """Reset random seed for deterministic replay.
        
        Allows reproducible execution results for backtesting and debugging.
        """
        self.random_seed = seed
        self.rng = random.Random(seed)
        logger.info(f"ExecutionSimulator seed reset to {seed}")
    
    def _get_random(self) -> float:
        """Get random value from local RNG."""
        return self.rng.random()
    
    def _get_random_uniform(self, a: float, b: float) -> float:
        """Get random uniform value from local RNG."""
        return self.rng.uniform(a, b)
    
    def simulate_execution(
        self,
        signal: Dict[str, Any],
        current_price: float,
        market_conditions: Optional[Dict[str, Any]] = None
    ) -> ExecutionResult:
        """
        Simulate realistic execution of a signal.
        
        Args:
            signal: Execution signal (from ExecutionPlanner or direct)
            current_price: Current market price (mid)
            market_conditions: Optional market state (volatility, volume, etc.)
            
        Returns:
            ExecutionResult with realistic fill details
        """
        signal_id = signal.get('id', 'unknown')
        ticker = signal.get('ticker', 'unknown')
        direction = signal.get('direction', 'long')
        target_quantity = signal.get('quantity', 0)
        
        if market_conditions is None:
            market_conditions = {}
        
        # Get market state
        volatility = market_conditions.get('realized_volatility', 0.02)
        avg_daily_volume = market_conditions.get('avg_daily_volume', 1000000)
        market_condition = self._determine_market_condition(market_conditions)
        
        # Calculate latency
        latency_ms = self._calculate_latency(target_quantity, volatility)
        
        # Calculate spread
        spread_bps = self._calculate_spread(current_price, volatility, market_condition)
        spread_cost = current_price * (spread_bps / 10000)
        
        # Calculate slippage
        slippage_bps = self._calculate_slippage(target_quantity, volatility, market_condition)
        slippage_cost = current_price * (slippage_bps / 10000)
        
        # Calculate market impact
        impact_bps = self._calculate_market_impact(target_quantity, avg_daily_volume)
        impact_cost = current_price * (impact_bps / 10000)
        
        # Total execution cost
        total_cost = spread_cost + slippage_cost + impact_cost
        total_cost_bps = spread_bps + slippage_bps + impact_bps
        
        # Calculate execution price based on direction
        if direction == 'long':
            # Buy: pay ask (mid + half spread + slippage + impact)
            execution_price = current_price + (spread_cost / 2) + slippage_cost + impact_cost
        else:  # short
            # Sell: receive bid (mid - half spread - slippage - impact)
            execution_price = current_price - (spread_cost / 2) - slippage_cost - impact_cost
        
        # Calculate fill ratio (partial fills)
        fill_ratio, partial_fills = self._calculate_fill_ratio(
            target_quantity,
            volatility,
            market_condition,
            avg_daily_volume
        )
        
        filled_quantity = target_quantity * fill_ratio
        
        # Determine status
        if fill_ratio < 0.01:
            status = "rejected"
        elif fill_ratio < 0.95:
            status = "partial"
        else:
            status = "filled"
        
        result = ExecutionResult(
            signal_id=signal_id,
            ticker=ticker,
            filled_quantity=filled_quantity,
            fill_ratio=fill_ratio,
            requested_price=current_price,
            execution_price=execution_price,
            spread_cost=spread_cost,
            slippage_cost=slippage_cost,
            market_impact=impact_cost,
            total_cost=total_cost,
            spread_bps=spread_bps,
            slippage_bps=slippage_bps,
            total_cost_bps=total_cost_bps,
            partial_fills=partial_fills,
            latency_ms=latency_ms,
            market_condition=market_condition,
            status=status
        )
        
        logger.debug(
            f"Simulated execution for {ticker}: {status}, "
            f"fill={fill_ratio:.1%}, cost={total_cost_bps:.1f}bps, "
            f"latency={latency_ms:.0f}ms"
        )
        
        return result
    
    def simulate_batch_execution(
        self,
        signals: List[Dict[str, Any]],
        current_prices: Dict[str, float],
        market_conditions: Optional[Dict[str, Any]] = None
    ) -> List[ExecutionResult]:
        """Simulate execution for multiple signals."""
        results = []
        
        for signal in signals:
            ticker = signal.get('ticker')
            current_price = current_prices.get(ticker, 0.0)
            
            if current_price <= 0:
                logger.warning(f"No price for {ticker}, skipping simulation")
                continue
            
            result = self.simulate_execution(signal, current_price, market_conditions)
            results.append(result)
            
            # Market impact accumulates for large batch orders
            if len(results) > 5:
                # Increase slippage for subsequent orders in large batches
                market_conditions = market_conditions or {}
                market_conditions['temp_impact_multiplier'] = 1 + (len(results) * 0.05)
        
        return results
    
    def _determine_market_condition(
        self,
        market_conditions: Dict[str, Any]
    ) -> MarketCondition:
        """Determine current market condition."""
        volatility = market_conditions.get('realized_volatility', 0.02)
        spread_bps = market_conditions.get('spread_bps', 10)
        volume_ratio = market_conditions.get('volume_ratio', 1.0)
        vix = market_conditions.get('vix', 15)
        
        if vix > 30 or volatility > 0.05:
            return MarketCondition.HIGH_VOLATILITY
        elif vix > 25 or volume_ratio < 0.5:
            return MarketCondition.LOW_LIQUIDITY
        elif volume_ratio > 2.0 and spread_bps < 5:
            return MarketCondition.HIGH_LIQUIDITY
        else:
            return MarketCondition.NORMAL
    
    def _calculate_latency(
        self,
        order_size: float,
        volatility: float
    ) -> float:
        """Calculate execution latency in milliseconds."""
        # Base latency
        latency = self.base_latency_ms
        
        # Size impact (larger orders take longer)
        size_factor = 1 + (order_size / 10000) * self.latency_size_factor
        latency *= size_factor
        
        # Volatility impact (higher vol = longer latency)
        vol_factor = 1 + (volatility / 0.02) * self.latency_volatility_factor
        latency *= vol_factor
        
        # Time of day effect (simplified)
        hour = datetime.now().hour
        if 9 <= hour <= 10:  # Market open
            latency *= 1.5
        elif 15 <= hour <= 16:  # Market close
            latency *= 1.3
        
        # Add jitter (±20%)
        jitter = self.rng.uniform(0.8, 1.2)
        latency *= jitter
        
        return latency
    
    def _calculate_spread(
        self,
        price: float,
        volatility: float,
        market_condition: MarketCondition
    ) -> float:
        """Calculate bid-ask spread in basis points."""
        # Base spread by price level
        if price < 10:
            base_spread = 50  # 50 bps for cheap stocks
        elif price < 50:
            base_spread = 20
        elif price < 100:
            base_spread = 10
        else:
            base_spread = 5
        
        # Volatility adjustment
        vol_adjustment = volatility * self.spread_volatility_factor * 100  # Convert to bps
        
        spread = base_spread + vol_adjustment
        
        # Market condition multiplier
        multipliers = {
            MarketCondition.HIGH_LIQUIDITY: 0.7,
            MarketCondition.NORMAL: 1.0,
            MarketCondition.LOW_LIQUIDITY: 1.5,
            MarketCondition.HIGH_VOLATILITY: 2.0,
            MarketCondition.STRESSED: 3.0
        }
        spread *= multipliers.get(market_condition, 1.0)
        
        return spread
    
    def _calculate_slippage(
        self,
        order_size: float,
        volatility: float,
        market_condition: MarketCondition
    ) -> float:
        """Calculate slippage in basis points."""
        # Base slippage
        slippage = self.slippage_base_bps
        
        # Size impact
        size_impact = (order_size / 1000) * self.slippage_size_factor
        slippage += size_impact
        
        # Volatility impact
        vol_impact = (volatility / 0.02) * self.slippage_volatility_factor
        slippage += vol_impact
        
        # Market condition multiplier
        multipliers = {
            MarketCondition.HIGH_LIQUIDITY: 0.8,
            MarketCondition.NORMAL: 1.0,
            MarketCondition.LOW_LIQUIDITY: 1.3,
            MarketCondition.HIGH_VOLATILITY: 1.8,
            MarketCondition.STRESSED: 2.5
        }
        slippage *= multipliers.get(market_condition, 1.0)
        
        return max(slippage, 0.1)  # Minimum 0.1 bps
    
    def _calculate_market_impact(
        self,
        order_size: float,
        avg_daily_volume: float
    ) -> float:
        """Calculate market impact in basis points."""
        if avg_daily_volume <= 0:
            return 0.0
        
        # Order size as % of ADV
        size_pct = order_size / avg_daily_volume
        
        # Square root impact model
        impact = self.impact_base_bps * (size_pct ** 0.5) * 100  # Scale to bps
        
        # Apply configurable factor
        impact *= self.impact_size_factor
        
        return impact
    
    def _calculate_fill_ratio(
        self,
        order_size: float,
        volatility: float,
        market_condition: MarketCondition,
        avg_daily_volume: float
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """
        Calculate fill ratio and partial fill details.
        Returns (fill_ratio, partial_fills_list)
        """
        # Small orders usually fill completely
        if order_size < 1000:
            return 1.0, []
        
        # Calculate base fill probability
        fill_prob = self.min_fill_probability
        
        # Adjust for order size
        size_pct = order_size / avg_daily_volume if avg_daily_volume > 0 else 0.01
        if size_pct > 0.01:  # >1% of ADV
            fill_prob -= size_pct * 10  # Reduce fill prob for large orders
        
        # Adjust for market conditions
        condition_adjustments = {
            MarketCondition.HIGH_LIQUIDITY: 0.05,
            MarketCondition.NORMAL: 0.0,
            MarketCondition.LOW_LIQUIDITY: -0.10,
            MarketCondition.HIGH_VOLATILITY: -0.15,
            MarketCondition.STRESSED: -0.25
        }
        fill_prob += condition_adjustments.get(market_condition, 0.0)
        
        # Clamp to valid range
        fill_prob = max(0.0, min(1.0, fill_prob))
        
        # Determine fill ratio
        if self.rng.random() < fill_prob:
            # Successful fill (possibly partial)
            if order_size < self.large_order_threshold:
                fill_ratio = self.rng.uniform(0.95, 1.0)
            else:
                fill_ratio = self.rng.uniform(0.7, 0.95)
        else:
            # Failed or severely partial fill
            fill_ratio = self.rng.uniform(0.0, 0.3)
        
        # Generate partial fills for larger orders
        partial_fills = []
        if fill_ratio < 1.0 and fill_ratio > 0.1:
            num_fills = min(3, int(order_size / 1000))
            if num_fills > 1:
                remaining = fill_ratio
                for i in range(num_fills):
                    fill_pct = remaining / (num_fills - i) * self.rng.uniform(0.8, 1.2)
                    fill_pct = min(fill_pct, remaining)
                    remaining -= fill_pct
                    
                    partial_fills.append({
                        'quantity': order_size * fill_pct,
                        'fill_ratio': fill_pct,
                        'fill_number': i + 1
                    })
        
        return fill_ratio, partial_fills
    
    def get_simulation_summary(self, results: List[ExecutionResult]) -> Dict[str, Any]:
        """Get summary statistics for simulation results."""
        if not results:
            return {}
        
        total_orders = len(results)
        filled = sum(1 for r in results if r.status == 'filled')
        partial = sum(1 for r in results if r.status == 'partial')
        rejected = sum(1 for r in results if r.status == 'rejected')
        
        avg_latency = sum(r.latency_ms for r in results) / total_orders
        avg_cost_bps = sum(r.total_cost_bps for r in results) / total_orders
        avg_fill_ratio = sum(r.fill_ratio for r in results) / total_orders
        
        return {
            'total_orders': total_orders,
            'filled': filled,
            'partial': partial,
            'rejected': rejected,
            'avg_latency_ms': avg_latency,
            'avg_cost_bps': avg_cost_bps,
            'avg_fill_ratio': avg_fill_ratio,
            'total_slippage_cost': sum(r.slippage_cost for r in results),
            'total_spread_cost': sum(r.spread_cost for r in results)
        }
