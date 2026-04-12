from __future__ import annotations
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from app.trading.base import TradingProvider
from app.trading.trade_lifecycle import TradePosition, OrderType, TradeState
from app.trading.execution_simulator import ExecutionSimulator

logger = logging.getLogger(__name__)

class LocalSimProvider(TradingProvider):
    """
    Local Simulation Provider.
    Simulates trading execution in-memory.
    """
    name = "local_sim"

    def __init__(self, initial_cash: float = 100000.0, config: Optional[Dict[str, Any]] = None):
        self.cash = initial_cash
        self.positions: Dict[str, TradePosition] = {}
        self.simulator = ExecutionSimulator(config or {})
        logger.info(f"LocalSimProvider initialized with ${initial_cash:,.2f}")

    async def submit_order(
        self,
        ticker: str,
        quantity: float,
        direction: str,
        order_type: OrderType = OrderType.MARKET,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None
    ) -> Dict[str, Any]:
        """Simulate order execution."""
        # Calculate realistic execution price using the simulator
        market_price = limit_price or 0.0 # Placeholder, simulator usually needs more context
        execution_result = self.simulator.calculate_execution_price(ticker, market_price, quantity)
        execution_price = execution_result.execution_price

        # Update local portfolio state
        cost = execution_price * quantity
        if direction.lower() in ["long", "buy", "up"]:
            self.cash -= cost
            if ticker in self.positions:
                pos = self.positions[ticker]
                new_qty = pos.total_quantity + quantity
                new_avg = (pos.average_entry_price * pos.total_quantity + cost) / new_qty
                pos.total_quantity = new_qty
                pos.filled_quantity = new_qty
                pos.average_entry_price = new_avg
            else:
                self.positions[ticker] = TradePosition(
                    ticker=ticker,
                    direction="long",
                    total_quantity=quantity,
                    filled_quantity=quantity,
                    remaining_quantity=0.0,
                    average_entry_price=execution_price,
                    current_price=execution_price,
                    unrealized_pnl=0.0,
                    unrealized_pnl_pct=0.0,
                    last_updated=datetime.now(timezone.utc)
                )
        else: # sell
            # Simpler sell logic for POC
            self.cash += cost
            if ticker in self.positions:
                pos = self.positions[ticker]
                pos.total_quantity -= quantity
                pos.filled_quantity -= quantity
                if pos.total_quantity <= 0:
                    del self.positions[ticker]

        order_id = str(uuid.uuid4())
        return {
            "id": order_id,
            "ticker": ticker,
            "direction": direction,
            "quantity": quantity,
            "status": "filled",
            "average_fill_price": execution_price,
            "filled_at": datetime.now(timezone.utc).isoformat()
        }

    async def get_positions(self) -> List[TradePosition]:
        return list(self.positions.values())

    async def get_account(self) -> Dict[str, Any]:
        equity = self.cash + sum(p.total_quantity * p.current_price for p in self.positions.values())
        return {
            "cash": self.cash,
            "equity": equity,
            "buying_power": self.cash, # simple
            "status": "ACTIVE"
        }

    async def sync(self) -> None:
        """Local sim is always stay in sync with itself."""
        pass
