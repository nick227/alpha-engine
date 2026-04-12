from __future__ import annotations
from typing import Protocol, List, Dict, Any, Optional
from datetime import datetime
from app.trading.trade_lifecycle import Trade, TradePosition, OrderType

class TradingProvider(Protocol):
    """Protocol for trading execution providers (Alpaca, Local Simulation, etc.)"""
    name: str

    async def submit_order(
        self,
        ticker: str,
        quantity: float,
        direction: str,
        order_type: OrderType = OrderType.MARKET,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None
    ) -> Dict[str, Any]:
        """Submit a new order."""
        ...

    async def get_positions(self) -> List[TradePosition]:
        """Fetch current open positions from the provider."""
        ...

    async def get_account(self) -> Dict[str, Any]:
        """Fetch account information (cash, buying power, etc.)."""
        ...

    async def sync(self) -> None:
        """Synchronize local state with provider state."""
        ...
