from __future__ import annotations
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.trading.models import Order as AlpacaOrder, Position as AlpacaPosition

from app.trading.base import TradingProvider
from app.trading.trade_lifecycle import TradePosition, OrderType, TradeState
from app.db.repository import AlphaRepository

logger = logging.getLogger(__name__)

class AlpacaProvider(TradingProvider):
    """
    Alpaca Paper Trading Provider.
    Bridges Alpha Engine to the official Alpaca Trading API.
    """
    name = "alpaca_paper"

    def __init__(self, api_key: str, api_secret: str, repository: AlphaRepository, paper: bool = True):
        self.client = TradingClient(api_key, api_secret, paper=paper)
        self.repository = repository
        self.paper = paper
        logger.info(f"AlpacaProvider initialized (paper={paper})")

    async def submit_order(
        self,
        ticker: str,
        quantity: Optional[float] = None,
        direction: str = "buy",
        order_type: OrderType = OrderType.MARKET,
        notional: Optional[float] = None,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Submit an order to Alpaca.
        Supports both quantity (shares) and notional (dollars) for market orders.
        """
        if quantity is None and notional is None:
            raise ValueError("Either quantity or notional must be provided")

        if order_type != OrderType.MARKET:
            raise ValueError(f"AlpacaProvider currently only supports MARKET orders. Got {order_type}")

        side = OrderSide.BUY if direction.lower() in ["long", "buy", "up"] else OrderSide.SELL
        
        # Enforce safety limits for testing
        if quantity and quantity > 1.0:
            logger.warning(f"Quantity {quantity} exceeds test limit. Capping at 1.0 share.")
            quantity = 1.0
        
        if notional and notional > 10.0:
            logger.warning(f"Notional ${notional} exceeds test limit. Capping at $10.0.")
            notional = 10.0

        order_data = MarketOrderRequest(
            symbol=ticker.upper(),
            qty=quantity,
            notional=notional,
            side=side,
            time_in_force=TimeInForce.DAY if notional else TimeInForce.GTC
        )

        try:
            order = self.client.submit_order(order_data)
            logger.info(f"Submitted {side} order for {ticker}: {order.id}")
            return self._map_order_to_dict(order)
        except Exception as e:
            logger.error(f"Failed to submit Alpaca order: {e}")
            raise

    async def get_positions(self) -> List[TradePosition]:
        """Fetch current open positions from Alpaca."""
        try:
            alpaca_positions = self.client.get_all_positions()
            return [self._map_position(p) for p in alpaca_positions]
        except Exception as e:
            logger.error(f"Failed to fetch Alpaca positions: {e}")
            return []

    async def get_account(self) -> Dict[str, Any]:
        """Fetch account information."""
        try:
            account = self.client.get_account()
            return {
                "cash": float(account.cash),
                "buying_power": float(account.buying_power),
                "equity": float(account.equity),
                "initial_margin": float(account.initial_margin),
                "maintenance_margin": float(account.maintenance_margin),
                "status": account.status
            }
        except Exception as e:
            logger.error(f"Failed to fetch Alpaca account: {e}")
            raise

    async def sync(self) -> None:
        """
        Synchronize local database with Alpaca account and positions.
        """
        logger.info("Synchronizing Alpaca state with local database...")
        try:
            # 1. Sync Positions
            positions = await self.get_positions()
            for pos in positions:
                self.repository.upsert_position({
                    "ticker": pos.ticker,
                    "direction": pos.direction,
                    "quantity": pos.total_quantity,
                    "average_entry_price": pos.average_entry_price,
                    "mode": "paper"
                })
            
            # 2. Sync account (log for now, could update a portfolio_state table)
            account = await self.get_account()
            logger.info(f"Synced Alpaca Account: Equity=${account['equity']:,.2f}, Cash=${account['cash']:,.2f}")
            
            # 3. Sync open orders (optional, but good for completeness)
            # orders = self.client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
            # ... update a paper_orders table ...
            
        except Exception as e:
            logger.error(f"Failed to sync Alpaca state: {e}")

    def _map_order_to_dict(self, order: AlpacaOrder) -> Dict[str, Any]:
        """Convert Alpaca order model to internal dict format."""
        return {
            "id": str(order.id),
            "client_order_id": order.client_order_id,
            "ticker": order.symbol,
            "direction": "long" if order.side == OrderSide.BUY else "short",
            "quantity": float(order.qty) if order.qty else 0.0,
            "filled_quantity": float(order.filled_qty or 0),
            "status": str(order.status),
            "created_at": order.created_at.isoformat() if order.created_at else None,
            "filled_at": order.filled_at.isoformat() if order.filled_at else None,
            "average_fill_price": float(order.filled_avg_price or 0)
        }

    def _map_position(self, p: AlpacaPosition) -> TradePosition:
        """Map Alpaca position to internal TradePosition."""
        return TradePosition(
            ticker=p.symbol,
            direction="long" if p.side == "long" else "short",
            total_quantity=float(p.qty),
            filled_quantity=float(p.qty),
            remaining_quantity=0.0,
            average_entry_price=float(p.avg_entry_price),
            current_price=float(p.current_price),
            unrealized_pnl=float(p.unrealized_pl),
            unrealized_pnl_pct=float(p.unrealized_plpc),
            last_updated=datetime.now(timezone.utc)
        )
