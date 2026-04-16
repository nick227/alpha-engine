from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import sqlite3

from app.core.repository_interface import SignalRepository, PriceRepository, Signal
from app.core.types import SignalDirection


class SQLiteSignalRepository(SignalRepository):
    """SQLite-backed signal repository implementation."""
    
    def __init__(self, db_path: str = "data/alpha.db"):
        """Initialize repository with database path."""
        self.db_path = db_path
        
    def get_signals(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get signals in the specified date range."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            cur.execute("""
                SELECT * FROM signals
                WHERE ts BETWEEN ? AND ?
            """, (start_date.isoformat(), end_date.isoformat()))
            
            rows = cur.fetchall()
            
            # Convert rows to dictionaries
            return [dict(row) for row in rows]
    
    def get_signal_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Get raw signal data for analysis."""
        signals = self.get_signals(start_date, end_date)
        
        # Transform signals into matrix format
        signal_matrix = {}
        for signal in signals:
            ts = signal['ts']
            symbol = signal['symbol']
            direction = signal['direction']
            
            if ts not in signal_matrix:
                signal_matrix[ts] = {}
                
            signal_matrix[ts][symbol] = direction
            
        return signal_matrix


class SQLitePriceRepository(PriceRepository):
    """SQLite-backed price repository implementation."""
    
    def __init__(self, db_path: str = "data/alpha.db"):
        """Initialize repository with database path."""
        self.db_path = db_path
        
    def get_price_data(self, start_date: datetime, end_date: datetime, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get price data for the specified date range and symbols."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            # Build query
            query = """
                SELECT * FROM prices
                WHERE ts BETWEEN ? AND ?
            """
            
            params = [start_date.isoformat(), end_date.isoformat()]
            
            if symbols:
                query += f" AND symbol IN ({','.join(['?'] * len(symbols))})"
                params.extend(symbols)
                
            cur.execute(query, params)
            
            rows = cur.fetchall()
            
            # Convert rows to dictionaries
            return [dict(row) for row in rows]
    
    def get_volatility_stats(self, window: int = 20) -> Dict[str, Any]:
        """Get volatility statistics for the portfolio universe."""
        # This would be implemented with proper volatility calculations
        # For now, return a placeholder
        return {
            "volatility": {
                "AAPL": 0.15,
                "MSFT": 0.12,
                "NVDA": 0.18,
                "AMZN": 0.14
            }
        }
        
    def get_volume_profile(self) -> Dict[str, Any]:
        """Get volume profile across symbols."""
        # For now, return a simple profile
        return {
            "volume": {
                "AAPL": 1000000,
                "MSFT": 800000,
                "NVDA": 500000,
                "AMZN": 600000
            }
        }
        
    def get_rsi(self, window: int = 14) -> Dict[str, Any]:
        """Get RSI data across symbols."""
        return {
            "rsi": {
                "AAPL": 55,
                "MSFT": 48,
                "NVDA": 62,
                "AMZN": 50
            }
        }
        
    def get_earnings_sentiment(self) -> Dict[str, Any]:
        """Get earnings sentiment data across symbols."""
        return {
            "sentiment": {
                "AAPL": 7.2,
                "MSFT": 6.8,
                "NVDA": 8.1,
                "AMZN": 6.5
            }
        }


# Create interface-compliant repositories
Repository = type(
    "Repository",
    (SQLiteSignalRepository, SQLitePriceRepository),
    {}
)

__all__ = ["Repository"]