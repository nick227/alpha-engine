"""
Regulatory Signal Generation

Converts SEC regulatory events into trading signals.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import sqlite3
import numpy as np

from .sec_ingest import RegulatoryEvent, get_sec_engine

logger = logging.getLogger(__name__)


class RegulatorySignalGenerator:
    """
    Converts SEC regulatory events into actionable trading signals.
    
    Regulatory events provide high-confidence, verified information
    that can serve as primary signals or confirmation layers.
    """
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self.signal_weights = {
            'insider_buy': 0.8,      # Strong bullish signal
            'insider_sell': -0.6,     # Bearish signal
            'merger': 0.9,           # Very strong event
            'exec_change': 0.3,        # Moderate signal
            'earnings': 0.5,           # Moderate signal
            'bankruptcy': -1.0,        # Very strong bearish
            'corp_event': 0.2           # Weak signal
        }
        
        logger.info("Regulatory Signal Generator initialized")
    
    def get_recent_events(self, hours_back: int = 24) -> List[RegulatoryEvent]:
        """
        Get recent regulatory events for signal generation.
        
        Args:
            hours_back: Hours to look back for events
            
        Returns:
            List of recent regulatory events
        """
        
        try:
            conn = sqlite3.connect(self.db_path)
            cutoff_time = (datetime.now() - timedelta(hours=hours_back)).isoformat()
            
            cursor = conn.execute("""
                SELECT symbol, company_name, event_type, filing_type, filing_date,
                       event_date, description, details, confidence, processed_at
                FROM regulatory_events
                WHERE processed_at >= ?
                ORDER BY processed_at DESC
            """, (cutoff_time,))
            
            events = []
            for row in cursor.fetchall():
                event = RegulatoryEvent(
                    symbol=row[0],
                    company_name=row[1],
                    event_type=row[2],
                    filing_type=row[3],
                    filing_date=row[4],
                    event_date=row[5],
                    description=row[6],
                    details=eval(row[7]) if row[7] else {},
                    confidence=row[8],
                    processed_at=row[9]
                )
                events.append(event)
            
            conn.close()
            return events
            
        except Exception as e:
            logger.error(f"Error getting recent events: {e}")
            return []
    
    def generate_insider_signals(self, events: List[RegulatoryEvent]) -> List[Dict[str, Any]]:
        """
        Generate signals from insider trading activity.
        
        Args:
            events: List of regulatory events
            
        Returns:
            List of insider trading signals
        """
        
        insider_signals = []
        
        for event in events:
            if event.event_type not in ['insider_buy', 'insider_sell']:
                continue
            
            try:
                # Calculate signal strength based on transaction size
                details = event.details
                net_activity = details.get('net_activity', 0)
                total_buys = details.get('total_buys', 0)
                total_sells = details.get('total_sells', 0)
                
                # Signal strength based on magnitude
                if abs(net_activity) < 1000:  # Small trades
                    magnitude = 0.3
                elif abs(net_activity) < 10000:  # Medium trades
                    magnitude = 0.6
                else:  # Large trades
                    magnitude = 1.0
                
                # Base signal
                base_signal = self.signal_weights[event.event_type]
                signal_strength = base_signal * magnitude
                
                # Additional context
                if event.event_type == 'insider_buy':
                    signal_direction = 'bullish'
                    confidence = min(0.9, 0.5 + magnitude * 0.4)
                else:
                    signal_direction = 'bearish'
                    confidence = min(0.9, 0.6 + magnitude * 0.3)
                
                signal = {
                    'symbol': event.symbol,
                    'signal_type': 'insider_activity',
                    'event_type': event.event_type,
                    'direction': signal_direction,
                    'strength': signal_strength,
                    'confidence': confidence,
                    'source': 'regulatory',
                    'event_date': event.event_date,
                    'description': event.description,
                    'details': {
                        'net_shares': net_activity,
                        'buys': total_buys,
                        'sells': total_sells,
                        'magnitude': magnitude
                    },
                    'expires_at': (datetime.now() + timedelta(days=7)).isoformat()
                }
                
                insider_signals.append(signal)
                
            except Exception as e:
                logger.warning(f"Error generating insider signal for {event.symbol}: {e}")
                continue
        
        logger.info(f"Generated {len(insider_signals)} insider signals")
        return insider_signals
    
    def generate_corporate_event_signals(self, events: List[RegulatoryEvent]) -> List[Dict[str, Any]]:
        """
        Generate signals from corporate events (mergers, exec changes, etc.).
        
        Args:
            events: List of regulatory events
            
        Returns:
            List of corporate event signals
        """
        
        corporate_signals = []
        
        for event in events:
            if event.event_type not in ['merger', 'exec_change', 'bankruptcy']:
                continue
            
            try:
                # High confidence for major corporate events
                signal_strength = self.signal_weights[event.event_type]
                
                if event.event_type == 'merger':
                    signal_direction = 'bullish'  # Acquisitions usually positive
                    confidence = 0.95
                    expiration_days = 30
                elif event.event_type == 'exec_change':
                    signal_direction = 'neutral'
                    confidence = 0.6
                    expiration_days = 14
                elif event.event_type == 'bankruptcy':
                    signal_direction = 'bearish'
                    confidence = 0.99
                    expiration_days = 90
                else:
                    signal_direction = 'neutral'
                    confidence = 0.5
                    expiration_days = 7
                
                signal = {
                    'symbol': event.symbol,
                    'signal_type': 'corporate_event',
                    'event_type': event.event_type,
                    'direction': signal_direction,
                    'strength': signal_strength,
                    'confidence': confidence,
                    'source': 'regulatory',
                    'event_date': event.event_date,
                    'description': event.description,
                    'details': event.details,
                    'expires_at': (datetime.now() + timedelta(days=expiration_days)).isoformat()
                }
                
                corporate_signals.append(signal)
                
            except Exception as e:
                logger.warning(f"Error generating corporate signal for {event.symbol}: {e}")
                continue
        
        logger.info(f"Generated {len(corporate_signals)} corporate event signals")
        return corporate_signals
    
    def generate_fundamental_signals(self, events: List[RegulatoryEvent]) -> List[Dict[str, Any]]:
        """
        Generate signals from fundamental filings (10-Q, 10-K).
        
        Args:
            events: List of regulatory events
            
        Returns:
            List of fundamental signals
        """
        
        fundamental_signals = []
        
        for event in events:
            if event.event_type not in ['earnings', 'annual_report']:
                continue
            
            try:
                details = event.details
                revenue = details.get('revenue', 0)
                net_income = details.get('net_income', 0)
                eps = details.get('earnings_per_share', 0)
                
                # Calculate fundamental strength
                if revenue > 0 and net_income > 0:
                    fundamental_health = 0.8
                elif revenue > 0 and net_income < 0:
                    fundamental_health = 0.4
                else:
                    fundamental_health = 0.2
                
                # Earnings surprise detection (would need historical data)
                earnings_surprise = 0  # Placeholder
                
                signal_strength = self.signal_weights[event.event_type] * fundamental_health
                
                signal = {
                    'symbol': event.symbol,
                    'signal_type': 'fundamental',
                    'event_type': event.event_type,
                    'direction': 'bullish' if net_income > 0 else 'bearish',
                    'strength': signal_strength,
                    'confidence': 0.7,  # Moderate confidence for fundamentals
                    'source': 'regulatory',
                    'event_date': event.event_date,
                    'description': event.description,
                    'details': {
                        'revenue': revenue,
                        'net_income': net_income,
                        'eps': eps,
                        'fundamental_health': fundamental_health,
                        'earnings_surprise': earnings_surprise
                    },
                    'expires_at': (datetime.now() + timedelta(days=21)).isoformat()  # Quarterly relevance
                }
                
                fundamental_signals.append(signal)
                
            except Exception as e:
                logger.warning(f"Error generating fundamental signal for {event.symbol}: {e}")
                continue
        
        logger.info(f"Generated {len(fundamental_signals)} fundamental signals")
        return fundamental_signals
    
    def generate_all_signals(self, hours_back: int = 24) -> List[Dict[str, Any]]:
        """
        Generate all types of regulatory signals.
        
        Args:
            hours_back: Hours to look back for events
            
        Returns:
            List of all generated signals
        """
        
        # Get recent events
        events = self.get_recent_events(hours_back)
        
        if not events:
            logger.info("No recent regulatory events found")
            return []
        
        # Generate different signal types
        insider_signals = self.generate_insider_signals(events)
        corporate_signals = self.generate_corporate_event_signals(events)
        fundamental_signals = self.generate_fundamental_signals(events)
        
        # Combine all signals
        all_signals = insider_signals + corporate_signals + fundamental_signals
        
        # Sort by strength and confidence
        all_signals.sort(key=lambda x: (x['strength'] * x['confidence']), reverse=True)
        
        logger.info(f"Generated {len(all_signals)} total regulatory signals")
        return all_signals
    
    def filter_signals_by_symbol(self, signals: List[Dict[str, Any]], 
                             symbols: List[str]) -> List[Dict[str, Any]]:
        """
        Filter signals for specific symbols.
        
        Args:
            signals: List of all signals
            symbols: List of symbols to filter for
            
        Returns:
            Filtered signals
        """
        
        filtered = []
        for signal in signals:
            if signal['symbol'] in symbols:
                filtered.append(signal)
        
        return filtered
    
    def get_active_signals(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """
        Get currently active regulatory signals.
        
        Args:
            symbols: Optional list of symbols to filter for
            
        Returns:
            List of active signals
        """
        
        all_signals = self.generate_all_signals(hours_back=24)
        
        if symbols:
            all_signals = self.filter_signals_by_symbol(all_signals, symbols)
        
        # Filter expired signals
        current_time = datetime.now()
        active_signals = []
        
        for signal in all_signals:
            expires_at = datetime.fromisoformat(signal['expires_at'])
            if expires_at > current_time:
                active_signals.append(signal)
        
        return active_signals


# Global signal generator
regulatory_signal_generator = None


def get_regulatory_signal_generator(db_path: str = "data/alpha.db") -> RegulatorySignalGenerator:
    """Get or create regulatory signal generator instance."""
    
    global regulatory_signal_generator
    if regulatory_signal_generator is None:
        regulatory_signal_generator = RegulatorySignalGenerator(db_path)
    return regulatory_signal_generator


def get_regulatory_signals(symbols: List[str] = None, hours_back: int = 24) -> List[Dict[str, Any]]:
    """
    Get regulatory signals for trading.
    
    Args:
        symbols: List of symbols to get signals for
        hours_back: Hours to look back for signals
        
    Returns:
        List of regulatory signals
    """
    
    try:
        generator = get_regulatory_signal_generator()
        return generator.get_active_signals(symbols)
    except Exception as e:
        logger.error(f"Error getting regulatory signals: {e}")
        return []
