"""
SEC EDGAR Data Ingestion Module

Integrates sec-api to collect verified regulatory signals from SEC filings.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import time
import sqlite3
from dataclasses import dataclass

try:
    from sec_api import QueryApi, ExtractorApi
    SEC_API_AVAILABLE = True
except ImportError:
    SEC_API_AVAILABLE = False
    logging.warning("sec-api not installed. Run: pip install sec-api")

logger = logging.getLogger(__name__)


@dataclass
class RegulatoryEvent:
    """Standardized regulatory event."""
    
    symbol: str
    company_name: str
    event_type: str  # "insider_buy", "insider_sell", "earnings", "merger", "exec_change", etc.
    source_type: str = "regulatory_event"
    filing_type: str  # "8-K", "10-Q", "10-K", "Form 4", etc.
    filing_date: str
    event_date: str
    description: str
    details: Dict[str, Any]
    confidence: float = 1.0  # SEC data is high confidence
    processed_at: str = None
    
    def __post_init__(self):
        if self.processed_at is None:
            self.processed_at = datetime.now().isoformat()


class SECIngestionEngine:
    """
    SEC EDGAR data ingestion engine using sec-api.
    
    Collects and normalizes regulatory signals from SEC filings.
    """
    
    def __init__(self, api_key: str = None, db_path: str = "data/alpha.db"):
        self.api_key = api_key or os.getenv('SEC_API_KEY')
        self.db_path = db_path
        
        if not self.api_key:
            raise ValueError("SEC_API_KEY required. Set environment variable or pass parameter")
        
        if not SEC_API_AVAILABLE:
            raise ImportError("sec-api not installed. Run: pip install sec-api")
        
        # Initialize API clients
        self.query_api = QueryApi(api_key=self.api_key)
        self.extractor_api = ExtractorApi(api_key=self.api_key)
        
        # Initialize database
        self._init_database()
        
        logger.info("SEC Ingestion Engine initialized")
    
    def _init_database(self):
        """Initialize database tables for regulatory events."""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Regulatory events table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS regulatory_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    source_type TEXT DEFAULT 'regulatory_event',
                    filing_type TEXT NOT NULL,
                    filing_date TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    description TEXT,
                    details TEXT,
                    confidence REAL DEFAULT 1.0,
                    processed_at TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, filing_type, filing_date, event_type)
                )
            """)
            
            # Event performance tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS regulatory_event_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    price_at_event REAL,
                    price_1d REAL,
                    price_3d REAL,
                    price_7d REAL,
                    price_30d REAL,
                    return_1d REAL,
                    return_3d REAL,
                    return_7d REAL,
                    return_30d REAL,
                    market_return_1d REAL,
                    market_return_3d REAL,
                    market_return_7d REAL,
                    market_return_30d REAL,
                    alpha_1d REAL,
                    alpha_3d REAL,
                    alpha_7d REAL,
                    alpha_30d REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (event_id) REFERENCES regulatory_events (id)
                )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def collect_form4_insider_trades(self, days_back: int = 7) -> List[RegulatoryEvent]:
        """
        Collect Form 4 filings for insider trading activity.
        
        Args:
            days_back: Number of days to look back for filings
            
        Returns:
            List of regulatory events for insider trades
        """
        
        logger.info(f"Collecting Form 4 insider trades for last {days_back} days")
        
        try:
            # Query for recent Form 4 filings
            query = {
                "query": {
                    "query_string": {
                        "query": f"formType:\"Form 4\" AND filedAt:[NOW-{days_back}DAYS TO NOW]"
                    }
                },
                "from": "0",
                "size": "100",
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            filings = self.query_api.get_filings(query)
            events = []
            
            for filing in filings.get('filings', []):
                try:
                    # Extract key information
                    symbol = filing.get('ticker', '').replace(' ', '')
                    company_name = filing.get('companyName', '')
                    filing_date = filing.get('filedAt', '').split('T')[0]
                    
                    if not symbol:
                        continue
                    
                    # Extract transaction details
                    transaction_details = self._extract_form4_details(filing)
                    
                    if not transaction_details:
                        continue
                    
                    # Determine if buy or sell
                    total_buys = sum(t['amount'] for t in transaction_details if t['action'] == 'buy')
                    total_sells = sum(t['amount'] for t in transaction_details if t['action'] == 'sell')
                    
                    if total_buys > total_sells:
                        event_type = "insider_buy"
                        description = f"Insider purchase: {total_buys:,.0f} shares"
                    elif total_sells > total_buys:
                        event_type = "insider_sell"
                        description = f"Insider sale: {total_sells:,.0f} shares"
                    else:
                        continue  # No net activity
                    
                    event = RegulatoryEvent(
                        symbol=symbol,
                        company_name=company_name,
                        event_type=event_type,
                        filing_type="Form 4",
                        filing_date=filing_date,
                        event_date=filing_date,
                        description=description,
                        details={
                            'transactions': transaction_details,
                            'total_buys': total_buys,
                            'total_sells': total_sells,
                            'net_activity': total_buys - total_sells
                        }
                    )
                    
                    events.append(event)
                    
                except Exception as e:
                    logger.warning(f"Error processing Form 4 filing: {e}")
                    continue
            
            logger.info(f"Collected {len(events)} Form 4 events")
            return events
            
        except Exception as e:
            logger.error(f"Error collecting Form 4 filings: {e}")
            return []
    
    def collect_8k_corporate_events(self, days_back: int = 7) -> List[RegulatoryEvent]:
        """
        Collect 8-K filings for corporate events.
        
        Args:
            days_back: Number of days to look back for filings
            
        Returns:
            List of regulatory events for corporate events
        """
        
        logger.info(f"Collecting 8-K corporate events for last {days_back} days")
        
        try:
            # Query for recent 8-K filings
            query = {
                "query": {
                    "query_string": {
                        "query": f"formType:\"8-K\" AND filedAt:[NOW-{days_back}DAYS TO NOW]"
                    }
                },
                "from": "0",
                "size": "100",
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            filings = self.query_api.get_filings(query)
            events = []
            
            for filing in filings.get('filings', []):
                try:
                    symbol = filing.get('ticker', '').replace(' ', '')
                    company_name = filing.get('companyName', '')
                    filing_date = filing.get('filedAt', '').split('T')[0]
                    
                    if not symbol:
                        continue
                    
                    # Extract 8-K details
                    eightk_details = self._extract_8k_details(filing)
                    
                    if not eightk_details:
                        continue
                    
                    # Categorize event type
                    event_type = eightk_details.get('category', 'corp_event')
                    description = eightk_details.get('description', '')
                    
                    event = RegulatoryEvent(
                        symbol=symbol,
                        company_name=company_name,
                        event_type=event_type,
                        filing_type="8-K",
                        filing_date=filing_date,
                        event_date=filing_date,
                        description=description,
                        details=eightk_details
                    )
                    
                    events.append(event)
                    
                except Exception as e:
                    logger.warning(f"Error processing 8-K filing: {e}")
                    continue
            
            logger.info(f"Collected {len(events)} 8-K events")
            return events
            
        except Exception as e:
            logger.error(f"Error collecting 8-K filings: {e}")
            return []
    
    def collect_10q_10k_fundamentals(self, days_back: int = 30) -> List[RegulatoryEvent]:
        """
        Collect 10-Q and 10-K filings for fundamental data.
        
        Args:
            days_back: Number of days to look back for filings
            
        Returns:
            List of regulatory events for fundamental updates
        """
        
        logger.info(f"Collecting 10-Q/10-K fundamentals for last {days_back} days")
        
        try:
            # Query for recent 10-Q and 10-K filings
            query = {
                "query": {
                    "query_string": {
                        "query": f"(formType:\"10-Q\" OR formType:\"10-K\") AND filedAt:[NOW-{days_back}DAYS TO NOW]"
                    }
                },
                "from": "0",
                "size": "50",
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            filings = self.query_api.get_filings(query)
            events = []
            
            for filing in filings.get('filings', []):
                try:
                    symbol = filing.get('ticker', '').replace(' ', '')
                    company_name = filing.get('companyName', '')
                    filing_date = filing.get('filedAt', '').split('T')[0]
                    form_type = filing.get('formType', '')
                    
                    if not symbol:
                        continue
                    
                    # Extract financial details
                    financial_details = self._extract_financial_details(filing)
                    
                    if not financial_details:
                        continue
                    
                    event_type = "earnings" if form_type == "10-Q" else "annual_report"
                    description = f"{form_type} filing: {financial_details.get('period', '')}"
                    
                    event = RegulatoryEvent(
                        symbol=symbol,
                        company_name=company_name,
                        event_type=event_type,
                        filing_type=form_type,
                        filing_date=filing_date,
                        event_date=filing_date,
                        description=description,
                        details=financial_details
                    )
                    
                    events.append(event)
                    
                except Exception as e:
                    logger.warning(f"Error processing {filing.get('formType', 'unknown')} filing: {e}")
                    continue
            
            logger.info(f"Collected {len(events)} 10-Q/10-K events")
            return events
            
        except Exception as e:
            logger.error(f"Error collecting 10-Q/10-K filings: {e}")
            return []
    
    def _extract_form4_details(self, filing: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract transaction details from Form 4 filing."""
        
        try:
            # Use sec-api extractor to get transaction details
            extraction = self.extractor_api.get_section(
                "ownershipDocument",
                filing['linkToFilingDetails']
            )
            
            transactions = []
            
            # Parse transactions (simplified - would need more sophisticated parsing)
            if 'table' in extraction:
                for row in extraction['table']:
                    try:
                        transaction = {
                            'insider': row.get('name', ''),
                            'title': row.get('title', ''),
                            'action': 'buy' if 'Purchase' in str(row) else 'sell',
                            'amount': int(row.get('amount', 0).replace(',', '')),
                            'price': float(row.get('price', 0).replace('$', '').replace(',', '')),
                            'date': row.get('date', '')
                        }
                        transactions.append(transaction)
                    except:
                        continue
            
            return transactions
            
        except Exception as e:
            logger.warning(f"Error extracting Form 4 details: {e}")
            return []
    
    def _extract_8k_details(self, filing: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract event details from 8-K filing."""
        
        try:
            # Use sec-api extractor to get 8-K details
            extraction = self.extractor_api.get_section(
                "document",
                filing['linkToFilingDetails']
            )
            
            # Categorize 8-K events
            content = str(extraction).lower()
            
            if 'merger' in content or 'acquisition' in content:
                category = 'merger'
                description = 'Merger or acquisition activity'
            elif 'director' in content or 'officer' in content:
                category = 'exec_change'
                description = 'Executive or director changes'
            elif 'earnings' in content or 'revenue' in content:
                category = 'earnings'
                description = 'Earnings announcement'
            elif 'bankruptcy' in content:
                category = 'bankruptcy'
                description = 'Bankruptcy filing'
            else:
                category = 'corp_event'
                description = 'Corporate event'
            
            return {
                'category': category,
                'description': description,
                'content_summary': content[:500] + '...' if len(content) > 500 else content
            }
            
        except Exception as e:
            logger.warning(f"Error extracting 8-K details: {e}")
            return None
    
    def _extract_financial_details(self, filing: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract financial details from 10-Q/10-K filing."""
        
        try:
            # Use sec-api extractor to get financial details
            extraction = self.extractor_api.get_section(
                "financials",
                filing['linkToFilingDetails']
            )
            
            # Parse financial metrics (simplified)
            return {
                'period': filing.get('periodOfReport', ''),
                'revenue': extraction.get('revenues', 0),
                'net_income': extraction.get('netIncome', 0),
                'earnings_per_share': extraction.get('eps', 0),
                'summary': str(extraction)[:200] + '...' if len(str(extraction)) > 200 else str(extraction)
            }
            
        except Exception as e:
            logger.warning(f"Error extracting financial details: {e}")
            return None
    
    def store_events(self, events: List[RegulatoryEvent]) -> int:
        """
        Store regulatory events in database.
        
        Args:
            events: List of regulatory events to store
            
        Returns:
            Number of events stored
        """
        
        if not events:
            return 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            stored_count = 0
            
            for event in events:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO regulatory_events 
                        (symbol, company_name, event_type, source_type, filing_type, 
                         filing_date, event_date, description, details, confidence, processed_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event.symbol,
                        event.company_name,
                        event.event_type,
                        event.source_type,
                        event.filing_type,
                        event.filing_date,
                        event.event_date,
                        event.description,
                        json.dumps(event.details),
                        event.confidence,
                        event.processed_at
                    ))
                    
                    if conn.total_changes > stored_count:
                        stored_count += 1
                        
                except Exception as e:
                    logger.warning(f"Error storing event for {event.symbol}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            logger.info(f"Stored {stored_count} regulatory events")
            return stored_count
            
        except Exception as e:
            logger.error(f"Error storing events: {e}")
            return 0
    
    def run_collection_cycle(self, days_back: int = 7) -> Dict[str, int]:
        """
        Run complete collection cycle for all filing types.
        
        Args:
            days_back: Number of days to look back for filings
            
        Returns:
            Dictionary with counts of events collected by type
        """
        
        logger.info(f"Starting SEC collection cycle for last {days_back} days")
        
        results = {
            'form4': 0,
            '8k': 0,
            '10q_10k': 0,
            'total': 0
        }
        
        try:
            # Collect Form 4 (insider trades)
            form4_events = self.collect_form4_insider_trades(days_back)
            results['form4'] = self.store_events(form4_events)
            
            # Collect 8-K (corporate events)
            eightk_events = self.collect_8k_corporate_events(days_back)
            results['8k'] = self.store_events(eightk_events)
            
            # Collect 10-Q/10-K (fundamentals)
            fundamental_events = self.collect_10q_10k_fundamentals(days_back)
            results['10q_10k'] = self.store_events(fundamental_events)
            
            results['total'] = sum(results.values())
            
            logger.info(f"Collection complete: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error in collection cycle: {e}")
            return results


# Global instance
sec_engine = None


def get_sec_engine(api_key: str = None) -> SECIngestionEngine:
    """Get or create SEC ingestion engine instance."""
    
    global sec_engine
    if sec_engine is None:
        sec_engine = SECIngestionEngine(api_key)
    return sec_engine


def run_sec_collection(days_back: int = 7, api_key: str = None) -> Dict[str, int]:
    """
    Run SEC data collection.
    
    Args:
        days_back: Number of days to look back
        api_key: SEC API key
        
    Returns:
        Collection results
    """
    
    try:
        engine = get_sec_engine(api_key)
        return engine.run_collection_cycle(days_back)
    except Exception as e:
        logger.error(f"SEC collection failed: {e}")
        return {'form4': 0, '8k': 0, '10q_10k': 0, 'total': 0}
