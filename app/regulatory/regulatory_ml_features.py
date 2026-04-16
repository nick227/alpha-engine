"""
Regulatory ML Feature Integration

Converts regulatory signals into high-quality ML features.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import sqlite3
import numpy as np

from .regulatory_signals import get_regulatory_signals

logger = logging.getLogger(__name__)


class RegulatoryMLFeatures:
    """
    Converts regulatory signals into ML features.
    
    Regulatory data provides high-confidence, verified signals
    that serve as excellent ML features due to their factual nature.
    """
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self.feature_cache = {}
        self.cache_expiry = {}
        
        logger.info("Regulatory ML Features initialized")
    
    def extract_regulatory_features(self, symbol: str, as_of: str = None) -> Dict[str, float]:
        """
        Extract regulatory-based features for ML.
        
        Args:
            symbol: Stock symbol
            as_of: Date for feature extraction (default: now)
            
        Returns:
            Dictionary of regulatory features
        """
        
        cache_key = f"{symbol}_{as_of or 'current'}"
        
        # Check cache
        if cache_key in self.feature_cache:
            expiry = self.cache_expiry.get(cache_key, datetime.now())
            if datetime.now() < expiry:
                return self.feature_cache[cache_key]
        
        try:
            # Get regulatory signals for this symbol
            signals = get_regulatory_signals([symbol], hours_back=72)  # 3 days back
            
            features = {
                # Insider activity features
                'regulatory_insider_buy_recent': 0.0,
                'regulatory_insider_sell_recent': 0.0,
                'regulatory_insider_net_activity': 0.0,
                'regulatory_insider_magnitude': 0.0,
                
                # Corporate event features
                'regulatory_merger_recent': 0.0,
                'regulatory_exec_change_recent': 0.0,
                'regulatory_bankruptcy_recent': 0.0,
                'regulatory_major_event_recent': 0.0,
                
                # Fundamental features
                'regulatory_earnings_recent': 0.0,
                'regulatory_fundamental_health': 0.5,  # Default neutral
                'regulatory_filing_frequency': 0.0,
                
                # Composite features
                'regulatory_signal_strength': 0.0,
                'regulatory_confidence': 0.0,
                'regulatory_bullish_bias': 0.0,
                'regulatory_bearish_bias': 0.0,
                'regulatory_event_count': 0.0
            }
            
            if not signals:
                # Cache empty features
                self.feature_cache[cache_key] = features
                self.cache_expiry[cache_key] = datetime.now() + timedelta(hours=1)
                return features
            
            # Process signals into features
            bullish_strength = 0.0
            bearish_strength = 0.0
            total_strength = 0.0
            total_confidence = 0.0
            event_count = len(signals)
            
            for signal in signals:
                strength = signal['strength']
                confidence = signal['confidence']
                event_type = signal['event_type']
                direction = signal['direction']
                
                total_strength += strength
                total_confidence += confidence
                
                # Directional bias
                if direction == 'bullish':
                    bullish_strength += strength
                elif direction == 'bearish':
                    bearish_strength += strength
                
                # Specific event features
                if event_type == 'insider_buy':
                    features['regulatory_insider_buy_recent'] = 1.0
                    details = signal.get('details', {})
                    features['regulatory_insider_net_activity'] = details.get('net_shares', 0) / 10000.0  # Normalize
                    features['regulatory_insider_magnitude'] = details.get('magnitude', 0)
                    
                elif event_type == 'insider_sell':
                    features['regulatory_insider_sell_recent'] = 1.0
                    details = signal.get('details', {})
                    features['regulatory_insider_net_activity'] = details.get('net_shares', 0) / 10000.0  # Normalize
                    features['regulatory_insider_magnitude'] = details.get('magnitude', 0)
                    
                elif event_type == 'merger':
                    features['regulatory_merger_recent'] = 1.0
                    features['regulatory_major_event_recent'] = 1.0
                    
                elif event_type == 'exec_change':
                    features['regulatory_exec_change_recent'] = 1.0
                    
                elif event_type == 'bankruptcy':
                    features['regulatory_bankruptcy_recent'] = 1.0
                    features['regulatory_major_event_recent'] = 1.0
                    
                elif event_type == 'earnings':
                    features['regulatory_earnings_recent'] = 1.0
                    details = signal.get('details', {})
                    features['regulatory_fundamental_health'] = details.get('fundamental_health', 0.5)
            
            # Calculate composite features
            if event_count > 0:
                features['regulatory_signal_strength'] = total_strength / event_count
                features['regulatory_confidence'] = total_confidence / event_count
                features['regulatory_event_count'] = event_count
                
                # Directional bias (normalized to [-1, 1])
                net_bias = bullish_strength - bearish_strength
                features['regulatory_bullish_bias'] = max(-1.0, min(1.0, net_bias / total_strength))
                features['regulatory_bearish_bias'] = max(-1.0, min(1.0, -net_bias / total_strength))
                
                # Filing frequency (how active is this company)
                features['regulatory_filing_frequency'] = min(1.0, event_count / 10.0)  # Normalize by expected max
            
            # Cache features
            self.feature_cache[cache_key] = features
            self.cache_expiry[cache_key] = datetime.now() + timedelta(hours=1)
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting regulatory features for {symbol}: {e}")
            return self._get_default_features()
    
    def _get_default_features(self) -> Dict[str, float]:
        """Get default regulatory features when no data available."""
        
        return {
            'regulatory_insider_buy_recent': 0.0,
            'regulatory_insider_sell_recent': 0.0,
            'regulatory_insider_net_activity': 0.0,
            'regulatory_insider_magnitude': 0.0,
            'regulatory_merger_recent': 0.0,
            'regulatory_exec_change_recent': 0.0,
            'regulatory_bankruptcy_recent': 0.0,
            'regulatory_major_event_recent': 0.0,
            'regulatory_earnings_recent': 0.0,
            'regulatory_fundamental_health': 0.5,
            'regulatory_filing_frequency': 0.0,
            'regulatory_signal_strength': 0.0,
            'regulatory_confidence': 0.0,
            'regulatory_bullish_bias': 0.0,
            'regulatory_bearish_bias': 0.0,
            'regulatory_event_count': 0.0
        }
    
    def get_feature_names(self) -> List[str]:
        """Get list of regulatory feature names."""
        
        return list(self._get_default_features().keys())
    
    def clear_cache(self):
        """Clear feature cache."""
        
        self.feature_cache.clear()
        self.cache_expiry.clear()
        logger.info("Regulatory feature cache cleared")


class RegulatoryFeatureTracker:
    """
    Tracks performance of regulatory features over time.
    """
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
        self._init_performance_table()
        
        logger.info("Regulatory Feature Tracker initialized")
    
    def _init_performance_table(self):
        """Initialize performance tracking table."""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS regulatory_feature_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    feature_name TEXT NOT NULL,
                    feature_value REAL,
                    event_type TEXT,
                    signal_direction TEXT,
                    signal_strength REAL,
                    signal_confidence REAL,
                    price_at_event REAL,
                    price_1d REAL,
                    price_3d REAL,
                    price_7d REAL,
                    return_1d REAL,
                    return_3d REAL,
                    return_7d REAL,
                    alpha_1d REAL,
                    alpha_3d REAL,
                    alpha_7d REAL,
                    event_date TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error initializing performance table: {e}")
            raise
    
    def track_feature_performance(self, symbol: str, features: Dict[str, float], 
                            event_date: str, price_at_event: float,
                            future_prices: Dict[str, float]) -> bool:
        """
        Track performance of regulatory features.
        
        Args:
            symbol: Stock symbol
            features: Regulatory features at event time
            event_date: Date of regulatory event
            price_at_event: Price at event time
            future_prices: Future prices (1d, 3d, 7d)
            
        Returns:
            Success of tracking
        """
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Calculate returns
            price_1d = future_prices.get('1d', price_at_event)
            price_3d = future_prices.get('3d', price_at_event)
            price_7d = future_prices.get('7d', price_at_event)
            
            return_1d = (price_1d - price_at_event) / price_at_event
            return_3d = (price_3d - price_at_event) / price_at_event
            return_7d = (price_7d - price_at_event) / price_at_event
            
            # Store each feature's performance
            for feature_name, feature_value in features.items():
                if feature_value == 0.0:
                    continue  # Skip inactive features
                
                # Get event type from feature name
                event_type = 'unknown'
                if 'insider' in feature_name:
                    event_type = 'insider_activity'
                elif 'merger' in feature_name or 'bankruptcy' in feature_name or 'exec_change' in feature_name:
                    event_type = 'corporate_event'
                elif 'earnings' in feature_name or 'fundamental' in feature_name:
                    event_type = 'fundamental'
                
                conn.execute("""
                    INSERT INTO regulatory_feature_performance 
                    (symbol, feature_name, feature_value, event_type, signal_direction,
                     signal_strength, signal_confidence, price_at_event, price_1d, price_3d, price_7d,
                     return_1d, return_3d, return_7d, event_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, feature_name, feature_value, event_type,
                    'unknown',  # Would be determined from signal
                    0.0, 0.0,  # Placeholder
                    price_at_event, price_1d, price_3d, price_7d,
                    return_1d, return_3d, return_7d, event_date
                ))
            
            conn.commit()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error tracking feature performance: {e}")
            return False
    
    def analyze_feature_performance(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Analyze performance of regulatory features.
        
        Args:
            days_back: Days to look back for analysis
            
        Returns:
            Performance analysis results
        """
        
        try:
            conn = sqlite3.connect(self.db_path)
            cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
            
            # Get feature performance
            cursor = conn.execute("""
                SELECT feature_name, event_type, COUNT(*) as sample_count,
                       AVG(return_1d) as avg_return_1d,
                       AVG(return_3d) as avg_return_3d,
                       AVG(return_7d) as avg_return_7d,
                       AVG(signal_strength) as avg_strength,
                       AVG(signal_confidence) as avg_confidence
                FROM regulatory_feature_performance
                WHERE event_date >= ?
                GROUP BY feature_name, event_type
                ORDER BY sample_count DESC, avg_return_3d DESC
            """, (cutoff_date,))
            
            results = {
                'feature_analysis': [],
                'best_performers': [],
                'worst_performers': [],
                'summary': {}
            }
            
            for row in cursor.fetchall():
                feature_analysis = {
                    'feature_name': row[0],
                    'event_type': row[1],
                    'sample_count': row[2],
                    'avg_return_1d': row[3],
                    'avg_return_3d': row[4],
                    'avg_return_7d': row[5],
                    'avg_strength': row[6],
                    'avg_confidence': row[7],
                    'overall_performance': (row[3] + row[4] + row[5]) / 3  # Average across horizons
                }
                results['feature_analysis'].append(feature_analysis)
            
            conn.close()
            
            # Find best and worst performers
            if results['feature_analysis']:
                results['feature_analysis'].sort(key=lambda x: x['overall_performance'], reverse=True)
                results['best_performers'] = results['feature_analysis'][:3]
                results['worst_performers'] = results['feature_analysis'][-3:]
                
                # Summary statistics
                all_performances = [f['overall_performance'] for f in results['feature_analysis']]
                results['summary'] = {
                    'total_features': len(results['feature_analysis']),
                    'avg_performance': np.mean(all_performances),
                    'best_performance': max(all_performances),
                    'worst_performance': min(all_performances),
                    'performance_std': np.std(all_performances)
                }
            
            return results
            
        except Exception as e:
            logger.error(f"Error analyzing feature performance: {e}")
            return {}


# Global instances
regulatory_ml_features = None
regulatory_feature_tracker = None


def get_regulatory_ml_features(db_path: str = "data/alpha.db") -> RegulatoryMLFeatures:
    """Get or create regulatory ML features instance."""
    
    global regulatory_ml_features
    if regulatory_ml_features is None:
        regulatory_ml_features = RegulatoryMLFeatures(db_path)
    return regulatory_ml_features


def get_regulatory_feature_tracker(db_path: str = "data/alpha.db") -> RegulatoryFeatureTracker:
    """Get or create regulatory feature tracker instance."""
    
    global regulatory_feature_tracker
    if regulatory_feature_tracker is None:
        regulatory_feature_tracker = RegulatoryFeatureTracker(db_path)
    return regulatory_feature_tracker


def extract_regulatory_features(symbol: str, as_of: str = None) -> Dict[str, float]:
    """
    Extract regulatory features for a symbol.
    
    Args:
        symbol: Stock symbol
        as_of: Date for feature extraction
        
    Returns:
        Dictionary of regulatory features
    """
    
    try:
        features = get_regulatory_ml_features()
        return features.extract_regulatory_features(symbol, as_of)
    except Exception as e:
        logger.error(f"Error extracting regulatory features: {e}")
        return RegulatoryMLFeatures()._get_default_features()
