"""
ML Training Dataset Builder

Creates clean training dataset from completed trades for ML model training.
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import json
import sqlite3
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dataclass
class TradeTrainingExample:
    """Single training example for ML model."""
    trade_id: str
    symbol: str
    entry_timestamp: datetime
    
    # Compressed features (what we actually feed to model)
    trend_strength: float
    trend_direction: str  # 'bull', 'bear', 'sideways'
    volatility_regime: str  # 'expansion', 'contraction', 'normal'
    position_in_range: float  # 0-1 normalized
    volume_anomaly: float  # z-score
    sector: str
    
    # Cross-asset context
    spy_trend: float  # SPY 5d return
    vix_level: float  # VIX percentile
    macro_context: str  # 'risk_on', 'risk_off', 'neutral'
    
    # Outcomes
    return_5d: float
    is_win: bool
    r_multiple: float
    
    # Raw features snapshot (for debugging)
    raw_features: Dict[str, Any]


class TrainingDatasetBuilder:
    """Build clean training dataset from completed trades."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    def build_training_dataset(
        self,
        start_date: datetime,
        end_date: datetime,
        min_return_period: int = 5
    ) -> List[TradeTrainingExample]:
        """
        Build training dataset from completed trades.
        
        Returns:
            List of training examples with compressed features
        """
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Get completed trades with outcomes
            query = """
            SELECT 
                t.id as trade_id,
                t.symbol,
                t.entry_timestamp,
                t.exit_timestamp,
                t.entry_price,
                t.exit_price,
                t.realized_pnl,
                t.strategy_type,
                fr.*,
                r.volatility_regime,
                r.trend_regime
            FROM trades t
            JOIN feature_rows fr ON t.symbol = fr.symbol AND DATE(t.entry_timestamp) = fr.as_of_date
            LEFT JOIN regime_classifications r ON DATE(t.entry_timestamp) = r.classification_date
            WHERE t.exit_timestamp IS NOT NULL
            AND t.entry_timestamp BETWEEN ? AND ?
            ORDER BY t.entry_timestamp
            """
            
            cursor = conn.execute(query, (start_date, end_date))
            rows = cursor.fetchall()
            
            training_examples = []
            
            for row in rows:
                example = self._convert_row_to_training_example(row, min_return_period)
                if example:
                    training_examples.append(example)
            
            logger.info(f"Built {len(training_examples)} training examples from {len(rows)} trades")
            return training_examples
            
        finally:
            conn.close()
    
    def _convert_row_to_training_example(
        self,
        row: sqlite3.Row,
        min_return_period: int
    ) -> Optional[TradeTrainingExample]:
        """Convert database row to training example with feature compression."""
        
        try:
            # Calculate outcomes
            entry_price = row['entry_price']
            exit_price = row['exit_price']
            return_5d = (exit_price - entry_price) / entry_price
            is_win = return_5d > 0
            r_multiple = return_5d / 0.02  # Assuming 2% risk per trade
            
            # Compress features
            trend_strength = self._calculate_trend_strength(row)
            trend_direction = self._classify_trend_direction(row)
            volatility_regime = row['volatility_regime'] or 'normal'
            position_in_range = row['price_percentile_252d'] or 0.5
            volume_anomaly = row['volume_zscore_20d'] or 0.0
            
            # Get cross-asset context
            spy_trend = self._get_spy_trend(row['entry_timestamp'])
            vix_level = self._get_vix_percentile(row['entry_timestamp'])
            macro_context = self._classify_macro_context(spy_trend, vix_level)
            
            # Build raw features snapshot
            raw_features = {
                'return_1d': row['return_1d'],
                'return_5d': row['return_5d'],
                'return_20d': row['return_20d'],
                'return_63d': row['return_63d'],
                'volatility_20d': row['volatility_20d'],
                'price_percentile_252d': row['price_percentile_252d'],
                'volume_zscore_20d': row['volume_zscore_20d'],
                'sector': row['sector']
            }
            
            return TradeTrainingExample(
                trade_id=row['trade_id'],
                symbol=row['symbol'],
                entry_timestamp=row['entry_timestamp'],
                trend_strength=trend_strength,
                trend_direction=trend_direction,
                volatility_regime=volatility_regime,
                position_in_range=position_in_range,
                volume_anomaly=volume_anomaly,
                sector=row['sector'] or 'UNK',
                spy_trend=spy_trend,
                vix_level=vix_level,
                macro_context=macro_context,
                return_5d=return_5d,
                is_win=is_win,
                r_multiple=r_multiple,
                raw_features=raw_features
            )
            
        except Exception as e:
            logger.error(f"Error converting row to training example: {e}")
            return None
    
    def _calculate_trend_strength(self, row: sqlite3.Row) -> float:
        """Calculate compressed trend strength from multiple returns."""
        
        returns = [
            row['return_5d'] or 0,
            row['return_20d'] or 0,
            row['return_63d'] or 0
        ]
        
        # Use absolute returns and weight recent more heavily
        weights = [0.5, 0.3, 0.2]
        weighted_return = sum(abs(r) * w for r, w in zip(returns, weights))
        
        return min(1.0, weighted_return / 0.1)  # Normalize to 0-1
    
    def _classify_trend_direction(self, row: sqlite3.Row) -> str:
        """Classify trend direction from returns."""
        
        return_5d = row['return_5d'] or 0
        return_20d = row['return_20d'] or 0
        
        if return_5d > 0.02 and return_20d > 0.05:
            return 'bull'
        elif return_5d < -0.02 and return_20d < -0.05:
            return 'bear'
        else:
            return 'sideways'
    
    def _get_spy_trend(self, timestamp: datetime) -> float:
        """Get SPY trend around trade entry."""
        # This would query SPY data for 5-day return
        # For now, return mock data
        return np.random.normal(0, 0.02)  # Replace with actual SPY query
    
    def _get_vix_percentile(self, timestamp: datetime) -> float:
        """Get VIX percentile at trade entry."""
        # This would query VIX data and calculate percentile
        # For now, return mock data
        return np.random.uniform(0.2, 0.8)  # Replace with actual VIX query
    
    def _classify_macro_context(self, spy_trend: float, vix_level: float) -> str:
        """Classify macro context from SPY and VIX."""
        
        if spy_trend > 0.02 and vix_level < 0.5:
            return 'risk_on'
        elif spy_trend < -0.02 and vix_level > 0.7:
            return 'risk_off'
        else:
            return 'neutral'
    
    def save_training_dataset(
        self,
        examples: List[TradeTrainingExample],
        output_path: str
    ):
        """Save training dataset to CSV for model training."""
        
        # Convert to DataFrame
        data = []
        for example in examples:
            data.append({
                'trade_id': example.trade_id,
                'symbol': example.symbol,
                'trend_strength': example.trend_strength,
                'trend_direction': example.trend_direction,
                'volatility_regime': example.volatility_regime,
                'position_in_range': example.position_in_range,
                'volume_anomaly': example.volume_anomaly,
                'sector': example.sector,
                'spy_trend': example.spy_trend,
                'vix_level': example.vix_level,
                'macro_context': example.macro_context,
                'return_5d': example.return_5d,
                'is_win': example.is_win,
                'r_multiple': example.r_multiple
            })
        
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(examples)} training examples to {output_path}")
    
    def create_training_table(self):
        """Create the ML training dataset table if it doesn't exist."""
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_training_dataset (
                    trade_id VARCHAR(36) PRIMARY KEY,
                    symbol VARCHAR(10),
                    entry_timestamp TIMESTAMP,
                    exit_timestamp TIMESTAMP,
                    
                    -- Entry features snapshot (JSON)
                    entry_features_snapshot JSON,
                    
                    -- Outcomes
                    return_5d DECIMAL(8,4),
                    return_20d DECIMAL(8,4),
                    is_win BOOLEAN,
                    r_multiple DECIMAL(8,4),
                    
                    -- Context
                    regime_at_entry VARCHAR(20),
                    sector_at_entry VARCHAR(10),
                    volatility_regime VARCHAR(20),
                    market_volatility DECIMAL(8,4),
                    
                    -- Metadata
                    strategy_type VARCHAR(50),
                    entry_price DECIMAL(10,2),
                    exit_price DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ml_entry_date ON ml_training_dataset(entry_timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ml_symbol ON ml_training_dataset(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ml_outcome ON ml_training_dataset(is_win)")
            
            conn.commit()
            logger.info("ML training dataset table created successfully")
            
        except Exception as e:
            logger.error(f"Error creating ML training table: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def populate_training_table(self, examples: List[TradeTrainingExample]):
        """Populate the ML training table with examples."""
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            for example in examples:
                conn.execute("""
                    INSERT OR REPLACE INTO ml_training_dataset 
                    (trade_id, symbol, entry_timestamp, exit_timestamp,
                     entry_features_snapshot, return_5d, is_win, r_multiple,
                     regime_at_entry, sector_at_entry, volatility_regime,
                     strategy_type, entry_price, exit_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    example.trade_id,
                    example.symbol,
                    example.entry_timestamp,
                    example.entry_timestamp + timedelta(days=5),  # Approximate exit
                    json.dumps(example.raw_features),
                    example.return_5d,
                    example.is_win,
                    example.r_multiple,
                    example.volatility_regime,
                    example.sector,
                    example.volatility_regime,
                    example.raw_features.get('strategy_type', 'unknown'),
                    example.raw_features.get('close', 0),
                    example.raw_features.get('close', 0) * (1 + example.return_5d)
                ))
            
            conn.commit()
            logger.info(f"Populated ML training table with {len(examples)} examples")
            
        except Exception as e:
            logger.error(f"Error populating ML training table: {e}")
            conn.rollback()
        finally:
            conn.close()
