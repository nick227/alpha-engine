"""
Predictive State Feature Engineering

Provides comprehensive feature set for strategy consumption.
Separates features from outcomes to prevent look-ahead bias.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
import pandas as pd

from app.core.types import RawEvent


class FeatureEngine:
    """
    Comprehensive feature engineering for Alpha Engine strategies.
    
    Core predictive features:
    - Multi-timeframe returns
    - Volatility metrics (realized + rolling)
    - Trend strength (ADX-style)
    - Regime classification
    - Volume anomalies
    - Cross-asset signals
    - Momentum windows
    - Gap detection
    - Mean reversion distance
    """
    
    def __init__(self):
        self.cross_asset_cache = {}
        
    def build_feature_set(
        self,
        ticker_bars: pd.DataFrame,
        event_ts: datetime,
        cross_asset_data: Optional[Dict[str, pd.DataFrame]] = None,
        lookback_days: int = 30
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Build comprehensive feature set with strict separation from outcomes.
        
        Returns:
            Tuple of (features, outcomes) where outcomes are for evaluation only
        """
        if ticker_bars.empty:
            return {}, {}
            
        # Ensure proper time handling
        bars = self._prepare_bars(ticker_bars)
        event_idx = self._find_event_index(bars, event_ts)
        
        if event_idx is None:
            return {}, {}
            
        # Extract features (strictly backward-looking)
        features = self._extract_features(bars, event_idx, cross_asset_data, lookback_days)
        
        # Extract outcomes (strictly forward-looking, for evaluation only)
        outcomes = self._extract_outcomes(bars, event_idx)
        
        return features, outcomes
    
    def _prepare_bars(self, bars: pd.DataFrame) -> pd.DataFrame:
        """Prepare and validate bars DataFrame."""
        bars = bars.copy()
        
        # Ensure timestamp is datetime UTC
        if not pd.api.types.is_datetime64_any_dtype(bars['timestamp']):
            bars['timestamp'] = pd.to_datetime(bars['timestamp'], utc=True)
        elif bars['timestamp'].dt.tz is None:
            bars['timestamp'] = bars['timestamp'].dt.tz_localize('UTC')
        else:
            bars['timestamp'] = bars['timestamp'].dt.tz_convert('UTC')
        
        # Sort by timestamp
        bars = bars.sort_values('timestamp').reset_index(drop=True)
        
        # Ensure numeric columns
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in bars.columns:
                bars[col] = pd.to_numeric(bars[col], errors='coerce')
        
        return bars
    
    def _find_event_index(self, bars: pd.DataFrame, event_ts: datetime) -> Optional[int]:
        """Find the last bar at or before event time."""
        event_ts = pd.Timestamp(event_ts).tz_convert('UTC')
        idx = bars['timestamp'].searchsorted(event_ts, side='right') - 1
        return idx if 0 <= idx < len(bars) else None
    
    def _extract_features(
        self,
        bars: pd.DataFrame,
        event_idx: int,
        cross_asset_data: Optional[Dict[str, pd.DataFrame]],
        lookback_days: int
    ) -> Dict[str, Any]:
        """Extract all predictive features."""
        features = {}
        
        # Basic price context
        current_bar = bars.iloc[event_idx]
        features['entry_price'] = float(current_bar['close'])
        features['entry_volume'] = float(current_bar.get('volume', 0))
        
        # 1. Multi-timeframe returns (backward-looking only)
        features.update(self._compute_returns(bars, event_idx))
        
        # 2. Volatility metrics
        features.update(self._compute_volatility_features(bars, event_idx))
        
        # 3. Trend strength (ADX-style)
        features.update(self._compute_trend_strength(bars, event_idx))
        
        # 4. Regime classification
        features.update(self._compute_regime_features(bars, event_idx))
        
        # 5. Volume analysis
        features.update(self._compute_volume_features(bars, event_idx))
        
        # 6. Cross-asset signals
        if cross_asset_data:
            features.update(self._compute_cross_asset_features(
                bars.iloc[event_idx]['timestamp'], cross_asset_data
            ))
        
        # 7. Momentum windows
        features.update(self._compute_momentum_features(bars, event_idx))
        
        # 8. Gap detection
        features.update(self._compute_gap_features(bars, event_idx))
        
        # 9. Mean reversion distance
        features.update(self._compute_mean_reversion_features(bars, event_idx))
        
        # 10. Market microstructure
        features.update(self._compute_microstructure_features(bars, event_idx))
        
        return features
    
    def _compute_returns(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, float]:
        """Compute multi-timeframe returns."""
        returns = {}
        current_price = float(bars.iloc[event_idx]['close'])
        
        # Define timeframes in minutes
        timeframes = {
            '1m': 1, '5m': 5, '15m': 15, '1h': 60, '4h': 240,
            '1d': 1440, '7d': 10080, '30d': 43200
        }
        
        for name, minutes in timeframes.items():
            past_idx = event_idx - minutes
            if past_idx >= 0:
                past_price = float(bars.iloc[past_idx]['close'])
                returns[f'return_{name}'] = (current_price - past_price) / past_price
            else:
                returns[f'return_{name}'] = 0.0
        
        return returns
    
    def _compute_volatility_features(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, Any]:
        """Compute comprehensive volatility features."""
        vol_features = {}
        
        # Lookback windows
        windows = [5, 10, 20, 50]
        
        for window in windows:
            if event_idx >= window:
                window_bars = bars.iloc[event_idx - window + 1:event_idx + 1]
                
                # Realized volatility (std of returns)
                returns = window_bars['close'].pct_change().dropna()
                if len(returns) > 1:
                    realized_vol = float(returns.std(ddof=0))
                    vol_features[f'realized_vol_{window}'] = realized_vol
                    
                    # Volatility percentiles
                    vol_features[f'vol_pctile_{window}'] = float(
                        (realized_vol - returns.mean()) / returns.std() if returns.std() > 0 else 0
                    )
                else:
                    vol_features[f'realized_vol_{window}'] = 0.0
                    vol_features[f'vol_pctile_{window}'] = 0.0
                
                # Parkinson volatility (high-low based)
                hl_vol = np.sqrt(0.361 * np.mean(
                    np.log(window_bars['high'] / window_bars['low']) ** 2
                ))
                vol_features[f'parkinson_vol_{window}'] = float(hl_vol)
                
                # Garman-Klass volatility
                gk_vol = np.sqrt(0.5 * np.mean(
                    np.log(window_bars['high'] / window_bars['low']) ** 2
                ) - (2 * np.log(2) - 1) * np.mean(
                    np.log(window_bars['close'] / window_bars['open']) ** 2
                ))
                vol_features[f'gk_vol_{window}'] = float(gk_vol)
        
        # Volatility regime
        if 'realized_vol_20' in vol_features:
            vol_features['vol_regime'] = self._classify_volatility_regime(
                vol_features['realized_vol_20']
            )
        
        return vol_features
    
    def _compute_trend_strength(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, float]:
        """Compute ADX-style trend strength features."""
        trend_features = {}
        
        if event_idx < 14:
            return trend_features
        
        # ADX calculation components
        window = 14
        lookback = bars.iloc[event_idx - window + 1:event_idx + 1]
        
        # True Range
        high_low = lookback['high'] - lookback['low']
        high_close_prev = np.abs(lookback['high'] - lookback['close'].shift(1))
        low_close_prev = np.abs(lookback['low'] - lookback['close'].shift(1))
        
        tr = np.maximum(high_low, np.maximum(high_close_prev, low_close_prev))
        atr = tr.rolling(window=window).mean().iloc[-1]
        
        # Directional Movement
        up_move = lookback['high'] - lookback['high'].shift(1)
        down_move = lookback['low'].shift(1) - lookback['low']
        
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
        
        plus_di = 100 * (pd.Series(plus_dm).rolling(window=window).mean() / atr)
        minus_di = 100 * (pd.Series(minus_dm).rolling(window=window).mean() / atr)
        
        # ADX
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = pd.Series(dx).rolling(window=window).mean().iloc[-1]
        
        trend_features['adx_14'] = float(adx)
        trend_features['plus_di_14'] = float(plus_di.iloc[-1]) if not plus_di.empty else 0.0
        trend_features['minus_di_14'] = float(minus_di.iloc[-1]) if not minus_di.empty else 0.0
        
        # Trend classification
        trend_features['trend_strength'] = self._classify_trend_strength(adx)
        trend_features['trend_direction'] = self._classify_trend_direction(
            plus_di.iloc[-1] if not plus_di.empty else 0,
            minus_di.iloc[-1] if not minus_di.empty else 0
        )
        
        return trend_features
    
    def _compute_regime_features(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, Any]:
        """Compute regime classification features."""
        regime_features = {}
        
        if event_idx < 20:
            return regime_features
        
        lookback = bars.iloc[event_idx - 20:event_idx + 1]
        current_price = float(bars.iloc[event_idx]['close'])
        
        # Price relative to moving averages
        for period in [5, 10, 20]:
            ma = lookback['close'].rolling(window=period).mean().iloc[-1]
            regime_features[f'price_above_ma_{period}'] = current_price > ma
            regime_features[f'price_ma_ratio_{period}'] = current_price / ma if ma > 0 else 1.0
        
        # Moving average crossovers
        ma_5 = lookback['close'].rolling(window=5).mean()
        ma_20 = lookback['close'].rolling(window=20).mean()
        
        if len(ma_5) >= 2 and len(ma_20) >= 2:
            current_cross = ma_5.iloc[-1] > ma_20.iloc[-1]
            prev_cross = ma_5.iloc[-2] > ma_20.iloc[-2]
            regime_features['ma_crossover_signal'] = current_cross and not prev_cross
            regime_features['ma_crossunder_signal'] = not current_cross and prev_cross
        
        # Volatility regime
        returns = lookback['close'].pct_change().dropna()
        if len(returns) > 1:
            vol = float(returns.std())
            regime_features['volatility_regime'] = self._classify_volatility_regime(vol)
        
        # Trend regime
        if 'adx_14' in regime_features:
            regime_features['trend_regime'] = self._classify_trend_regime(
                regime_features['adx_14'], regime_features.get('trend_direction', 'neutral')
            )
        
        return regime_features
    
    def _compute_volume_features(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, Any]:
        """Compute volume-based features."""
        volume_features = {}
        
        if event_idx < 20:
            return volume_features
        
        lookback = bars.iloc[event_idx - 20:event_idx + 1]
        current_volume = float(bars.iloc[event_idx].get('volume', 0))
        
        # Volume ratios
        avg_volumes = [5, 10, 20]
        for period in avg_volumes:
            avg_vol = lookback['volume'].rolling(window=period).mean().iloc[-1]
            volume_features[f'volume_ratio_{period}'] = current_volume / avg_vol if avg_vol > 0 else 1.0
        
        # Volume trend
        volume_features['volume_trend_5'] = self._compute_trend(
            lookback['volume'].tail(5).values
        )
        
        # Volume anomaly detection
        recent_volumes = lookback['volume'].tail(10)
        volume_features['volume_anomaly'] = self._detect_volume_anomaly(
            current_volume, recent_volumes
        )
        
        # Price-Volume divergence
        price_change = bars.iloc[event_idx]['close'] - bars.iloc[event_idx-1]['close']
        volume_change = current_volume - float(bars.iloc[event_idx-1].get('volume', 0))
        
        volume_features['price_volume_divergence'] = self._compute_divergence(
            price_change, volume_change
        )
        
        return volume_features
    
    def _compute_cross_asset_features(
        self,
        event_time: pd.Timestamp,
        cross_asset_data: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """Compute cross-asset correlation features."""
        cross_features = {}
        
        # Key cross-asset symbols
        assets = ['VIX', 'DXY', 'BTC', 'OIL']
        
        for asset in assets:
            if asset in cross_asset_data:
                asset_bars = cross_asset_data[asset]
                asset_features = self._extract_asset_features(asset_bars, event_time)
                cross_features.update({f'{asset.lower()}_{k}': v 
                                     for k, v in asset_features.items()})
        
        # Cross-asset regime
        cross_features['cross_asset_regime'] = self._classify_cross_asset_regime(
            cross_features
        )
        
        return cross_features
    
    def _compute_momentum_features(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, float]:
        """Compute momentum-based features."""
        momentum_features = {}
        
        # Various momentum windows
        windows = [3, 5, 10, 20]
        
        for window in windows:
            if event_idx >= window:
                window_bars = bars.iloc[event_idx - window:event_idx + 1]
                
                # Price momentum
                start_price = float(window_bars.iloc[0]['close'])
                end_price = float(window_bars.iloc[-1]['close'])
                momentum_features[f'momentum_{window}'] = (end_price - start_price) / start_price
                
                # Momentum acceleration
                if window >= 5:
                    mid_point = window // 2
                    mid_price = float(window_bars.iloc[mid_point]['close'])
                    first_momentum = (mid_price - start_price) / start_price
                    second_momentum = (end_price - mid_price) / mid_price
                    momentum_features[f'momentum_accel_{window}'] = second_momentum - first_momentum
        
        # RSI momentum
        if event_idx >= 14:
            rsi = self._compute_rsi(bars.iloc[event_idx - 13:event_idx + 1]['close'])
            momentum_features['rsi_momentum'] = rsi
        
        # Momentum rank
        current_price = float(bars.iloc[event_idx]['close'])
        if event_idx >= 50:
            lookback_prices = bars.iloc[event_idx - 50:event_idx + 1]['close']
            momentum_features['momentum_rank_50'] = self._compute_rank(
                current_price, lookback_prices
            )
        
        return momentum_features
    
    def _compute_gap_features(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, Any]:
        """Compute gap detection features."""
        gap_features = {}
        
        if event_idx == 0:
            return gap_features
        
        current_bar = bars.iloc[event_idx]
        prev_bar = bars.iloc[event_idx - 1]
        
        # Gap up/down
        gap_up = current_bar['low'] > prev_bar['high']
        gap_down = current_bar['high'] < prev_bar['low']
        
        gap_features['gap_up'] = gap_up
        gap_features['gap_down'] = gap_down
        
        # Gap size
        if gap_up:
            gap_features['gap_size'] = (current_bar['low'] - prev_bar['high']) / prev_bar['high']
        elif gap_down:
            gap_features['gap_size'] = (prev_bar['low'] - current_bar['high']) / prev_bar['low']
        else:
            gap_features['gap_size'] = 0.0
        
        # Gap fill probability (based on historical fills)
        if event_idx >= 20:
            gap_features['gap_fill_probability'] = self._estimate_gap_fill_probability(
                bars, event_idx
            )
        
        return gap_features
    
    def _compute_mean_reversion_features(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, float]:
        """Compute mean reversion distance features."""
        reversion_features = {}
        
        if event_idx < 20:
            return reversion_features
        
        lookback = bars.iloc[event_idx - 20:event_idx + 1]
        current_price = float(bars.iloc[event_idx]['close'])
        
        # Distance from various means
        means = {
            'sma_5': lookback['close'].rolling(window=5).mean().iloc[-1],
            'sma_10': lookback['close'].rolling(window=10).mean().iloc[-1],
            'sma_20': lookback['close'].rolling(window=20).mean().iloc[-1],
            'ema_12': lookback['close'].ewm(span=12).mean().iloc[-1],
            'ema_26': lookback['close'].ewm(span=26).mean().iloc[-1]
        }
        
        for name, mean_val in means.items():
            if mean_val > 0:
                reversion_features[f'distance_from_{name}'] = (current_price - mean_val) / mean_val
        
        # Bollinger Band distance
        if len(lookback) >= 20:
            bb_period = 20
            bb_std = 2
            sma = lookback['close'].rolling(window=bb_period).mean().iloc[-1]
            std = lookback['close'].rolling(window=bb_period).std().iloc[-1]
            
            upper_band = sma + (bb_std * std)
            lower_band = sma - (bb_std * std)
            
            reversion_features['bb_position'] = (current_price - lower_band) / (upper_band - lower_band)
            reversion_features['bb_width'] = (upper_band - lower_band) / sma if sma > 0 else 0
        
        # Mean reversion score
        reversion_features['mean_reversion_score'] = self._compute_mean_reversion_score(
            reversion_features
        )
        
        return reversion_features
    
    def _compute_microstructure_features(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, float]:
        """Compute market microstructure features."""
        micro_features = {}
        
        if event_idx == 0:
            return micro_features
        
        current_bar = bars.iloc[event_idx]
        prev_bar = bars.iloc[event_idx - 1]
        
        # Intraday patterns
        micro_features['intraday_return'] = (
            current_bar['close'] - current_bar['open']
        ) / current_bar['open'] if current_bar['open'] > 0 else 0
        
        # Overnight gap
        micro_features['overnight_gap'] = (
            current_bar['open'] - prev_bar['close']
        ) / prev_bar['close'] if prev_bar['close'] > 0 else 0
        
        # Range expansion
        current_range = current_bar['high'] - current_bar['low']
        prev_range = prev_bar['high'] - prev_bar['low']
        micro_features['range_expansion'] = (
            current_range / prev_range if prev_range > 0 else 1.0
        )
        
        # Close position in range
        if current_range > 0:
            micro_features['close_position'] = (
                (current_bar['close'] - current_bar['low']) / current_range
            )
        else:
            micro_features['close_position'] = 0.5
        
        return micro_features
    
    def _extract_outcomes(self, bars: pd.DataFrame, event_idx: int) -> Dict[str, Any]:
        """Extract forward-looking outcomes for evaluation only."""
        outcomes = {}
        current_price = float(bars.iloc[event_idx]['close'])
        
        # Future returns for various horizons
        horizons = [1, 5, 15, 60, 240, 1440]  # minutes
        
        for minutes in horizons:
            future_idx = event_idx + minutes
            if future_idx < len(bars):
                future_price = float(bars.iloc[future_idx]['close'])
                future_return = (future_price - current_price) / current_price
                outcomes[f'future_return_{minutes}m'] = future_return
            else:
                outcomes[f'future_return_{minutes}m'] = 0.0
        
        # Max runup and drawdown in next 15 minutes
        if event_idx + 15 < len(bars):
            future_window = bars.iloc[event_idx + 1:event_idx + 16]
            max_high = future_window['high'].max()
            min_low = future_window['low'].min()
            
            outcomes['max_runup_15m'] = (max_high - current_price) / current_price
            outcomes['max_drawdown_15m'] = (min_low - current_price) / current_price
        else:
            outcomes['max_runup_15m'] = 0.0
            outcomes['max_drawdown_15m'] = 0.0
        
        return outcomes
    
    # Helper methods
    def _classify_volatility_regime(self, vol: float) -> str:
        """Classify volatility regime."""
        if vol < 0.01:
            return "LOW"
        elif vol < 0.03:
            return "NORMAL"
        else:
            return "HIGH"
    
    def _classify_trend_strength(self, adx: float) -> str:
        """Classify trend strength based on ADX."""
        if adx < 20:
            return "WEAK"
        elif adx < 40:
            return "MODERATE"
        else:
            return "STRONG"
    
    def _classify_trend_direction(self, plus_di: float, minus_di: float) -> str:
        """Classify trend direction."""
        if plus_di > minus_di:
            return "BULLISH"
        elif minus_di > plus_di:
            return "BEARISH"
        else:
            return "NEUTRAL"
    
    def _classify_trend_regime(self, adx: float, direction: str) -> str:
        """Classify overall trend regime."""
        strength = self._classify_trend_strength(adx)
        return f"{strength}_{direction}"
    
    def _compute_rsi(self, prices: pd.Series, period: int = 14) -> float:
        """Compute RSI."""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi.iloc[-1]) if not rsi.empty else 50.0
    
    def _compute_trend(self, values: np.ndarray) -> float:
        """Compute linear trend slope."""
        if len(values) < 2:
            return 0.0
        x = np.arange(len(values))
        return float(np.polyfit(x, values, 1)[0])
    
    def _detect_volume_anomaly(self, current_volume: float, recent_volumes: pd.Series) -> bool:
        """Detect volume anomalies using z-score."""
        if len(recent_volumes) < 5:
            return False
        
        mean_vol = recent_volumes.mean()
        std_vol = recent_volumes.std()
        
        if std_vol == 0:
            return False
        
        z_score = abs(current_volume - mean_vol) / std_vol
        return z_score > 2.0
    
    def _compute_divergence(self, price_change: float, volume_change: float) -> float:
        """Compute price-volume divergence."""
        if volume_change == 0:
            return 0.0
        
        # Negative divergence: price up, volume down (or vice versa)
        divergence = -price_change * volume_change
        return float(divergence)
    
    def _compute_rank(self, value: float, series: pd.Series) -> float:
        """Compute percentile rank of value in series."""
        return float((series < value).sum() / len(series))
    
    def _extract_asset_features(self, asset_bars: pd.DataFrame, event_time: pd.Timestamp) -> Dict[str, float]:
        """Extract features from cross-asset data."""
        features = {}
        
        if asset_bars.empty:
            return features
        
        # Find nearest bar
        asset_bars = asset_bars.sort_values('timestamp')
        idx = asset_bars['timestamp'].searchsorted(event_time, side='right') - 1
        
        if 0 <= idx < len(asset_bars):
            bar = asset_bars.iloc[idx]
            features['price'] = float(bar['close'])
            features['volume'] = float(bar.get('volume', 0))
            
            # Recent returns
            if idx >= 5:
                past_price = float(asset_bars.iloc[idx - 5]['close'])
                features['return_5'] = (features['price'] - past_price) / past_price
        
        return features
    
    def _classify_cross_asset_regime(self, cross_features: Dict[str, Any]) -> str:
        """Classify cross-asset regime."""
        vix = cross_features.get('vix_price', 20)
        dxy = cross_features.get('dxy_price', 100)
        
        if vix > 30:
            return "RISK_OFF"
        elif vix < 15 and dxy > 102:
            return "RISK_ON"
        else:
            return "NEUTRAL"
    
    def _estimate_gap_fill_probability(self, bars: pd.DataFrame, event_idx: int) -> float:
        """Estimate probability of gap fill based on historical patterns."""
        # Simplified: look at similar gaps in recent history
        if event_idx < 50:
            return 0.5
        
        lookback = 50
        filled_count = 0
        total_count = 0
        
        for i in range(event_idx - lookback, event_idx):
            if i > 0:
                current = bars.iloc[i]
                prev = bars.iloc[i - 1]
                
                # Check for gap
                gap_up = current['low'] > prev['high']
                gap_down = current['high'] < prev['low']
                
                if gap_up or gap_down:
                    total_count += 1
                    # Check if gap filled in next 10 bars
                    if i + 10 < len(bars):
                        future_window = bars.iloc[i + 1:i + 11]
                        if gap_up and future_window['low'].min() <= prev['high']:
                            filled_count += 1
                        elif gap_down and future_window['high'].max() >= prev['low']:
                            filled_count += 1
        
        return filled_count / total_count if total_count > 0 else 0.5
    
    def _compute_mean_reversion_score(self, reversion_features: Dict[str, float]) -> float:
        """Compute overall mean reversion score."""
        # Combine various mean reversion signals
        score = 0.0
        count = 0
        
        for key, value in reversion_features.items():
            if 'distance_from_' in key:
                score += abs(value)
                count += 1
            elif key == 'bb_position':
                score += abs(value - 0.5) * 2
                count += 1
        
        return score / count if count > 0 else 0.0
