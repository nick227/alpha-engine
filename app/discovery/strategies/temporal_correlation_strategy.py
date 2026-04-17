"""
Temporal Correlation Strategy

A time-based strategy that leverages temporal correlation analysis insights
to optimize entry timing, position sizing, and risk management based on:
- Market sentiment conditions
- Economic event timing
- Volatility regime adaptation
- Seasonal patterns
- Sector rotation signals
- Historical performance periods

This strategy flows from the discovery system and uses correlation insights
to enhance the bear expansion base strategy with temporal optimizations.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
import pandas as pd
from dataclasses import dataclass
from enum import Enum

from app.discovery.strategies.base_strategy import BaseStrategy, Signal, SignalType
from app.core.regime_v3 import RegimeClassifierV3, TrendRegime, VolatilityRegime
from scripts.analysis.temporal_correlation_analyzer import TemporalCorrelationAnalyzer
from scripts.analysis.insights_engine import InsightsEngine


class TemporalSignalType(Enum):
    """Temporal signal types for time-based strategy."""
    SENTIMENT_TIMING = "sentiment_timing"
    ECONOMIC_EVENT_TIMING = "economic_event_timing"
    VOLATILITY_REGIME = "volatility_regime"
    SEASONAL_PATTERN = "seasonal_pattern"
    SECTOR_ROTATION = "sector_rotation"
    HISTORICAL_PERIOD = "historical_period"


@dataclass
class TemporalSignal:
    """Temporal signal with timing and confidence information."""
    signal_type: TemporalSignalType
    strength: float  # 0-1
    confidence: float  # 0-1
    direction: str  # "increase", "decrease", "neutral"
    timeframe: str  # "immediate", "intraday", "daily", "weekly"
    rationale: str
    supporting_data: Dict[str, Any]
    expiry: datetime  # When signal expires


@dataclass
class MarketConditions:
    """Current market conditions for temporal analysis."""
    sentiment_score: float  # -1 to 1
    vix_level: float
    vix_percentile: float
    economic_events: List[Dict[str, Any]]
    sector_performance: Dict[str, float]
    day_of_week: int
    month: int
    quarter: int
    is_preferred_historical_period: bool


class TemporalCorrelationStrategy(BaseStrategy):
    """
    Time-based strategy that uses temporal correlation insights to optimize
    the bear expansion strategy with timing and position sizing adjustments.
    """
    
    def __init__(self, 
                 base_strategy: BaseStrategy,
                 correlation_analyzer: TemporalCorrelationAnalyzer = None,
                 insights_engine: InsightsEngine = None,
                 config: Dict[str, Any] = None):
        
        super().__init__("temporal_correlation_strategy")
        
        self.base_strategy = base_strategy
        self.correlation_analyzer = correlation_analyzer
        self.insights_engine = insights_engine
        
        # Configuration
        self.config = config or self._get_default_config()
        
        # Temporal signals storage
        self.active_signals: List[TemporalSignal] = []
        self.signal_history: List[TemporalSignal] = []
        
        # Market conditions cache
        self.market_conditions: Optional[MarketConditions] = None
        self.last_conditions_update: Optional[datetime] = None
        
        # Performance tracking
        self.temporal_adjustments: List[Dict[str, Any]] = []
        
        # Initialize correlation analysis if not provided
        if self.correlation_analyzer is None:
            self._initialize_correlation_analysis()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration for temporal strategy."""
        
        return {
            # Signal thresholds
            "sentiment_threshold": 0.6,
            "volatility_threshold": 0.7,
            "economic_event_threshold": 0.5,
            "seasonal_threshold": 0.4,
            
            # Position sizing adjustments
            "max_position_multiplier": 2.0,
            "min_position_multiplier": 0.3,
            "volatility_adjustment_factor": 0.5,
            
            # Timing windows
            "signal_expiry_hours": {
                "sentiment": 24,
                "economic": 48,
                "volatility": 72,
                "seasonal": 168,  # 1 week
                "sector": 24
            },
            
            # Risk management
            "max_concurrent_signals": 5,
            "confidence_threshold": 0.6,
            "conflict_resolution": "highest_confidence"
        }
    
    def _initialize_correlation_analysis(self):
        """Initialize correlation analysis with historical data."""
        
        try:
            # This would typically load pre-computed correlation analysis
            # For now, we'll create a placeholder
            print("Initializing correlation analysis...")
            # In production, load from saved analysis results
        except Exception as e:
            print(f"Warning: Could not initialize correlation analysis: {e}")
    
    def analyze_market_conditions(self, current_data: Dict[str, Any]) -> MarketConditions:
        """Analyze current market conditions for temporal signals."""
        
        now = datetime.now()
        
        # Check if we need to update conditions (cache for 1 hour)
        if (self.market_conditions is None or 
            self.last_conditions_update is None or
            (now - self.last_conditions_update).total_seconds() > 3600):
            
            # Get sentiment score (would come from sentiment analysis)
            sentiment_score = self._calculate_sentiment_score(current_data)
            
            # Get VIX data
            vix_level = current_data.get('vix', 20.0)
            vix_percentile = self._calculate_vix_percentile(vix_level)
            
            # Get economic events
            economic_events = self._get_today_economic_events()
            
            # Get sector performance
            sector_performance = self._get_sector_performance(current_data)
            
            # Time-based factors
            day_of_week = now.weekday()  # 0=Monday, 6=Sunday
            month = now.month
            quarter = (month - 1) // 3 + 1
            
            # Check if we're in a historically good period
            is_preferred_period = self._check_historical_period_preference(now)
            
            self.market_conditions = MarketConditions(
                sentiment_score=sentiment_score,
                vix_level=vix_level,
                vix_percentile=vix_percentile,
                economic_events=economic_events,
                sector_performance=sector_performance,
                day_of_week=day_of_week,
                month=month,
                quarter=quarter,
                is_preferred_historical_period=is_preferred_period
            )
            
            self.last_conditions_update = now
        
        return self.market_conditions
    
    def _calculate_sentiment_score(self, current_data: Dict[str, Any]) -> float:
        """Calculate market sentiment score from -1 to 1."""
        
        # In production, this would use real sentiment analysis
        # For now, use placeholder logic
        
        # Sample sentiment factors
        news_sentiment = current_data.get('news_sentiment', 0.0)
        market_momentum = current_data.get('market_momentum', 0.0)
        
        # Combine factors
        sentiment_score = (news_sentiment * 0.6 + market_momentum * 0.4)
        
        # Clamp to -1 to 1
        return max(-1.0, min(1.0, sentiment_score))
    
    def _calculate_vix_percentile(self, vix_level: float) -> float:
        """Calculate VIX percentile based on historical data."""
        
        # In production, use historical VIX distribution
        # For now, use approximation
        
        # Historical VIX typically ranges 10-80
        # Map to percentile (simplified)
        if vix_level <= 15:
            return 0.1  # 10th percentile
        elif vix_level <= 20:
            return 0.3  # 30th percentile
        elif vix_level <= 25:
            return 0.6  # 60th percentile
        elif vix_level <= 35:
            return 0.8  # 80th percentile
        else:
            return 0.95  # 95th percentile
    
    def _get_today_economic_events(self) -> List[Dict[str, Any]]:
        """Get today's economic events."""
        
        # In production, fetch from economic calendar API
        # For now, return sample events
        
        today = datetime.now().date()
        
        sample_events = [
            {
                'date': today,
                'time': '10:00',
                'event': 'FOMC Meeting Minutes',
                'category': 'monetary_policy',
                'impact': 'high',
                'forecast': None,
                'actual': None
            },
            {
                'date': today,
                'time': '08:30',
                'event': 'CPI Data Release',
                'category': 'inflation',
                'impact': 'high',
                'forecast': 3.4,
                'actual': 3.5
            }
        ]
        
        return sample_events
    
    def _get_sector_performance(self, current_data: Dict[str, Any]) -> Dict[str, float]:
        """Get current sector performance data."""
        
        # In production, fetch real sector ETF data
        # For now, return sample data
        
        return {
            'Technology': 0.02,
            'Financials': -0.01,
            'Healthcare': 0.005,
            'Energy': 0.03,
            'Consumer': -0.005,
            'Industrial': 0.01,
            'Materials': 0.015,
            'Utilities': -0.002,
            'Real Estate': 0.008,
            'Communications': 0.012
        }
    
    def _check_historical_period_preference(self, current_date: datetime) -> bool:
        """Check if current date is in a historically preferred period."""
        
        # In production, use actual historical analysis
        # For now, use sample seasonal preferences
        
        # Sample: Q4 (Oct-Dec) tends to be strong
        month = current_date.month
        quarter = (month - 1) // 3 + 1
        
        # Prefer Q4 and avoid Q2 (Apr-Jun)
        if quarter == 4:
            return True
        elif quarter == 2:
            return False
        else:
            return True  # Neutral for Q1, Q3
    
    def generate_temporal_signals(self, market_conditions: MarketConditions) -> List[TemporalSignal]:
        """Generate temporal signals based on market conditions."""
        
        signals = []
        now = datetime.now()
        
        # 1. Sentiment Timing Signal
        sentiment_signal = self._generate_sentiment_signal(market_conditions, now)
        if sentiment_signal:
            signals.append(sentiment_signal)
        
        # 2. Economic Event Timing Signal
        economic_signal = self._generate_economic_event_signal(market_conditions, now)
        if economic_signal:
            signals.append(economic_signal)
        
        # 3. Volatility Regime Signal
        volatility_signal = self._generate_volatility_signal(market_conditions, now)
        if volatility_signal:
            signals.append(volatility_signal)
        
        # 4. Seasonal Pattern Signal
        seasonal_signal = self._generate_seasonal_signal(market_conditions, now)
        if seasonal_signal:
            signals.append(seasonal_signal)
        
        # 5. Sector Rotation Signal
        sector_signal = self._generate_sector_signal(market_conditions, now)
        if sector_signal:
            signals.append(sector_signal)
        
        # 6. Historical Period Signal
        historical_signal = self._generate_historical_signal(market_conditions, now)
        if historical_signal:
            signals.append(historical_signal)
        
        # Filter by confidence threshold
        filtered_signals = [
            signal for signal in signals 
            if signal.confidence >= self.config["confidence_threshold"]
        ]
        
        # Limit number of concurrent signals
        if len(filtered_signals) > self.config["max_concurrent_signals"]:
            # Sort by confidence and take top N
            filtered_signals.sort(key=lambda x: x.confidence, reverse=True)
            filtered_signals = filtered_signals[:self.config["max_concurrent_signals"]]
        
        return filtered_signals
    
    def _generate_sentiment_signal(self, conditions: MarketConditions, now: datetime) -> Optional[TemporalSignal]:
        """Generate sentiment-based timing signal."""
        
        sentiment_score = conditions.sentiment_score
        threshold = self.config["sentiment_threshold"]
        
        if abs(sentiment_score) < threshold:
            return None
        
        # Determine signal strength and direction
        strength = abs(sentiment_score)
        direction = "increase" if sentiment_score > 0 else "decrease"
        confidence = min(strength * 1.2, 1.0)  # Scale confidence
        
        # Calculate expiry
        expiry_hours = self.config["signal_expiry_hours"]["sentiment"]
        expiry = now + timedelta(hours=expiry_hours)
        
        rationale = f"Market sentiment is {'positive' if sentiment_score > 0 else 'negative'} ({sentiment_score:.2f})"
        
        return TemporalSignal(
            signal_type=TemporalSignalType.SENTIMENT_TIMING,
            strength=strength,
            confidence=confidence,
            direction=direction,
            timeframe="daily",
            rationale=rationale,
            supporting_data={
                "sentiment_score": sentiment_score,
                "threshold": threshold
            },
            expiry=expiry
        )
    
    def _generate_economic_event_signal(self, conditions: MarketConditions, now: datetime) -> Optional[TemporalSignal]:
        """Generate economic event timing signal."""
        
        high_impact_events = [
            event for event in conditions.economic_events 
            if event.get('impact') == 'high'
        ]
        
        if not high_impact_events:
            return None
        
        # Check for surprises (actual vs forecast)
        surprises = []
        for event in high_impact_events:
            if event.get('forecast') and event.get('actual'):
                surprise = abs(event['actual'] - event['forecast']) / event['forecast']
                surprises.append((event, surprise))
        
        if not surprises:
            return None
        
        # Get biggest surprise
        biggest_surprise = max(surprises, key=lambda x: x[1])
        event, surprise_magnitude = biggest_surprise
        
        # Determine signal direction based on event type and surprise
        event_category = event.get('category', '')
        
        # Simplified logic for direction
        if event_category == 'inflation':
            direction = "decrease" if surprise_magnitude > 0.02 else "neutral"
        elif event_category == 'monetary_policy':
            direction = "decrease" if surprise_magnitude > 0.01 else "neutral"
        else:
            direction = "neutral"
        
        if direction == "neutral":
            return None
        
        strength = min(surprise_magnitude * 5, 1.0)  # Scale to 0-1
        confidence = 0.8  # High confidence for high-impact events
        
        expiry_hours = self.config["signal_expiry_hours"]["economic"]
        expiry = now + timedelta(hours=expiry_hours)
        
        rationale = f"High-impact {event_category} event with {surprise_magnitude:.1%} surprise"
        
        return TemporalSignal(
            signal_type=TemporalSignalType.ECONOMIC_EVENT_TIMING,
            strength=strength,
            confidence=confidence,
            direction=direction,
            timeframe="intraday",
            rationale=rationale,
            supporting_data={
                "event": event,
                "surprise_magnitude": surprise_magnitude
            },
            expiry=expiry
        )
    
    def _generate_volatility_signal(self, conditions: MarketConditions, now: datetime) -> Optional[TemporalSignal]:
        """Generate volatility regime signal."""
        
        vix_percentile = conditions.vix_percentile
        threshold = self.config["volatility_threshold"]
        
        if abs(vix_percentile - 0.5) < threshold:
            return None
        
        # Determine direction based on VIX level
        if vix_percentile > 0.8:  # High volatility
            direction = "decrease"
            strength = (vix_percentile - 0.5) * 2
        elif vix_percentile < 0.2:  # Low volatility
            direction = "increase"
            strength = (0.5 - vix_percentile) * 2
        else:
            return None
        
        strength = min(strength, 1.0)
        confidence = 0.7  # Moderate confidence for volatility signals
        
        expiry_hours = self.config["signal_expiry_hours"]["volatility"]
        expiry = now + timedelta(hours=expiry_hours)
        
        rationale = f"VIX at {vix_percentile:.1%} percentile ({'high' if vix_percentile > 0.5 else 'low'} volatility)"
        
        return TemporalSignal(
            signal_type=TemporalSignalType.VOLATILITY_REGIME,
            strength=strength,
            confidence=confidence,
            direction=direction,
            timeframe="daily",
            rationale=rationale,
            supporting_data={
                "vix_percentile": vix_percentile,
                "vix_level": conditions.vix_level
            },
            expiry=expiry
        )
    
    def _generate_seasonal_signal(self, conditions: MarketConditions, now: datetime) -> Optional[TemporalSignal]:
        """Generate seasonal pattern signal."""
        
        threshold = self.config["seasonal_threshold"]
        
        # Check for strong seasonal patterns
        seasonal_factors = {}
        
        # Month-based patterns (simplified)
        if conditions.month in [11, 12]:  # Nov, Dec - historically strong
            seasonal_factors["month"] = 0.7
        elif conditions.month in [4, 5]:  # Apr, May - historically weak
            seasonal_factors["month"] = -0.5
        else:
            seasonal_factors["month"] = 0.0
        
        # Quarter-based patterns
        if conditions.quarter == 4:  # Q4 - strong
            seasonal_factors["quarter"] = 0.6
        elif conditions.quarter == 2:  # Q2 - weak
            seasonal_factors["quarter"] = -0.4
        else:
            seasonal_factors["quarter"] = 0.0
        
        # Day of week patterns (simplified)
        if conditions.day_of_week in [0, 4]:  # Monday, Friday - often weaker
            seasonal_factors["day_of_week"] = -0.3
        elif conditions.day_of_week in [1, 2, 3]:  # Tue-Thu - often stronger
            seasonal_factors["day_of_week"] = 0.2
        else:
            seasonal_factors["day_of_week"] = 0.0
        
        # Combine seasonal factors
        total_seasonal_score = sum(seasonal_factors.values()) / len(seasonal_factors)
        
        if abs(total_seasonal_score) < threshold:
            return None
        
        direction = "increase" if total_seasonal_score > 0 else "decrease"
        strength = abs(total_seasonal_score)
        confidence = 0.5  # Lower confidence for seasonal signals
        
        expiry_hours = self.config["signal_expiry_hours"]["seasonal"]
        expiry = now + timedelta(hours=expiry_hours)
        
        rationale = f"Seasonal pattern: {conditions.month}/{conditions.quarter} with score {total_seasonal_score:.2f}"
        
        return TemporalSignal(
            signal_type=TemporalSignalType.SEASONAL_PATTERN,
            strength=strength,
            confidence=confidence,
            direction=direction,
            timeframe="weekly",
            rationale=rationale,
            supporting_data={
                "seasonal_factors": seasonal_factors,
                "total_score": total_seasonal_score
            },
            expiry=expiry
        )
    
    def _generate_sector_signal(self, conditions: MarketConditions, now: datetime) -> Optional[TemporalSignal]:
        """Generate sector rotation signal."""
        
        # Find best and worst performing sectors
        sector_performance = conditions.sector_performance
        
        if not sector_performance:
            return None
        
        # Get top performing sector
        best_sector = max(sector_performance.items(), key=lambda x: x[1])
        worst_sector = min(sector_performance.items(), key=lambda x: x[1])
        
        # Check if there's a significant divergence
        performance_spread = best_sector[1] - worst_sector[1]
        
        if performance_spread < 0.02:  # Less than 2% spread
            return None
        
        # In production, this would use correlation analysis to determine
        # if the best sector is positively correlated with our strategy
        
        # For now, assume technology sector correlation
        if best_sector[0] == 'Technology' and best_sector[1] > 0.01:
            direction = "increase"
            strength = min(best_sector[1] * 5, 1.0)
            confidence = 0.6
        elif worst_sector[0] == 'Technology' and worst_sector[1] < -0.01:
            direction = "decrease"
            strength = min(abs(worst_sector[1]) * 5, 1.0)
            confidence = 0.6
        else:
            return None
        
        expiry_hours = self.config["signal_expiry_hours"]["sector"]
        expiry = now + timedelta(hours=expiry_hours)
        
        rationale = f"Sector rotation: {best_sector[0]} (+{best_sector[1]:.1%}) vs {worst_sector[0]} ({worst_sector[1]:.1%})"
        
        return TemporalSignal(
            signal_type=TemporalSignalType.SECTOR_ROTATION,
            strength=strength,
            confidence=confidence,
            direction=direction,
            timeframe="daily",
            rationale=rationale,
            supporting_data={
                "best_sector": best_sector,
                "worst_sector": worst_sector,
                "performance_spread": performance_spread
            },
            expiry=expiry
        )
    
    def _generate_historical_signal(self, conditions: MarketConditions, now: datetime) -> Optional[TemporalSignal]:
        """Generate historical period preference signal."""
        
        if not conditions.is_preferred_historical_period:
            return None
        
        # This would use actual historical analysis
        # For now, assume we're in a good period
        
        direction = "increase"
        strength = 0.6  # Moderate strength
        confidence = 0.5  # Lower confidence for historical patterns
        
        expiry_hours = self.config["signal_expiry_hours"]["seasonal"]  # Use seasonal expiry
        expiry = now + timedelta(hours=expiry_hours)
        
        rationale = f"Historical period preference: Q{conditions.quarter} month {conditions.month}"
        
        return TemporalSignal(
            signal_type=TemporalSignalType.HISTORICAL_PERIOD,
            strength=strength,
            confidence=confidence,
            direction=direction,
            timeframe="weekly",
            rationale=rationale,
            supporting_data={
                "quarter": conditions.quarter,
                "month": conditions.month,
                "is_preferred": conditions.is_preferred_historical_period
            },
            expiry=expiry
        )
    
    def calculate_position_multiplier(self, signals: List[TemporalSignal]) -> float:
        """Calculate position size multiplier based on temporal signals."""
        
        if not signals:
            return 1.0
        
        # Start with base multiplier
        multiplier = 1.0
        
        # Process each signal
        for signal in signals:
            signal_weight = signal.confidence * signal.strength
            
            if signal.direction == "increase":
                multiplier += signal_weight * 0.5  # Max +0.5 per signal
            elif signal.direction == "decrease":
                multiplier -= signal_weight * 0.7  # Max -0.7 per signal (stronger risk reduction)
        
        # Apply bounds
        max_multiplier = self.config["max_position_multiplier"]
        min_multiplier = self.config["min_position_multiplier"]
        
        multiplier = max(min_multiplier, min(max_multiplier, multiplier))
        
        return multiplier
    
    def adjust_base_signal(self, base_signal: Signal, temporal_signals: List[TemporalSignal]) -> Signal:
        """Adjust base strategy signal based on temporal insights."""
        
        if not temporal_signals:
            return base_signal
        
        # Calculate position multiplier
        position_multiplier = self.calculate_position_multiplier(temporal_signals)
        
        # Adjust signal strength
        adjusted_strength = base_signal.strength * position_multiplier
        
        # Clamp to valid range
        adjusted_strength = max(0.0, min(1.0, adjusted_strength))
        
        # Create adjusted signal
        adjusted_signal = Signal(
            symbol=base_signal.symbol,
            signal_type=base_signal.signal_type,
            strength=adjusted_strength,
            confidence=base_signal.confidence,
            timestamp=base_signal.timestamp,
            metadata={
                **base_signal.metadata,
                "temporal_adjustments": {
                    "position_multiplier": position_multiplier,
                    "temporal_signals": [
                        {
                            "type": signal.signal_type.value,
                            "direction": signal.direction,
                            "strength": signal.strength,
                            "confidence": signal.confidence,
                            "rationale": signal.rationale
                        }
                        for signal in temporal_signals
                    ]
                }
            }
        )
        
        # Track adjustment
        self.temporal_adjustments.append({
            "timestamp": datetime.now(),
            "base_signal_strength": base_signal.strength,
            "adjusted_signal_strength": adjusted_strength,
            "position_multiplier": position_multiplier,
            "temporal_signals_count": len(temporal_signals),
            "temporal_signals": [signal.rationale for signal in temporal_signals]
        })
        
        return adjusted_signal
    
    def analyze(self, market_data: Dict[str, Any]) -> List[Signal]:
        """
        Main analysis method that combines base strategy with temporal insights.
        """
        
        # Get base strategy signals
        base_signals = self.base_strategy.analyze(market_data)
        
        if not base_signals:
            return []
        
        # Analyze current market conditions
        market_conditions = self.analyze_market_conditions(market_data)
        
        # Generate temporal signals
        temporal_signals = self.generate_temporal_signals(market_conditions)
        
        # Update active signals (remove expired ones)
        now = datetime.now()
        self.active_signals = [
            signal for signal in self.active_signals 
            if signal.expiry > now
        ]
        
        # Add new signals
        self.active_signals.extend(temporal_signals)
        
        # Adjust base signals with temporal insights
        adjusted_signals = []
        for base_signal in base_signals:
            adjusted_signal = self.adjust_base_signal(base_signal, self.active_signals)
            
            # Only include signals that still have meaningful strength
            if adjusted_signal.strength > 0.1:  # Minimum threshold
                adjusted_signals.append(adjusted_signal)
        
        # Store signal history
        self.signal_history.extend(temporal_signals)
        
        return adjusted_signals
    
    def get_temporal_insights(self) -> Dict[str, Any]:
        """Get current temporal insights for reporting."""
        
        if not self.active_signals:
            return {
                "active_signals": [],
                "position_multiplier": 1.0,
                "market_conditions": None,
                "insights": "No active temporal signals"
            }
        
        # Calculate current position multiplier
        position_multiplier = self.calculate_position_multiplier(self.active_signals)
        
        # Categorize signals
        signal_summary = {}
        for signal in self.active_signals:
            signal_type = signal.signal_type.value
            if signal_type not in signal_summary:
                signal_summary[signal_type] = {
                    "count": 0,
                    "avg_confidence": 0.0,
                    "avg_strength": 0.0,
                    "directions": []
                }
            
            signal_summary[signal_type]["count"] += 1
            signal_summary[signal_type]["avg_confidence"] += signal.confidence
            signal_summary[signal_type]["avg_strength"] += signal.strength
            signal_summary[signal_type]["directions"].append(signal.direction)
        
        # Calculate averages
        for signal_type, summary in signal_summary.items():
            count = summary["count"]
            summary["avg_confidence"] /= count
            summary["avg_strength"] /= count
            summary["dominant_direction"] = max(set(summary["directions"]), key=summary["directions"].count)
        
        # Generate insights summary
        insights = []
        if position_multiplier > 1.2:
            insights.append("Strong temporal support for increased position sizing")
        elif position_multiplier < 0.8:
            insights.append("Temporal signals suggest reducing exposure")
        elif len(self.active_signals) >= 3:
            insights.append("Multiple temporal factors aligning")
        else:
            insights.append("Moderate temporal influence")
        
        return {
            "active_signals": [
                {
                    "type": signal.signal_type.value,
                    "direction": signal.direction,
                    "strength": signal.strength,
                    "confidence": signal.confidence,
                    "rationale": signal.rationale,
                    "expires_in": (signal.expiry - datetime.now()).total_seconds() / 3600  # hours
                }
                for signal in self.active_signals
            ],
            "signal_summary": signal_summary,
            "position_multiplier": position_multiplier,
            "market_conditions": self.market_conditions,
            "insights": insights,
            "last_update": datetime.now().isoformat()
        }
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get performance report for temporal adjustments."""
        
        if not self.temporal_adjustments:
            return {
                "total_adjustments": 0,
                "avg_position_multiplier": 1.0,
                "insights": "No temporal adjustments made yet"
            }
        
        # Calculate statistics
        total_adjustments = len(self.temporal_adjustments)
        avg_multiplier = np.mean([adj["position_multiplier"] for adj in self.temporal_adjustments])
        
        # Analyze adjustment effectiveness
        increase_adjustments = [adj for adj in self.temporal_adjustments if adj["position_multiplier"] > 1.0]
        decrease_adjustments = [adj for adj in self.temporal_adjustments if adj["position_multiplier"] < 1.0]
        
        # Most common signal types
        signal_types = {}
        for adj in self.temporal_adjustments:
            for signal in adj["temporal_signals"]:
                signal_types[signal] = signal_types.get(signal, 0) + 1
        
        most_common_signals = sorted(signal_types.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            "total_adjustments": total_adjustments,
            "avg_position_multiplier": avg_multiplier,
            "increase_adjustments": len(increase_adjustments),
            "decrease_adjustments": len(decrease_adjustments),
            "most_common_signals": most_common_signals,
            "recent_adjustments": self.temporal_adjustments[-10:],  # Last 10 adjustments
            "insights": f"Made {total_adjustments} temporal adjustments with avg multiplier of {avg_multiplier:.2f}"
        }


# Factory function for discovery system integration
def create_temporal_correlation_strategy(base_strategy: BaseStrategy, 
                                       config: Dict[str, Any] = None) -> TemporalCorrelationStrategy:
    """
    Factory function to create temporal correlation strategy for discovery system.
    """
    
    return TemporalCorrelationStrategy(
        base_strategy=base_strategy,
        config=config
    )


# Example usage
if __name__ == "__main__":
    # This would be integrated into the discovery system
    print("Temporal Correlation Strategy - Example Usage")
    print("=" * 50)
    
    # Create a sample base strategy (would be bear_expansion_strategy in production)
    from app.discovery.strategies.bear_expansion_strategy import BearExpansionStrategy
    
    base_strategy = BearExpansionStrategy()
    
    # Create temporal correlation strategy
    temporal_strategy = create_temporal_correlation_strategy(base_strategy)
    
    # Sample market data
    sample_market_data = {
        'vix': 22.5,
        'news_sentiment': 0.3,
        'market_momentum': 0.1,
        'symbol_data': {
            'AAPL': {'close': 150.0, 'volume': 1000000},
            'MSFT': {'close': 250.0, 'volume': 800000}
        }
    }
    
    # Analyze with temporal insights
    signals = temporal_strategy.analyze(sample_market_data)
    
    print(f"Generated {len(signals)} temporal-adjusted signals")
    
    # Get temporal insights
    insights = temporal_strategy.get_temporal_insights()
    print(f"Position multiplier: {insights['position_multiplier']:.2f}")
    print(f"Active signals: {len(insights['active_signals'])}")
    
    for signal in insights['active_signals']:
        print(f"  - {signal['type']}: {signal['direction']} (confidence: {signal['confidence']:.2f})")
    
    print(f"\nInsights: {insights['insights'][0]}")
