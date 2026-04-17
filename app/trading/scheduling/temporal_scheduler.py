"""
Temporal Scheduling Framework

Base classes and interfaces for hierarchical trade scheduling system.
Provides foundation for year/quarter/month/week/daily cascading decisions.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

from app.trading.scheduling.temporal_correlation_integration import (
    TemporalCorrelationAnalyzer, 
    InsightsEngine
)

logger = logging.getLogger(__name__)


class FlexibilityMode(Enum):
    """Flexibility modes for scheduling decisions"""
    STRICT = "strict"
    ADAPTIVE = "adaptive"
    OPPORTUNISTIC = "opportunistic"
    CONSERVATIVE = "conservative"


class StrategicMode(Enum):
    """Strategic modes for yearly planning"""
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"
    OPPORTUNISTIC = "opportunistic"
    DEFENSIVE = "defensive"
    CAUTIOUS = "cautious"


class ControlMode(Enum):
    """Control modes for monthly scheduling"""
    FULL_AUTOMATIC = "full_automatic"
    SEMI_AUTOMATIC = "semi_automatic"
    MANUAL_APPROVAL = "manual_approval"
    MONITOR_ONLY = "monitor_only"


class ExecutionMode(Enum):
    """Execution modes for daily scheduling"""
    IMMEDIATE = "immediate"
    STAGGERED = "staggered"
    OPTIMAL_TIMING = "optimal_timing"
    MARKET_ADAPTIVE = "market_adaptive"


@dataclass
class TemporalInsights:
    """Temporal insights for a specific time period"""
    period_type: str  # yearly, quarterly, monthly, weekly, daily
    date: datetime
    sentiment_score: float = 0.0
    economic_events: List[Dict] = field(default_factory=list)
    volatility_regime: str = "normal"
    seasonal_multiplier: float = 1.0
    sector_rotation: Dict[str, float] = field(default_factory=dict)
    historical_performance: Dict[str, float] = field(default_factory=dict)
    confidence_level: float = 0.5
    recommendations: List[str] = field(default_factory=list)


@dataclass
class SchedulingDecision:
    """Base scheduling decision with temporal adjustments"""
    base_decision: Dict[str, Any]
    temporal_adjustments: Dict[str, Any]
    final_decision: Dict[str, Any]
    flexibility_mode: FlexibilityMode
    confidence: float
    reasoning: List[str]
    timestamp: datetime = field(default_factory=datetime.now)
    manual_overrides: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ManualOverride:
    """Manual override record for audit trail"""
    timestamp: datetime
    level: str  # yearly, quarterly, monthly, weekly, daily
    override_data: Dict[str, Any]
    reason: str
    approved_by: str
    impact_assessment: Dict[str, Any]
    previous_state: Dict[str, Any]


class TemporalScheduler(ABC):
    """Base class for all temporal scheduling components"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.temporal_analyzer = TemporalCorrelationAnalyzer()
        self.insights_engine = InsightsEngine(self.temporal_analyzer)
        self.flexibility_mode = FlexibilityMode(config.get('flexibility_mode', 'adaptive'))
        self.manual_overrides: Dict[str, ManualOverride] = {}
        self.decision_history: List[SchedulingDecision] = []
        
    @abstractmethod
    def create_schedule(self, *args, **kwargs) -> Dict[str, Any]:
        """Create schedule for the specific time period"""
        pass
    
    def get_temporal_insights(self, time_period: str, date: datetime) -> TemporalInsights:
        """Get temporal insights for specific time period"""
        try:
            insights_data = self.insights_engine.get_period_insights(time_period, date)
            return TemporalInsights(
                period_type=time_period,
                date=date,
                sentiment_score=insights_data.get('sentiment_score', 0.0),
                economic_events=insights_data.get('economic_events', []),
                volatility_regime=insights_data.get('volatility_regime', 'normal'),
                seasonal_multiplier=insights_data.get('seasonal_multiplier', 1.0),
                sector_rotation=insights_data.get('sector_rotation', {}),
                historical_performance=insights_data.get('historical_performance', {}),
                confidence_level=insights_data.get('confidence_level', 0.5),
                recommendations=insights_data.get('recommendations', [])
            )
        except Exception as e:
            logger.error(f"Error getting temporal insights for {time_period}: {e}")
            return TemporalInsights(period_type=time_period, date=date)
    
    def apply_flexibility_mode(self, base_decision: Dict, temporal_insights: TemporalInsights) -> SchedulingDecision:
        """Apply flexibility mode to scheduling decisions"""
        
        adjustments = {}
        reasoning = []
        
        if self.flexibility_mode == FlexibilityMode.STRICT:
            adjustments, reasoning = self.apply_strict_mode(base_decision, temporal_insights)
        elif self.flexibility_mode == FlexibilityMode.ADAPTIVE:
            adjustments, reasoning = self.apply_adaptive_mode(base_decision, temporal_insights)
        elif self.flexibility_mode == FlexibilityMode.OPPORTUNISTIC:
            adjustments, reasoning = self.apply_opportunistic_mode(base_decision, temporal_insights)
        elif self.flexibility_mode == FlexibilityMode.CONSERVATIVE:
            adjustments, reasoning = self.apply_conservative_mode(base_decision, temporal_insights)
        
        # Apply manual overrides if any
        final_decision = self.apply_manual_overrides(base_decision, adjustments)
        
        decision = SchedulingDecision(
            base_decision=base_decision,
            temporal_adjustments=adjustments,
            final_decision=final_decision,
            flexibility_mode=self.flexibility_mode,
            confidence=temporal_insights.confidence_level,
            reasoning=reasoning,
            manual_overrides=self.manual_overrides
        )
        
        self.decision_history.append(decision)
        return decision
    
    def apply_strict_mode(self, base_decision: Dict, insights: TemporalInsights) -> Tuple[Dict, List[str]]:
        """Apply strict mode - minimal adjustments"""
        adjustments = {}
        reasoning = ["Strict mode applied - minimal temporal adjustments"]
        
        # Only apply critical risk adjustments
        if insights.volatility_regime == "high":
            adjustments['risk_multiplier'] = 0.7
            reasoning.append("High volatility regime - reduced risk exposure")
        
        return adjustments, reasoning
    
    def apply_adaptive_mode(self, base_decision: Dict, insights: TemporalInsights) -> Tuple[Dict, List[str]]:
        """Apply adaptive mode - balanced adjustments"""
        adjustments = {}
        reasoning = ["Adaptive mode applied - balanced temporal adjustments"]
        
        # Sentiment-based adjustments
        if insights.sentiment_score > 0.7:
            adjustments['sentiment_multiplier'] = 1.2
            reasoning.append(f"Positive sentiment ({insights.sentiment_score:.2f}) - increased exposure")
        elif insights.sentiment_score < -0.3:
            adjustments['sentiment_multiplier'] = 0.8
            reasoning.append(f"Negative sentiment ({insights.sentiment_score:.2f}) - reduced exposure")
        
        # Seasonal adjustments
        if abs(insights.seasonal_multiplier - 1.0) > 0.1:
            adjustments['seasonal_multiplier'] = insights.seasonal_multiplier
            reasoning.append(f"Seasonal pattern ({insights.seasonal_multiplier:.2f}) - adjusted allocation")
        
        # Volatility regime adjustments
        if insights.volatility_regime == "high":
            adjustments['risk_multiplier'] = 0.8
            reasoning.append("High volatility regime - moderate risk reduction")
        elif insights.volatility_regime == "low":
            adjustments['risk_multiplier'] = 1.1
            reasoning.append("Low volatility regime - slight risk increase")
        
        return adjustments, reasoning
    
    def apply_opportunistic_mode(self, base_decision: Dict, insights: TemporalInsights) -> Tuple[Dict, List[str]]:
        """Apply opportunistic mode - aggressive adjustments"""
        adjustments = {}
        reasoning = ["Opportunistic mode applied - aggressive temporal adjustments"]
        
        # Amplify positive signals
        if insights.sentiment_score > 0.5:
            adjustments['sentiment_multiplier'] = 1.5
            reasoning.append(f"Positive sentiment ({insights.sentiment_score:.2f}) - aggressive increase")
        
        # Exploit seasonal strength
        if insights.seasonal_multiplier > 1.1:
            adjustments['seasonal_multiplier'] = insights.seasonal_multiplier * 1.2
            reasoning.append(f"Strong seasonal pattern - opportunistic scaling")
        
        # Economic event opportunities
        high_impact_events = [e for e in insights.economic_events if e.get('impact') == 'high']
        if high_impact_events:
            adjustments['event_multiplier'] = 1.1
            reasoning.append(f"{len(high_impact_events)} high-impact events - opportunistic positioning")
        
        # Volatility opportunities
        if insights.volatility_regime == "expansion":
            adjustments['volatility_multiplier'] = 1.3
            reasoning.append("Volatility expansion - opportunistic breakout positioning")
        
        return adjustments, reasoning
    
    def apply_conservative_mode(self, base_decision: Dict, insights: TemporalInsights) -> Tuple[Dict, List[str]]:
        """Apply conservative mode - risk-focused adjustments"""
        adjustments = {}
        reasoning = ["Conservative mode applied - risk-focused adjustments"]
        
        # Reduce exposure in uncertain conditions
        if insights.confidence_level < 0.6:
            adjustments['confidence_multiplier'] = 0.7
            reasoning.append(f"Low confidence ({insights.confidence_level:.2f}) - conservative sizing")
        
        # Volatility protection
        if insights.volatility_regime in ["high", "expansion"]:
            adjustments['risk_multiplier'] = 0.6
            reasoning.append(f"Volatility regime ({insights.volatility_regime}) - strong risk reduction")
        
        # Economic event caution
        high_impact_events = [e for e in insights.economic_events if e.get('impact') == 'high']
        if high_impact_events:
            adjustments['event_multiplier'] = 0.8
            reasoning.append(f"{len(high_impact_events)} high-impact events - conservative positioning")
        
        # Sentiment caution
        if insights.sentiment_score < -0.2:
            adjustments['sentiment_multiplier'] = 0.7
            reasoning.append(f"Negative sentiment ({insights.sentiment_score:.2f}) - conservative reduction")
        
        return adjustments, reasoning
    
    def apply_manual_overrides(self, base_decision: Dict, adjustments: Dict) -> Dict[str, Any]:
        """Apply manual overrides to decisions"""
        final_decision = base_decision.copy()
        
        # Apply temporal adjustments
        for key, value in adjustments.items():
            final_decision[f"temporal_{key}"] = value
        
        # Apply manual overrides
        for override_key, override in self.manual_overrides.items():
            if override.level == self.__class__.__name__.lower().replace('scheduler', ''):
                final_decision.update(override.override_data)
        
        return final_decision
    
    def record_manual_override(self, override: ManualOverride):
        """Record manual override for audit trail"""
        override_id = f"{override.level}_{override.timestamp.isoformat()}"
        self.manual_overrides[override_id] = override
        
        logger.info(
            f"Manual override recorded: {override.level} - {override.reason} "
            f"by {override.approved_by}"
        )
    
    def get_decision_history(self, limit: int = 10) -> List[SchedulingDecision]:
        """Get recent decision history"""
        return self.decision_history[-limit:]
    
    def get_override_history(self, level: Optional[str] = None) -> List[ManualOverride]:
        """Get manual override history"""
        overrides = list(self.manual_overrides.values())
        if level:
            overrides = [o for o in overrides if o.level == level]
        return sorted(overrides, key=lambda x: x.timestamp, reverse=True)


class SchedulingMetrics:
    """Metrics tracking for scheduling performance"""
    
    def __init__(self):
        self.metrics = {
            'decisions_made': 0,
            'manual_overrides': 0,
            'temporal_adjustments_applied': 0,
            'flexibility_mode_switches': 0,
            'performance_vs_plan': {},
            'temporal_accuracy': {},
            'override_effectiveness': {}
        }
        self.start_time = datetime.now()
    
    def record_decision(self, decision: SchedulingDecision):
        """Record a scheduling decision"""
        self.metrics['decisions_made'] += 1
        
        if decision.manual_overrides:
            self.metrics['manual_overrides'] += len(decision.manual_overrides)
        
        if decision.temporal_adjustments:
            self.metrics['temporal_adjustments_applied'] += 1
    
    def record_mode_switch(self, old_mode: str, new_mode: str, reason: str):
        """Record flexibility mode switch"""
        self.metrics['flexibility_mode_switches'] += 1
        logger.info(f"Flexibility mode switched: {old_mode} -> {new_mode} - {reason}")
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        runtime = datetime.now() - self.start_time
        
        return {
            **self.metrics,
            'runtime_hours': runtime.total_seconds() / 3600,
            'decisions_per_hour': self.metrics['decisions_made'] / max(runtime.total_seconds() / 3600, 1),
            'override_rate': self.metrics['manual_overrides'] / max(self.metrics['decisions_made'], 1),
            'temporal_adjustment_rate': self.metrics['temporal_adjustments_applied'] / max(self.metrics['decisions_made'], 1)
        }
