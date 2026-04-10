"""
Confidence Calibration System

Maps raw confidence scores to actual win probabilities based on historical performance.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class CalibrationPoint:
    """Single calibration data point"""
    confidence: float
    actual_outcome: bool  # True for win, False for loss
    timestamp: datetime
    strategy_id: str
    regime: str
    ticker: str


@dataclass
class CalibrationCurve:
    """Calibration curve for a specific context"""
    strategy_id: str
    regime: str
    confidence_bins: List[float]  # Bin edges
    win_rates: List[float]  # Win rate for each bin
    sample_counts: List[int]  # Number of samples in each bin
    last_updated: datetime
    
    def get_calibrated_probability(self, confidence: float) -> float:
        """Get calibrated win probability for a confidence score"""
        if not self.confidence_bins or not self.win_rates:
            return confidence  # Fallback to raw confidence
        
        # Find appropriate bin
        for i, bin_edge in enumerate(self.confidence_bins):
            if confidence <= bin_edge:
                return self.win_rates[min(i, len(self.win_rates) - 1)]
        
        # If confidence exceeds all bins, use last bin
        return self.win_rates[-1]


class ConfidenceCalibrator:
    """Calibrates confidence scores to actual win probabilities"""
    
    def __init__(self, min_samples_per_bin: int = 50, num_bins: int = 10):
        self.min_samples_per_bin = min_samples_per_bin
        self.num_bins = num_bins
        self.calibration_curves: Dict[Tuple[str, str], CalibrationCurve] = {}
        self.global_curve: Optional[CalibrationCurve] = None
        
    def add_outcome(self, confidence: float, actual_outcome: bool, 
                   strategy_id: str, regime: str, ticker: str, 
                   timestamp: Optional[datetime] = None):
        """Add a prediction outcome for calibration"""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
            
        # Store for later calibration
        # In production, this would be persisted to database
        pass
    
    def calibrate_from_outcomes(self, outcomes: List[CalibrationPoint]) -> Dict[Tuple[str, str], CalibrationCurve]:
        """Calibrate confidence scores from historical outcomes"""
        
        # Group outcomes by strategy and regime
        grouped_outcomes = defaultdict(list)
        for outcome in outcomes:
            key = (outcome.strategy_id, outcome.regime)
            grouped_outcomes[key].append(outcome)
        
        # Create calibration curves
        new_curves = {}
        
        for (strategy_id, regime), strategy_outcomes in grouped_outcomes.items():
            if len(strategy_outcomes) < self.min_samples_per_bin:
                logger.warning(f"Insufficient samples for {strategy_id}/{regime}: {len(strategy_outcomes)}")
                continue
            
            curve = self._create_calibration_curve(strategy_outcomes, strategy_id, regime)
            if curve:
                new_curves[(strategy_id, regime)] = curve
        
        # Update calibration curves
        self.calibration_curves.update(new_curves)
        
        # Create global curve
        all_outcomes = [outcome for outcomes in grouped_outcomes.values() for outcome in outcomes]
        if len(all_outcomes) >= self.min_samples_per_bin * 2:
            self.global_curve = self._create_calibration_curve(all_outcomes, "global", "all")
        
        logger.info(f"Created {len(new_curves)} calibration curves")
        return new_curves
    
    def _create_calibration_curve(self, outcomes: List[CalibrationPoint], 
                                 strategy_id: str, regime: str) -> Optional[CalibrationCurve]:
        """Create calibration curve from outcomes"""
        
        if not outcomes:
            return None
        
        # Extract confidences and outcomes
        confidences = [outcome.confidence for outcome in outcomes]
        actual_outcomes = [outcome.actual_outcome for outcome in outcomes]
        
        # Create bins
        confidence_bins = np.linspace(0, 1, self.num_bins + 1)[1:]  # Bin edges
        win_rates = []
        sample_counts = []
        
        # Calculate win rate for each bin
        for i in range(len(confidence_bins)):
            if i == 0:
                # First bin: 0 to first edge
                mask = [conf <= confidence_bins[i] for conf in confidences]
            else:
                # Subsequent bins: previous edge to current edge
                mask = [(confidence_bins[i-1] < conf <= confidence_bins[i]) for conf in confidences]
            
            bin_outcomes = [actual_outcomes[j] for j, m in enumerate(mask) if m]
            
            if len(bin_outcomes) >= self.min_samples_per_bin // 2:  # Allow smaller bins
                win_rate = sum(bin_outcomes) / len(bin_outcomes)
            else:
                # Use neighboring bins or fallback
                win_rate = 0.5  # Default to 50%
            
            win_rates.append(win_rate)
            sample_counts.append(len(bin_outcomes))
        
        # Smooth the curve to avoid overfitting
        win_rates = self._smooth_curve(win_rates)
        
        return CalibrationCurve(
            strategy_id=strategy_id,
            regime=regime,
            confidence_bins=confidence_bins.tolist(),
            win_rates=win_rates,
            sample_counts=sample_counts,
            last_updated=datetime.now(timezone.utc)
        )
    
    def _smooth_curve(self, win_rates: List[float], smoothing_factor: float = 0.3) -> List[float]:
        """Apply smoothing to calibration curve"""
        if len(win_rates) <= 2:
            return win_rates
        
        smoothed = [win_rates[0]]  # Keep first point
        
        for i in range(1, len(win_rates) - 1):
            # Weighted average with neighbors
            smoothed_val = (
                smoothing_factor * win_rates[i] +
                (1 - smoothing_factor) * 0.5 * (win_rates[i-1] + win_rates[i+1])
            )
            smoothed.append(smoothed_val)
        
        smoothed.append(win_rates[-1])  # Keep last point
        return smoothed
    
    def get_calibrated_confidence(self, confidence: float, strategy_id: str, 
                                regime: str) -> float:
        """Get calibrated confidence score"""
        
        # Try strategy-specific calibration
        key = (strategy_id, regime)
        if key in self.calibration_curves:
            return self.calibration_curves[key].get_calibrated_probability(confidence)
        
        # Try regime-specific calibration
        regime_curves = [curve for (s, r), curve in self.calibration_curves.items() if r == regime]
        if regime_curves:
            # Average across all strategies in this regime
            calibrated_probs = [curve.get_calibrated_probability(confidence) for curve in regime_curves]
            return np.mean(calibrated_probs)
        
        # Try strategy-specific across all regimes
        strategy_curves = [curve for (s, r), curve in self.calibration_curves.items() if s == strategy_id]
        if strategy_curves:
            calibrated_probs = [curve.get_calibrated_probability(confidence) for curve in strategy_curves]
            return np.mean(calibrated_probs)
        
        # Fallback to global curve
        if self.global_curve:
            return self.global_curve.get_calibrated_probability(confidence)
        
        # Final fallback - apply basic calibration
        return self._basic_calibration(confidence)
    
    def _basic_calibration(self, confidence: float) -> float:
        """Basic calibration function when no historical data available"""
        # Apply sigmoid-like transformation to reduce overconfidence
        # This pushes extreme values toward the middle
        calibrated = 0.5 + 0.4 * np.tanh(2 * (confidence - 0.5))
        return max(0.1, min(0.9, calibrated))  # Clamp to reasonable range
    
    def get_calibration_stats(self) -> Dict[str, Any]:
        """Get calibration statistics"""
        stats = {
            'total_curves': len(self.calibration_curves),
            'global_curve_available': self.global_curve is not None,
            'curves_by_strategy': defaultdict(int),
            'curves_by_regime': defaultdict(int)
        }
        
        for (strategy_id, regime) in self.calibration_curves.keys():
            stats['curves_by_strategy'][strategy_id] += 1
            stats['curves_by_regime'][regime] += 1
        
        return dict(stats)
    
    def should_recalibrate(self, strategy_id: str, regime: str, 
                          max_age_days: int = 7) -> bool:
        """Check if calibration curve needs updating"""
        key = (strategy_id, regime)
        if key not in self.calibration_curves:
            return True
        
        curve = self.calibration_curves[key]
        age = datetime.now(timezone.utc) - curve.last_updated
        return age.days > max_age_days


class CalibrationIntegrator:
    """Integrates calibration with the existing prediction pipeline"""
    
    def __init__(self, calibrator: ConfidenceCalibrator):
        self.calibrator = calibrator
        self.outcome_buffer: List[CalibrationPoint] = []
        self.buffer_size = 1000
        
    def calibrate_prediction(self, prediction: Dict[str, Any]) -> Dict[str, Any]:
        """Calibrate a single prediction"""
        
        raw_confidence = prediction.get('confidence', 0.5)
        strategy_id = prediction.get('strategy_id', 'unknown')
        regime = prediction.get('regime', 'UNKNOWN')
        
        calibrated_confidence = self.calibrator.get_calibrated_confidence(
            raw_confidence, strategy_id, regime
        )
        
        # Create calibrated prediction
        calibrated_prediction = prediction.copy()
        calibrated_prediction.update({
            'raw_confidence': raw_confidence,
            'confidence': calibrated_confidence,
            'calibration_applied': True
        })
        
        return calibrated_prediction
    
    def record_outcome(self, prediction: Dict[str, Any], actual_outcome: bool):
        """Record prediction outcome for future calibration"""
        
        outcome_point = CalibrationPoint(
            confidence=prediction.get('raw_confidence', prediction.get('confidence', 0.5)),
            actual_outcome=actual_outcome,
            timestamp=datetime.now(timezone.utc),
            strategy_id=prediction.get('strategy_id', 'unknown'),
            regime=prediction.get('regime', 'UNKNOWN'),
            ticker=prediction.get('ticker', 'unknown')
        )
        
        self.outcome_buffer.append(outcome_point)
        
        # Calibrate when buffer is full
        if len(self.outcome_buffer) >= self.buffer_size:
            self.calibrator.calibrate_from_outcomes(self.outcome_buffer)
            self.outcome_buffer = []
    
    def force_calibration(self):
        """Force calibration with current buffer"""
        if self.outcome_buffer:
            self.calibrator.calibrate_from_outcomes(self.outcome_buffer)
            self.outcome_buffer = []
            return True
        return False
