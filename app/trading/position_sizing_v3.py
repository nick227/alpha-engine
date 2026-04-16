"""
Enhanced Position Sizing (v3)

Implements quality-score based position sizing with regime awareness.
Uses the Q² formula to push capital into top decile signals.
"""

from __future__ import annotations
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging

from app.core.regime_v3 import QualityScoreV3, PositionSizerV3

logger = logging.getLogger(__name__)


@dataclass
class PositionAllocation:
    """Individual position allocation details"""
    ticker: str
    base_allocation: float  # Base allocation (e.g., 2% = 0.02)
    quality_score: float
    adjusted_allocation: float  # After quality adjustment
    position_size: float  # Dollar amount
    regime: str
    strategy_type: str


class EnhancedPositionSizer:
    """
    Enhanced position sizing with quality-score weighting.
    
    Core formula: position_size = base_size * (Q²)
    
    This dramatically increases allocations to top-decile signals
    while reducing exposure to low-quality signals.
    """
    
    def __init__(
        self,
        base_position_size: float = 0.02,  # 2% base position
        max_total_allocation: float = 0.95,  # 95% max total allocation
        min_position_size: float = 0.005,  # 0.5% min position
        max_position_size: float = 0.10,  # 10% max position
        use_squared_quality: bool = True
    ):
        self.base_position_size = base_position_size
        self.max_total_allocation = max_total_allocation
        self.min_position_size = min_position_size
        self.max_position_size = max_position_size
        self.use_squared_quality = use_squared_quality
        
        # Track allocation statistics
        self.allocation_stats = {
            'total_allocations': 0,
            'avg_quality_score': 0.0,
            'top_decile_allocations': 0,
            'bottom_decile_allocations': 0,
            'regime_distribution': {}
        }
    
    def calculate_allocations(
        self,
        signals: List[Dict[str, Any]],
        total_capital: float
    ) -> Dict[str, PositionAllocation]:
        """
        Calculate position allocations for multiple signals.
        
        Args:
            signals: List of signals with quality scores
            total_capital: Total portfolio capital
            
        Returns:
            Dictionary of position allocations by ticker
        """
        
        allocations = {}
        total_weight = 0.0
        quality_scores = []
        
        # Calculate raw allocations
        for signal in signals:
            ticker = signal['ticker']
            quality_score = signal.get('quality_score', 0.5)
            
            # Calculate quality-adjusted allocation
            adjusted_allocation = PositionSizerV3.calculate_position_size(
                self.base_position_size,
                quality_score,
                self.use_squared_quality
            )
            
            # Apply position size limits
            adjusted_allocation = max(self.min_position_size, 
                                    min(self.max_position_size, adjusted_allocation))
            
            # Create allocation object
            allocation = PositionAllocation(
                ticker=ticker,
                base_allocation=self.base_position_size,
                quality_score=quality_score,
                adjusted_allocation=adjusted_allocation,
                position_size=adjusted_allocation * total_capital,
                regime=signal.get('regime', 'UNKNOWN'),
                strategy_type=signal.get('strategy_type', 'unknown')
            )
            
            allocations[ticker] = allocation
            total_weight += adjusted_allocation
            quality_scores.append(quality_score)
        
        # Normalize if total exceeds maximum
        if total_weight > self.max_total_allocation:
            scale_factor = self.max_total_allocation / total_weight
            for allocation in allocations.values():
                allocation.adjusted_allocation *= scale_factor
                allocation.position_size *= scale_factor
                total_weight = self.max_total_allocation
        
        # Update statistics
        self._update_allocation_stats(allocations, quality_scores)
        
        logger.info(f"Calculated {len(allocations)} allocations, total weight: {total_weight:.2%}")
        
        return allocations
    
    def calculate_single_allocation(
        self,
        signal: Dict[str, Any],
        total_capital: float
    ) -> PositionAllocation:
        """
        Calculate allocation for a single signal.
        """
        
        quality_score = signal.get('quality_score', 0.5)
        
        # Calculate quality-adjusted allocation
        adjusted_allocation = PositionSizerV3.calculate_position_size(
            self.base_position_size,
            quality_score,
            self.use_squared_quality
        )
        
        # Apply position size limits
        adjusted_allocation = max(self.min_position_size,
                                min(self.max_position_size, adjusted_allocation))
        
        # Create allocation object
        allocation = PositionAllocation(
            ticker=signal['ticker'],
            base_allocation=self.base_position_size,
            quality_score=quality_score,
            adjusted_allocation=adjusted_allocation,
            position_size=adjusted_allocation * total_capital,
            regime=signal.get('regime', 'UNKNOWN'),
            strategy_type=signal.get('strategy_type', 'unknown')
        )
        
        return allocation
    
    def _update_allocation_stats(
        self,
        allocations: Dict[str, PositionAllocation],
        quality_scores: List[float]
    ) -> None:
        """Update allocation statistics"""
        
        self.allocation_stats['total_allocations'] = len(allocations)
        
        if quality_scores:
            self.allocation_stats['avg_quality_score'] = sum(quality_scores) / len(quality_scores)
            
            # Count top/bottom decile allocations
            quality_threshold_top = sorted(quality_scores)[int(0.9 * len(quality_scores))]
            quality_threshold_bottom = sorted(quality_scores)[int(0.1 * len(quality_scores))]
            
            self.allocation_stats['top_decile_allocations'] = sum(
                1 for q in quality_scores if q >= quality_threshold_top
            )
            self.allocation_stats['bottom_decile_allocations'] = sum(
                1 for q in quality_scores if q <= quality_threshold_bottom
            )
        
        # Regime distribution
        regime_counts = {}
        for allocation in allocations.values():
            regime = allocation.regime
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
        
        self.allocation_stats['regime_distribution'] = regime_counts
    
    def get_allocation_summary(self) -> Dict[str, Any]:
        """Get summary of allocation statistics"""
        
        return {
            'allocation_stats': self.allocation_stats.copy(),
            'sizing_parameters': {
                'base_position_size': self.base_position_size,
                'max_total_allocation': self.max_total_allocation,
                'min_position_size': self.min_position_size,
                'max_position_size': self.max_position_size,
                'use_squared_quality': self.use_squared_quality
            }
        }
    
    def simulate_capital_impact(
        self,
        signals: List[Dict[str, Any]],
        total_capital: float
    ) -> Dict[str, Any]:
        """
        Simulate the impact of quality-based position sizing.
        
        Returns comparison between equal-weight and quality-weighted allocations.
        """
        
        # Equal-weight baseline
        equal_weight = total_capital / len(signals) if signals else 0
        
        # Quality-weighted allocations
        quality_allocations = self.calculate_allocations(signals, total_capital)
        
        # Calculate impact metrics
        total_quality_weight = sum(a.adjusted_allocation for a in quality_allocations.values())
        
        # Top vs bottom decile comparison
        quality_scores = [s.get('quality_score', 0.5) for s in signals]
        if quality_scores:
            top_threshold = sorted(quality_scores)[int(0.9 * len(quality_scores))]
            bottom_threshold = sorted(quality_scores)[int(0.1 * len(quality_scores))]
            
            top_allocations = [a for a in quality_allocations.values() 
                             if a.quality_score >= top_threshold]
            bottom_allocations = [a for a in quality_allocations.values() 
                                if a.quality_score <= bottom_threshold]
            
            avg_top_allocation = sum(a.position_size for a in top_allocations) / len(top_allocations) if top_allocations else 0
            avg_bottom_allocation = sum(a.position_size for a in bottom_allocations) / len(bottom_allocations) if bottom_allocations else 0
            
            top_vs_bottom_ratio = avg_top_allocation / avg_bottom_allocation if avg_bottom_allocation > 0 else 0
        else:
            top_vs_bottom_ratio = 1.0
        
        return {
            'equal_weight_per_position': equal_weight,
            'total_quality_weight': total_quality_weight,
            'quality_vs_equal_ratio': total_quality_weight / (len(signals) * self.base_position_size) if signals else 1.0,
            'top_vs_bottom_ratio': top_vs_bottom_ratio,
            'expected_sharpe_boost': top_vs_bottom_ratio ** 0.5,  # Rough estimate
            'num_positions': len(signals),
            'avg_quality_score': sum(quality_scores) / len(quality_scores) if quality_scores else 0
        }


class RegimeAwarePortfolioManager:
    """
    Portfolio manager that combines regime gating with quality-based position sizing.
    """
    
    def __init__(
        self,
        position_sizer: EnhancedPositionSizer,
        max_concurrent_positions: int = 20,
        max_regime_concentration: float = 0.40  # Max 40% in any single regime
    ):
        self.position_sizer = position_sizer
        self.max_concurrent_positions = max_concurrent_positions
        self.max_regime_concentration = max_regime_concentration
        
        # Track current positions
        self.current_positions: Dict[str, PositionAllocation] = {}
        
        # Track portfolio metrics
        self.portfolio_stats = {
            'total_positions': 0,
            'regime_exposure': {},
            'strategy_exposure': {},
            'quality_distribution': {
                'top_quartile': 0,
                'mid_quartiles': 0,
                'bottom_quartile': 0
            }
        }
    
    def update_portfolio(
        self,
        new_signals: List[Dict[str, Any]],
        total_capital: float,
        existing_positions: Optional[Dict[str, PositionAllocation]] = None
    ) -> Dict[str, PositionAllocation]:
        """
        Update portfolio with new signals, applying regime concentration limits.
        """
        
        if existing_positions:
            self.current_positions = existing_positions.copy()
        
        # Calculate allocations for new signals
        new_allocations = self.position_sizer.calculate_allocations(new_signals, total_capital)
        
        # Apply regime concentration limits
        filtered_allocations = self._apply_regime_limits(new_allocations)
        
        # Apply position count limits
        if len(filtered_allocations) + len(self.current_positions) > self.max_concurrent_positions:
            # Keep highest quality signals
            all_signals = list(filtered_allocations.items()) + list(self.current_positions.items())
            all_signals.sort(key=lambda x: x[1].quality_score, reverse=True)
            
            # Keep top N signals
            selected_signals = all_signals[:self.max_concurrent_positions]
            filtered_allocations = dict(selected_signals)
        
        # Update portfolio statistics
        self._update_portfolio_stats(filtered_allocations)
        
        self.current_positions = filtered_allocations
        
        return filtered_allocations
    
    def _apply_regime_limits(
        self,
        allocations: Dict[str, PositionAllocation]
    ) -> Dict[str, PositionAllocation]:
        """Apply regime concentration limits"""
        
        # Calculate current regime exposure
        regime_exposure = {}
        for allocation in allocations.values():
            regime = allocation.regime
            regime_exposure[regime] = regime_exposure.get(regime, 0) + allocation.adjusted_allocation
        
        # Reduce allocations that exceed limits
        filtered_allocations = {}
        for ticker, allocation in allocations.items():
            regime = allocation.regime
            current_exposure = regime_exposure[regime]
            
            if current_exposure <= self.max_regime_concentration:
                filtered_allocations[ticker] = allocation
            else:
                # Scale down allocation
                scale_factor = self.max_regime_concentration / current_exposure
                allocation.adjusted_allocation *= scale_factor
                allocation.position_size *= scale_factor
                filtered_allocations[ticker] = allocation
        
        return filtered_allocations
    
    def _update_portfolio_stats(
        self,
        allocations: Dict[str, PositionAllocation]
    ) -> None:
        """Update portfolio statistics"""
        
        self.portfolio_stats['total_positions'] = len(allocations)
        
        # Regime exposure
        regime_exposure = {}
        for allocation in allocations.values():
            regime = allocation.regime
            regime_exposure[regime] = regime_exposure.get(regime, 0) + allocation.adjusted_allocation
        
        self.portfolio_stats['regime_exposure'] = regime_exposure
        
        # Strategy exposure
        strategy_exposure = {}
        for allocation in allocations.values():
            strategy = allocation.strategy_type
            strategy_exposure[strategy] = strategy_exposure.get(strategy, 0) + allocation.adjusted_allocation
        
        self.portfolio_stats['strategy_exposure'] = strategy_exposure
        
        # Quality distribution
        quality_scores = [a.quality_score for a in allocations.values()]
        if quality_scores:
            q75 = sorted(quality_scores)[int(0.75 * len(quality_scores))]
            q25 = sorted(quality_scores)[int(0.25 * len(quality_scores))]
            
            self.portfolio_stats['quality_distribution'] = {
                'top_quartile': sum(1 for q in quality_scores if q >= q75),
                'mid_quartiles': sum(1 for q in quality_scores if q25 <= q < q75),
                'bottom_quartile': sum(1 for q in quality_scores if q < q25)
            }
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get comprehensive portfolio summary"""
        
        return {
            'portfolio_stats': self.portfolio_stats,
            'position_sizer_stats': self.position_sizer.get_allocation_summary(),
            'current_positions': {
                ticker: {
                    'allocation': allocation.adjusted_allocation,
                    'position_size': allocation.position_size,
                    'quality_score': allocation.quality_score,
                    'regime': allocation.regime,
                    'strategy': allocation.strategy_type
                }
                for ticker, allocation in self.current_positions.items()
            }
        }
