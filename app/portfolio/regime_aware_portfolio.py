"""
Regime-Aware Portfolio: Integrates adaptive ML signals into portfolio construction.

Creates the final layer: specialized signals → portfolio construction.
"""

from __future__ import annotations

from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from app.ml.adaptive_ml_integration import get_adaptive_ml_integration
from app.core.environment_v3 import bucket_env_v3


class PortfolioAxis(Enum):
    """Portfolio construction axes for regime-aware allocation."""
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    VOLATILITY = "volatility"
    QUALITY = "quality"
    SECTOR_ROTATION = "sector_rotation"
    LIQUIDITY = "liquidity"


@dataclass
class RegimePortfolio:
    """Regime-aware portfolio allocation."""
    regime: Tuple[str, str, str, str, str, str]
    allocations: Dict[str, float]
    primary_axis: PortfolioAxis
    secondary_axis: PortfolioAxis
    risk_adjustment: float
    expected_return: float
    confidence: float


class RegimeAwarePortfolio:
    """
    Constructs portfolios based on regime-aware ML signals.
    
    Specialization → stronger signals → portfolio integration.
    """
    
    def __init__(self):
        self.adaptive_ml = get_adaptive_ml_integration()
        self.regime_portfolios: Dict[str, RegimePortfolio] = {}
        
    def construct_regime_portfolio(self, env_bucket: Tuple[str, str, str, str, str, str], 
                              ml_candidates: List[Dict]) -> RegimePortfolio:
        """Construct portfolio for specific regime."""
        
        regime_name = "_".join(env_bucket)
        
        # Determine portfolio axes based on regime
        primary_axis, secondary_axis = self.determine_portfolio_axes(env_bucket)
        
        # Calculate allocations based on ML signals
        allocations = self.calculate_regime_allocations(ml_candidates, primary_axis, secondary_axis)
        
        # Risk adjustment based on regime characteristics
        risk_adjustment = self.calculate_risk_adjustment(env_bucket)
        
        # Expected return from ML predictions
        expected_return = self.calculate_expected_return(ml_candidates, allocations)
        
        # Confidence based on model performance
        confidence = self.calculate_portfolio_confidence(env_bucket, ml_candidates)
        
        portfolio = RegimePortfolio(
            regime=env_bucket,
            allocations=allocations,
            primary_axis=primary_axis,
            secondary_axis=secondary_axis,
            risk_adjustment=risk_adjustment,
            expected_return=expected_return,
            confidence=confidence
        )
        
        self.regime_portfolios[regime_name] = portfolio
        
        return portfolio
    
    def determine_portfolio_axes(self, env_bucket: Tuple[str, str, str, str, str, str]) -> Tuple[PortfolioAxis, PortfolioAxis]:
        """Determine primary and secondary portfolio axes based on regime."""
        
        vol_regime, trend_regime, disp_regime, liq_regime, sector_regime, industry_disp = env_bucket
        
        # Primary axis based on dominant regime characteristic
        if vol_regime == "HI_VOL":
            primary_axis = PortfolioAxis.VOLATILITY
        elif trend_regime == "TREND":
            primary_axis = PortfolioAxis.MOMENTUM
        elif disp_regime == "HI_DISP":
            primary_axis = PortfolioAxis.SECTOR_ROTATION
        elif liq_regime == "LO_LIQ":
            primary_axis = PortfolioAxis.LIQUIDITY
        else:
            primary_axis = PortfolioAxis.QUALITY
        
        # Secondary axis based on sector leadership
        if sector_regime == "technology":
            secondary_axis = PortfolioAxis.MOMENTUM
        elif sector_regime == "financials":
            secondary_axis = PortfolioAxis.MEAN_REVERSION
        elif sector_regime == "healthcare":
            secondary_axis = PortfolioAxis.QUALITY
        elif sector_regime == "energy":
            secondary_axis = PortfolioAxis.VOLATILITY
        else:
            secondary_axis = PortfolioAxis.SECTOR_ROTATION
        
        return primary_axis, secondary_axis
    
    def calculate_regime_allocations(self, ml_candidates: List[Dict], 
                                 primary_axis: PortfolioAxis, 
                                 secondary_axis: PortfolioAxis) -> Dict[str, float]:
        """Calculate portfolio allocations based on ML signals and axes."""
        
        allocations = {}
        total_weight = 0.0
        
        for candidate in ml_candidates[:20]:  # Top 20 candidates
            symbol = candidate["symbol"]
            ml_score = candidate["ml_score"]
            confidence = candidate["ml_confidence"]
            
            # Base allocation from ML score
            base_allocation = ml_score * confidence
            
            # Adjust based on portfolio axes
            axis_multiplier = self.calculate_axis_multiplier(candidate, primary_axis, secondary_axis)
            
            # Final allocation
            final_allocation = base_allocation * axis_multiplier
            allocations[symbol] = final_allocation
            total_weight += final_allocation
        
        # Normalize to 100%
        if total_weight > 0:
            for symbol in allocations:
                allocations[symbol] = (allocations[symbol] / total_weight) * 100
        
        return allocations
    
    def calculate_axis_multiplier(self, candidate: Dict, primary_axis: PortfolioAxis, 
                              secondary_axis: PortfolioAxis) -> float:
        """Calculate multiplier based on how well candidate fits portfolio axes."""
        
        features = candidate["features"]
        multipliers = {
            PortfolioAxis.MOMENTUM: self.calculate_momentum_fit(features),
            PortfolioAxis.MEAN_REVERSION: self.calculate_mean_reversion_fit(features),
            PortfolioAxis.VOLATILITY: self.calculate_volatility_fit(features),
            PortfolioAxis.QUALITY: self.calculate_quality_fit(features),
            PortfolioAxis.SECTOR_ROTATION: self.calculate_sector_rotation_fit(features),
            PortfolioAxis.LIQUIDITY: self.calculate_liquidity_fit(features),
        }
        
        primary_fit = multipliers.get(primary_axis, 1.0)
        secondary_fit = multipliers.get(secondary_axis, 1.0)
        
        # Combined fit (primary gets 70% weight, secondary 30%)
        combined_fit = (primary_fit * 0.7) + (secondary_fit * 0.3)
        
        return max(0.5, min(2.0, combined_fit))
    
    def calculate_momentum_fit(self, features: Dict) -> float:
        """Calculate how well stock fits momentum strategy."""
        
        short_trend = features.get("return_5d", 0.0)
        medium_trend = features.get("return_63d", 0.0)
        
        # Strong positive trends = high momentum fit
        if short_trend > 0.01 and medium_trend > 0.05:
            return 1.5
        elif short_trend > 0.005 and medium_trend > 0.02:
            return 1.2
        elif short_trend < -0.01 and medium_trend < -0.05:
            return 0.8  # Negative momentum
        else:
            return 1.0
    
    def calculate_mean_reversion_fit(self, features: Dict) -> float:
        """Calculate how well stock fits mean reversion strategy."""
        
        volatility = features.get("volatility_20d", 0.02)
        price_percentile = features.get("price_percentile_252d", 0.5)
        
        # Low price percentiles with high volatility = good mean reversion
        if price_percentile < 0.2 and volatility > 0.025:
            return 1.4
        elif price_percentile < 0.3 and volatility > 0.02:
            return 1.2
        else:
            return 1.0
    
    def calculate_volatility_fit(self, features: Dict) -> float:
        """Calculate how well stock fits volatility strategy."""
        
        volatility = features.get("volatility_20d", 0.02)
        
        # High volatility stocks = good for volatility strategies
        if volatility > 0.04:
            return 1.3
        elif volatility > 0.025:
            return 1.1
        else:
            return 0.8
    
    def calculate_quality_fit(self, features: Dict) -> float:
        """Calculate how well stock fits quality strategy."""
        
        volume = features.get("dollar_volume", 1000000)
        price_percentile = features.get("price_percentile_252d", 0.5)
        
        # High volume, reasonable price = quality
        if volume > 5000000 and 0.2 < price_percentile < 0.8:
            return 1.3
        elif volume > 2000000:
            return 1.1
        else:
            return 0.9
    
    def calculate_sector_rotation_fit(self, features: Dict) -> float:
        """Calculate how well stock fits sector rotation strategy."""
        
        sector = features.get("sector")
        peer_relative = features.get("peer_relative_return_63d", 0.0)
        
        # Strong relative performance = good for sector rotation
        if peer_relative > 0.05:
            return 1.4
        elif peer_relative > 0.02:
            return 1.2
        elif peer_relative < -0.05:
            return 0.8
        else:
            return 1.0
    
    def calculate_liquidity_fit(self, features: Dict) -> float:
        """Calculate how well stock fits liquidity strategy."""
        
        volume = features.get("dollar_volume", 1000000)
        volume_zscore = features.get("volume_zscore_20d", 0.0)
        
        # High volume with recent spike = liquidity play
        if volume > 10000000 and volume_zscore > 2.0:
            return 1.5
        elif volume > 5000000 and volume_zscore > 1.0:
            return 1.2
        else:
            return 1.0
    
    def calculate_risk_adjustment(self, env_bucket: Tuple[str, str, str, str, str, str]) -> float:
        """Calculate risk adjustment based on regime."""
        
        vol_regime, trend_regime, disp_regime, liq_regime, sector_regime, industry_disp = env_bucket
        
        # Higher risk adjustment in volatile regimes
        if vol_regime == "HI_VOL":
            return 0.8  # Reduce risk
        elif liq_regime == "LO_LIQ":
            return 0.7  # Further reduce risk
        elif industry_disp == "HI_INDUSTRY_DISP":
            return 0.85  # Moderate risk reduction
        else:
            return 1.0  # Normal risk
    
    def calculate_expected_return(self, ml_candidates: List[Dict], allocations: Dict[str, float]) -> float:
        """Calculate portfolio expected return from ML predictions."""
        
        expected_return = 0.0
        
        for symbol, allocation in allocations.items():
            # Find corresponding candidate
            candidate = next((c for c in ml_candidates if c["symbol"] == symbol), None)
            if candidate:
                weight = allocation / 100.0  # Convert to decimal
                prediction = candidate["ml_prediction"]
                expected_return += weight * prediction
        
        return expected_return
    
    def calculate_portfolio_confidence(self, env_bucket: Tuple[str, str, str, str, str, str], 
                                   ml_candidates: List[Dict]) -> float:
        """Calculate overall portfolio confidence."""
        
        # Base confidence from ML predictions
        confidences = [c["ml_confidence"] for c in ml_candidates[:20]]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.5
        
        # Adjust for regime stability
        vol_regime, trend_regime, disp_regime, liq_regime, sector_regime, industry_disp = env_bucket
        
        # Higher confidence in stable regimes
        if vol_regime == "LO_VOL" and trend_regime == "TREND":
            regime_stability = 1.2
        elif vol_regime == "HI_VOL" and trend_regime == "CHOP":
            regime_stability = 0.8
        else:
            regime_stability = 1.0
        
        return min(0.95, avg_confidence * regime_stability)
    
    def get_current_portfolio(self, db_path: str, as_of: str, features: Dict[str, Any]) -> RegimePortfolio:
        """Get current regime-aware portfolio."""
        
        # Run adaptive ML discovery
        ml_discovery = self.adaptive_ml.run_adaptive_ml_discovery(features, db_path, as_of)
        
        # Get environment bucket
        env_bucket = ml_discovery["environment"]["env_bucket"]
        ml_candidates = ml_discovery["candidates"]
        
        # Construct portfolio for current regime
        portfolio = self.construct_regime_portfolio(env_bucket, ml_candidates)
        
        print(f"=== REGIME-AWARE PORTFOLIO ===")
        print(f"Regime: {env_bucket}")
        print(f"Primary axis: {portfolio.primary_axis.value}")
        print(f"Secondary axis: {portfolio.secondary_axis.value}")
        print(f"Expected return: {portfolio.expected_return:.4f}")
        print(f"Risk adjustment: {portfolio.risk_adjustment:.2f}")
        print(f"Confidence: {portfolio.confidence:.2f}")
        print(f"Top allocations: {dict(list(portfolio.allocations.items())[:5])}")
        
        return portfolio
    
    def analyze_portfolio_performance(self) -> Dict[str, Any]:
        """Analyze portfolio performance across regimes."""
        
        print("=== PORTFOLIO PERFORMANCE ANALYSIS ===")
        
        analysis = {
            "total_portfolios": len(self.regime_portfolios),
            "regime_performance": {},
            "axis_effectiveness": {},
            "overall_metrics": {}
        }
        
        # Analyze performance by regime
        for regime_name, portfolio in self.regime_portfolios.items():
            analysis["regime_performance"][regime_name] = {
                "expected_return": portfolio.expected_return,
                "risk_adjustment": portfolio.risk_adjustment,
                "confidence": portfolio.confidence,
                "primary_axis": portfolio.primary_axis.value,
                "secondary_axis": portfolio.secondary_axis.value,
                "diversification": len(portfolio.allocations),
            }
        
        # Analyze axis effectiveness
        axis_performance = defaultdict(list)
        for portfolio in self.regime_portfolios.values():
            axis_performance = portfolio.expected_return * portfolio.confidence
            axis_performance[portfolio.primary_axis.value].append(axis_performance)
            axis_performance[portfolio.secondary_axis.value].append(axis_performance)
        
        for axis, performances in axis_performance.items():
            avg_performance = sum(performances) / len(performances)
            analysis["axis_effectiveness"][axis] = avg_performance
        
        return analysis


# Global regime-aware portfolio instance
_regime_aware_portfolio = None


def get_regime_aware_portfolio() -> RegimeAwarePortfolio:
    """Get global regime-aware portfolio instance."""
    global _regime_aware_portfolio
    if _regime_aware_portfolio is None:
        _regime_aware_portfolio = RegimeAwarePortfolio()
    return _regime_aware_portfolio
