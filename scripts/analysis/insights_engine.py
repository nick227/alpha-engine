"""
Insights Engine - Actionable recommendations based on temporal correlation analysis.

Transforms correlation data into actionable trading insights and recommendations
for strategy optimization, risk management, and timing improvements.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json
from dataclasses import dataclass
from enum import Enum

from temporal_correlation_analyzer import TemporalCorrelationAnalyzer


class InsightType(Enum):
    TIMING = "timing"
    RISK_MANAGEMENT = "risk_management"
    STRATEGY_OPTIMIZATION = "strategy_optimization"
    MARKET_CONDITION = "market_condition"
    SEASONAL = "seasonal"


@dataclass
class Insight:
    """Individual insight with recommendation."""
    insight_type: InsightType
    title: str
    description: str
    confidence: float  # 0-1
    impact: str  # "low", "medium", "high"
    actionable_steps: List[str]
    supporting_data: Dict[str, Any]
    priority: int  # 1-10, lower is higher priority


class InsightsEngine:
    """
    Generates actionable insights from temporal correlation analysis.
    """
    
    def __init__(self, correlation_analyzer: TemporalCorrelationAnalyzer):
        self.analyzer = correlation_analyzer
        self.insights = []
        
    def generate_all_insights(self, correlation_report: Dict[str, Any] = None) -> List[Insight]:
        """Generate all insights from correlation analysis."""
        
        if correlation_report is None:
            correlation_report = self.analyzer.generate_comprehensive_correlation_report()
        
        self.insights = []
        
        # Generate different types of insights
        self.insights.extend(self._generate_timing_insights(correlation_report))
        self.insights.extend(self._generate_risk_management_insights(correlation_report))
        self.insights.extend(self._generate_strategy_optimization_insights(correlation_report))
        self.insights.extend(self._generate_market_condition_insights(correlation_report))
        self.insights.extend(self._generate_seasonal_insights(correlation_report))
        
        # Sort by priority and confidence
        self.insights.sort(key=lambda x: (x.priority, -x.confidence))
        
        return self.insights
    
    def _generate_timing_insights(self, report: Dict[str, Any]) -> List[Insight]:
        """Generate timing-related insights."""
        
        insights = []
        
        # Sentiment timing
        sentiment_data = report.get('sentiment_correlation', {})
        if 'correlation_with_sentiment' in sentiment_data:
            corr = sentiment_data['correlation_with_sentiment']
            
            if abs(corr) > 0.5:
                sentiment_type = "positive" if corr > 0 else "negative"
                insights.append(Insight(
                    insight_type=InsightType.TIMING,
                    title=f"Market Sentiment Timing Opportunity",
                    description=f"Strategy shows {sentiment_type} correlation with market sentiment (r={corr:.2f})",
                    confidence=min(abs(corr), 0.9),
                    impact="high" if abs(corr) > 0.7 else "medium",
                    actionable_steps=[
                        f"Increase position size during {sentiment_type} sentiment periods",
                        f"Reduce exposure during {'negative' if sentiment_type == 'positive' else 'positive'} sentiment",
                        "Monitor sentiment indicators daily",
                        "Set up sentiment-based alerts"
                    ],
                    supporting_data={
                        "correlation": corr,
                        "sentiment_performance": sentiment_data.get('sentiment_performance', {})
                    },
                    priority=2 if abs(corr) > 0.7 else 5
                ))
        
        # Economic event timing
        economic_data = report.get('economic_event_impact', {})
        high_impact = economic_data.get('high_impact_performance', {})
        
        if high_impact:
            with_events = high_impact.get('with_high_impact', {}).get('avg_performance', 0)
            without_events = high_impact.get('without_high_impact', {}).get('avg_performance', 0)
            
            if abs(with_events - without_events) > 1000:
                better = "better" if with_events > without_events else "worse"
                action = "increase" if with_events > without_events else "reduce"
                
                insights.append(Insight(
                    insight_type=InsightType.TIMING,
                    title=f"Economic Event Timing Strategy",
                    description=f"Strategy performs {better} during high-impact economic events (${abs(with_events - without_events):,.0f} difference)",
                    confidence=0.7,
                    impact="high",
                    actionable_steps=[
                        f"{action.capitalize()} exposure around FOMC meetings and major data releases",
                        "Create economic calendar alerts",
                        "Consider pre-positioning ahead of high-impact events",
                        "Monitor event surprises vs expectations"
                    ],
                    supporting_data={
                        "performance_difference": with_events - without_events,
                        "with_events_performance": with_events,
                        "without_events_performance": without_events
                    },
                    priority=3
                ))
        
        # Volatility regime timing
        vol_data = report.get('volatility_regime_analysis', {})
        regime_performance = vol_data.get('regime_performance', {})
        
        if regime_performance:
            high_vol = regime_performance.get('high_volatility', {}).get('avg_performance', 0)
            low_vol = regime_performance.get('low_volatility', {}).get('avg_performance', 0)
            
            if abs(high_vol - low_vol) > 1500:
                better_regime = "high" if high_vol > low_vol else "low"
                
                insights.append(Insight(
                    insight_type=InsightType.TIMING,
                    title=f"Volatility Regime Optimization",
                    description=f"Strategy performs significantly better in {better_regime} volatility environments",
                    confidence=0.8,
                    impact="high",
                    actionable_steps=[
                        f"Scale strategy exposure based on VIX levels",
                        f"Increase positions during {better_regime} volatility periods",
                        "Implement volatility-based position sizing",
                        "Set VIX threshold alerts"
                    ],
                    supporting_data={
                        "high_vol_performance": high_vol,
                        "low_vol_performance": low_vol,
                        "performance_gap": high_vol - low_vol
                    },
                    priority=2
                ))
        
        return insights
    
    def _generate_risk_management_insights(self, report: Dict[str, Any]) -> List[Insight]:
        """Generate risk management insights."""
        
        insights = []
        
        # Volatility transition risk
        vol_data = report.get('volatility_regime_analysis', {})
        transition_analysis = vol_data.get('transition_analysis', {})
        
        if transition_analysis:
            transition_perf = transition_analysis.get('transition_periods', {}).get('avg_performance', 0)
            stable_perf = transition_analysis.get('stable_periods', {}).get('avg_performance', 0)
            
            if transition_perf < stable_perf - 500:
                insights.append(Insight(
                    insight_type=InsightType.RISK_MANAGEMENT,
                    title="Volatility Transition Risk Detected",
                    description=f"Strategy underperforms during volatility regime transitions by ${stable_perf - transition_perf:,.0f}",
                    confidence=0.8,
                    impact="high",
                    actionable_steps=[
                        "Reduce position sizes during VIX spikes",
                        "Implement volatility-based stop losses",
                        "Consider hedging during transition periods",
                        "Monitor VIX change rate as risk indicator"
                    ],
                    supporting_data={
                        "transition_performance": transition_perf,
                        "stable_performance": stable_perf,
                        "performance_gap": stable_perf - transition_perf
                    },
                    priority=1
                ))
        
        # Sector concentration risk
        sector_data = report.get('sector_rotation_impact', {})
        best_sector = sector_data.get('best_correlated_sector', {})
        
        if best_sector and abs(best_sector.get('correlation', 0)) > 0.7:
            insights.append(Insight(
                insight_type=InsightType.RISK_MANAGEMENT,
                title=f"Sector Concentration in {best_sector.get('name', 'Unknown')}",
                description=f"Strong correlation with {best_sector.get('name', 'Unknown')} sector indicates concentration risk",
                confidence=0.7,
                impact="medium",
                actionable_steps=[
                    f"Monitor {best_sector.get('name', 'Unknown')} sector health closely",
                    "Consider sector diversification",
                    "Set sector-specific risk limits",
                    "Create sector rotation alerts"
                ],
                supporting_data=best_sector,
                priority=4
            ))
        
        # Negative correlation indicators
        indicator_data = report.get('market_indicator_correlation', {})
        negative_correlations = {k: v for k, v in indicator_data.items() 
                              if v.get('performance_correlation', 0) < -0.5}
        
        if negative_correlations:
            worst_indicator = min(negative_correlations.items(), 
                                key=lambda x: x[1].get('performance_correlation', 0))
            
            insights.append(Insight(
                insight_type=InsightType.RISK_MANAGEMENT,
                title=f"{worst_indicator[0]} Risk Factor",
                description=f"Strong negative correlation with {worst_indicator[0]} (r={worst_indicator[1].get('performance_correlation', 0):.2f})",
                confidence=0.6,
                impact="medium",
                actionable_steps=[
                    f"Monitor {worst_indicator[0]} levels as risk indicator",
                    f"Reduce exposure when {worst_indicator[0]} moves unfavorably",
                    f"Consider {worst_indicator[0]}-based hedging strategies",
                    "Set up indicator threshold alerts"
                ],
                supporting_data=worst_indicator[1],
                priority=5
            ))
        
        return insights
    
    def _generate_strategy_optimization_insights(self, report: Dict[str, Any]) -> List[Insight]:
        """Generate strategy optimization insights."""
        
        insights = []
        
        # High correlation sectors for enhancement
        sector_data = report.get('sector_rotation_impact', {})
        sector_correlations = sector_data.get('sector_correlations', {})
        
        if sector_correlations:
            # Find top correlated sectors
            sorted_sectors = sorted(sector_correlations.items(), 
                                  key=lambda x: abs(x[1].get('performance_correlation', 0)), 
                                  reverse=True)
            
            top_sectors = sorted_sectors[:3]  # Top 3
            
            if len(top_sectors) > 0 and abs(top_sectors[0][1].get('performance_correlation', 0)) > 0.6:
                sector_names = [s[0] for s in top_sectors]
                correlations = [s[1].get('performance_correlation', 0) for s in top_sectors]
                
                insights.append(Insight(
                    insight_type=InsightType.STRATEGY_OPTIMIZATION,
                    title="Sector-Based Strategy Enhancement",
                    description=f"Strong correlations with {', '.join(sector_names)} suggest sector-based optimization potential",
                    confidence=0.7,
                    impact="medium",
                    actionable_steps=[
                        f"Add sector momentum filters for {', '.join(sector_names)}",
                        "Develop sector rotation sub-strategies",
                        "Implement sector-specific position sizing",
                        "Create sector health indicators"
                    ],
                    supporting_data={
                        "top_sectors": sector_names,
                        "correlations": correlations
                    },
                    priority=6
                ))
        
        # Positive correlation indicators for signals
        indicator_data = report.get('market_indicator_correlation', {})
        positive_correlations = {k: v for k, v in indicator_data.items() 
                               if v.get('performance_correlation', 0) > 0.5}
        
        if positive_correlations:
            best_indicator = max(positive_correlations.items(), 
                               key=lambda x: x[1].get('performance_correlation', 0))
            
            insights.append(Insight(
                insight_type=InsightType.STRATEGY_OPTIMIZATION,
                title=f"{best_indicator[0]} Signal Enhancement",
                description=f"Strong positive correlation with {best_indicator[0]} suggests signal enhancement opportunity",
                confidence=0.6,
                impact="medium",
                actionable_steps=[
                    f"Incorporate {best_indicator[0]} as entry/exit signal",
                    f"Use {best_indicator[0]} for confirmation signals",
                    f"Develop {best_indicator[0]}-based sub-strategy",
                    "Backtest indicator-based optimizations"
                ],
                supporting_data=best_indicator[1],
                priority=7
            ))
        
        return insights
    
    def _generate_market_condition_insights(self, report: Dict[str, Any]) -> List[Insight]:
        """Generate market condition insights."""
        
        insights = []
        
        # Economic category performance
        economic_data = report.get('economic_event_impact', {})
        category_performance = economic_data.get('category_performance', {})
        
        if category_performance:
            # Find best and worst performing categories
            sorted_categories = sorted(category_performance.items(), 
                                    key=lambda x: x[1].get('avg_performance', 0), 
                                    reverse=True)
            
            if len(sorted_categories) >= 2:
                best_category = sorted_categories[0]
                worst_category = sorted_categories[-1]
                
                performance_diff = best_category[1].get('avg_performance', 0) - worst_category[1].get('avg_performance', 0)
                
                if performance_diff > 2000:
                    insights.append(Insight(
                        insight_type=InsightType.MARKET_CONDITION,
                        title="Economic Environment Specialization",
                        description=f"Strategy performs significantly better during {best_category[0]} events vs {worst_category[0]} events",
                        confidence=0.7,
                        impact="medium",
                        actionable_steps=[
                            f"Increase exposure during {best_category[0]} events",
                            f"Reduce risk during {worst_category[0]} events",
                            "Create economic environment filters",
                            "Develop category-specific risk management"
                        ],
                        supporting_data={
                            "best_category": best_category,
                            "worst_category": worst_category,
                            "performance_difference": performance_diff
                        },
                        priority=8
                    ))
        
        # Headline sentiment impact
        sentiment_data = report.get('sentiment_correlation', {})
        sentiment_performance = sentiment_data.get('sentiment_performance', {})
        
        if sentiment_performance:
            # Check if there's a clear pattern
            positive_perf = sentiment_performance.get('positive', {}).get('avg_performance', 0)
            negative_perf = sentiment_performance.get('negative', {}).get('avg_performance', 0)
            
            if abs(positive_perf - negative_perf) > 1500:
                better_sentiment = "positive" if positive_perf > negative_perf else "negative"
                
                insights.append(Insight(
                    insight_type=InsightType.MARKET_CONDITION,
                    title="News Sentiment Strategy Adjustment",
                    description=f"Strategy performs better during {better_sentiment} news environments",
                    confidence=0.6,
                    impact="medium",
                    actionable_steps=[
                        f"Scale up during {better_sentiment} news periods",
                        "Implement news sentiment filters",
                        "Create news-based position sizing",
                        "Monitor headline sentiment daily"
                    ],
                    supporting_data={
                        "positive_performance": positive_perf,
                        "negative_performance": negative_perf,
                        "performance_difference": positive_perf - negative_perf
                    },
                    priority=9
                ))
        
        return insights
    
    def _generate_seasonal_insights(self, report: Dict[str, Any]) -> List[Insight]:
        """Generate seasonal insights."""
        
        insights = []
        
        # Monthly patterns
        seasonal_data = report.get('seasonal_patterns', {})
        best_month = seasonal_data.get('best_month', {})
        worst_month = seasonal_data.get('worst_month', {})
        
        if best_month and worst_month:
            month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            
            performance_diff = best_month.get('avg_performance', 0) - worst_month.get('avg_performance', 0)
            
            if performance_diff > 2000:
                insights.append(Insight(
                    insight_type=InsightType.SEASONAL,
                    title=f"Seasonal Pattern: {month_names[best_month.get('month', 1)]} vs {month_names[worst_month.get('month', 1)]}",
                    description=f"Strong seasonal performance difference: ${best_month.get('avg_performance', 0):.0f} in {month_names[best_month.get('month', 1)]} vs ${worst_month.get('avg_performance', 0):.0f} in {month_names[worst_month.get('month', 1)]}",
                    confidence=0.6,
                    impact="medium",
                    actionable_steps=[
                        f"Increase capital allocation in {month_names[best_month.get('month', 1)]}",
                        f"Reduce risk in {month_names[worst_month.get('month', 1)]}",
                        "Implement seasonal position sizing",
                        "Create seasonal performance alerts"
                    ],
                    supporting_data={
                        "best_month": best_month,
                        "worst_month": worst_month,
                        "performance_difference": performance_diff
                    },
                    priority=10
                ))
        
        # Quarterly patterns
        quarterly_data = seasonal_data.get('quarterly_analysis', {})
        best_quarter = seasonal_data.get('best_quarter', {})
        worst_quarter = seasonal_data.get('worst_quarter', {})
        
        if best_quarter and worst_quarter:
            quarter_names = ['', 'Q1', 'Q2', 'Q3', 'Q4']
            
            performance_diff = best_quarter.get('avg_performance', 0) - worst_quarter.get('avg_performance', 0)
            
            if performance_diff > 3000:
                insights.append(Insight(
                    insight_type=InsightType.SEASONAL,
                    title=f"Quarterly Performance Pattern",
                    description=f"Strong quarterly pattern: {quarter_names[best_quarter.get('quarter', 1)]} outperforms {quarter_names[worst_quarter.get('quarter', 1)]} by ${performance_diff:,.0f}",
                    confidence=0.7,
                    impact="medium",
                    actionable_steps=[
                        f"Optimize strategy for {quarter_names[best_quarter.get('quarter', 1)]} conditions",
                        f"Implement defensive measures in {quarter_names[worst_quarter.get('quarter', 1)]}",
                        "Create quarterly strategy adjustments",
                        "Review quarterly performance drivers"
                    ],
                    supporting_data={
                        "best_quarter": best_quarter,
                        "worst_quarter": worst_quarter,
                        "performance_difference": performance_diff
                    },
                    priority=8
                ))
        
        return insights
    
    def get_insights_by_type(self, insight_type: InsightType) -> List[Insight]:
        """Get insights filtered by type."""
        return [insight for insight in self.insights if insight.insight_type == insight_type]
    
    def get_high_priority_insights(self, max_priority: int = 5) -> List[Insight]:
        """Get high priority insights."""
        return [insight for insight in self.insights if insight.priority <= max_priority]
    
    def get_high_confidence_insights(self, min_confidence: float = 0.7) -> List[Insight]:
        """Get high confidence insights."""
        return [insight for insight in self.insights if insight.confidence >= min_confidence]
    
    def get_high_impact_insights(self) -> List[Insight]:
        """Get high impact insights."""
        return [insight for insight in self.insights if insight.impact == "high"]
    
    def generate_executive_summary(self) -> Dict[str, Any]:
        """Generate executive summary of insights."""
        
        if not self.insights:
            return {"error": "No insights available"}
        
        # Count insights by type
        insight_counts = {}
        for insight_type in InsightType:
            insight_counts[insight_type.value] = len(self.get_insights_by_type(insight_type))
        
        # Get top insights
        top_insights = self.insights[:5]  # Top 5 by priority/confidence
        
        # Get high impact insights
        high_impact = self.get_high_impact_insights()
        
        # Get actionable themes
        themes = self._extract_themes()
        
        return {
            "summary": {
                "total_insights": len(self.insights),
                "high_priority": len(self.get_high_priority_insights()),
                "high_confidence": len(self.get_high_confidence_insights()),
                "high_impact": len(high_impact),
                "insight_types": insight_counts
            },
            "top_insights": [
                {
                    "title": insight.title,
                    "description": insight.description,
                    "priority": insight.priority,
                    "confidence": insight.confidence,
                    "impact": insight.impact
                }
                for insight in top_insights
            ],
            "high_impact_insights": [
                {
                    "title": insight.title,
                    "description": insight.description,
                    "actionable_steps": insight.actionable_steps[:3]  # Top 3 steps
                }
                for insight in high_impact
            ],
            "key_themes": themes,
            "recommendations": self._generate_overall_recommendations()
        }
    
    def _extract_themes(self) -> List[str]:
        """Extract common themes from insights."""
        
        themes = []
        
        # Analyze insight descriptions for common themes
        all_descriptions = " ".join([insight.description.lower() for insight in self.insights])
        
        # Common theme keywords
        theme_keywords = {
            "volatility": ["volatility", "vix", "vol regime"],
            "sentiment": ["sentiment", "news", "headline"],
            "economic": ["economic", "fomc", "data", "event"],
            "sector": ["sector", "industry", "etf"],
            "seasonal": ["month", "quarter", "seasonal"],
            "risk": ["risk", "drawdown", "transition"],
            "timing": ["timing", "correlation", "opportunity"]
        }
        
        for theme, keywords in theme_keywords.items():
            if any(keyword in all_descriptions for keyword in keywords):
                themes.append(theme)
        
        return themes
    
    def _generate_overall_recommendations(self) -> List[str]:
        """Generate overall recommendations based on all insights."""
        
        recommendations = []
        
        # Analyze all insights to generate overarching recommendations
        
        # Check for volatility-related insights
        vol_insights = self.get_insights_by_type(InsightType.RISK_MANAGEMENT)
        if any("volatility" in insight.description.lower() for insight in vol_insights):
            recommendations.append("Implement volatility-based risk management system")
        
        # Check for timing opportunities
        timing_insights = self.get_insights_by_type(InsightType.TIMING)
        if len(timing_insights) >= 2:
            recommendations.append("Develop market timing enhancement module")
        
        # Check for sector concentration
        sector_insights = [insight for insight in self.insights if "sector" in insight.description.lower()]
        if len(sector_insights) >= 2:
            recommendations.append("Create sector rotation and diversification strategy")
        
        # Check for seasonal patterns
        seasonal_insights = self.get_insights_by_type(InsightType.SEASONAL)
        if len(seasonal_insights) >= 2:
            recommendations.append("Implement seasonal position sizing and risk adjustments")
        
        # Check for economic event sensitivity
        economic_insights = [insight for insight in self.insights if "economic" in insight.description.lower()]
        if len(economic_insights) >= 2:
            recommendations.append("Build economic calendar integration and event-based positioning")
        
        # General high-impact recommendations
        high_impact = self.get_high_impact_insights()
        if len(high_impact) >= 3:
            recommendations.append("Prioritize implementation of high-impact insights first")
        
        return recommendations
    
    def export_insights(self, filename: str = None) -> str:
        """Export insights to JSON file."""
        
        if filename is None:
            filename = f"insights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Convert insights to serializable format
        serializable_insights = []
        for insight in self.insights:
            serializable_insights.append({
                "insight_type": insight.insight_type.value,
                "title": insight.title,
                "description": insight.description,
                "confidence": insight.confidence,
                "impact": insight.impact,
                "actionable_steps": insight.actionable_steps,
                "supporting_data": insight.supporting_data,
                "priority": insight.priority
            })
        
        export_data = {
            "export_timestamp": datetime.now().isoformat(),
            "executive_summary": self.generate_executive_summary(),
            "detailed_insights": serializable_insights
        }
        
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        return filename
    
    def print_insights_summary(self):
        """Print formatted insights summary."""
        
        if not self.insights:
            print("No insights available")
            return
        
        print("\n" + "="*80)
        print("ACTIONABLE INSIGHTS SUMMARY")
        print("="*80)
        
        # Executive summary
        summary = self.generate_executive_summary()
        
        print(f"\nOVERVIEW:")
        print(f"  Total Insights: {summary['summary']['total_insights']}")
        print(f"  High Priority: {summary['summary']['high_priority']}")
        print(f"  High Confidence: {summary['summary']['high_confidence']}")
        print(f"  High Impact: {summary['summary']['high_impact']}")
        
        print(f"\nKEY THEMES: {', '.join(summary['key_themes'])}")
        
        print(f"\nTOP 5 INSIGHTS:")
        print("-" * 50)
        
        for i, insight in enumerate(summary['top_insights'], 1):
            print(f"\n{i}. {insight['title']}")
            print(f"   {insight['description']}")
            print(f"   Priority: {insight['priority']}/10 | Confidence: {insight['confidence']:.1%} | Impact: {insight['impact']}")
        
        print(f"\nOVERALL RECOMMENDATIONS:")
        print("-" * 30)
        
        for i, rec in enumerate(summary['recommendations'], 1):
            print(f"{i}. {rec}")
        
        print("\n" + "="*80)


def main():
    """Main function to generate and display insights."""
    
    print("Generating Actionable Insights...")
    
    # Initialize analyzer
    base_analyzer = StrategyPerformanceAnalyzer()
    
    # Load data and run analysis
    if not base_analyzer.load_historical_data():
        return
    
    if not base_analyzer.calculate_regimes():
        return
    
    base_analyzer.simulate_bear_expansion_strategy()
    
    # Initialize correlation analyzer
    correlation_analyzer = TemporalCorrelationAnalyzer(base_analyzer)
    correlation_report = correlation_analyzer.generate_comprehensive_correlation_report()
    
    # Generate insights
    insights_engine = InsightsEngine(correlation_analyzer)
    insights = insights_engine.generate_all_insights(correlation_report)
    
    # Display insights
    insights_engine.print_insights_summary()
    
    # Export insights
    filename = insights_engine.export_insights()
    print(f"\nInsights exported to: {filename}")
    
    return insights_engine


if __name__ == "__main__":
    engine = main()
