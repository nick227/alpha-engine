"""
Regime-Aware System Validator v2

Tests actual implementation without assuming improvements.
Focus on: leakage detection, regime coverage, quality discrimination, sizing stability.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import json
from typing import Dict, List, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

import sys
from pathlib import Path

_here = Path(__file__).resolve()
sys.path.insert(0, str(_here.parents[2]))

# Import the regime-aware components
from app.core.regime_v3 import RegimeClassifierV3, SignalGating, QualityScoreV3, PositionSizerV3
from app.trading.position_sizing_v3 import EnhancedPositionSizer, RegimeAwarePortfolioManager


def get_historical_data():
    """Get historical data for leakage testing"""
    conn = sqlite3.connect("data/alpha.db")
    
    # Get price data with technical indicators
    query = """
    SELECT 
        ticker, 
        date, 
        close,
        volume,
        ma50,
        ma200,
        atr
    FROM price_data 
    WHERE ma50 IS NOT NULL AND ma200 IS NOT NULL
    ORDER BY ticker, date
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if len(df) == 0:
        print("ERROR: No historical data found")
        return None
    
    df['date'] = pd.to_datetime(df['date'])
    return df


def test_lookahead_leakage(df):
    """Test for lookahead leakage in regime calculations"""
    
    print("=== LOOKAHEAD LEAKAGE TEST ===")
    
    leakage_issues = []
    
    # Test 1: Rolling percentile calculation
    print("1. Testing ATR percentile calculation...")
    
    classifier = RegimeClassifierV3()
    
    for ticker in df['ticker'].unique()[:5]:  # Test first 5 tickers
        ticker_data = df[df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date')
        
        for i in range(50, len(ticker_data)):  # Start from row 50 to have history
            current_row = ticker_data.iloc[i]
            
            # Get historical ATR up to current row (NOT including future)
            historical_atr = ticker_data.iloc[:i]['atr'].dropna().tolist()
            
            if len(historical_atr) < 20:
                continue
            
            # Calculate percentile using only historical data
            current_atr = current_row['atr']
            current_price = current_row['close']
            
            # This should use only historical data
            atr_percentile = np.searchsorted(
                np.sort(historical_atr), current_atr
            ) / len(historical_atr)
            
            # Check if calculation is correct
            manual_percentile = sum(1 for x in historical_atr if x <= current_atr) / len(historical_atr)
            
            if abs(atr_percentile - manual_percentile) > 0.01:
                leakage_issues.append(f"ATR percentile calculation error for {ticker}")
                break
    
    # Test 2: MA calculations (should be from historical data)
    print("2. Testing MA calculations...")
    
    for ticker in df['ticker'].unique()[:5]:
        ticker_data = df[df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date')
        
        for i in range(200, len(ticker_data)):  # Need 200 days for MA200
            current_row = ticker_data.iloc[i]
            
            # Calculate MA50 manually
            manual_ma50 = ticker_data.iloc[max(0, i-49):i+1]['close'].mean()
            manual_ma200 = ticker_data.iloc[max(0, i-199):i+1]['close'].mean()
            
            # Compare with stored values
            if abs(manual_ma50 - current_row['ma50']) > 0.01:
                leakage_issues.append(f"MA50 calculation error for {ticker}")
            
            if abs(manual_ma200 - current_row['ma200']) > 0.01:
                leakage_issues.append(f"MA200 calculation error for {ticker}")
    
    # Test 3: Current bar usage
    print("3. Testing current bar usage...")
    
    # Check if current bar values are used for same-bar decisions
    for ticker in df['ticker'].unique()[:5]:
        ticker_data = df[df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date')
        
        for i in range(50, len(ticker_data)):
            current_row = ticker_data.iloc[i]
            
            # If using current bar's close for MA calculation, that's leakage
            # MA should be calculated from previous bars only
            if i > 0:
                prev_row = ticker_data.iloc[i-1]
                
                # Check if MA50 includes current bar (it shouldn't for same-bar decisions)
                if abs(current_row['ma50'] - prev_row['ma50']) > 0.5:  # Large jump suggests current bar inclusion
                    leakage_issues.append(f"Potential current bar inclusion in MA for {ticker}")
    
    if leakage_issues:
        print("LEAKAGE ISSUES FOUND:")
        for issue in leakage_issues:
            print(f"  - {issue}")
        return False
    else:
        print("No lookahead leakage detected")
        return True


def test_regime_coverage(df):
    """Test regime distribution and signal blocking"""
    
    print("\n=== REGIME COVERAGE TEST ===")
    
    classifier = RegimeClassifierV3()
    regime_stats = {}
    signal_stats = {}
    
    # Calculate regimes for all data
    all_regimes = []
    
    for ticker in df['ticker'].unique():
        ticker_data = df[df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date')
        
        for i in range(50, len(ticker_data)):  # Need history for ATR
            current_row = ticker_data.iloc[i]
            
            # Get historical ATR
            historical_atr = ticker_data.iloc[:i]['atr'].dropna().tolist()
            
            if len(historical_atr) < 20:
                continue
            
            # Calculate regime
            try:
                classification = classifier.classify_market(
                    ticker=ticker,
                    current_price=current_row['close'],
                    ma50=current_row['ma50'],
                    ma200=current_row['ma200'],
                    atr=current_row['atr'],
                    historical_atr=historical_atr
                )
                
                all_regimes.append(classification.combined_regime)
                
                # Count regime
                regime_key = classification.combined_regime
                regime_stats[regime_key] = regime_stats.get(regime_key, 0) + 1
                
            except Exception as e:
                continue
    
    # Test signal gating for each regime
    strategies = ['volatility_breakout', 'momentum', 'mean_reversion']
    
    for strategy in strategies:
        blocked = 0
        passed = 0
        
        for regime_str in set(all_regimes):
            # Parse regime string
            trend_str, vol_str = regime_str.strip('()').split(', ')
            
            from app.core.regime_v3 import RegimeClassification, TrendRegime, VolatilityRegime
            
            regime = RegimeClassification(
                trend_regime=TrendRegime(trend_str),
                volatility_regime=VolatilityRegime(vol_str),
                combined_regime=regime_str,
                price_vs_ma50=0.0,
                ma50_vs_ma200=0.0,
                atr_percentile=0.5,
                volatility_value=0.02
            )
            
            allowed, reason = SignalGating.gate_signal(strategy, regime)
            
            if allowed:
                passed += regime_stats.get(regime_str, 0)
            else:
                blocked += regime_stats.get(regime_str, 0)
        
        total = blocked + passed
        signal_stats[strategy] = {
            'total_signals': total,
            'blocked_signals': blocked,
            'passed_signals': passed,
            'block_rate': blocked / total if total > 0 else 0,
            'pass_rate': passed / total if total > 0 else 0
        }
    
    # Print results
    print("A. Regime Distribution:")
    total_observations = sum(regime_stats.values())
    for regime, count in sorted(regime_stats.items()):
        percentage = count / total_observations * 100
        print(f"  {regime}: {count} ({percentage:.1f}%)")
    
    print("\nB. Signal Blocking by Strategy:")
    for strategy, stats in signal_stats.items():
        print(f"  {strategy}:")
        print(f"    Total: {stats['total_signals']}")
        print(f"    Blocked: {stats['blocked_signals']} ({stats['block_rate']:.1%})")
        print(f"    Passed: {stats['passed_signals']} ({stats['pass_rate']:.1%})")
    
    # Check for over-gating
    print("\nC. Over-gating Check:")
    for strategy, stats in signal_stats.items():
        if stats['pass_rate'] < 0.2:  # Less than 20% pass rate
            print(f"  WARNING: {strategy} severely over-gated ({stats['pass_rate']:.1%} pass)")
        elif stats['pass_rate'] < 0.4:  # Less than 40% pass rate
            print(f"  CAUTION: {strategy} heavily gated ({stats['pass_rate']:.1%} pass)")
        else:
            print(f"  OK: {strategy} reasonable gating ({stats['pass_rate']:.1%} pass)")
    
    return {
        'regime_distribution': regime_stats,
        'signal_blocking': signal_stats,
        'total_observations': total_observations
    }


def test_quality_discrimination(df):
    """Test quality score discrimination with real data"""
    
    print("\n=== QUALITY DISCRIMINATION TEST ===")
    
    # Generate mock signals with real regime data
    signals = []
    
    classifier = RegimeClassifierV3()
    
    for ticker in df['ticker'].unique()[:20]:  # Test 20 tickers
        ticker_data = df[df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date')
        
        for i in range(50, len(ticker_data)):
            current_row = ticker_data.iloc[i]
            
            # Get historical ATR
            historical_atr = ticker_data.iloc[:i]['atr'].dropna().tolist()
            
            if len(historical_atr) < 20:
                continue
            
            # Calculate regime
            try:
                classification = classifier.classify_market(
                    ticker=ticker,
                    current_price=current_row['close'],
                    ma50=current_row['ma50'],
                    ma200=current_row['ma200'],
                    atr=current_row['atr'],
                    historical_atr=historical_atr
                )
                
                # Create mock signal
                signal = {
                    'ticker': ticker,
                    'date': current_row['date'],
                    'signal_strength': np.random.uniform(0.3, 1.0),
                    'regime': classification,
                    'strategy_type': np.random.choice(['volatility_breakout', 'momentum', 'mean_reversion']),
                    'agreement_score': np.random.uniform(0.2, 1.0),
                    'liquidity_confidence': np.random.uniform(0.4, 1.0),
                    'entry_price': current_row['close'],
                    'exit_price': None,  # Will calculate later
                    'return': None       # Will calculate later
                }
                
                # Calculate quality score
                quality = QualityScoreV3.calculate_quality_score(
                    signal_strength=signal['signal_strength'],
                    regime=signal['regime'],
                    strategy_type=signal['strategy_type'],
                    agreement_score=signal['agreement_score'],
                    liquidity_confidence=signal['liquidity_confidence']
                )
                
                signal['quality_score'] = quality
                
                # Simulate trade outcome (simplified)
                # Look ahead 5 days for exit (this is for testing only)
                if i + 5 < len(ticker_data):
                    exit_price = ticker_data.iloc[i + 5]['close']
                    signal['exit_price'] = exit_price
                    signal['return'] = (exit_price / signal['entry_price']) - 1
                
                signals.append(signal)
                
            except Exception as e:
                continue
    
    if len(signals) < 100:
        print(f"ERROR: Only {len(signals)} signals generated for testing")
        return None
    
    # Filter signals with exit prices
    complete_signals = [s for s in signals if s['exit_price'] is not None]
    
    print(f"Generated {len(complete_signals)} complete signals")
    
    # Calculate deciles
    quality_scores = [s['quality_score'] for s in complete_signals]
    decile_thresholds = np.percentile(quality_scores, np.arange(0, 101, 10))
    
    decile_stats = {}
    
    for i in range(10):
        lower_threshold = decile_thresholds[i]
        upper_threshold = decile_thresholds[i + 1]
        
        decile_signals = [s for s in complete_signals 
                         if lower_threshold <= s['quality_score'] < upper_threshold]
        
        if not decile_signals:
            continue
        
        # Calculate metrics for this decile
        returns = [s['return'] for s in decile_signals]
        win_rate = sum(1 for r in returns if r > 0) / len(returns)
        avg_return = np.mean(returns)
        expectancy = win_rate * avg_return - (1 - win_rate) * abs(avg_return)
        
        decile_stats[f'Decile {i+1}'] = {
            'trade_count': len(decile_signals),
            'win_rate': win_rate,
            'avg_return': avg_return,
            'expectancy': expectancy,
            'avg_quality': np.mean([s['quality_score'] for s in decile_signals])
        }
    
    # Print decile analysis
    print("C. Quality Deciles Analysis:")
    print("  Decile | Trades | Win Rate | Avg Return | Expectancy | Avg Quality")
    print("  -------|--------|----------|------------|------------|------------")
    
    for decile, stats in decile_stats.items():
        print(f"  {decile:<7} | {stats['trade_count']:6} | {stats['win_rate']:8.1%} | {stats['avg_return']:10.2%} | {stats['expectancy']:10.3%} | {stats['avg_quality']:10.3f}")
    
    # Test discrimination
    top_decile = decile_stats.get('Decile 10', {})
    bottom_decile = decile_stats.get('Decile 1', {})
    
    if top_decile and bottom_decile:
        win_rate_separation = top_decile['win_rate'] - bottom_decile['win_rate']
        return_separation = top_decile['avg_return'] - bottom_decile['avg_return']
        
        print(f"\nD. Discrimination Quality:")
        print(f"  Top vs Bottom Win Rate: {win_rate_separation:+.1%}")
        print(f"  Top vs Bottom Return: {return_separation:+.2%}")
        
        # Check if discrimination is adequate
        if win_rate_separation < 0.10:  # Less than 10% separation
            print("  WARNING: Poor win rate discrimination")
        elif win_rate_separation < 0.20:  # Less than 20% separation
            print("  CAUTION: Moderate win rate discrimination")
        else:
            print("  GOOD: Strong win rate discrimination")
        
        if return_separation < 0.02:  # Less than 2% separation
            print("  WARNING: Poor return discrimination")
        elif return_separation < 0.05:  # Less than 5% separation
            print("  CAUTION: Moderate return discrimination")
        else:
            print("  GOOD: Strong return discrimination")
    
    return decile_stats


def test_sizing_stability(signals_data):
    """Test position sizing stability and concentration"""
    
    print("\n=== SIZING STABILITY TEST ===")
    
    if not signals_data:
        print("No signals data available for sizing test")
        return None
    
    # Create mock signals with quality scores
    mock_signals = []
    for i in range(100):  # 100 mock signals
        quality = np.random.beta(2, 2)  # Beta distribution
        mock_signals.append({
            'ticker': f'SIGNAL_{i}',
            'quality_score': quality,
            'regime': np.random.choice(['(BULL, EXPANSION)', '(BEAR, EXPANSION)', '(CHOP, COMPRESSION)']),
            'strategy_type': np.random.choice(['volatility_breakout', 'momentum', 'mean_reversion'])
        })
    
    # Test different sizing parameters
    total_capital = 1000000  # $1M
    
    # Test 1: Basic Q² sizing
    sizer_basic = EnhancedPositionSizer(
        base_position_size=0.02,
        use_squared_quality=True
    )
    
    allocations_basic = sizer_basic.calculate_allocations(mock_signals, total_capital)
    
    # Test 2: Capped Q² sizing
    sizer_capped = EnhancedPositionSizer(
        base_position_size=0.02,
        max_position_size=0.05,  # 5% cap
        use_squared_quality=True
    )
    
    allocations_capped = sizer_capped.calculate_allocations(mock_signals, total_capital)
    
    # Test 3: Linear Q sizing (no square)
    sizer_linear = EnhancedPositionSizer(
        base_position_size=0.02,
        use_squared_quality=False
    )
    
    allocations_linear = sizer_linear.calculate_allocations(mock_signals, total_capital)
    
    # Analyze concentration
    def analyze_concentration(allocations, name):
        positions = list(allocations.values())
        position_sizes = [p.position_size for p in positions]
        quality_scores = [p.quality_score for p in positions]
        
        # Sort by quality
        sorted_positions = sorted(zip(position_sizes, quality_scores), key=lambda x: x[1], reverse=True)
        
        # Top 10 positions
        top_10_size = sum(p[0] for p in sorted_positions[:10])
        top_10_pct = top_10_size / total_capital
        
        # Largest position
        largest_position = max(position_sizes)
        largest_pct = largest_position / total_capital
        
        # Top decile vs bottom decile
        top_decile_threshold = np.percentile(quality_scores, 90)
        bottom_decile_threshold = np.percentile(quality_scores, 10)
        
        top_decile_positions = [p for p in positions if p.quality_score >= top_decile_threshold]
        bottom_decile_positions = [p for p in positions if p.quality_score <= bottom_decile_threshold]
        
        avg_top = np.mean([p.position_size for p in top_decile_positions]) if top_decile_positions else 0
        avg_bottom = np.mean([p.position_size for p in bottom_decile_positions]) if bottom_decile_positions else 0
        
        concentration_ratio = avg_top / avg_bottom if avg_bottom > 0 else float('inf')
        
        print(f"D. {name} Concentration Analysis:")
        print(f"  Largest position: {largest_pct:.1%}")
        print(f"  Top 10 positions: {top_10_pct:.1%}")
        print(f"  Top/Bottom ratio: {concentration_ratio:.1f}x")
        
        # Check for dangerous concentration
        if largest_pct > 0.15:  # >15% in one position
            print(f"  WARNING: Excessive concentration in single position")
        if top_10_pct > 0.60:  # >60% in top 10
            print(f"  WARNING: Excessive concentration in top 10")
        if concentration_ratio > 10:  # >10x ratio
            print(f"  WARNING: Extreme concentration ratio")
        
        return {
            'largest_pct': largest_pct,
            'top_10_pct': top_10_pct,
            'concentration_ratio': concentration_ratio
        }
    
    print("D. Capital Concentration Analysis:")
    basic_stats = analyze_concentration(allocations_basic, "Basic Q²")
    capped_stats = analyze_concentration(allocations_capped, "Capped Q²")
    linear_stats = analyze_concentration(allocations_linear, "Linear Q")
    
    return {
        'basic': basic_stats,
        'capped': capped_stats,
        'linear': linear_stats
    }


def generate_validation_report(leakage_ok, regime_coverage, quality_deciles, sizing_stability):
    """Generate comprehensive validation report"""
    
    print("\n" + "="*60)
    print("REGIME-AWARE SYSTEM VALIDATION REPORT")
    print("="*60)
    
    # Overall assessment
    issues = []
    
    if not leakage_ok:
        issues.append("LOOKAHEAD LEAKAGE DETECTED")
    
    if regime_coverage:
        for strategy, stats in regime_coverage['signal_blocking'].items():
            if stats['pass_rate'] < 0.2:
                issues.append(f"OVER-GATING: {strategy}")
    
    if quality_deciles:
        top_decile = quality_deciles.get('Decile 10', {})
        bottom_decile = quality_deciles.get('Decile 1', {})
        
        if top_decile and bottom_decile:
            win_rate_sep = top_decile['win_rate'] - bottom_decile['win_rate']
            if win_rate_sep < 0.10:
                issues.append("POOR QUALITY DISCRIMINATION")
    
    if sizing_stability:
        if sizing_stability['basic']['largest_pct'] > 0.15:
            issues.append("EXCESSIVE CONCENTRATION")
    
    # Print summary tables
    print("\nA. REGIME DISTRIBUTION")
    if regime_coverage:
        total = regime_coverage['total_observations']
        for regime, count in sorted(regime_coverage['regime_distribution'].items()):
            print(f"  {regime}: {count:4} ({count/total:5.1%})")
    
    print("\nB. SIGNAL FUNNEL")
    if regime_coverage:
        for strategy, stats in regime_coverage['signal_blocking'].items():
            print(f"  {strategy}:")
            print(f"    Raw: {stats['total_signals']:4}")
            print(f"    Blocked by regime: {stats['blocked_signals']:4} ({stats['block_rate']:5.1%})")
            print(f"    Passed gating: {stats['passed_signals']:4} ({stats['pass_rate']:5.1%})")
    
    print("\nC. QUALITY DECILES")
    if quality_deciles:
        print("  Decile | Trades | Win%  | Ret%  | Exp%  | Qual")
        print("  -------|--------|-------|-------|-------|------")
        for decile, stats in quality_deciles.items():
            print(f"  {decile:<7} | {stats['trade_count']:6} | {stats['win_rate']:5.1%} | {stats['avg_return']:5.1%} | {stats['expectancy']:5.1%} | {stats['avg_quality']:4.2f}")
    
    print("\nD. CAPITAL CONCENTRATION")
    if sizing_stability:
        print(f"  Largest position: {sizing_stability['basic']['largest_pct']:5.1%}")
        print(f"  Top 10 positions: {sizing_stability['basic']['top_10_pct']:5.1%}")
        print(f"  Top/Bottom ratio: {sizing_stability['basic']['concentration_ratio']:4.1f}x")
    
    # Final assessment
    print("\n" + "="*60)
    print("ASSESSMENT")
    print("="*60)
    
    if issues:
        print("CRITICAL ISSUES FOUND:")
        for issue in issues:
            print(f"  - {issue}")
        print("\nRECOMMENDATION: Do not deploy - fix critical issues first")
    else:
        print("No critical issues detected")
        print("\nRECOMMENDATION: Ready for paper trading validation")
    
    # Hypotheses (not results)
    print("\n" + "="*60)
    print("HYPOTHESES (NOT RESULTS)")
    print("="*60)
    print("Architecture improved with:")
    print("  - Regime-aware signal gating")
    print("  - Quality-based position sizing")
    print("  - Enhanced risk management")
    print("\nHypothesized benefits (require backtesting validation):")
    print("  - Higher Sharpe ratio")
    print("  - Lower trade count")
    print("  - Better quality concentration")
    print("  - Reduced drawdowns")
    
    return {
        'leakage_ok': leakage_ok,
        'regime_coverage': regime_coverage,
        'quality_deciles': quality_deciles,
        'sizing_stability': sizing_stability,
        'issues': issues
    }


def main():
    """Main validation function"""
    
    print("Regime-Aware System Validator v2")
    print("Testing implementation without assuming improvements\n")
    
    # Get historical data
    df = get_historical_data()
    if df is None:
        return
    
    # Test 1: Lookahead leakage
    leakage_ok = test_lookahead_leakage(df)
    
    # Test 2: Regime coverage
    regime_coverage = test_regime_coverage(df)
    
    # Test 3: Quality discrimination
    quality_deciles = test_quality_discrimination(df)
    
    # Test 4: Sizing stability
    sizing_stability = test_sizing_stability(quality_deciles)
    
    # Generate report
    report = generate_validation_report(leakage_ok, regime_coverage, quality_deciles, sizing_stability)
    
    return report


if __name__ == "__main__":
    report = main()
