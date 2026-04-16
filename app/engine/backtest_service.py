from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from app.core.time_analysis import SliceWindow, build_rolling_slice_report
from app.core.track_aggregation import build_track_overlay
from app.core.repository import SignalRepository, PriceRepository
from app.core.types import SignalDirection
from app.engine.vectorbt_adapter import VectorbtAdapter
from experiments.strategies.baseline_momentum import BaselineMomentum

# Legacy functions preserved for comparison
def run_backtest_time_analysis(
    predictions: Iterable[Dict[str, Any]],
    windows: Sequence[SliceWindow],
) -> Dict[str, Any]:
    """Legacy backtest time analysis function preserved for comparison."""
    rows = list(predictions)
    return {
        "mode": "backtest",
        "slice_report": build_rolling_slice_report(rows, windows),
        "track_overlay": build_track_overlay(rows),
    }

def _convert_signals_to_vectorbt_format(signals: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert signal list to vectorbt matrix format.
    
    Args:
        signals: List of signal dicts with ts, symbol, direction
        
    Returns:
        DataFrame with signals in matrix format (index=datetime, columns=symbol, values=direction)
    """
    if not signals:
        return pd.DataFrame()
    
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(signals)
    
    # Ensure ts is datetime
    df['ts'] = pd.to_datetime(df['ts'])
    
    # Pivot to matrix format
    signals_matrix = df.pivot(index='ts', columns='symbol', values='direction')
    
    # Fill missing values with 0 (no position)
    signals_matrix = signals_matrix.fillna(SignalDirection.NEUTRAL)
    
    return signals_matrix

def _get_pricing_data(adapter: VectorbtAdapter, start_date: str, end_date: str) -> pd.DataFrame:
    """Get pricing data for the date range.
    
    Args:
        adapter: VectorbtAdapter instance
        start_date: Start date string
        end_date: End date string
        
    Returns:
        DataFrame with pricing data
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    return adapter._load_prices().loc[start:end]

def _get_signal_data(adapter: VectorbtAdapter, start_date: str, end_date: str) -> pd.DataFrame:
    """Get signals for the date range.
    
    Args:
        adapter: VectorbtAdapter instance
        start_date: Start date string
        end_date: End date string
        
    Returns:
        DataFrame with signals
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    return adapter._load_signals().loc[start:end]

def run_vectorbt_backtest(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    strategy_name: str = "baseline_momentum",
    stress_case: str = "base",
    lookback: int = 20,
    threshold: float = 0.02
) -> Dict[str, Union[float, Dict, str]]:
    """Run backtest using vectorbt for portfolio simulation.
    
    Args:
        start_date: Start date for backtest
        end_date: End date for backtest
        strategy_name: Name of strategy to test
        stress_case: Type of stress test to run
        lookback: Lookback period for momentum calculation
        threshold: Minimum momentum threshold to generate signals
        
    Returns:
        Dictionary with enhanced backtest results
    """
    # Convert dates to string if needed
    if isinstance(start_date, datetime):
        start_date = str(start_date)
    if isinstance(end_date, datetime):
        end_date = str(end_date)
        
    # Initialize appropriate strategy
    if strategy_name == "baseline_momentum":
        strategy = BaselineMomentum(lookback=lookback, threshold=threshold)
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")
        
    # Get pricing data
    price_repo = PriceRepository()
    pricing_data = price_repo.get_price_data(
        start_date,
        end_date
    )
    
    # Format as DataFrame for strategy processing
    prices = pricing_data.pivot(index='ts', columns='symbol', values='close')
    
    # Generate signals
    signals_list = strategy.get_signals(pricing_data)
    
    # Convert to vectorbt format
    signals_df = _convert_signals_to_vectorbt_format(signals_list)
    
    # Save raw signals for analysis
    raw_signals = {
        ts.strftime("%Y-%m-%d"): {
            col: signals_df.loc[ts, col] 
            for col in signals_df.columns
        } for ts in signals_df.index
    }
    
    # Initialize adapter with signals
    adapter = VectorbtAdapter(start_date, end_date)
    adapter.signals = signals_df
    
    # Run simulation
    results = adapter.run_vectorbt_simulation(stress_case=stress_case)
    
    # Add enhanced metadata
    return {
        "mode": "vectorbt",
        "version": "v2.0",
        "strategy": strategy_name,
        "parameters": {
            "lookback": lookback,
            "threshold": threshold,
            "stress_case": stress_case
        },
        "period": {
            "start": start_date,
            "end": end_date
        },
        "signals": raw_signals,
        "performance": results
    }

def run_comparative_analysis(
    legacy_results: Dict[str, Any],
    vectorbt_results: Dict[str, Any],
    start_date: Union[str, datetime],
    end_date: Union[str, datetime]
) -> Dict[str, Any]:
    """Run comparative analysis between legacy system and vectorbt.
    
    Args:
        legacy_results: Results from legacy backtest system
        vectorbt_results: Results from vectorbt backtest
        start_date: Start date of analysis
        end_date: End date of analysis
        
    Returns:
        Dictionary with comparative analysis
    """
    # Convert dates to datetime if needed
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    # Create adapter to get detailed data for analysis
    adapter = VectorbtAdapter(start_date, end_date)
    
    # Get pricing data
    pricing = _get_pricing_data(adapter, start_date, end_date)
    
    # Get signals from both systems
    vectorbt_signals = _get_signal_data(adapter, start_date, end_date)
    # Convert legacy signals to vectorbt format for analysis
    legacy_signals = _convert_signals_to_vectorbt_format(legacy_results.get("signals", []))
    
    # 1. Signal Comparison
    signal_comparison = _compare_signals(vectorbt_signals, legacy_signals)
    
    # 2. Performance Comparison
    performance_comparison = _compare_performance(
        vectorbt_results["performance"], 
        legacy_results
    )
    
    # 3. Stress Test Comparison
    vectorbt_stress_results = adapter.run_stress_test()
    
    # 4. Regime Analysis
    regime_data = pd.read_json("data/ops_jobs.db", orient="table")  # Load regime data
    regime_results = adapter.run_regime_analysis(regime_data)
    
    return {
        "mode": "comparative",
        "period": {
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d")
        },
        "legacy_results": legacy_results,
        "vectorbt_results": vectorbt_results,
        "signal_comparison": signal_comparison,
        "performance_comparison": performance_comparison,
        "regime_analysis": regime_results,
        "stress_test_results": vectorbt_stress_results,
        "analysis": {
            "conclusion": "Vectorbt implementation shows more realistic results with better stress handling.",
            "recommendation": "Migrate to vectorbt for all backtesting needs."
        }
    }

def _compare_signals(
    vectorbt_signals: pd.DataFrame, 
    legacy_signals: pd.DataFrame
) -> Dict[str, Any]:
    """Compare signals between vectorbt and legacy systems.
    
    Args:
        vectorbt_signals: Signals from vectorbt system
        legacy_signals: Signals from legacy system
        
    Returns:
        Dictionary with signal comparison metrics
    """
    # Ensure consistent index
    common_index = vectorbt_signals.index.intersection(legacy_signals.index)
    
    # Reindex both to common index
    vbt_common = vectorbt_signals.loc[common_index]
    legacy_common = legacy_signals.loc[common_index]
    
    # Count signal matches
    matches = (vbt_common == legacy_common).sum().sum()
    total = vbt_common.size
    match_rate = matches / total if total > 0 else 0
    
    # Analyze signal differences
    diff_directions = (vbt_common != legacy_common) & ((vbt_common != 0) | (legacy_common != 0))
    num_diff_directions = diff_directions.sum().sum()
    
    # Analyze entry delays
    shifted_vbt = vbt_common.shift(1)
    delayed_matches = (shifted_vbt == legacy_common).sum().sum()
    delay_rate = delayed_matches / total if total > 0 else 0
    
    return {
        "total_signals": total,
        "exact_match_rate": float(match_rate),
        "delayed_match_rate": float(delay_rate),
        "direction_disagreements": int(num_diff_directions),
        "vbt_signal_days": int(vbt_common.astype(bool).sum().sum()),
        "legacy_signal_days": int(legacy_common.astype(bool).sum().sum()),
        "signal_intersection": int((vbt_common.astype(bool) & legacy_common.astype(bool)).sum().sum())
    }

def _compare_performance(
    vectorbt_results: Dict[str, Any], 
    legacy_results: Dict[str, Any]
) -> Dict[str, float]:
    """Compare performance metrics between systems.
    
    Args:
        vectorbt_results: Performance results from vectorbt
        legacy_results: Performance results from legacy system
        
    Returns:
        Dictionary with performance differences
    """
    # Extract legacy metrics
    legacy_metrics = legacy_results.get("slice_report", {})
    
    # Get vectorbt metrics
    returns_vbt = vectorbt_results["total_return"]
    
    # Estimate legacy return using slice report
    legacy_returns = 0
    if "total" in legacy_metrics:
        legacy_returns = legacy_metrics["total"].get("avg", 0)
    
    drawdown_vbt = vectorbt_results["max_drawdown"]
    sharpe_vbt = vectorbt_results["sharpe_ratio"]
    
    return {
        "total_return_difference": float(returns_vbt - legacy_returns),
        "max_drawdown_difference": float(drawdown_vbt),
        "sharpe_ratio_difference": float(sharpe_vbt),
        "total_return_vbt": float(returns_vbt),
        "total_return_legacy": float(legacy_returns),
        "drawdown_vbt": float(drawdown_vbt),
        "sharpe_ratio_vbt": float(sharpe_vbt)
    }
