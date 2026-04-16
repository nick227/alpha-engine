import pandas as pd
import numpy as np
import vectorbt as vbt
from datetime import timedelta
from app.core.repository import SignalRepository, PriceRepository
from app.core.types import Signal, PriceData

class VectorbtAdapter:
    """Adapter for vectorbt portfolio simulation with advanced cost modeling.
    
    Responsibilities:
    1. Load prices and signals from DB
    2. Pivot data to matrix format
    3. Enforce T+1 shift for signals
    4. Run vectorbt simulations with sophisticated costs
    5. Return standardized results
    """
    
    def __init__(self, start_date: str, end_date: str):
        """Initialize adapter with date range."""
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.signal_repo = SignalRepository()
        self.price_repo = PriceRepository()
        
    def _load_prices(self) -> pd.DataFrame:
        """Load price data for all symbols in the period.
        
        Returns:
            DataFrame with price data in matrix format
            (index=datetime, columns=symbol, values=price)
        """
        price_data = self.price_repo.get_price_data(
            self.start_date - timedelta(days=1),  # Get previous day's close for T+1
            self.end_date
        )
        
        # Pivot to matrix format
        prices = price_data.pivot(index='ts', columns='symbol', values='close')
        
        # Forward fill missing prices
        prices = prices.ffill()
        
        return prices
    
    def _load_signals(self) -> pd.DataFrame:
        """Load signals and convert to matrix format.
        
        Returns:
            DataFrame with signals in matrix format
            (index=datetime, columns=symbol, values=signal_direction)
        """
        signals = self.signal_repo.get_signals(
            self.start_date,
            self.end_date
        )
        
        # Convert to DataFrame if not already
        if not isinstance(signals, pd.DataFrame):
            signals = pd.DataFrame([s.model_dump() for s in signals])
            
        # Pivot to matrix format
        signals_matrix = signals.pivot(
            index='ts', 
            columns='symbol', 
            values='direction'
        )
        
        # Fill missing symbols with 0 (no position)
        # Keep all dates in range even if no signals
        symbols = signals_matrix.columns.unique()
        date_range = pd.date_range(
            start=self.start_date,
            end=self.end_date,
            freq='D' if 'D' in prices.index.freq else prices.index.freq
        )
        
        # Reindex with date range and symbols
        signals_resampled = signals_matrix.reindex(index=date_range, columns=symbols, fill_value=0)
        
        return signals_resampled
    
    def _enforce_t_plus_1(self, signals: pd.DataFrame) -> pd.DataFrame:
        """Enforce T+1 execution delay.
        
        Args:
            signals: Signal matrix with current date index
            
        Returns:
            DataFrame with signals shifted by one trading day
        """
        # First ensure the index is a DatetimeIndex
        if not isinstance(signals.index, pd.DatetimeIndex):
            signals.index = pd.to_datetime(signals.index)
            
        # Shift signals by 1 period
        shifted_signals = signals.shift(1)
        
        # First day gets zero positions
        shifted_signals.iloc[0] = 0
        
        return shifted_signals
    
    def _create_cost_adjustment(self, prices: pd.DataFrame, signals: pd.DataFrame, 
                              volatility: pd.DataFrame, volume: pd.DataFrame,
                              base_cost: float = 0.001) -> tuple:
        """Create cost-adjusted price matrix with advanced transaction cost modeling.
        
        Args:
            prices: Price data in matrix format
            signals: Signal matrix with directions
            volatility: Volatility matrix for each asset
            volume: Volume matrix for each asset
            base_cost: Starting cost basis (market maker fees)
            
        Returns:
            tuple: (long_cost_adjustment, short_cost_adjustment)
        """
        # 1. Slippage from volatility
        volatility_slippage = 0.1 * (volatility.ffill() / prices.ffill())
        
        # 2. Volume impact
        volume_rank = volume.rank(pct=True)
        volume_impact = (1 - volume_rank) * base_cost * 0.5  # High volume reduces impact
        
        # 3. Market impact from position size
        position_sizes = (signals != 0).groupby(signals.index).count()
        total_positions = position_sizes.sum(axis=1)
        market_impact = total_positions.rolling(5).mean() / 100 * base_cost
        
        # Calculate long/short cost multipliers
        long_cost_multiplier = (
            1 + base_cost + 
            volatility_slippage + 
            volume_impact + 
            market_impact.reindex(volatility.index, method='ffill')
        )
        
        # Shorts cost more than longs by volatility factor
        short_cost_multiplier = long_cost_multiplier * (1 + volatility.ffill() / prices.ffill())
        
        return long_cost_multiplier, short_cost_multiplier
    
    def _create_mask(self, signals: pd.DataFrame, mask_type: str = 'entries') -> pd.DataFrame:
        """Create entry/exit mask from signals with improved liquidity filtering.
        
        Args:
            signals: Signal matrix with direction values
            mask_type: Type of mask to create ('entries' or 'exits')
            
        Returns:
            DataFrame with boolean mask for entries or exits
        """
        if mask_type == 'entries':
            return (signals == 1).astype(bool)
        elif mask_type == 'exits':
            return (signals == -1).astype(bool)
        else:
            raise ValueError(f"Unknown mask type: {mask_type}")
    
    def _load_volatility(self, window: int = 20) -> pd.DataFrame:
        """Load volatility data for all symbols in the period."""
        return self.price_repo.get_volatility_stats(window)
    
    def _load_volume(self) -> pd.DataFrame:
        """Load volume data for all symbols in the period."""
        return self.price_repo.get_volume_profile()
    
    def run_vectorbt_simulation(self, stress_case: str = "base") -> dict:
        """Run portfolio simulation with sophisticated transaction cost modeling.
        
        Args:
            stress_case: Type of stress test to run ('base', 'liquidity', 'volatility', 'crowded')
            
        Returns:
            Dictionary with simulation results including robust metrics
        """
        # 1. Load data
        prices = self._load_prices()
        signals = self._load_signals()
        volatility = self._load_volatility()
        volume = self._load_volume()
        
        # 2. Enforce T+1 execution
        shifted_signals = self._enforce_t_plus_1(signals)
        
        # 3. Create entry and exit masks
        entries = self._create_mask(shifted_signals, 'entries')
        exits = self._create_mask(shifted_signals, 'exits')
        
        # 4. Apply cost-adjusted pricing
        long_costs, short_costs = self._create_cost_adjustment(
            prices=prices,
            signals=signals,
            volatility=volatility,
            volume=volume
        )
        
        # 5. Run simulation with stress case adjustments
        if stress_case == "liquidity":
            stress_factor = 2.0  # Double cost for low-volume trades
            long_costs *= stress_factor
            short_costs *= stress_factor
            
        elif stress_case == "volatility":
            # Add volatility-based slippage
            vol_slippage = volatility.ffill() / prices.ffill()
            long_costs += vol_slippage
            short_costs += vol_slippage
            
        elif stress_case == "crowded":
            # Crowded trades have higher impact
            crowding_factor = 1.5
            long_costs *= crowding_factor
            short_costs *= crowding_factor
            
        # Apply cost models to prices
        long_adjusted = prices.ffill() * long_costs
        short_adjusted = prices.ffill() * (1 - short_costs)
        
        pf = vbt.Portfolio.from_signals(
            prices=prices,
            entries=entries,
            exits=exits,
            long_entries=entries,
            long_exits=exits,
            short_entries=exits,  # Invert positions for shorts
            short_exits=entries,
            val_price=prices,
            init_cash=100000,
            fees=0.001,
            slippage=0.001,
            freq='D'
        )
        
        # 6. Get metrics (enhanced)
        metrics = {
            'total_return': float(pf.total_return()),
            'sharpe_ratio': float(pf.sharpe_ratio()),
            'max_drawdown': float(pf.max_drawdown()),
            'win_rate': float(pf.win_rate()),
            'equity_curve': pf.equity.to_dict(),
            'total_trades': len(pf.trades),
            'avg_return': float(pf.trades.avg_return()),
            'trade_duration': str(pf.trades.avg_duration()),
            'final_value': float(pf.final_value()),
            'avg_cost_per_trade': float(pf.trades.avg_pnl_per_order() / 
                                      (pf.trades.avg_cum_fees() + 0.0001)),
            'cost_ratio': float((pf.fees_paid + pf.slippage_cost) / abs(pf.total_pnl))
        }
        
        return metrics
    
    def run_stress_test(self, slippage_range: list = [0.001, 0.002, 0.003, 0.004, 0.005], 
                       delay_days: int = 1) -> dict:
        """Run stress tests with varying slippage and execution delays.
        
        Args:
            slippage_range: List of slippage percentages to test
            delay_days: Number of days to delay execution
            
        Returns:
            Dictionary with stress test results
        """
        results = {}
        
        for slippage in slippage_range:
            # Delay execution by specified days
            if delay_days > 0:
                self.start_date = self.start_date - timedelta(days=delay_days)
                delay_results = self.run_vectorbt_simulation(slippage)
                results[f"delay_{delay_days}d_slippage_{slippage:.3%}"] = delay_results
                
            # Regular execution
            base_results = self.run_vectorbt_simulation(slippage)
            results[f"slippage_{slippage:.3%}"] = base_results
            
        return results
    
    def run_regime_analysis(self, regime_data: pd.DataFrame) -> dict:
        """Run analysis by regime.
        
        Args:
            regime_data: DataFrame with regime labels (index=dts, values=regime)
            
        Returns:
            Dictionary with results by regime
        """
        # Convert regime data to DatetimeIndex if needed
        if not isinstance(regime_data.index, pd.DatetimeIndex):
            regime_data.index = pd.to_datetime(regime_data.index)
            
        unique_regimes = regime_data.iloc[:, 0].unique()
        
        results = {}
        for regime in unique_regimes:
            regime_mask = regime_data.iloc[:, 0] == regime
            
            regime_start = regime_mask.index[regime_mask.argmin()]
            regime_end = regime_mask.index[- regime_mask[::-1].argmin()]
            
            # Create adapter for this regime
            regime_adapter = VectorbtAdapter(
                start_date=regime_start,
                end_date=regime_end
            )
            
            regime_results = regime_adapter.run_vectorbt_simulation()
            results[f"regime_{regime}"] = regime_results
            
        return results
