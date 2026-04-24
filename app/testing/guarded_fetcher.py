"""
Guarded Data Fetcher - Hard network blocking for dry-run/no-fetch modes.

Wraps all data providers with:
- Budget enforcement (api_calls, days, tickers)
- Counter tracking (cache hits/misses, errors)
- Hard blocks in no-fetch mode
- Retry loop protection
"""

from __future__ import annotations

import logging
import functools
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar('T')


class NetworkBlockedError(RuntimeError):
    """Raised when network fetch attempted in no-fetch mode."""
    pass


class BudgetExceededError(RuntimeError):
    """Raised when API call budget exceeded."""
    pass


# Import TestPhase for type hints only
class TestPhase(Enum):
    """Placeholder - real TestPhase imported in safe_backfill."""
    pass


@dataclass
class ExecutionCounters:
    """Comprehensive counters for testing verification."""
    
    # API & Cache
    api_calls: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    
    # Pipeline
    events: int = 0
    predictions: int = 0
    trades: int = 0
    outcomes: int = 0
    
    # Learning
    learner_updates: int = 0
    weight_updates: int = 0
    
    # Errors
    errors: int = 0
    warnings: int = 0
    
    def reset(self):
        """Reset all counters to zero."""
        self.api_calls = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.events = 0
        self.predictions = 0
        self.trades = 0
        self.outcomes = 0
        self.learner_updates = 0
        self.weight_updates = 0
        self.errors = 0
        self.warnings = 0
    
    def check_minima(self, phase) -> Tuple[bool, List[str]]:
        """
        Verify minimum counts for each phase.
        Returns (passed, list of failures).
        """
        failures = []
        phase_value = getattr(phase, 'value', str(phase))
        
        if 'dry' in phase_value:
            if self.api_calls > 0:
                failures.append(f"DRY RUN: api_calls={self.api_calls}, expected 0")
            if self.predictions == 0:
                failures.append(f"DRY RUN: predictions={self.predictions}, expected >0")
            if self.trades == 0:
                failures.append(f"DRY RUN: trades={self.trades}, expected >0")
            if self.learner_updates == 0:
                failures.append(f"DRY RUN: learner_updates={self.learner_updates}, expected >0")
                
        elif 'single' in phase_value:
            if self.api_calls == 0:
                failures.append(f"SINGLE DAY: api_calls={self.api_calls}, expected >0")
            if self.events < 10:
                failures.append(f"SINGLE DAY: events={self.events}, expected >=10")
            if self.predictions < 5:
                failures.append(f"SINGLE DAY: predictions={self.predictions}, expected >=5")
            if self.trades < 1:
                failures.append(f"SINGLE DAY: trades={self.trades}, expected >=1")
            if self.outcomes < 1:
                failures.append(f"SINGLE DAY: outcomes={self.outcomes}, expected >=1")
            if self.weight_updates == 0:
                failures.append(f"SINGLE DAY: weight_updates={self.weight_updates}, expected >0")
                
        elif 'cache' in phase_value:
            if self.api_calls > 0:
                failures.append(f"CACHE TEST: api_calls={self.api_calls}, expected 0")
            if self.cache_hits == 0:
                failures.append(f"CACHE TEST: cache_hits={self.cache_hits}, expected >0")
                
        return len(failures) == 0, failures
    
    def log_summary(self):
        """Log all counters in formatted table."""
        logger.info("=" * 50)
        logger.info("EXECUTION COUNTERS SUMMARY")
        logger.info("=" * 50)
        logger.info(f"API & Cache:")
        logger.info(f"  api_calls:     {self.api_calls}")
        logger.info(f"  cache_hits:    {self.cache_hits}")
        logger.info(f"  cache_misses:  {self.cache_misses}")
        logger.info(f"Pipeline:")
        logger.info(f"  events:        {self.events}")
        logger.info(f"  predictions:   {self.predictions}")
        logger.info(f"  trades:        {self.trades}")
        logger.info(f"  outcomes:      {self.outcomes}")
        logger.info(f"Learning:")
        logger.info(f"  learner_updates: {self.learner_updates}")
        logger.info(f"  weight_updates:  {self.weight_updates}")
        logger.info(f"Errors:")
        logger.info(f"  errors:        {self.errors}")
        logger.info(f"  warnings:      {self.warnings}")
        logger.info("=" * 50)


@dataclass
class BudgetGuard:
    """Hard limits to prevent runaway API usage."""
    
    max_api_calls: int = 500
    max_days: int = 5
    max_tickers: int = 5
    
    current_api_calls: int = 0
    current_days: int = 0
    current_tickers: int = 0
    
    def check_budget(self, _operation: str) -> bool:
        """
        Check if operation can proceed within budget.
        Returns True if allowed, False if exceeded.
        """
        if self.current_api_calls >= self.max_api_calls:
            logger.error(f"BUDGET EXCEEDED: api_calls {self.current_api_calls}/{self.max_api_calls}")
            return False
        if self.current_days >= self.max_days:
            logger.error(f"BUDGET EXCEEDED: days {self.current_days}/{self.max_days}")
            return False
        if self.current_tickers >= self.max_tickers:
            logger.error(f"BUDGET EXCEEDED: tickers {self.current_tickers}/{self.max_tickers}")
            return False
        return True
    
    def record_api_call(self, ticker: str = None):
        """Record an API call."""
        self.current_api_calls += 1
        if ticker and ticker not in getattr(self, '_seen_tickers', set()):
            if not hasattr(self, '_seen_tickers'):
                self._seen_tickers = set()
            self._seen_tickers.add(ticker)
            self.current_tickers = len(self._seen_tickers)
    
    def record_days(self, days: int):
        """Record days range."""
        self.current_days = max(self.current_days, days)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current budget status."""
        return {
            'api_calls': {'current': self.current_api_calls, 'max': self.max_api_calls, 'pct': self.current_api_calls / self.max_api_calls * 100},
            'days': {'current': self.current_days, 'max': self.max_days, 'pct': self.current_days / self.max_days * 100},
            'tickers': {'current': self.current_tickers, 'max': self.max_tickers, 'pct': self.current_tickers / self.max_tickers * 100},
        }


@dataclass
class ConfigValidator:
    """
    Phase 0: Configuration Validation
    
    Checks before any testing:
    - adapters enabled
    - keys present
    - cache dir exists
    - DB writable
    - bars provider reachable
    
    Prevents wasting runs on broken config.
    """
    
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def validate(self) -> Tuple[bool, List[str]]:
        """
        Run all validation checks.
        Returns (passed, errors).
        """
        self.errors = []
        self.warnings = []
        
        self._check_adapters()
        self._check_api_keys()
        self._check_cache_dir()
        self._check_database()
        self._check_provider_reachable()
        
        return len(self.errors) == 0, self.errors
    
    def _check_adapters(self):
        """Check required adapters are enabled."""
        try:
            # Check Alpha Vantage adapter
            from app.ingest.alpha_vantage import AlphaVantageAdapter
            adapter = AlphaVantageAdapter()
            if not hasattr(adapter, 'is_enabled'):
                self.warnings.append("Alpha Vantage adapter missing is_enabled check")
        except ImportError:
            self.errors.append("Alpha Vantage adapter not available")
        except Exception as e:
            self.errors.append(f"Alpha Vantage adapter error: {e}")
    
    def _check_api_keys(self):
        """Check API keys present."""
        import os
        
        required_keys = ['ALPHA_VANTAGE_API_KEY', 'POLYGON_API_KEY']
        for key in required_keys:
            value = os.getenv(key)
            if not value:
                self.warnings.append(f"Environment variable {key} not set")
            elif len(value) < 10:
                self.warnings.append(f"Environment variable {key} looks truncated")
    
    def _check_cache_dir(self):
        """Check cache directory exists and writable."""
        import os
        from pathlib import Path
        
        cache_dir = os.getenv('CACHE_DIR', './cache')
        path = Path(cache_dir)
        
        if not path.exists():
            try:
                path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created cache directory: {cache_dir}")
            except Exception as e:
                self.errors.append(f"Cannot create cache directory {cache_dir}: {e}")
                return
        
        if not os.access(path, os.W_OK):
            self.errors.append(f"Cache directory not writable: {cache_dir}")
    
    def _check_database(self):
        """Check database is accessible."""
        try:
            from app.db.database import Database
            db = Database()
            # Try a simple query
            db.execute("SELECT 1")
            logger.info("Database connection verified")
        except ImportError:
            self.warnings.append("Database module not available")
        except Exception as e:
            self.errors.append(f"Database connection failed: {e}")
    
    def _check_provider_reachable(self):
        """Check if bars provider is reachable (lightweight)."""
        try:
            import socket
            # Check if we can resolve the API host
            socket.gethostbyname("www.alphavantage.co")
            logger.info("Bars provider host reachable")
        except Exception as e:
            self.warnings.append(f"Bars provider may be unreachable: {e}")


@dataclass
class FetchContext:
    """Context for guarded fetches."""
    counters: ExecutionCounters
    budget_guard: BudgetGuard
    no_fetch: bool = False
    use_cached_only: bool = False
    dry_run: bool = False
    source: str = "unknown"  # bars, ingest, fallback, retry


def guarded_fetch(
    ctx: FetchContext,
    fetch_func: Callable[..., T],
    args: tuple = None,
    kwargs: dict = None,
    cache_key: str = None,
    cache_lookup: Callable[[str], Optional[T]] = None,
    cache_store: Callable[[str, T], None] = None,
) -> T:
    """
    Execute a guarded fetch with full protection.
    
    Protection layers (in order):
    1. Check no_fetch mode - HARD BLOCK if enabled
    2. Check cache first - return cached if hit
    3. Check budget - HARD BLOCK if exceeded
    4. Execute fetch with counter tracking
    5. Store in cache
    6. Track errors in retry loops
    
    Args:
        ctx: Fetch context with counters, budget, mode flags
        fetch_func: The actual fetch function to call
        *args: Args for fetch_func
        cache_key: Optional cache key for lookup/store
        cache_lookup: Function to check cache
        cache_store: Function to store in cache
        **kwargs: Kwargs for fetch_func
        
    Returns:
        Fetch result
        
    Raises:
        NetworkBlockedError: If no_fetch mode and no cache hit
        BudgetExceededError: If API budget exceeded
    """
    source = ctx.source or "unknown"
    
    # LAYER 1: Hard block in no-fetch mode
    if ctx.no_fetch or ctx.dry_run:
        # Try cache first even in no-fetch mode
        if cache_key and cache_lookup:
            cached = cache_lookup(cache_key)
            if cached is not None:
                logger.debug(f"[{source}] Cache hit (no-fetch mode): {cache_key}")
                ctx.counters.cache_hits += 1
                return cached
        
        # Hard block - no cache and no-fetch mode
        raise NetworkBlockedError(
            f"Network fetch attempted in no-fetch mode from [{source}]. "
            f"Args: {args}, kwargs: {kwargs}. "
            f"Enable network or provide cached data."
        )
    
    # LAYER 2: Check cache first (if not use_cached_only bypass)
    if cache_key and cache_lookup and not ctx.use_cached_only:
        cached = cache_lookup(cache_key)
        if cached is not None:
            logger.debug(f"[{source}] Cache hit: {cache_key}")
            ctx.counters.cache_hits += 1
            return cached
    
    # LAYER 3: Check budget
    if not ctx.budget_guard.check_budget(f"{source}_fetch"):
        raise BudgetExceededError(
            f"Budget exceeded for [{source}]. "
            f"API calls: {ctx.budget_guard.current_api_calls}/{ctx.budget_guard.max_api_calls}, "
            f"Days: {ctx.budget_guard.current_days}/{ctx.budget_guard.max_days}, "
            f"Tickers: {ctx.budget_guard.current_tickers}/{ctx.budget_guard.max_tickers}"
        )
    
    # LAYER 4: Execute fetch with tracking
    logger.debug(f"[{source}] Executing fetch: {cache_key or 'no-cache'}")
    ctx.counters.api_calls += 1
    ctx.budget_guard.record_api_call()
    ctx.counters.cache_misses += 1
    
    try:
        call_args = args or ()
        call_kwargs = kwargs or {}
        result = fetch_func(*call_args, **call_kwargs)
    except Exception as e:
        ctx.counters.errors += 1
        logger.error(f"[{source}] Fetch failed: {e}")
        raise
    
    # LAYER 5: Store in cache
    if cache_key and cache_store:
        try:
            cache_store(cache_key, result)
            logger.debug(f"[{source}] Cached: {cache_key}")
        except Exception as e:
            logger.warning(f"[{source}] Cache store failed: {e}")
    
    return result


def create_guarded_bars_provider(
    original_provider: Callable,
    ctx: FetchContext
) -> Callable:
    """
    Wrap a bars provider with full guarding.
    
    Usage:
        guarded_bars = create_guarded_bars_provider(
            original_fetch_bars,
            FetchContext(counters, budget, no_fetch=True)
        )
        bars = guarded_bars(ticker="SPY", days=30)
    """
    def guarded_bars_fetch(ticker: str, days: int, **kwargs) -> Any:
        # Update days budget tracking
        ctx.budget_guard.record_days(days)
        ctx.budget_guard.record_api_call(ticker)
        
        cache_key = f"bars:{ticker}:{days}:{kwargs.get('interval', '1d')}"
        
        return guarded_fetch(
            ctx=FetchContext(
                counters=ctx.counters,
                budget_guard=ctx.budget_guard,
                no_fetch=ctx.no_fetch,
                use_cached_only=ctx.use_cached_only,
                dry_run=ctx.dry_run,
                source="bars_provider"
            ),
            fetch_func=original_provider,
            cache_key=cache_key,
            args=(ticker, days),
            kwargs=kwargs,
        )
    
    return guarded_bars_fetch


def create_guarded_ingest_adapter(
    original_adapter: Callable,
    ctx: FetchContext
) -> Callable:
    """Wrap an ingest adapter with full guarding."""
    def guarded_ingest(*args, **kwargs) -> Any:
        return guarded_fetch(
            ctx=FetchContext(
                counters=ctx.counters,
                budget_guard=ctx.budget_guard,
                no_fetch=ctx.no_fetch,
                use_cached_only=ctx.use_cached_only,
                dry_run=ctx.dry_run,
                source="ingest_adapter"
            ),
            fetch_func=original_adapter,
            args=args,
            kwargs=kwargs,
        )
    
    return guarded_ingest


def create_guarded_fallback_provider(
    primary: Callable,
    fallback: Callable,
    ctx: FetchContext
) -> Callable:
    """
    Create a guarded fallback provider.
    
    Tracks both primary and fallback calls separately.
    Budget check happens BEFORE any attempt.
    """
    def guarded_fallback(*args, **kwargs) -> Any:
        ctx.source = "primary_provider"
        
        try:
            return guarded_fetch(
                ctx=FetchContext(
                    counters=ctx.counters,
                    budget_guard=ctx.budget_guard,
                    no_fetch=ctx.no_fetch,
                    use_cached_only=ctx.use_cached_only,
                    dry_run=ctx.dry_run,
                    source="primary_provider"
                ),
                fetch_func=primary,
                args=args,
                kwargs=kwargs,
            )
        except Exception as primary_error:
            logger.warning(f"Primary provider failed: {primary_error}")
            
            # Retry with fallback
            ctx.source = "fallback_provider"
            return guarded_fetch(
                ctx=FetchContext(
                    counters=ctx.counters,
                    budget_guard=ctx.budget_guard,
                    no_fetch=ctx.no_fetch,
                    use_cached_only=ctx.use_cached_only,
                    dry_run=ctx.dry_run,
                    source="fallback_provider"
                ),
                fetch_func=fallback,
                args=args,
                kwargs=kwargs,
            )
    
    return guarded_fallback


def with_retry_and_budget(
    max_retries: int = 3,
    ctx: Optional[FetchContext] = None
) -> Callable:
    """
    Decorator for retry loops with budget protection.
    
    Each retry attempt counts toward budget.
    If budget exceeded during retries, raises BudgetExceededError.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    
                    if ctx:
                        ctx.counters.errors += 1
                        
                        # Check budget before retry
                        if not ctx.budget_guard.check_budget(f"retry_{attempt}"):
                            raise BudgetExceededError(
                                f"Budget exceeded during retry {attempt}"
                            ) from e
                    
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            
            # All retries exhausted
            raise last_error
        
        return wrapper
    return decorator


class GuardedCache:
    """
    Cache wrapper that tracks hits/misses in counters.
    """
    
    def __init__(self, cache_impl: Dict[str, Any], counters: ExecutionCounters):
        self.cache = cache_impl
        self.counters = counters
    
    def get(self, key: str) -> Optional[Any]:
        """Get from cache with counter tracking."""
        if key in self.cache:
            self.counters.cache_hits += 1
            logger.debug(f"Cache hit: {key}")
            return self.cache[key]
        else:
            self.counters.cache_misses += 1
            logger.debug(f"Cache miss: {key}")
            return None
    
    def set(self, key: str, value: Any) -> None:
        """Store in cache."""
        self.cache[key] = value
        logger.debug(f"Cache store: {key}")
    
    def clear(self) -> None:
        """Clear cache."""
        self.cache.clear()
        logger.info("Cache cleared")
