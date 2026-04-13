from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class DiscoveryCandidate:
    symbol: str
    strategy_type: str
    score: float
    reason: str
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class FeatureRow:
    symbol: str
    as_of_date: str
    close: float | None
    volume: float | None
    dollar_volume: float | None
    avg_dollar_volume_20d: float | None
    return_1d: float | None
    return_5d: float | None
    return_20d: float | None
    return_63d: float | None
    return_252d: float | None
    volatility_20d: float | None
    max_drawdown_252d: float | None
    price_percentile_252d: float | None
    volume_zscore_20d: float | None
    dollar_volume_zscore_20d: float | None
    revenue_ttm: float | None
    revenue_growth: float | None
    shares_outstanding: float | None
    shares_growth: float | None
    sector: str | None
    industry: str | None
    sector_return_63d: float | None
    peer_relative_return_63d: float | None
    price_bucket: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "as_of_date": self.as_of_date,
            "close": self.close,
            "volume": self.volume,
            "dollar_volume": self.dollar_volume,
            "avg_dollar_volume_20d": self.avg_dollar_volume_20d,
            "return_1d": self.return_1d,
            "return_5d": self.return_5d,
            "return_20d": self.return_20d,
            "return_63d": self.return_63d,
            "return_252d": self.return_252d,
            "volatility_20d": self.volatility_20d,
            "max_drawdown_252d": self.max_drawdown_252d,
            "price_percentile_252d": self.price_percentile_252d,
            "volume_zscore_20d": self.volume_zscore_20d,
            "dollar_volume_zscore_20d": self.dollar_volume_zscore_20d,
            "revenue_ttm": self.revenue_ttm,
            "revenue_growth": self.revenue_growth,
            "shares_outstanding": self.shares_outstanding,
            "shares_growth": self.shares_growth,
            "sector": self.sector,
            "industry": self.industry,
            "sector_return_63d": self.sector_return_63d,
            "peer_relative_return_63d": self.peer_relative_return_63d,
            "price_bucket": self.price_bucket,
        }

