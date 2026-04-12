from __future__ import annotations
from app.ingest.adapters.base import SourceAdapter
from app.ingest.adapters.alpaca_news import AlpacaNewsAdapter
from app.ingest.adapters.yahoo_finance import YahooFinanceAdapter
from app.ingest.adapters.fred_macro import FredMacroAdapter
from app.ingest.adapters.reddit_social import RedditSocialAdapter
from app.ingest.adapters.custom_bundle import CustomBundleAdapter
from app.ingest.adapters.google_trends import GoogleTrendsAdapter
from app.ingest.adapters.etf_flows import EtfFlowsAdapter
from app.ingest.adapters.earnings_calendar import EarningsCalendarAdapter
from app.ingest.adapters.options_flow import OptionsFlowAdapter
from app.ingest.adapters.fear_greed import FearGreedAdapter
from app.ingest.adapters.cross_asset import CrossAssetAdapter
from app.ingest.adapters.market_breadth import MarketBreadthAdapter
from app.ingest.adapters.market_baseline import MarketBaselineAdapter
from app.ingest.adapters.yfinance_macro import YFinanceMacroAdapter

# Dump adapters — read pre-downloaded parquet files; no live API calls.
from app.ingest.adapters.stooq_dump import StooqDumpAdapter
from app.ingest.adapters.fnspid_dump import FnspidDumpAdapter
from app.ingest.adapters.fred_dump import FredDumpAdapter
from app.ingest.adapters.csv_price_dump import CSVPriceDumpAdapter
from app.ingest.adapters.analyst_ratings_dump import AnalystRatingsDumpAdapter
from app.ingest.adapters.alpha_vantage_dump import AlphaVantageDumpAdapter
from app.ingest.adapters.tiingo_dump import TiingoDumpAdapter

ADAPTERS = {
    # ── API adapters (live; skipped for historical windows) ─────────── #
    "alpaca_news": AlpacaNewsAdapter(),
    "yahoo_finance": YahooFinanceAdapter(),
    "fred_macro": FredMacroAdapter(),
    "reddit_social": RedditSocialAdapter(),
    "custom_bundle": CustomBundleAdapter(),
    "google_trends": GoogleTrendsAdapter(),
    "etf_flows": EtfFlowsAdapter(),
    "earnings_calendar": EarningsCalendarAdapter(),
    "options_flow": OptionsFlowAdapter(),
    "fear_greed": FearGreedAdapter(),
    "cross_asset": CrossAssetAdapter(),
    "market_breadth": MarketBreadthAdapter(),
    "market_baseline": MarketBaselineAdapter(),
    "yfinance_macro": YFinanceMacroAdapter(),

    # ── Dump adapters (priority 1; serve all historical data) ────────── #
    "stooq_dump": StooqDumpAdapter(),
    "fnspid_dump": FnspidDumpAdapter(),
    "fred_dump": FredDumpAdapter(),
    "csv_price_dump": CSVPriceDumpAdapter(),
    "analyst_ratings_dump": AnalystRatingsDumpAdapter(),
    "alpha_vantage_dump": AlphaVantageDumpAdapter(),
    "tiingo_dump": TiingoDumpAdapter(),
}

def resolve_adapter(name: str) -> SourceAdapter | None:
    return ADAPTERS.get(name)
