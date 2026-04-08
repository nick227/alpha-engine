from __future__ import annotations
from app.ingest.adapters.base import SourceAdapter
from app.ingest.adapters.alpaca_news import AlpacaNewsAdapter
from app.ingest.adapters.yahoo_finance import YahooFinanceAdapter
from app.ingest.adapters.fred_macro import FredMacroAdapter
from app.ingest.adapters.reddit_social import RedditSocialAdapter
from app.ingest.adapters.custom_bundle import CustomBundleAdapter

ADAPTERS = {
    "alpaca_news": AlpacaNewsAdapter(),
    "yahoo_finance": YahooFinanceAdapter(),
    "fred_macro": FredMacroAdapter(),
    "reddit_social": RedditSocialAdapter(),
    "custom_bundle": CustomBundleAdapter(),
}

def resolve_adapter(name: str) -> SourceAdapter | None:
    return ADAPTERS.get(name)

