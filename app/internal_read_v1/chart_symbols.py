"""Ticker symbol normalization for internal read API."""

from __future__ import annotations


def normalize_ticker(ticker: str) -> str:
    return str(ticker or "").strip().upper()
