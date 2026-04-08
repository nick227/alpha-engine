from __future__ import annotations

import re
from collections import Counter
from typing import Dict, Iterable
from uuid import uuid4

from app.core.types import RawEvent, ScoredEvent

CATEGORY_RULES: list[tuple[str, tuple[str, ...], str]] = [
    ("guidance_raise", ("raises guidance", "raised guidance", "outlook raised", "stronger than expected"), "positive"),
    ("guidance_cut", ("cuts guidance", "lowered guidance", "outlook cut", "weaker than expected"), "negative"),
    ("supplier_disruption", ("shortage", "supply chain", "disruption", "delay", "shipment issue"), "negative"),
    ("regulatory_approval", ("approval", "authorized", "cleared", "green light"), "positive"),
    ("datacenter_demand", ("datacenter", "gpu demand", "ai infrastructure", "capex", "server demand"), "positive"),
    ("dilution_risk", ("offering", "secondary offering", "dilution", "capital raise"), "negative"),
]

INTENSITY_TERMS = {
    "surge": 0.15,
    "record": 0.15,
    "massive": 0.10,
    "sharply": 0.08,
    "stronger": 0.06,
    "weaker": 0.06,
}

TERM_CLOUDS: Dict[str, Dict[str, float]] = {
    "NVDA": {"nvidia": 1.0, "gpu": 0.8, "datacenter": 0.6, "ai infrastructure": 0.5, "copper": 0.1},
    "AMD": {"amd": 1.0, "gpu": 0.6, "datacenter": 0.4, "server": 0.3},
    "SMCI": {"super micro": 1.0, "server": 0.8, "rack": 0.5, "datacenter": 0.4},
    "AAPL": {"apple": 1.0, "iphone": 0.8, "app store": 0.4, "services": 0.4},
    "TSLA": {"tesla": 1.0, "ev": 0.6, "battery": 0.5, "lithium": 0.2},
}

STOPWORDS = {
    "the", "a", "an", "and", "or", "for", "to", "of", "in", "on", "after", "with", "over", "under",
    "despite", "into", "from", "at", "by", "new", "points", "concern", "wins"
}


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(value, high))


def _normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s.%/-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _pick_category(text: str) -> tuple[str, str]:
    for category, terms, direction in CATEGORY_RULES:
        if any(term in text for term in terms):
            return category, direction
    return "general_media", "neutral"


def _weighted_match_score(text: str, ticker: str) -> tuple[float, list[str]]:
    cloud = TERM_CLOUDS.get(ticker.upper(), {})
    hits: list[str] = []
    score = 0.0
    for term, weight in cloud.items():
        if term in text:
            score += weight
            hits.append(term)
    return score, hits


def _extract_explanation_terms(text: str, seeded_hits: Iterable[str]) -> list[str]:
    hits = list(seeded_hits)
    if hits:
        return hits[:5]
    tokens = [t for t in text.split() if t not in STOPWORDS and len(t) > 2]
    return [word for word, _ in Counter(tokens).most_common(5)]


def _build_concept_tags(text: str, category: str, explanation_terms: list[str]) -> list[str]:
    tags = [category]
    if "datacenter" in text or "server" in text:
        tags.append("AI_infrastructure")
    if "supply" in text or "shortage" in text or "delay" in text:
        tags.append("supply_chain")
    if re.search(r"\b\d+(?:\.\d+)?%\b", text):
        tags.append("numeric_signal")
    for term in explanation_terms[:2]:
        if term not in tags:
            tags.append(term)
    return tags[:5]


def score_event(raw: RawEvent) -> ScoredEvent:
    text = _normalize(raw.text)
    ticker = raw.tickers[0] if raw.tickers else "SPY"
    category, direction = _pick_category(text)
    cloud_score, cloud_hits = _weighted_match_score(text, ticker)
    explanation_terms = _extract_explanation_terms(text, cloud_hits)

    intensity_boost = sum(weight for term, weight in INTENSITY_TERMS.items() if term in text)
    numeric_boost = 0.1 if re.search(r"\b\d+(?:\.\d+)?%\b", text) else 0.0
    materiality = _clamp(0.35 + min(cloud_score * 0.25, 0.4) + intensity_boost + numeric_boost)
    company_relevance = _clamp(0.25 + min(cloud_score * 0.45, 0.7))
    confidence = _clamp(0.42 + (0.28 if category != "general_media" else 0.0) + min(cloud_score * 0.18, 0.22) + numeric_boost)
    concept_tags = _build_concept_tags(text, category, explanation_terms)

    return ScoredEvent(
        id=str(uuid4()),
        raw_event_id=raw.id,
        primary_ticker=ticker,
        category=category,
        materiality=materiality,
        direction=direction,
        confidence=confidence,
        company_relevance=company_relevance,
        concept_tags=concept_tags,
        explanation_terms=explanation_terms,
        scorer_version="v2.1",
        taxonomy_version="v1",
    )
