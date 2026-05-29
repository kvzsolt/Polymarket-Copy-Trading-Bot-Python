from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


CRYPTO_TERMS = {
    "crypto",
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "sol",
    "xrp",
    "doge",
    "bnb",
    "binance",
    "usdt",
    "usdc",
    "stablecoin",
    "blockchain",
    "coinbase",
    "altcoin",
    "memecoin",
    "litecoin",
    "ltc",
}

SPORTS_ESPORTS_TERMS = {
    "nba",
    "nfl",
    "mlb",
    "nhl",
    "soccer",
    "football",
    "tennis",
    "ufc",
    "mma",
    "boxing",
    "f1",
    "formula 1",
    "nascar",
    "cs2",
    "counter-strike",
    "valorant",
    "league of legends",
    "lol",
    "dota",
    "overwatch",
    "esports",
}

POLITICS_MACRO_TERMS = {
    "trump",
    "biden",
    "election",
    "president",
    "senate",
    "congress",
    "fed",
    "interest rate",
    "cpi",
    "inflation",
    "recession",
    "gdp",
    "unemployment",
    "tariff",
    "war",
    "ceasefire",
}


@dataclass(frozen=True)
class MarketClass:
    family: str
    is_crypto: bool
    is_fast_crypto: bool
    is_sports_esports: bool
    is_politics_macro: bool


class CandidateClassifier:
    def __init__(self, config):
        self.config = config

    def classify_signal(self, signal, market=None) -> MarketClass:
        text = " ".join(
            str(x or "").lower()
            for x in [
                getattr(signal, "title", ""),
                getattr(signal, "market_slug", ""),
                getattr(signal, "event_slug", ""),
                getattr(signal, "outcome", ""),
                getattr(market, "question", "") if market else "",
                getattr(market, "slug", "") if market else "",
                getattr(market, "event_slug", "") if market else "",
                getattr(market, "category", "") if market else "",
            ]
        )
        is_crypto = any(term in text for term in CRYPTO_TERMS)
        is_sports = any(term in text for term in SPORTS_ESPORTS_TERMS)
        is_politics = any(term in text for term in POLITICS_MACRO_TERMS)

        resolution_minutes = None
        if market and getattr(market, "end_date_ts", None):
            resolution_minutes = (int(market.end_date_ts) - int(signal.first_ts)) / 60.0

        fast_crypto_max = float(
            self.config.discovery.discovery_crypto_fast_max_resolution_minutes
        )
        is_fast_crypto = bool(
            is_crypto
            and resolution_minutes is not None
            and resolution_minutes <= fast_crypto_max
        )

        if is_fast_crypto:
            family = "fast_crypto"
        elif is_crypto:
            family = "crypto"
        elif is_sports:
            family = "sports_esports"
        elif is_politics:
            family = "politics_macro"
        else:
            family = "other"

        return MarketClass(
            family=family,
            is_crypto=is_crypto,
            is_fast_crypto=is_fast_crypto,
            is_sports_esports=is_sports,
            is_politics_macro=is_politics,
        )


def is_fast_crypto_specialist(classes: list[MarketClass], signals: list, config) -> bool:
    if not signals:
        return False

    fast_crypto_count = sum(1 for c in classes if c.is_fast_crypto)
    non_crypto_count = sum(1 for c in classes if not c.is_crypto)
    signal_ratio = fast_crypto_count / len(signals)

    fast_crypto_notional = sum(
        (s.total_usdc_size for s, c in zip(signals, classes) if c.is_fast_crypto),
        start=Decimal("0"),
    )
    total_notional = sum((s.total_usdc_size for s in signals), start=Decimal("0"))
    notional_ratio = (
        float(fast_crypto_notional / total_notional) if total_notional > 0 else 0.0
    )

    return (
        signal_ratio >= config.discovery.discovery_crypto_fast_reject_signal_ratio
        or notional_ratio >= config.discovery.discovery_crypto_fast_reject_notional_ratio
    ) and non_crypto_count < config.discovery.discovery_crypto_fast_min_non_crypto_signals
