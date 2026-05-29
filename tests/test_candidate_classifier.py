from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from domain.discovery_models import MarketMeta
from services.discovery.candidate_classifier import (
    CandidateClassifier,
    is_fast_crypto_specialist,
)
from services.discovery.signal_grouper import GroupedSignal


def _config():
    return SimpleNamespace(
        discovery=SimpleNamespace(
            discovery_crypto_fast_max_resolution_minutes=30,
            discovery_crypto_fast_reject_signal_ratio=0.70,
            discovery_crypto_fast_reject_notional_ratio=0.70,
            discovery_crypto_fast_min_non_crypto_signals=5,
        )
    )


def _signal(title: str, ts: int = 100, notional: str = "10"):
    return GroupedSignal(
        wallet="0x1111111111111111111111111111111111111111",
        condition_id=title,
        token_id=title,
        side="BUY",
        first_ts=ts,
        last_ts=ts,
        total_size=Decimal("10"),
        total_usdc_size=Decimal(notional),
        vwap_price=Decimal("1"),
        tx_hashes=(),
        trade_ids=(),
        title=title,
        market_slug=title.lower().replace(" ", "-"),
        event_slug="",
        outcome="Yes",
        raw_count=1,
    )


def test_fast_crypto_specialist_rejected():
    config = _config()
    classifier = CandidateClassifier(config)
    signals = [_signal("BTC above price") for _ in range(10)]
    market = MarketMeta(condition_id="c", question="BTC above price", end_date_ts=100 + 10 * 60)
    classes = [classifier.classify_signal(signal, market) for signal in signals]

    assert is_fast_crypto_specialist(classes, signals, config)


def test_occasional_crypto_allowed_with_non_crypto_history():
    config = _config()
    classifier = CandidateClassifier(config)
    crypto = _signal("BTC above price")
    sports = [_signal("NBA finals winner") for _ in range(6)]
    signals = [crypto, *sports]
    crypto_market = MarketMeta(condition_id="c", question="BTC above price", end_date_ts=100 + 10 * 60)
    sports_market = MarketMeta(condition_id="s", question="NBA finals winner", end_date_ts=100 + 7 * 86400)
    classes = [classifier.classify_signal(crypto, crypto_market)] + [
        classifier.classify_signal(signal, sports_market) for signal in sports
    ]

    assert not is_fast_crypto_specialist(classes, signals, config)

