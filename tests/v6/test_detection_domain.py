"""Characterization tests for canonical V6 detection events.

These contracts pin the immutable boundary types used by every detection
source adapter and downstream copy-decision / sizing / risk path. Vendor
payloads are normalized into these values before reaching application code.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from domain.detection import (
    DetectionFinality,
    DetectionMode,
    LeaderTradeEvent,
    TradeSide,
)


def _event(**changes: object) -> LeaderTradeEvent:
    values: dict[str, object] = {
        "leader_wallet": "0x" + "11" * 20,
        "token_id": "123",
        "condition_id": "0x" + "22" * 32,
        "side": TradeSide.BUY,
        "price": Decimal("0.42"),
        "shares": Decimal("10"),
        "notional": Decimal("4.20"),
        "mode": DetectionMode.POLLING,
        "finality": DetectionFinality.CONFIRMED,
        "detected_at": datetime(2026, 8, 5, 1, 2, 3, tzinfo=UTC),
        "venue_timestamp": datetime(2026, 8, 5, 1, 2, 2, tzinfo=UTC),
        "transaction_hash": "0x" + "33" * 32,
        "log_index": 7,
        "order_hash": "0x" + "44" * 32,
        "source_identity": "row-1",
    }
    values.update(changes)
    return LeaderTradeEvent(**values)  # type: ignore[arg-type]


def test_canonical_event_has_stable_exact_and_economic_keys() -> None:
    event = _event()
    same_economics = _event(
        mode=DetectionMode.ON_CHAIN_CONFIRMED,
        source_identity="log-7",
    )

    assert event.exact_key != same_economics.exact_key
    assert event.economic_key == same_economics.economic_key


def test_pending_event_is_explicitly_provisional() -> None:
    event = _event(
        mode=DetectionMode.ON_CHAIN_PENDING,
        finality=DetectionFinality.PROVISIONAL,
        log_index=None,
    )
    assert event.is_provisional is True
    assert event.is_actionable is True


@pytest.mark.parametrize("price", [Decimal("0"), Decimal("1"), Decimal("NaN")])
def test_invalid_price_fails_at_domain_boundary(price: Decimal) -> None:
    with pytest.raises(ValueError, match="price"):
        _event(price=price)


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(detected_at=datetime(2026, 8, 5, 1, 2, 3))


def test_leader_wallet_is_normalized_to_lowercase() -> None:
    event = _event(leader_wallet="0x" + "AB" * 20)
    assert event.leader_wallet == "0x" + "ab" * 20


def test_invalid_leader_wallet_is_rejected() -> None:
    with pytest.raises(ValueError, match="leader_wallet"):
        _event(leader_wallet="nope")


def test_negative_log_index_is_rejected() -> None:
    with pytest.raises(ValueError, match="log_index"):
        _event(log_index=-1)