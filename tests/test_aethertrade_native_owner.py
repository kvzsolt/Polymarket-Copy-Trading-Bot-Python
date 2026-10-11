"""Native V2 order ownership and inclusion identity remain distinct from roles."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal

import pytest

from config.addresses import POLYMARKET_CTF_EXCHANGE_V2_ADDRESS

EXCHANGE = POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower()
OWNER = "0x" + "11" * 20
COUNTERPARTY = "0x" + "22" * 20
TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"


class LogFixture:
    """Construct one seven-word native V2 owner-fill fixture."""

    @staticmethod
    def make(*, side: int = 0, counterparty: str = COUNTERPARTY) -> dict[str, object]:
        """Return exact encoded financial units and complete inclusion coordinates."""
        maker, taker = (48_000_000, 100_000_000) if side == 0 else (100_000_000, 48_000_000)
        return {
            "address": EXCHANGE,
            "topics": [
                TOPIC,
                "0x" + "aa" * 32,
                "0x" + OWNER[2:].rjust(64, "0"),
                "0x" + counterparty[2:].rjust(64, "0"),
            ],
            "data": "0x"
            + "".join(f"{word:064x}" for word in (side, 123, maker, taker, 1000, 0, 0)),
            "transactionHash": "0x" + "bb" * 32,
            "logIndex": "0x3",
            "blockNumber": "0x10",
            "blockHash": "0x" + "cc" * 32,
            "removed": False,
        }


@pytest.mark.parametrize("side", [0, 1])
@pytest.mark.parametrize("counterparty", [COUNTERPARTY, EXCHANGE])
def test_native_owner_fill_has_its_own_side_token_and_fee(side: int, counterparty: str) -> None:
    """Native taker summary still owns topic2 and must not be inverted/dropped."""
    from services.onchain_detection.native_owner import NativeOwnerLogDecoder

    fill = NativeOwnerLogDecoder().decode(LogFixture.make(side=side, counterparty=counterparty))
    assert fill.owner == OWNER
    assert fill.side == ("BUY" if side == 0 else "SELL")
    assert fill.token_id == "123"
    assert fill.price == Decimal("0.48")
    assert fill.shares == Decimal("100")
    assert fill.fee == Decimal("0.001")
    assert fill.native_taker is (counterparty == EXCHANGE)


def test_repeated_fills_and_reinclusions_have_distinct_correct_identities() -> None:
    """Action ID retains every log; inclusion ID changes when its block changes."""
    from services.onchain_detection.native_owner import NativeOwnerLogDecoder

    decoder = NativeOwnerLogDecoder()
    first = decoder.decode(LogFixture.make())
    repeated = decoder.decode(LogFixture.make() | {"logIndex": "0x4"})
    reincluded = decoder.decode(LogFixture.make() | {"blockHash": "0x" + "dd" * 32})
    assert first.action_id != repeated.action_id
    assert first.action_id == reincluded.action_id
    assert first.inclusion_id != reincluded.inclusion_id


def test_removed_first_is_preserved_as_evidence_and_not_a_fresh_signal() -> None:
    """The decoder does not discard a tombstone before an add has been observed."""
    from services.onchain_detection.native_owner import NativeOwnerLogDecoder

    fill = NativeOwnerLogDecoder().decode(LogFixture.make() | {"removed": True})
    assert fill.removed is True
    assert fill.action_id


@pytest.mark.parametrize(
    "mutation",
    [
        {"removed": "false"},
        {"blockNumber": True},
        {"blockHash": ""},
        {"logIndex": -1},
        {"transactionHash": "0x123"},
        {"address": COUNTERPARTY},
        {"data": "0x00"},
        {"topics": [TOPIC] * 5},
    ],
)
def test_malformed_native_log_is_rejected(mutation: dict[str, object]) -> None:
    """Malformed coordinates or log shape cannot create guessed economic evidence."""
    from services.onchain_detection.native_owner import NativeOwnerLogDecoder

    with pytest.raises(ValueError):
        NativeOwnerLogDecoder().decode(LogFixture.make() | mutation)


def test_nonzero_address_padding_is_rejected() -> None:
    """Truncating malformed indexed topics would invent a different wallet owner."""
    from services.onchain_detection.native_owner import NativeOwnerLogDecoder

    log = deepcopy(LogFixture.make())
    log["topics"][2] = "0x" + "11" * 32
    with pytest.raises(ValueError, match="padding"):
        NativeOwnerLogDecoder().decode(log)
