"""Tests for deterministic Alchemy pending-transaction qualification."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from eth_abi import encode

from pmcopy.domain.source_transactions import SourceSide, WatchedRole
from pmcopy.services.onchain_detection.calldata_decoder import (
    MATCH_ORDERS_SELECTOR,
    SAFE_EXEC_TRANSACTION_SELECTOR,
    SourceTransactionCalldataDecoder,
)
from pmcopy.services.onchain_detection.pending_transaction_factory import PendingTransactionFactory

_CHAIN_ID = 137
_NOW = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
_TX_HASH = "0x" + "ab" * 32
_LEADER = "0x" + "11" * 20
_SENDER = "0x" + "22" * 20
_EXCHANGE = "0x" + "33" * 20
_SAFE = "0x" + "44" * 20

_ORDER_TUPLE = (
    "(uint256,address,address,uint256,uint256,uint256,uint8,uint8,uint256,bytes32,bytes32,bytes)"
)
_MATCH_ORDERS_TYPE = (
    f"(bytes32,{_ORDER_TUPLE},{_ORDER_TUPLE}[],uint256,uint256[],uint256,uint256[])"
)
_SAFE_EXEC_TYPES = (
    "address",
    "uint256",
    "bytes",
    "uint8",
    "uint256",
    "uint256",
    "uint256",
    "address",
    "address",
    "bytes",
)


class FakeClock:
    """Fixed deterministic clock."""

    def now(self) -> datetime:
        """Return the fixed test instant."""

        return _NOW


class PendingNotificationFactory:
    """Build deterministic Alchemy pending transaction payloads."""

    def inner_calldata(self, *, maker: str = _LEADER) -> bytes:
        """Return canonical matchOrders calldata with one watched maker."""

        taker = self._order("0x" + "55" * 20, side=1)
        makers = [self._order(maker, side=0)]
        payload = [
            (
                b"\x66" * 32,
                taker,
                makers,
                1_000_000,
                [500_000],
                0,
                [0],
            )
        ]
        return MATCH_ORDERS_SELECTOR + encode([_MATCH_ORDERS_TYPE], payload)

    def safe_calldata(self, inner: bytes) -> bytes:
        """Wrap ``inner`` in a Safe CALL to the configured exchange."""

        payload = (
            _EXCHANGE,
            0,
            inner,
            0,
            0,
            0,
            0,
            "0x" + "00" * 20,
            "0x" + "00" * 20,
            b"\x77" * 65,
        )
        return SAFE_EXEC_TRANSACTION_SELECTOR + encode(_SAFE_EXEC_TYPES, payload)

    def notification(self, *, target: str, calldata: bytes) -> dict[str, object]:
        """Return an Alchemy full pending transaction object."""

        return {
            "hash": _TX_HASH,
            "from": _SENDER,
            "to": target,
            "nonce": "0x7",
            "input": "0x" + calldata.hex(),
        }

    @staticmethod
    def _order(maker: str, *, side: int) -> tuple[object, ...]:
        return (
            123,
            maker,
            maker,
            1001,
            500_000,
            1_000_000,
            side,
            0,
            1_700_000_000,
            b"\x88" * 32,
            b"\x99" * 32,
            b"\xaa" * 4,
        )


def _factory() -> PendingTransactionFactory:
    decoder = SourceTransactionCalldataDecoder(exchange_addresses=frozenset({_EXCHANGE}))
    return PendingTransactionFactory(
        chain_id=_CHAIN_ID,
        calldata_decoder=decoder,
        clock=FakeClock(),
        confirmation_timeout=timedelta(seconds=60),
    )


def test_factory_qualifies_direct_pending_match_with_injected_deadline() -> None:
    """A direct matching notification becomes one immutable pending candidate."""

    fixtures = PendingNotificationFactory()

    pending = _factory().from_notification(
        fixtures.notification(target=_EXCHANGE, calldata=fixtures.inner_calldata()),
        watched_wallets=frozenset({_LEADER}),
    )

    assert pending is not None
    assert pending.transaction_hash == _TX_HASH
    assert pending.sender == _SENDER
    assert pending.nonce == 7
    assert pending.observed_at == _NOW
    assert pending.expires_at == _NOW + timedelta(seconds=60)
    assert len(pending.matches) == 1
    match = pending.matches[0]
    assert match.leader_wallet == _LEADER
    assert match.watched_role is WatchedRole.MAKER
    assert match.order_position == 0
    assert match.exchange_address == _EXCHANGE
    assert match.side is SourceSide.BUY


def test_factory_qualifies_safe_wrapped_inner_selector() -> None:
    """A Safe outer call keeps its sender nonce and configured inner exchange."""

    fixtures = PendingNotificationFactory()
    safe_calldata = fixtures.safe_calldata(fixtures.inner_calldata())

    pending = _factory().from_notification(
        fixtures.notification(target=_SAFE, calldata=safe_calldata),
        watched_wallets=frozenset({_LEADER}),
    )

    assert pending is not None
    assert pending.target == _SAFE
    assert pending.matches[0].exchange_address == _EXCHANGE


def test_factory_rejects_malformed_nonmatching_and_unwatched_notifications() -> None:
    """Unqualified pending input never reaches reservation state."""

    fixtures = PendingNotificationFactory()
    factory = _factory()

    malformed = fixtures.notification(target=_EXCHANGE, calldata=b"\x00")
    wrong_selector = fixtures.notification(
        target=_EXCHANGE,
        calldata=b"\xde\xad\xbe\xef" + b"\x00" * 32,
    )
    unwatched = fixtures.notification(target=_EXCHANGE, calldata=fixtures.inner_calldata())

    assert factory.from_notification(malformed, frozenset({_LEADER})) is None
    assert factory.from_notification(wrong_selector, frozenset({_LEADER})) is None
    assert factory.from_notification(unwatched, frozenset({"0x" + "ff" * 20})) is None
