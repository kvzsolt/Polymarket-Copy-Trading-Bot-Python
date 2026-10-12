"""Tests for receipt-backed source fill confirmation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pmcopy.config.addresses import POLYMARKET_CTF_EXCHANGE_V2_ADDRESS
from pmcopy.domain.source_transactions import (
    EvmReceipt,
    PendingSourceMatch,
    PendingTransaction,
    ReceiptStatus,
    ReservedTransaction,
    SourceSide,
    WatchedRole,
)
from pmcopy.services.onchain_detection.order_filled_detector import ORDER_FILLED_TOPIC0
from pmcopy.services.onchain_detection.order_filled_receipt_decoder import OrderFilledReceiptDecoder

_CHAIN_ID = 137
_TX_HASH = "0x" + "ab" * 32
_BLOCK_HASH = "0x" + "bc" * 32
_ORDER_HASH = "0x" + "12" * 32
_LEADER = "0x" + "11" * 20
_OTHER = "0x" + "22" * 20


class ReceiptFixtureFactory:
    """Build deterministic pending reservations and mined receipt logs."""

    def __init__(self) -> None:
        self._now = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)

    def reservation(self, *, order_hash: str = _ORDER_HASH) -> ReservedTransaction:
        """Return a qualified pending reservation."""

        pending = PendingTransaction(
            chain_id=_CHAIN_ID,
            transaction_hash=_TX_HASH,
            sender="0x" + "33" * 20,
            nonce=4,
            target=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
            observed_at=self._now,
            expires_at=self._now + timedelta(seconds=60),
            matches=(
                PendingSourceMatch(
                    leader_wallet=_LEADER,
                    watched_role=WatchedRole.MAKER,
                    exchange_address=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
                    order_position=0,
                    order_hash=order_hash,
                    token_id="1001",
                    condition_id="0x" + "45" * 32,
                    side=SourceSide.BUY,
                ),
            ),
        )
        return ReservedTransaction(
            reservation_id=f"{_CHAIN_ID}:{_TX_HASH}",
            transaction=pending,
            reserved_at=self._now,
        )

    def receipt(
        self,
        *,
        order_hash: str = _ORDER_HASH,
        maker: str = _LEADER,
    ) -> EvmReceipt:
        """Return a successful receipt with one OrderFilled log."""

        data = b"".join(
            value.to_bytes(32, "big")
            for value in (
                0,
                1001,
                500_000,
                1_000_000,
                0,
                0,
                0,
            )
        )
        log = {
            "address": POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
            "topics": [
                "0x" + ORDER_FILLED_TOPIC0.hex(),
                order_hash,
                self._topic_address(maker),
                self._topic_address(_OTHER),
            ],
            "data": "0x" + data.hex(),
            "transactionHash": _TX_HASH,
            "logIndex": "0x3",
            "blockNumber": "0x65",
        }
        return EvmReceipt(
            chain_id=_CHAIN_ID,
            transaction_hash=_TX_HASH,
            status=ReceiptStatus.SUCCESS,
            block_number=101,
            block_hash=_BLOCK_HASH,
            logs=(log,),
        )

    @staticmethod
    def _topic_address(address: str) -> str:
        return "0x" + address[2:].rjust(64, "0")


def test_receipt_decoder_returns_exact_decimal_confirmed_fill() -> None:
    """A matching receipt log becomes one exact Decimal source fill."""

    fixtures = ReceiptFixtureFactory()
    decoder = OrderFilledReceiptDecoder()

    fills = decoder.decode_confirmed_fills(fixtures.reservation(), fixtures.receipt())

    assert len(fills) == 1
    fill = fills[0]
    assert fill.log_index == 3
    assert fill.order_hash == _ORDER_HASH
    assert fill.leader_wallet == _LEADER
    assert fill.side is SourceSide.BUY
    assert fill.shares == Decimal("1")
    assert fill.notional == Decimal("0.5")
    assert fill.price == Decimal("0.5")


def test_receipt_decoder_rejects_log_not_qualified_by_pending_match() -> None:
    """A successful unrelated fill cannot confirm the reservation."""

    fixtures = ReceiptFixtureFactory()
    decoder = OrderFilledReceiptDecoder()

    fills = decoder.decode_confirmed_fills(
        fixtures.reservation(),
        fixtures.receipt(order_hash="0x" + "99" * 32),
    )

    assert fills == ()


def test_receipt_decoder_eliminates_duplicate_log_observations() -> None:
    """The same log repeated in a receipt produces one source fill."""

    fixtures = ReceiptFixtureFactory()
    decoder = OrderFilledReceiptDecoder()
    receipt = fixtures.receipt()
    repeated = EvmReceipt(
        chain_id=receipt.chain_id,
        transaction_hash=receipt.transaction_hash,
        status=receipt.status,
        block_number=receipt.block_number,
        block_hash=receipt.block_hash,
        logs=receipt.logs + receipt.logs,
    )

    fills = decoder.decode_confirmed_fills(fixtures.reservation(), repeated)

    assert len(fills) == 1
