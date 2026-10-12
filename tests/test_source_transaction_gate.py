"""Deterministic tests for source-transaction reservation and confirmation."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from pmcopy.domain.source_transactions import (
    ConfirmedExecutionClaim,
    ConfirmedSourceFill,
    EvmHead,
    EvmReceipt,
    LeaderTradeCorrelation,
    PendingSourceMatch,
    PendingTransaction,
    ReceiptStatus,
    ReservedTransaction,
    SourceSide,
    TransactionState,
    WatchedRole,
)
from pmcopy.services.onchain_detection.source_transaction_gate import (
    SourceTransactionGate,
    SourceTransactionRegistry,
)

_CHAIN_ID = 137
_TX_HASH = "0x" + "ab" * 32
_REPLACEMENT_HASH = "0x" + "cd" * 32
_ORDER_HASH = "0x" + "12" * 32
_SECOND_ORDER_HASH = "0x" + "34" * 32
_LEADER = "0x" + "11" * 20
_FOLLOWER = "0x" + "22" * 20
_SENDER = "0x" + "33" * 20
_EXCHANGE = "0x" + "44" * 20
_BLOCK_HASH = "0x" + "56" * 32


class FakeClock:
    """Controllable timezone-aware clock."""

    def __init__(self, current: datetime) -> None:
        self._current = current

    def now(self) -> datetime:
        """Return the configured instant."""

        return self._current

    def advance(self, delta: timedelta) -> None:
        """Advance the configured instant by ``delta``."""

        self._current += delta


class FakeEvmRpc:
    """Receipt and confirmed-nonce lookup fake with call accounting."""

    def __init__(self) -> None:
        self.receipts: dict[str, EvmReceipt | None] = {}
        self.confirmed_nonces: dict[str, int] = {}
        self.receipt_calls: list[str] = []
        self.nonce_calls: list[str] = []

    async def get_transaction_receipt(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> EvmReceipt | None:
        """Return the configured receipt and record the lookup."""

        assert chain_id == _CHAIN_ID
        self.receipt_calls.append(transaction_hash)
        return self.receipts.get(transaction_hash)

    async def get_confirmed_nonce(self, chain_id: int, address: str) -> int:
        """Return the configured confirmed nonce and record the lookup."""

        assert chain_id == _CHAIN_ID
        self.nonce_calls.append(address)
        return self.confirmed_nonces.get(address, 0)


class FakeReceiptDecoder:
    """Deterministic receipt-to-fill decoder."""

    def __init__(self) -> None:
        self.fills: dict[str, tuple[ConfirmedSourceFill, ...]] = {}

    def decode_confirmed_fills(
        self,
        reservation: ReservedTransaction,
        receipt: EvmReceipt,
    ) -> tuple[ConfirmedSourceFill, ...]:
        """Return fills configured for ``receipt``."""

        del reservation
        return self.fills.get(receipt.transaction_hash, ())


class FakeExecutionPort:
    """Capture source fills released for copy execution."""

    def __init__(self) -> None:
        self.claims: list[ConfirmedExecutionClaim] = []

    async def execute_confirmed_source(self, claim: ConfirmedExecutionClaim) -> None:
        """Record one confirmed-source execution claim."""

        self.claims.append(claim)


class FailOnceExecutionPort(FakeExecutionPort):
    """Fail the first downstream delivery before recording its claim."""

    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def execute_confirmed_source(self, claim: ConfirmedExecutionClaim) -> None:
        """Raise once, then record the retried atomic claim."""

        self.attempts += 1
        if self.attempts == 1:
            raise RuntimeError("downstream unavailable")
        await super().execute_confirmed_source(claim)


def _clock() -> FakeClock:
    return FakeClock(datetime(2026, 8, 13, 10, 0, tzinfo=UTC))


def _pending(clock: FakeClock, *, tx_hash: str = _TX_HASH, nonce: int = 7) -> PendingTransaction:
    return PendingTransaction(
        chain_id=_CHAIN_ID,
        transaction_hash=tx_hash,
        sender=_SENDER,
        nonce=nonce,
        target=_EXCHANGE,
        observed_at=clock.now(),
        expires_at=clock.now() + timedelta(seconds=60),
        matches=(
            PendingSourceMatch(
                leader_wallet=_LEADER,
                watched_role=WatchedRole.MAKER,
                exchange_address=_EXCHANGE,
                order_position=0,
                order_hash=_ORDER_HASH,
                token_id="1001",
                condition_id="0x" + "78" * 32,
                side=SourceSide.BUY,
            ),
        ),
    )


def _correlation(
    *, order_hash: str = _ORDER_HASH, event_id: str = "feed-1"
) -> LeaderTradeCorrelation:
    return LeaderTradeCorrelation(
        source_event_id=event_id,
        follower_account=_FOLLOWER,
        copy_task_id="task-1",
        chain_id=_CHAIN_ID,
        transaction_hash=_TX_HASH,
        leader_wallet=_LEADER,
        token_id="1001",
        side=SourceSide.BUY,
        order_hash=order_hash,
    )


def _fill(
    *,
    order_hash: str = _ORDER_HASH,
    log_index: int = 3,
) -> ConfirmedSourceFill:
    return ConfirmedSourceFill(
        chain_id=_CHAIN_ID,
        transaction_hash=_TX_HASH,
        log_index=log_index,
        order_hash=order_hash,
        leader_wallet=_LEADER,
        watched_role=WatchedRole.MAKER,
        exchange_address=_EXCHANGE,
        token_id="1001",
        condition_id="0x" + "78" * 32,
        side=SourceSide.BUY,
        shares=Decimal("4"),
        notional=Decimal("2"),
        price=Decimal("0.5"),
        block_number=101,
        block_hash=_BLOCK_HASH,
    )


def _head(clock: FakeClock, *, number: int = 101) -> EvmHead:
    return EvmHead(
        chain_id=_CHAIN_ID,
        number=number,
        block_hash=_BLOCK_HASH,
        parent_hash="0x" + "45" * 32,
        observed_at=clock.now(),
    )


def _successful_receipt(*, transaction_hash: str = _TX_HASH) -> EvmReceipt:
    return EvmReceipt(
        chain_id=_CHAIN_ID,
        transaction_hash=transaction_hash,
        status=ReceiptStatus.SUCCESS,
        block_number=101,
        block_hash=_BLOCK_HASH,
        logs=(),
    )


def _gate(
    clock: FakeClock,
) -> tuple[
    SourceTransactionGate,
    SourceTransactionRegistry,
    FakeEvmRpc,
    FakeReceiptDecoder,
    FakeExecutionPort,
]:
    registry = SourceTransactionRegistry(clock=clock)
    rpc = FakeEvmRpc()
    decoder = FakeReceiptDecoder()
    executor = FakeExecutionPort()
    gate = SourceTransactionGate(
        registry=registry,
        evm_rpc=rpc,
        receipt_decoder=decoder,
        execution=executor,
        clock=clock,
    )
    return gate, registry, rpc, decoder, executor


@pytest.mark.asyncio
async def test_matching_pending_transaction_reserves_exactly_once() -> None:
    """Duplicate pending observations return one reservation."""

    clock = _clock()
    gate, registry, _, _, _ = _gate(clock)
    pending = _pending(clock)

    first, second = await asyncio.gather(
        gate.observe_pending(pending),
        gate.observe_pending(pending),
    )

    assert sum(result is not None for result in (first, second)) == 1
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.RESERVED
    assert await registry.reservation_count() == 1


@pytest.mark.asyncio
async def test_concurrent_duplicate_leader_events_execute_once_after_confirmation() -> None:
    """Two feed observations cannot claim the same source fill twice."""

    clock = _clock()
    gate, _, rpc, decoder, executor = _gate(clock)
    await gate.observe_pending(_pending(clock))
    event = _correlation()

    await asyncio.gather(
        gate.observe_leader_event(event),
        gate.observe_leader_event(event),
    )
    assert executor.claims == []

    rpc.receipts[_TX_HASH] = _successful_receipt()
    decoder.fills[_TX_HASH] = (_fill(),)
    await gate.on_new_head(_head(clock))

    assert len(executor.claims) == 1


@pytest.mark.asyncio
async def test_matching_pending_transaction_never_executes_before_confirmation() -> None:
    """Pending detection only reserves and correlates work."""

    clock = _clock()
    gate, _, _, _, executor = _gate(clock)

    await gate.observe_pending(_pending(clock))
    await gate.observe_leader_event(_correlation())

    assert executor.claims == []


@pytest.mark.asyncio
async def test_new_head_confirms_and_executes_immediately_once() -> None:
    """A successful receipt on a head releases one confirmed source fill."""

    clock = _clock()
    gate, registry, rpc, decoder, executor = _gate(clock)
    await gate.observe_pending(_pending(clock))
    await gate.observe_leader_event(_correlation())
    rpc.receipts[_TX_HASH] = _successful_receipt()
    decoder.fills[_TX_HASH] = (_fill(),)

    confirmed = await gate.on_new_head(_head(clock))

    assert len(confirmed) == 1
    assert len(executor.claims) == 1
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.CONSUMED

    await gate.on_new_head(_head(clock))
    assert rpc.receipt_calls == [_TX_HASH]
    assert len(executor.claims) == 1


@pytest.mark.asyncio
async def test_mined_log_notification_confirms_exact_transaction_immediately() -> None:
    """An included log checks only its reservation without waiting for polling."""

    clock = _clock()
    gate, registry, rpc, decoder, executor = _gate(clock)
    await gate.observe_pending(_pending(clock))
    await gate.observe_leader_event(_correlation())
    rpc.receipts[_TX_HASH] = _successful_receipt()
    decoder.fills[_TX_HASH] = (_fill(),)

    confirmed = await gate.on_mined_transaction(_CHAIN_ID, _TX_HASH)

    assert confirmed is not None
    assert rpc.receipt_calls == [_TX_HASH]
    assert len(executor.claims) == 1
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.CONSUMED


@pytest.mark.asyncio
async def test_receipt_is_not_polled_without_a_new_head() -> None:
    """Time passing without a block never triggers a receipt lookup."""

    clock = _clock()
    gate, _, rpc, _, _ = _gate(clock)
    await gate.observe_pending(_pending(clock))

    clock.advance(timedelta(minutes=5))

    assert rpc.receipt_calls == []


@pytest.mark.asyncio
async def test_reverted_receipt_prevents_execution_and_finalizes_state() -> None:
    """Receipt status zero transitions the reservation to reverted."""

    clock = _clock()
    gate, registry, rpc, _, executor = _gate(clock)
    await gate.observe_pending(_pending(clock))
    await gate.observe_leader_event(_correlation())
    rpc.receipts[_TX_HASH] = EvmReceipt(
        chain_id=_CHAIN_ID,
        transaction_hash=_TX_HASH,
        status=ReceiptStatus.REVERTED,
        block_number=101,
        block_hash=_BLOCK_HASH,
        logs=(),
    )

    await gate.on_new_head(_head(clock))

    assert executor.claims == []
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.REVERTED


@pytest.mark.asyncio
async def test_same_sender_nonce_replacement_prevents_execution() -> None:
    """An advanced confirmed nonce marks a receipt-less source as replaced."""

    clock = _clock()
    gate, registry, rpc, _, executor = _gate(clock)
    await gate.observe_pending(_pending(clock, tx_hash=_TX_HASH, nonce=7))
    await gate.observe_pending(_pending(clock, tx_hash=_REPLACEMENT_HASH, nonce=7))
    rpc.confirmed_nonces[_SENDER] = 8

    await gate.on_new_head(_head(clock))

    assert executor.claims == []
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.REPLACED


@pytest.mark.asyncio
async def test_timeout_expires_unconfirmed_reservation_on_head() -> None:
    """The configured deadline is evaluated only when a head arrives."""

    clock = _clock()
    gate, registry, rpc, _, executor = _gate(clock)
    await gate.observe_pending(_pending(clock))
    clock.advance(timedelta(seconds=60))

    await gate.on_new_head(_head(clock))

    assert executor.claims == []
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.EXPIRED
    assert rpc.receipt_calls == [_TX_HASH]


@pytest.mark.asyncio
async def test_successful_receipt_without_matching_fill_is_rejected() -> None:
    """Successful outer execution without a copyable fill remains inert."""

    clock = _clock()
    gate, registry, rpc, _, executor = _gate(clock)
    await gate.observe_pending(_pending(clock))
    await gate.observe_leader_event(_correlation())
    rpc.receipts[_TX_HASH] = _successful_receipt()

    await gate.on_new_head(_head(clock))

    assert executor.claims == []
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.REJECTED


@pytest.mark.asyncio
async def test_disconnect_blocks_heads_until_deterministic_resynchronization() -> None:
    """Reconnect resync confirms once and cannot replay a consumed fill."""

    clock = _clock()
    gate, registry, rpc, decoder, executor = _gate(clock)
    await gate.observe_pending(_pending(clock))
    await gate.observe_leader_event(_correlation())
    rpc.receipts[_TX_HASH] = _successful_receipt()
    decoder.fills[_TX_HASH] = (_fill(),)

    await gate.on_disconnect()
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.RESYNC

    await gate.on_new_head(_head(clock))
    assert rpc.receipt_calls == []
    assert executor.claims == []

    await gate.resynchronize()
    await gate.resynchronize()

    assert len(executor.claims) == 1
    assert rpc.receipt_calls == [_TX_HASH]
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.CONSUMED


@pytest.mark.asyncio
async def test_empty_startup_restore_resynchronizes_before_first_reservation() -> None:
    """An empty durable snapshot does not leave the new process disconnected."""

    clock = _clock()
    gate, registry, _, _, _ = _gate(clock)

    await gate.restore()
    await gate.resynchronize()
    reservation = await gate.observe_pending(_pending(clock))

    assert reservation is not None
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.RESERVED


@pytest.mark.asyncio
async def test_two_distinct_fills_in_one_transaction_are_consumed_independently() -> None:
    """Composite fill identity preserves legitimate multi-fill transactions."""

    clock = _clock()
    gate, _, rpc, decoder, executor = _gate(clock)
    await gate.observe_pending(_pending(clock))
    await gate.observe_leader_event(_correlation(order_hash=_ORDER_HASH, event_id="feed-a"))
    await gate.observe_leader_event(_correlation(order_hash=_SECOND_ORDER_HASH, event_id="feed-b"))
    rpc.receipts[_TX_HASH] = _successful_receipt()
    decoder.fills[_TX_HASH] = (
        _fill(order_hash=_ORDER_HASH, log_index=3),
        _fill(order_hash=_SECOND_ORDER_HASH, log_index=4),
    )

    await gate.on_new_head(_head(clock))

    assert len(executor.claims) == 2
    assert {claim.fill.log_index for claim in executor.claims} == {3, 4}


@pytest.mark.asyncio
async def test_failed_downstream_delivery_releases_claim_for_idempotent_retry() -> None:
    """A claim becomes consumed only after the downstream boundary returns."""

    clock = _clock()
    registry = SourceTransactionRegistry(clock=clock)
    rpc = FakeEvmRpc()
    decoder = FakeReceiptDecoder()
    executor = FailOnceExecutionPort()
    gate = SourceTransactionGate(
        registry=registry,
        evm_rpc=rpc,
        receipt_decoder=decoder,
        execution=executor,
        clock=clock,
    )
    await gate.observe_pending(_pending(clock))
    event = _correlation()
    await gate.observe_leader_event(event)
    rpc.receipts[_TX_HASH] = _successful_receipt()
    decoder.fills[_TX_HASH] = (_fill(),)

    with pytest.raises(RuntimeError, match="downstream unavailable"):
        await gate.on_new_head(_head(clock))

    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.CONFIRMED
    await gate.observe_leader_event(event)

    assert executor.attempts == 2
    assert len(executor.claims) == 1
    assert await registry.state_for(_CHAIN_ID, _TX_HASH) is TransactionState.CONSUMED


@pytest.mark.asyncio
async def test_failed_delivery_releases_unattempted_claims_from_same_batch() -> None:
    """A failed batch can retry both the failed and not-yet-attempted claims."""

    clock = _clock()
    registry = SourceTransactionRegistry(clock=clock)
    rpc = FakeEvmRpc()
    decoder = FakeReceiptDecoder()
    executor = FailOnceExecutionPort()
    gate = SourceTransactionGate(
        registry=registry,
        evm_rpc=rpc,
        receipt_decoder=decoder,
        execution=executor,
        clock=clock,
    )
    await gate.observe_pending(_pending(clock))
    first_event = _correlation(order_hash=_ORDER_HASH, event_id="feed-a")
    second_event = _correlation(order_hash=_SECOND_ORDER_HASH, event_id="feed-b")
    await gate.observe_leader_event(first_event)
    await gate.observe_leader_event(second_event)
    rpc.receipts[_TX_HASH] = _successful_receipt()
    decoder.fills[_TX_HASH] = (
        _fill(order_hash=_ORDER_HASH, log_index=3),
        _fill(order_hash=_SECOND_ORDER_HASH, log_index=4),
    )

    with pytest.raises(RuntimeError, match="downstream unavailable"):
        await gate.on_new_head(_head(clock))

    await gate.observe_leader_event(first_event)
    await gate.observe_leader_event(second_event)

    assert executor.attempts == 3
    assert {claim.fill.log_index for claim in executor.claims} == {3, 4}


def test_source_transaction_models_are_immutable() -> None:
    """Lifecycle values cannot be mutated across synchronization boundaries."""

    pending = _pending(_clock())

    with pytest.raises(FrozenInstanceError):
        pending.nonce = 9  # type: ignore[misc]
