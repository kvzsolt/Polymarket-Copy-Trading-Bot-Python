"""Race-safe, block-event-driven source-transaction confirmation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from domain.source_transactions import (
    ConfirmedExecutionClaim,
    ConfirmedSourceFill,
    ConfirmedTransaction,
    CopyConsumptionKey,
    EvmHead,
    LeaderTradeCorrelation,
    PendingTransaction,
    ReceiptStatus,
    ReservedTransaction,
    SourceTransactionRecordSnapshot,
    SourceTransactionSnapshot,
    TransactionState,
)
from interfaces.source_transactions import (
    Clock,
    ConfirmedReceiptDecoder,
    ConfirmedSourceExecutionPort,
    EvmRpcPort,
    SourceTransactionStateStore,
)


@dataclass(slots=True)
class _TransactionRecord:
    """Encapsulated mutable state protected by the registry lock."""

    reservation: ReservedTransaction
    state: TransactionState
    correlations: dict[tuple[str, str, str], LeaderTradeCorrelation] = field(default_factory=dict)
    confirmed: ConfirmedTransaction | None = None
    terminal_reason: str | None = None


class InMemorySourceTransactionStateStore:
    """Process-local snapshot store used when no durable adapter is configured."""

    def __init__(self) -> None:
        """Initialize an empty state image."""

        self._snapshot = SourceTransactionSnapshot.empty()

    async def load(self) -> SourceTransactionSnapshot:
        """Return the latest immutable state image."""

        return self._snapshot

    async def save(self, snapshot: SourceTransactionSnapshot) -> None:
        """Atomically replace the process-local state image."""

        self._snapshot = snapshot


class SourceTransactionRegistry:
    """Own all atomic reservation, correlation, and consumption mutations."""

    _TERMINAL_STATES = frozenset(
        {
            TransactionState.REJECTED,
            TransactionState.REVERTED,
            TransactionState.REPLACED,
            TransactionState.EXPIRED,
        }
    )

    def __init__(
        self,
        clock: Clock,
        state_store: SourceTransactionStateStore | None = None,
    ) -> None:
        """Initialize an empty in-process registry.

        Args:
            clock: Deterministic clock used for reservation timestamps.
            state_store: Atomic crash-recovery snapshot adapter.
        """

        self._clock = clock
        self._state_store = state_store or InMemorySourceTransactionStateStore()
        self._lock = asyncio.Lock()
        self._records: dict[tuple[int, str], _TransactionRecord] = {}
        self._waiting_correlations: dict[
            tuple[int, str],
            dict[tuple[str, str, str], LeaderTradeCorrelation],
        ] = {}
        self._claimed: set[CopyConsumptionKey] = set()
        self._inflight: set[CopyConsumptionKey] = set()
        self._checking: set[tuple[int, str]] = set()
        self._last_heads: dict[int, tuple[int, str]] = {}
        self._connected = True

    async def restore(self) -> None:
        """Load durable state and fail unfinished work into resynchronization."""

        snapshot = await self._state_store.load()
        async with self._lock:
            self._records.clear()
            self._waiting_correlations.clear()
            self._claimed = set(snapshot.claimed)
            self._inflight.clear()
            self._checking.clear()
            self._last_heads.clear()
            self._connected = False
            for saved in snapshot.records:
                state = saved.state
                if state in {
                    TransactionState.OBSERVED,
                    TransactionState.RESERVED,
                    TransactionState.CONFIRMED,
                    TransactionState.RESYNC,
                }:
                    state = TransactionState.RESYNC
                correlations = {
                    (
                        correlation.follower_account,
                        correlation.copy_task_id,
                        correlation.source_event_id,
                    ): correlation
                    for correlation in saved.correlations
                }
                transaction = saved.reservation.transaction
                self._records[(transaction.chain_id, transaction.transaction_hash)] = (
                    _TransactionRecord(
                        reservation=saved.reservation,
                        state=state,
                        correlations=correlations,
                        confirmed=saved.confirmed,
                        terminal_reason=saved.terminal_reason,
                    )
                )
            for correlation in snapshot.waiting_correlations:
                key = (correlation.chain_id, correlation.transaction_hash)
                event_key = (
                    correlation.follower_account,
                    correlation.copy_task_id,
                    correlation.source_event_id,
                )
                self._waiting_correlations.setdefault(key, {})[event_key] = correlation
            await self._persist_locked()

    async def reserve(self, pending: PendingTransaction) -> ReservedTransaction | None:
        """Atomically reserve ``pending`` if its chain hash is unseen.

        Args:
            pending: Qualified pending source transaction.

        Returns:
            The unique reservation, or ``None`` for a duplicate observation.
        """

        key = (pending.chain_id, pending.transaction_hash)
        async with self._lock:
            if key in self._records:
                return None
            reservation = ReservedTransaction(
                reservation_id=f"{pending.chain_id}:{pending.transaction_hash}",
                transaction=pending,
                reserved_at=self._clock.now(),
            )
            correlations = self._waiting_correlations.pop(key, {})
            self._records[key] = _TransactionRecord(
                reservation=reservation,
                state=(TransactionState.RESERVED if self._connected else TransactionState.RESYNC),
                correlations=correlations,
            )
            await self._persist_locked()
            return reservation

    async def add_correlation(
        self,
        correlation: LeaderTradeCorrelation,
    ) -> tuple[ConfirmedExecutionClaim, ...]:
        """Record a leader event and claim any already-confirmed matching fill.

        Args:
            correlation: Follower-scoped leader event to associate by source hash.

        Returns:
            Newly claimed executions, empty until confirmation or during resync.
        """

        key = (correlation.chain_id, correlation.transaction_hash)
        event_key = (
            correlation.follower_account,
            correlation.copy_task_id,
            correlation.source_event_id,
        )
        async with self._lock:
            record = self._records.get(key)
            if record is None:
                waiting = self._waiting_correlations.setdefault(key, {})
                if event_key not in waiting:
                    waiting[event_key] = correlation
                    await self._persist_locked()
                return ()
            if event_key not in record.correlations:
                record.correlations[event_key] = correlation
                await self._persist_locked()
            if not self._connected or record.confirmed is None:
                return ()
            claims = self._claim_ready_locked(record)
            if claims:
                await self._persist_locked()
            return claims

    async def reservations_for_head(
        self,
        head: EvmHead,
    ) -> tuple[ReservedTransaction, ...]:
        """Return reserved transactions once for a unique connected head.

        Args:
            head: Newly observed Polygon head.

        Returns:
            Active reservations to inspect for receipts.
        """

        async with self._lock:
            if not self._connected:
                return ()
            identity = (head.number, head.block_hash)
            if self._last_heads.get(head.chain_id) == identity:
                return ()
            self._last_heads[head.chain_id] = identity
            reservations = tuple(
                record.reservation
                for record in self._records.values()
                if record.reservation.transaction.chain_id == head.chain_id
                and record.state is TransactionState.RESERVED
                and (
                    record.reservation.transaction.chain_id,
                    record.reservation.transaction.transaction_hash,
                )
                not in self._checking
            )
            self._checking.update(
                (reservation.transaction.chain_id, reservation.transaction.transaction_hash)
                for reservation in reservations
            )
            return reservations

    async def reservation_for_transaction(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> ReservedTransaction | None:
        """Atomically claim one active reservation for an included log.

        Args:
            chain_id: Source EVM chain identifier.
            transaction_hash: Included transaction hash from a log notification.

        Returns:
            Reservation to resolve, or ``None`` when absent, inactive, or
            already being checked.
        """

        key = (chain_id, transaction_hash.lower())
        async with self._lock:
            record = self._records.get(key)
            if (
                not self._connected
                or record is None
                or record.state is not TransactionState.RESERVED
                or key in self._checking
            ):
                return None
            self._checking.add(key)
            return record.reservation

    async def resync_reservations(self) -> tuple[ReservedTransaction, ...]:
        """Return reservations requiring deterministic reconnect resolution."""

        async with self._lock:
            if self._connected:
                return ()
            reservations = tuple(
                record.reservation
                for record in self._records.values()
                if record.state is TransactionState.RESYNC
                and (
                    record.reservation.transaction.chain_id,
                    record.reservation.transaction.transaction_hash,
                )
                not in self._checking
            )
            self._checking.update(
                (reservation.transaction.chain_id, reservation.transaction.transaction_hash)
                for reservation in reservations
            )
            return reservations

    async def complete_confirmation_check(self, reservation: ReservedTransaction) -> None:
        """Release the in-flight receipt-check claim for ``reservation``."""

        transaction = reservation.transaction
        async with self._lock:
            self._checking.discard((transaction.chain_id, transaction.transaction_hash))

    async def confirm(
        self,
        confirmed: ConfirmedTransaction,
    ) -> tuple[ConfirmedExecutionClaim, ...]:
        """Atomically store confirmation and claim matching correlations.

        Args:
            confirmed: Successfully included transaction with decoded fills.

        Returns:
            Newly claimed follower/fill pairs when execution is connected.
        """

        transaction = confirmed.reservation.transaction
        key = (transaction.chain_id, transaction.transaction_hash)
        async with self._lock:
            record = self._records.get(key)
            if record is None or record.state in self._TERMINAL_STATES:
                return ()
            if record.confirmed is None:
                record.confirmed = confirmed
                record.state = TransactionState.CONFIRMED
                self._mark_nonce_siblings_replaced_locked(record)
            if not self._connected:
                await self._persist_locked()
                return ()
            claims = self._claim_ready_locked(record)
            await self._persist_locked()
            return claims

    async def finalize(
        self,
        reservation: ReservedTransaction,
        state: TransactionState,
        reason: str,
    ) -> None:
        """Transition an active reservation to a failure terminal state.

        Args:
            reservation: Reservation to finalize.
            state: Rejected, reverted, replaced, or expired terminal state.
            reason: Redacted structured reason suitable for persistence/logging.

        Raises:
            ValueError: If ``state`` is not a failure terminal state.
        """

        if state not in self._TERMINAL_STATES:
            raise ValueError(f"{state.value} is not a failure terminal state")
        transaction = reservation.transaction
        key = (transaction.chain_id, transaction.transaction_hash)
        async with self._lock:
            record = self._records.get(key)
            if record is None or record.state in self._TERMINAL_STATES:
                return
            if record.confirmed is not None or record.state is TransactionState.CONSUMED:
                return
            record.state = state
            record.terminal_reason = reason
            await self._persist_locked()

    async def on_disconnect(self) -> None:
        """Block execution and mark unresolved reservations for resync."""

        async with self._lock:
            self._connected = False
            for record in self._records.values():
                if record.state is TransactionState.RESERVED:
                    record.state = TransactionState.RESYNC
            await self._persist_locked()

    async def complete_resync(self) -> tuple[ConfirmedExecutionClaim, ...]:
        """Resume reservations and atomically release confirmed claims."""

        async with self._lock:
            if self._connected:
                return ()
            for record in self._records.values():
                if record.state is TransactionState.RESYNC:
                    record.state = TransactionState.RESERVED
            self._connected = True
            claims: list[ConfirmedExecutionClaim] = []
            for record in self._records.values():
                if record.confirmed is not None:
                    claims.extend(self._claim_ready_locked(record))
            await self._persist_locked()
            return tuple(claims)

    async def state_for(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> TransactionState | None:
        """Return the current lifecycle state for a chain transaction hash."""

        async with self._lock:
            record = self._records.get((chain_id, transaction_hash.lower()))
            return record.state if record is not None else None

    async def complete_execution_claim(self, claim: ConfirmedExecutionClaim) -> None:
        """Persist a claim only after downstream delivery succeeds.

        Args:
            claim: Successfully delivered follower/fill claim.
        """

        consumption_key = claim.consumption_key
        source_fill = consumption_key.source_fill
        async with self._lock:
            self._inflight.discard(consumption_key)
            self._claimed.add(consumption_key)
            record = self._records.get((source_fill.chain_id, source_fill.transaction_hash))
            if record is not None:
                record.state = TransactionState.CONSUMED
            await self._persist_locked()

    async def release_execution_claims(
        self,
        claims: tuple[ConfirmedExecutionClaim, ...],
    ) -> None:
        """Atomically release failed and unattempted claims from one batch.

        Args:
            claims: Claims that did not complete downstream delivery.
        """

        async with self._lock:
            self._inflight.difference_update(claim.consumption_key for claim in claims)

    async def reservation_count(self) -> int:
        """Return the number of unique transaction reservations."""

        async with self._lock:
            return len(self._records)

    async def _persist_locked(self) -> None:
        records = tuple(
            SourceTransactionRecordSnapshot(
                reservation=record.reservation,
                state=record.state,
                correlations=tuple(record.correlations.values()),
                confirmed=record.confirmed,
                terminal_reason=record.terminal_reason,
            )
            for record in self._records.values()
        )
        waiting = tuple(
            correlation
            for correlations in self._waiting_correlations.values()
            for correlation in correlations.values()
        )
        await self._state_store.save(
            SourceTransactionSnapshot(
                records=records,
                waiting_correlations=waiting,
                claimed=tuple(self._claimed),
            )
        )

    def _mark_nonce_siblings_replaced_locked(self, confirmed_record: _TransactionRecord) -> None:
        transaction = confirmed_record.reservation.transaction
        for record in self._records.values():
            candidate = record.reservation.transaction
            if record is confirmed_record or record.state in self._TERMINAL_STATES:
                continue
            if (
                candidate.chain_id == transaction.chain_id
                and candidate.sender == transaction.sender
                and candidate.nonce == transaction.nonce
                and record.confirmed is None
            ):
                record.state = TransactionState.REPLACED
                record.terminal_reason = "same_nonce_transaction_confirmed"

    def _claim_ready_locked(
        self,
        record: _TransactionRecord,
    ) -> tuple[ConfirmedExecutionClaim, ...]:
        confirmed = record.confirmed
        if confirmed is None:
            return ()
        claims: list[ConfirmedExecutionClaim] = []
        for correlation in record.correlations.values():
            for fill in confirmed.fills:
                if not self._correlation_matches_fill(correlation, fill):
                    continue
                consumption_key = CopyConsumptionKey(
                    follower_account=correlation.follower_account,
                    copy_task_id=correlation.copy_task_id,
                    source_fill=fill.identity,
                )
                if consumption_key in self._claimed or consumption_key in self._inflight:
                    continue
                self._inflight.add(consumption_key)
                claims.append(
                    ConfirmedExecutionClaim(
                        consumption_key=consumption_key,
                        correlation=correlation,
                        fill=fill,
                        source_receipt=record.reservation.transaction.source_receipt,
                    )
                )
        return tuple(claims)

    @staticmethod
    def _correlation_matches_fill(
        correlation: LeaderTradeCorrelation,
        fill: ConfirmedSourceFill,
    ) -> bool:
        if correlation.log_index is not None and correlation.log_index != fill.log_index:
            return False
        if correlation.order_hash is not None and correlation.order_hash != fill.order_hash:
            return False
        return (
            correlation.chain_id == fill.chain_id
            and correlation.transaction_hash == fill.transaction_hash
            and correlation.leader_wallet == fill.leader_wallet
            and correlation.token_id == fill.token_id
            and correlation.side is fill.side
        )


class SourceTransactionGate:
    """Coordinate pending reservation, head-driven confirmation, and release."""

    def __init__(
        self,
        registry: SourceTransactionRegistry,
        evm_rpc: EvmRpcPort,
        receipt_decoder: ConfirmedReceiptDecoder,
        execution: ConfirmedSourceExecutionPort,
        clock: Clock,
    ) -> None:
        """Initialize the confirmation gate with narrow injected ports.

        Args:
            registry: Sole owner of transaction and consumption state.
            evm_rpc: Read-only Polygon receipt and nonce lookup port.
            receipt_decoder: Successful receipt fill decoder.
            execution: Downstream confirmed-source execution boundary.
            clock: Deterministic clock for deadline evaluation.
        """

        self._registry = registry
        self._evm_rpc = evm_rpc
        self._receipt_decoder = receipt_decoder
        self._execution = execution
        self._clock = clock

    async def restore(self) -> None:
        """Load durable reservation and consumption state fail-closed."""

        await self._registry.restore()

    async def observe_pending(
        self,
        pending: PendingTransaction,
    ) -> ReservedTransaction | None:
        """Atomically reserve a qualified pending source transaction.

        Args:
            pending: Selector- and address-qualified pending transaction.

        Returns:
            The unique reservation, or ``None`` for a duplicate observation.
        """

        return await self._registry.reserve(pending)

    async def observe_leader_event(
        self,
        correlation: LeaderTradeCorrelation,
    ) -> None:
        """Correlate a leader event without bypassing source confirmation.

        Args:
            correlation: Follower-scoped event associated with a source hash.
        """

        claims = await self._registry.add_correlation(correlation)
        await self._execute_claims(claims)

    async def on_new_head(self, head: EvmHead) -> tuple[ConfirmedTransaction, ...]:
        """Resolve active reservations once for a newly observed block.

        Args:
            head: Unique Polygon ``newHeads`` notification.

        Returns:
            Transactions newly confirmed by this head.
        """

        reservations = await self._registry.reservations_for_head(head)
        confirmed: list[ConfirmedTransaction] = []
        for reservation in reservations:
            result = await self._resolve_reservation(reservation)
            if result is not None:
                confirmed.append(result)
        return tuple(confirmed)

    async def on_mined_transaction(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> ConfirmedTransaction | None:
        """Resolve the exact reservation named by an included log.

        Args:
            chain_id: Source EVM chain identifier.
            transaction_hash: Included transaction hash from the log.

        Returns:
            Newly confirmed transaction, or ``None`` when not reserved or
            still unavailable.
        """

        reservation = await self._registry.reservation_for_transaction(
            chain_id,
            transaction_hash,
        )
        if reservation is None:
            return None
        return await self._resolve_reservation(reservation)

    async def on_disconnect(self) -> None:
        """Fail closed until reconnect resynchronization completes."""

        await self._registry.on_disconnect()

    async def resynchronize(self) -> tuple[ConfirmedTransaction, ...]:
        """Resolve in-flight reservations once and safely resume execution."""

        reservations = await self._registry.resync_reservations()
        confirmed: list[ConfirmedTransaction] = []
        for reservation in reservations:
            result = await self._resolve_reservation(reservation)
            if result is not None:
                confirmed.append(result)
        claims = await self._registry.complete_resync()
        await self._execute_claims(claims)
        return tuple(confirmed)

    async def _resolve_reservation(
        self,
        reservation: ReservedTransaction,
    ) -> ConfirmedTransaction | None:
        transaction = reservation.transaction
        try:
            receipt = await self._evm_rpc.get_transaction_receipt(
                transaction.chain_id,
                transaction.transaction_hash,
            )
            if receipt is None:
                confirmed_nonce = await self._evm_rpc.get_confirmed_nonce(
                    transaction.chain_id,
                    transaction.sender,
                )
                if confirmed_nonce > transaction.nonce:
                    await self._registry.finalize(
                        reservation,
                        TransactionState.REPLACED,
                        "confirmed_nonce_advanced",
                    )
                elif self._clock.now() >= transaction.expires_at:
                    await self._registry.finalize(
                        reservation,
                        TransactionState.EXPIRED,
                        "confirmation_deadline_elapsed",
                    )
                return None
            if receipt.transaction_hash != transaction.transaction_hash:
                await self._registry.finalize(
                    reservation,
                    TransactionState.REJECTED,
                    "receipt_transaction_mismatch",
                )
                return None
            if receipt.status is ReceiptStatus.REVERTED:
                await self._registry.finalize(
                    reservation,
                    TransactionState.REVERTED,
                    "receipt_status_reverted",
                )
                return None
            fills = self._receipt_decoder.decode_confirmed_fills(reservation, receipt)
            if not fills:
                await self._registry.finalize(
                    reservation,
                    TransactionState.REJECTED,
                    "receipt_has_no_matching_fill",
                )
                return None
            confirmed = ConfirmedTransaction(
                reservation=reservation,
                block_number=receipt.block_number,
                block_hash=receipt.block_hash,
                fills=fills,
            )
            claims = await self._registry.confirm(confirmed)
            await self._execute_claims(claims)
            return confirmed
        finally:
            await self._registry.complete_confirmation_check(reservation)

    async def _execute_claims(
        self,
        claims: tuple[ConfirmedExecutionClaim, ...],
    ) -> None:
        for index, claim in enumerate(claims):
            try:
                await self._execution.execute_confirmed_source(claim)
            except BaseException:
                await self._registry.release_execution_claims(claims[index:])
                raise
            await self._registry.complete_execution_claim(claim)


__all__ = [
    "InMemorySourceTransactionStateStore",
    "SourceTransactionGate",
    "SourceTransactionRegistry",
]
