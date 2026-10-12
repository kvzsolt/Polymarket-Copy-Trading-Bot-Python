"""Narrow ports used by source-transaction confirmation services."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from pmcopy.domain.source_transactions import (
    ConfirmedExecutionClaim,
    ConfirmedSourceFill,
    ConfirmedTransaction,
    EvmHead,
    EvmReceipt,
    LeaderTradeCorrelation,
    PendingTransaction,
    ReservedTransaction,
    SourceTransactionSnapshot,
)


class Clock(Protocol):
    """Provide a deterministic timezone-aware current time."""

    def now(self) -> datetime:
        """Return the current timezone-aware instant."""

        ...


class EvmRpcPort(Protocol):
    """Read only the chain state needed to resolve reservations."""

    async def get_transaction_receipt(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> EvmReceipt | None:
        """Return a mined receipt, or ``None`` while unknown or pending."""

        ...

    async def get_confirmed_nonce(self, chain_id: int, address: str) -> int:
        """Return the address nonce at the confirmed chain head."""

        ...


class ConfirmedReceiptDecoder(Protocol):
    """Translate a successful receipt into matching source fills."""

    def decode_confirmed_fills(
        self,
        reservation: ReservedTransaction,
        receipt: EvmReceipt,
    ) -> tuple[ConfirmedSourceFill, ...]:
        """Return confirmed fills that satisfy the pending qualification."""

        ...


class ConfirmedSourceExecutionPort(Protocol):
    """Accept atomically claimed confirmed source fills for copy processing."""

    async def execute_confirmed_source(self, claim: ConfirmedExecutionClaim) -> None:
        """Process one follower-scoped confirmed source claim."""

        ...


class PendingTransactionBuilder(Protocol):
    """Qualify provider notifications into pending domain transactions."""

    def from_notification(
        self,
        notification: dict[str, object],
        watched_wallets: frozenset[str],
    ) -> PendingTransaction | None:
        """Return a qualified pending transaction or ``None``."""

        ...


class SourceConfirmationGate(Protocol):
    """Coordinate reservation and confirmed-source release."""

    async def restore(self) -> None:
        """Load durable state before the event connection starts."""

        ...

    async def observe_pending(
        self,
        pending: PendingTransaction,
    ) -> ReservedTransaction | None:
        """Reserve a qualified pending transaction atomically."""

        ...

    async def observe_leader_event(self, correlation: LeaderTradeCorrelation) -> None:
        """Associate a leader event with a source transaction."""

        ...

    async def on_new_head(self, head: EvmHead) -> tuple[ConfirmedTransaction, ...]:
        """Resolve reservations made visible by a new head."""

        ...

    async def on_mined_transaction(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> ConfirmedTransaction | None:
        """Resolve one exact reservation named by an included log."""

        ...

    async def on_disconnect(self) -> None:
        """Fail closed until deterministic resynchronization."""

        ...

    async def resynchronize(self) -> tuple[ConfirmedTransaction, ...]:
        """Resolve in-flight reservations and safely resume."""

        ...


class SourceTransactionStateStore(Protocol):
    """Persist the minimum source state needed for crash-safe idempotency."""

    async def load(self) -> SourceTransactionSnapshot:
        """Return the latest durable source transaction snapshot."""

        ...

    async def save(self, snapshot: SourceTransactionSnapshot) -> None:
        """Atomically replace the durable source transaction snapshot."""

        ...
