"""Narrow settlement ports; no vendor clients or signing credentials cross them."""

from contextlib import AbstractAsyncContextManager
from typing import Protocol

from domain.redemption import (
    RedemptionAttempt,
    RedemptionAuthorization,
    RedemptionConfirmation,
    RedemptionMarket,
    RedemptionSnapshot,
)


class SettlementClock(Protocol):
    """Inject time and waiting for deterministic expiry and polling."""

    def now(self) -> int:
        """Return Unix seconds."""
        ...

    def monotonic(self) -> float:
        """Return monotonically increasing elapsed seconds."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Wait without blocking the event loop."""
        ...


class SettlementGateway(Protocol):
    """Separate public discovery, signing, submission, and receipt verification."""

    async def inventory(self, wallet: str) -> tuple[RedemptionMarket, ...]:
        """Read all held binary conditions or fail on incomplete inventory."""
        ...

    async def inspect(self, wallet: str, market: RedemptionMarket) -> RedemptionSnapshot:
        """Validate metadata and return current authoritative chain evidence."""
        ...

    async def prepare(self, wallet: str, snapshot: RedemptionSnapshot) -> RedemptionAuthorization:
        """Prepare an expiring signed batch without submitting it."""
        ...

    async def submit(self, authorization: RedemptionAuthorization) -> str:
        """Submit exactly once and return the Relayer reference, or raise."""
        ...

    async def confirm(self, wallet: str, attempt: RedemptionAttempt) -> RedemptionConfirmation:
        """Check Relayer and receipt evidence without posting another transaction."""
        ...

    async def finalized_timestamp(self) -> int:
        """Return finalized chain time for conservative authorization expiry."""
        ...


class RedemptionJournal(Protocol):
    """Persist reconciliation ownership under a cross-process wallet lock."""

    def lock(self, wallet: str) -> AbstractAsyncContextManager[None]:
        """Hold exclusive settlement ownership until the command exits."""
        ...

    async def pending(self, wallet: str) -> tuple[RedemptionAttempt, ...]:
        """Read all attempts that might already have been submitted."""
        ...

    async def save(self, attempt: RedemptionAttempt) -> None:
        """Commit attempt state before returning, without storing signatures."""
        ...
