"""Small injected ports separating local durability, clocks and remote delivery."""

from typing import Protocol

from domain.evidence import EvidenceEvent, EvidenceHealth, EvidenceHttpResponse, OutboxItem


class EvidenceClock(Protocol):
    """Native epoch-nanosecond clock with an explicit observer identity."""

    @property
    def clock_id(self) -> str:
        """Identify the same observer clock for comparable capture observations."""
        ...

    def time_ns(self) -> int:
        """Sample native epoch nanoseconds, never reconstructed millisecond precision."""
        ...


class EvidenceOutbox(Protocol):
    """Persist locally before network dispatch and remove only confirmed prefixes."""

    def append(self, event: EvidenceEvent) -> int:
        """Commit one immutable observation and return its local sequence."""
        ...

    def peek(self, limit: int = 100) -> tuple[OutboxItem, ...]:
        """Read a bounded deliverable prefix without deleting anything."""
        ...

    def acknowledge(self, batch: tuple[OutboxItem, ...], response: object) -> None:
        """Remove payloads only for the exact complete acknowledged prefix."""
        ...

    def quarantine(self, batch: tuple[OutboxItem, ...], reason: str) -> None:
        """Retain conflicting evidence and stop sending behind it."""
        ...

    def invalidate(self, reason: str) -> None:
        """Durably latch continuity loss and reserve a receiver-visible gap event."""
        ...

    def health(self) -> EvidenceHealth:
        """Return observed continuity and delivery state without secrets."""
        ...


class EvidenceTransport(Protocol):
    """Asynchronous private write boundary to one fixed evidence receiver."""

    async def post(self, body: bytes) -> EvidenceHttpResponse:
        """Send one bounded batch; ambiguous failures remain caller-owned evidence."""
        ...

    async def aclose(self) -> None:
        """Release network resources without deleting queued observations."""
        ...
