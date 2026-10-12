"""Bounded delivery from one explicit historical job to the existing evidence owner."""

from __future__ import annotations

from typing import Protocol

from pmcopy.domain.evidence import EvidenceEvent


class HistoryDeliveryJournal(Protocol):
    """The journal retains immutable bytes until the local publisher owns them."""

    def pending(self, limit: int = 100) -> tuple[EvidenceEvent, ...]:
        """Return one bounded ordered prefix."""
        ...

    def mark_published(self, identity: str) -> None:
        """Acknowledge only after the evidence owner's durable append."""
        ...


class HistoryEvidenceSink(Protocol):
    """Reuse the application's already-owned local evidence outbox."""

    def append_event(self, value: object) -> int:
        """Commit unchanged identity and bytes synchronously."""
        ...


class HistoryImportRuntime:
    """Never create a publisher, contact Hungary, or run a trading command."""

    def __init__(
        self, journal: HistoryDeliveryJournal, sink: HistoryEvidenceSink, *, batch_size: int = 100
    ) -> None:
        """Inject one explicit job and its existing process-local evidence owner."""
        if type(batch_size) is not int or not 1 <= batch_size <= 100:
            raise ValueError("invalid historical delivery batch size")
        self._journal, self._sink, self._batch_size = journal, sink, batch_size

    def publish_local_once(self) -> int:
        """A crash between append and mark replays the same canonical event safely."""
        delivered = 0
        for event in self._journal.pending(self._batch_size):
            self._sink.append_event(event)
            self._journal.mark_published(event.event_id)
            delivered += 1
        return delivered
