"""Deliver one explicit finite history job through the existing local evidence owner."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from config.evidence import EvidenceConfig
from orchestrators.evidence_drain_runtime import DrainWaiter, EvidenceDrainRuntime
from orchestrators.history_import_runtime import HistoryEvidenceSink, HistoryImportRuntime
from storage.history_import import HistoryImportJournal


class HistoryJournalOwner(Protocol):
    """Release a local journal without controlling any other runtime resource."""

    def close(self) -> None:
        """Close this journal's SQLite connection."""
        ...


class HistoryTransfer(Protocol):
    """Publish a bounded immutable batch with no provider or cross-region request."""

    def publish_local_once(self) -> int:
        """Return the count acknowledged by the existing local evidence outbox."""
        ...


class HistoryEvidenceDrainRuntime:
    """Own the history journal lifecycle independently of the trading loop."""

    def __init__(
        self,
        journal: HistoryJournalOwner,
        transfer: HistoryTransfer,
        *,
        waiter: DrainWaiter | None = None,
    ) -> None:
        """Inject local resources and reuse the bounded, interruptible drain scheduler."""
        self._journal, self._transfer = journal, transfer
        self._worker = EvidenceDrainRuntime(self, waiter=waiter)
        self._closed = False

    @classmethod
    def from_config(
        cls,
        config: EvidenceConfig,
        sink: HistoryEvidenceSink,
    ) -> HistoryEvidenceDrainRuntime | None:
        """Open only a reviewed existing job; disabled configuration has no side effects."""
        if config.history_journal_path is None:
            return None
        journal = HistoryImportJournal.open_existing(Path(config.history_journal_path))
        try:
            return cls(journal, HistoryImportRuntime(journal, sink, batch_size=25))
        except BaseException:
            journal.close()
            raise

    @property
    def status(self) -> str:
        """Expose safe delivery health without journal paths or raw exception details."""
        return self._worker.status

    async def start(self) -> None:
        """Start local delivery without waiting for any provider or Hungary service."""
        await self._worker.start()

    async def drain_once(self) -> int:
        """Keep SQLite access on its owning event-loop thread and bound work per turn."""
        return self._transfer.publish_local_once()

    async def aclose(self) -> None:
        """Stop delivery before closing its journal; repeated cleanup is harmless."""
        if not self._closed:
            await self._worker.aclose()
            self._journal.close()
            self._closed = True
