"""Read-only source timestamp contract shared by detection adapters."""

from typing import Protocol
from domain.source_time import TransportReceipt


class BlockTimestampReader(Protocol):
    """Read immutable block time without substituting local processing time."""

    async def timestamp_ms(self, block_hash: str) -> int | None:
        """Return coarse Unix milliseconds for the block, or missing evidence."""
        ...


class ReceiptClock(Protocol):
    """Capture the wall/monotonic pair under one explicit session identity."""

    def capture(self) -> TransportReceipt:
        """Return receipt evidence sampled at the transport boundary."""
        ...
