"""Tap original market frames into local evidence without a network dependency."""

from hashlib import sha256
from typing import Protocol

from pmcopy.interfaces.source_time import ReceiptClock


class MarketEvidenceWriter(Protocol):
    """Bounded local persistence shared with source observations."""

    def append_event(self, event: dict[str, object]) -> int:
        """Durably append one validated original observation."""
        ...

    def invalidate(self, reason: str) -> None:
        """Latch evidence loss visibly and reserve a durable gap."""
        ...


class MarketEvidenceTap:
    """Preserve raw receipt order; local evidence failure never stops risk quotes."""

    def __init__(
        self, writer: MarketEvidenceWriter, clock: ReceiptClock, *, stream_id: str
    ) -> None:
        """Inject local storage and the same observer clock used by chain capture."""
        if not stream_id:
            raise ValueError("market evidence stream identity is required")
        self._writer, self._clock = writer, clock
        self._stream = sha256(stream_id.encode()).hexdigest()
        self._generation = 0
        self._sequence = 0
        self._failed = False

    @property
    def failed(self) -> bool:
        """Expose sticky local loss independently of market connection health."""
        return self._failed

    def observe(self, raw: object) -> bool:
        """Capture before decoding; return false when local evidence is incomplete."""
        receipt = self._clock.capture()
        try:
            frame = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            if not isinstance(frame, str) or len(frame.encode("utf-8")) > 262144:
                raise ValueError("invalid market frame")
        except (UnicodeError, ValueError):
            self._invalidate("invalid_market_frame")
            return False
        if frame in ("PING", "ping", "PONG", "pong", ""):
            return True
        if receipt.wall_timestamp_ns is None:
            self._invalidate("missing_native_clock")
            return False
        self._sequence += 1
        stream = sha256(f"{self._stream}:{receipt.boot_id}:{self._generation}".encode()).hexdigest()
        try:
            self._writer.append_event(
                {
                    "kind": "market_frame",
                    "schema_version": 2,
                    "received_ns": str(receipt.wall_timestamp_ns),
                    "clock_id": receipt.boot_id,
                    "stream_id": stream,
                    "sequence": self._sequence,
                    "frame": frame,
                }
            )
        except Exception:
            self._invalidate("market_evidence_unavailable")
            return False
        return True

    def disconnected(self) -> None:
        """Invalidate book continuity and require a new stream snapshot on reconnect."""
        receipt = self._clock.capture()
        self._generation += 1
        self._sequence = 0
        if receipt.wall_timestamp_ns is None:
            self._invalidate("missing_native_clock")
            return
        try:
            self._writer.append_event(
                {
                    "kind": "gap",
                    "provider": "clob",
                    "reason": "disconnected",
                    "received_ns": str(receipt.wall_timestamp_ns),
                    "clock_id": receipt.boot_id,
                }
            )
        except Exception:
            self._invalidate("market_evidence_unavailable")

    def _invalidate(self, reason: str) -> None:
        self._failed = True
        try:
            self._writer.invalidate(reason)
        except Exception:
            self._failed = True
