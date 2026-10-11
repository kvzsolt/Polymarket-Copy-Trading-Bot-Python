"""Immutable producer evidence and delivery state; no venue or network dependencies."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

PROTOCOL_VERSION = "aethertrade.evidence.v1"


class EvidenceError(ValueError):
    """Expose a safe machine-readable failure without payloads or credentials."""


@dataclass(frozen=True, slots=True)
class EvidenceEvent:
    """Canonical v1 payload bytes excluding event_id, with a stable producer identity."""

    canonical_bytes: bytes
    event_id: str

    @property
    def digest(self) -> str:
        """Return pmrecon's content digest independent of an explicit producer ID."""
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    def wire(self) -> dict[str, Any]:
        """Return a detached wire payload so callers cannot mutate retained evidence."""
        return {**json.loads(self.canonical_bytes), "event_id": self.event_id}


@dataclass(frozen=True, slots=True)
class OutboxItem:
    """One durable local sequence, unrelated to the receiver's global sequence."""

    sequence: int
    event: EvidenceEvent


@dataclass(frozen=True, slots=True)
class EvidenceHealth:
    """Sticky continuity loss and delivery blocks are distinct from queue depth."""

    valid: bool
    blocked: bool
    reason: str | None
    pending: int
    pending_bytes: int
    quarantined: int


@dataclass(frozen=True, slots=True)
class EvidenceHttpResponse:
    """A bounded decoded receiver reply; no URL, token or raw exception retained."""

    status: int
    body: object
