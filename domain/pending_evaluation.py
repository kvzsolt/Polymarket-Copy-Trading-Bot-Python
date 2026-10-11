"""Read-only pending order evidence; no execution intents or wallet authority guesses."""

from dataclasses import dataclass
import re
from typing import Literal


class PendingEvaluationError(ValueError):
    """Reject unsupported evidence using a safe error code, never raw provider contents."""


@dataclass(frozen=True, slots=True)
class PendingOrder:
    """One native V2 signed order with its role/index inside a single transaction."""

    salt: int
    owner: str
    signer: str
    token_id: int
    maker_amount: int
    taker_amount: int
    side: int
    signature_type: int
    timestamp: int
    metadata: bytes
    builder: bytes
    signature: bytes
    role: Literal["maker", "taker"]
    index: int

    def __post_init__(self) -> None:
        """Keep all financial and identity words exact and bounded to the official ABI."""
        for value in (
            self.salt,
            self.token_id,
            self.maker_amount,
            self.taker_amount,
            self.timestamp,
        ):
            if type(value) is not int or not 0 <= value < 2**256:
                raise PendingEvaluationError("pending_invalid_uint256")
        if self.maker_amount == 0 or self.taker_amount == 0:
            raise PendingEvaluationError("pending_invalid_amounts")
        if type(self.side) is not int or self.side not in (0, 1):
            raise PendingEvaluationError("pending_invalid_side")
        if type(self.signature_type) is not int or self.signature_type not in (0, 1, 2, 3):
            raise PendingEvaluationError("pending_invalid_signature_type")
        for name in ("owner", "signer"):
            value = getattr(self, name)
            if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{40}", value):
                raise PendingEvaluationError("pending_invalid_address")
            object.__setattr__(self, name, value.lower())
        if any(
            not isinstance(value, bytes) or len(value) != 32
            for value in (self.metadata, self.builder)
        ):
            raise PendingEvaluationError("pending_invalid_bytes32")
        if not isinstance(self.signature, bytes) or len(self.signature) > 8192:
            raise PendingEvaluationError("pending_signature_exceeds_bound")
        if (
            self.role not in ("maker", "taker")
            or type(self.index) is not int
            or not 0 <= self.index <= 4096
        ):
            raise PendingEvaluationError("pending_invalid_order_location")


@dataclass(frozen=True, slots=True)
class PendingEvaluationLimits:
    """Finite read-only sampling session; no unbounded hash map or body lookup queue."""

    max_hashes: int = 1000
    max_body_lookups: int = 100
    max_inflight: int = 4
    duration_ns: int = 60_000_000_000
    body_timeout_ns: int = 3_000_000_000
    backoff_ns: int = 5_000_000_000
    max_frame_bytes: int = 16384

    def __post_init__(self) -> None:
        """Reject configuration that would remove a hard work or time bound."""
        bounds = (
            (self.max_hashes, 10000),
            (self.max_body_lookups, 1000),
            (self.max_inflight, 16),
            (self.duration_ns, 300_000_000_000),
            (self.body_timeout_ns, 30_000_000_000),
            (self.backoff_ns, 60_000_000_000),
            (self.max_frame_bytes, 16384),
        )
        if any(type(value) is not int or not 0 < value <= maximum for value, maximum in bounds):
            raise PendingEvaluationError("pending_invalid_limits")


@dataclass(frozen=True, slots=True)
class PendingObservation:
    """Small immutable session summary without a profitability score or execution intent."""

    transaction_hash: str
    stage: str
    source_event_id: str | None
