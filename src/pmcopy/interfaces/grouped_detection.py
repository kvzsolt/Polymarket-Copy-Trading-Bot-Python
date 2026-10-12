"""Narrow persistence port for grouped-source identity and policy projections."""

from collections.abc import Mapping, Sequence
from typing import Literal, Protocol

from pmcopy.domain.grouped_detection import NativeOwnerFill, TraderDetectionPolicy, TraderDetectionState
from pmcopy.domain.source_time import TransportReceipt

ObservationDisposition = Literal["new", "removed", "duplicate", "recovered", "reincluded"]


class DetectionStateStore(Protocol):
    """Record source evidence independently from per-follower execution consumption."""

    def configure(self, policies: Sequence[TraderDetectionPolicy]) -> None:
        """Persist desired policies while clearing stale effective session state."""
        ...

    def observe(self, fill: NativeOwnerFill, *, recovered: bool) -> ObservationDisposition:
        """Persist the inclusion or tombstone and classify dispatch eligibility."""
        ...

    def set_effective(self, wallets: Sequence[str], mode: str, reason: str | None) -> None:
        """Persist observed source state without rewriting desired policies."""
        ...

    def await_restart(self, policies: Sequence[TraderDetectionPolicy]) -> None:
        """Expose unacknowledged new owners without claiming live coverage."""
        ...

    def record_observation(self, wallet: str, received_ms: int) -> None:
        """Persist original local receipt time of valid evidence."""
        ...

    def states(self) -> tuple[TraderDetectionState, ...]:
        """Return configured and effective states in stable wallet order."""
        ...

    def checkpoint(self, source: str) -> int | None:
        """Read the last source height, recovered with overlap after a reconnect."""
        ...

    def advance_checkpoint(self, source: str, block_number: int) -> None:
        """Persist a monotonically increasing source height."""
        ...

    def included_blocks(self, start: int, end: int) -> tuple[tuple[int, str], ...]:
        """Read prior block identities in a bounded recovery interval."""
        ...

    def invalidate_block(self, number: int, block_hash: str) -> int:
        """Retain removal tombstones for a canonically replaced block."""
        ...


class RawObservationSink(Protocol):
    """Append local raw evidence before financial derivation, never remote I/O."""

    def record(
        self,
        provider: str,
        kind: str,
        payload: object,
        receipt: "TransportReceipt",
        *,
        recovered: bool,
    ) -> None:
        """Durably record the original source payload and clock precision."""
        ...


class NativeObservationSink(Protocol):
    """Consume one raw log while preserving source and historical provenance."""

    async def observe(
        self, payload: "Mapping[str, object]", receipt: "TransportReceipt", *, recovered: bool
    ) -> str:
        """Record evidence and dispatch only eligible fresh owner actions."""
        ...


class AdmittedRpcReader(Protocol):
    """Read approved provider data with shared pre-dispatch budget admission."""

    async def call(
        self, provider: str, method: str, params: list[object], *, category: str = "essential"
    ) -> object:
        """Perform one admitted read without implicit retry."""
        ...
