"""Per-cycle discovery outcome counters.

``FalconDiscoveryService.run`` returns accepted candidates only, so an empty
roster alone never explains itself.  This module carries the small immutable
counter set that lets the orchestration layer log *why* a cycle produced no
leader without re-reading the persisted snapshots.

Pure domain data: no I/O, no provider access and no secrets.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable, Mapping

__all__ = ["DiscoveryCycleSummary"]


@dataclass(frozen=True, slots=True)
class DiscoveryCycleSummary:
    """Summarize one discovery cycle's accept/reject outcome.

    Attributes:
        accepted: Number of wallets that passed every gate.
        rejected: Number of wallets that failed at least one gate.
        rejection_reasons: Read-only histogram of machine-readable rejection
            reasons; one wallet contributes to every reason it raised.
    """

    accepted: int
    rejected: int
    rejection_reasons: Mapping[str, int]

    def __post_init__(self) -> None:
        counts = {str(key): int(value) for key, value in dict(self.rejection_reasons).items()}
        object.__setattr__(self, "rejection_reasons", MappingProxyType(counts))

    @classmethod
    def from_reasons(
        cls,
        *,
        accepted: int,
        rejected_reasons: Iterable[Iterable[str]],
    ) -> "DiscoveryCycleSummary":
        """Build a summary from the per-wallet rejection reason lists.

        Args:
            accepted: Number of wallets that passed every gate.
            rejected_reasons: One iterable of reasons per rejected wallet.

        Returns:
            The immutable summary, with an empty histogram when nothing was
            rejected.
        """
        histogram: Counter[str] = Counter()
        rejected = 0
        for reasons in rejected_reasons:
            rejected += 1
            histogram.update(str(reason) for reason in reasons)
        return cls(accepted=int(accepted), rejected=rejected, rejection_reasons=dict(histogram))

    def as_log_extras(self) -> dict[str, object]:
        """Return the structured logging extras describing this cycle."""
        return {
            "accepted_count": self.accepted,
            "rejected_count": self.rejected,
            "rejection_reasons": dict(self.rejection_reasons),
        }
