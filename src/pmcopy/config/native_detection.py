"""Restart-applied native owner detection settings, without provider secrets."""

from dataclasses import dataclass
from collections.abc import Iterable
from pmcopy.domain.grouped_detection import TraderDetectionPolicy


@dataclass(frozen=True)
class NativeDetectionConfig:
    """Configure mined-by-default owner policies and finite local state."""

    state_path: str = "data/native_detection.sqlite3"
    traders: tuple[TraderDetectionPolicy, ...] = ()
    pending_enabled: bool = False
    maximum_inclusions: int = 1_000_000
    recovery_maximum_gap: int = 5000
    recovery_overlap: int = 8

    def __post_init__(self) -> None:
        """Reject duplicate owners and unbounded persistence/recovery settings."""
        if not self.state_path or len({policy.wallet for policy in self.traders}) != len(
            self.traders
        ):
            raise ValueError("invalid native detection state path or duplicate owner policy")
        if type(self.pending_enabled) is not bool:
            raise ValueError("pending_enabled must be a boolean")
        if (
            type(self.maximum_inclusions) is not int
            or not 1 <= self.maximum_inclusions <= 10_000_000
        ):
            raise ValueError("invalid native evidence capacity")
        if (
            not 1 <= self.recovery_overlap <= 100
            or not self.recovery_overlap <= self.recovery_maximum_gap <= 5000
        ):
            raise ValueError("invalid native recovery bounds")

    def policies_for(self, wallets: Iterable[str]) -> tuple[TraderDetectionPolicy, ...]:
        """Return one explicit/default policy for every active owner without truncation."""
        overrides = {policy.wallet: policy for policy in self.traders}
        return tuple(
            overrides.get(wallet, TraderDetectionPolicy(wallet))
            for wallet in sorted({value.lower() for value in wallets})
        )
