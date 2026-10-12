"""Optional read-only input seam for externally computed copyability evidence.

No reader is installed by default. These records neither run research nor
authorize execution; mandatory admission and financial checks remain separate.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol


class AssessmentConclusion(StrEnum):
    """A negative assessment differs from a lack of adequate evidence."""

    FAVORABLE = "favorable"
    UNFAVORABLE = "unfavorable"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


@dataclass(frozen=True, slots=True)
class AssessmentScope:
    """Leader/category and exact stake/delay assumptions used by a method."""

    leader_wallet: str
    category: str
    sizing_mode: str
    collateral_amount: Decimal
    assumed_delay_seconds: Decimal

    def __post_init__(self) -> None:
        wallet = self.leader_wallet.strip().lower()
        if (
            len(wallet) != 42
            or not wallet.startswith("0x")
            or any(c not in "0123456789abcdef" for c in wallet[2:])
        ):
            raise ValueError("Assessment leader must be a valid wallet")
        if not self.category.strip() or not self.sizing_mode.strip():
            raise ValueError("Assessment category and sizing mode are required")
        for value in (self.collateral_amount, self.assumed_delay_seconds):
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                raise ValueError(
                    "Assessment amounts and delays must be finite non-negative Decimals"
                )
        if self.collateral_amount == 0:
            raise ValueError("Assessment collateral must be positive")
        object.__setattr__(self, "leader_wallet", wallet)


@dataclass(frozen=True, slots=True)
class CachedCopyabilityAssessment:
    """Immutable method-specific evidence with explicit freshness and assumptions.

    A score, when supplied, retains the method's units. It is never treated
    as a win probability or generated from simulated follower fills here.
    """

    scope: AssessmentScope
    data_as_of: datetime
    expires_at: datetime
    policy_revision: str
    method_version: str
    conclusion: AssessmentConclusion
    score: Decimal | None = None

    def __post_init__(self) -> None:
        for instant in (self.data_as_of, self.expires_at):
            if instant.tzinfo is None or instant.utcoffset() is None:
                raise ValueError("Assessment timestamps must be timezone-aware")
        if self.expires_at <= self.data_as_of:
            raise ValueError("Assessment expiry must follow its data as-of time")
        if not self.policy_revision.strip() or not self.method_version.strip():
            raise ValueError("Assessment policy and method versions are required")
        if not isinstance(self.conclusion, AssessmentConclusion):
            raise ValueError("Assessment conclusion is invalid")
        if self.score is not None and (
            not isinstance(self.score, Decimal) or not self.score.is_finite()
        ):
            raise ValueError("Assessment score must be a finite Decimal or missing")

    def applies_to(self, scope: AssessmentScope, *, now: datetime) -> bool:
        """Return whether current evidence covers these exact copying assumptions."""
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Assessment evaluation time must be timezone-aware")
        return scope == self.scope and self.data_as_of <= now < self.expires_at


class CachedCopyabilityReader(Protocol):
    """Read an existing local assessment without starting research or placing orders."""

    def read(self, scope: AssessmentScope) -> CachedCopyabilityAssessment | None:
        """Return cached evidence or None; callers must still validate its expiry."""
        ...
