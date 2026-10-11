"""Exact resolved-position evidence independent of exchange and signing adapters."""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
import re


class SettlementUnits:
    """Render six-decimal integer units without context-sensitive arithmetic."""

    @staticmethod
    def decimal(units: int) -> Decimal:
        """Return an exact decimal, even for the full uint256 range."""
        if type(units) is not int or units < 0:
            raise ValueError("Invalid settlement units")
        whole, fraction = divmod(units, 1_000_000)
        return Decimal(f"{whole}.{fraction:06d}")


@dataclass(frozen=True, slots=True)
class RedemptionMarket:
    """Identify both raw outcome tokens of a standard or negative-risk condition."""

    condition_id: str
    token_ids: tuple[str, str]
    negative_risk: bool
    title: str = ""

    def __post_init__(self) -> None:
        """Reject incomplete binary identity instead of guessing a market type."""
        if (
            not isinstance(self.condition_id, str)
            or re.fullmatch(r"0x[0-9a-fA-F]{64}", self.condition_id) is None
        ):
            raise ValueError("Invalid redemption condition identity")
        if not isinstance(self.token_ids, tuple) or len(self.token_ids) != 2:
            raise ValueError("Redemption requires two outcome identities")
        for token in self.token_ids:
            if not isinstance(token, str) or re.fullmatch(r"[1-9][0-9]*", token) is None:
                raise ValueError("Invalid redemption outcome identity")
            if int(token) >= 2**256:
                raise ValueError("Redemption outcome identity exceeds uint256")
        if self.token_ids[0] == self.token_ids[1] or type(self.negative_risk) is not bool:
            raise ValueError("Conflicting redemption market identity")


@dataclass(frozen=True, slots=True)
class RedemptionSnapshot:
    """Retain exact share balances, payout fractions, and required approval."""

    market: RedemptionMarket
    balances: tuple[int, int]
    payout_numerators: tuple[int, int]
    payout_denominator: int
    approval_required: bool

    def __post_init__(self) -> None:
        """Validate integer chain evidence before making any settlement decision."""
        if not isinstance(self.market, RedemptionMarket):
            raise ValueError("Invalid redemption market")
        for values in (self.balances, self.payout_numerators):
            if not isinstance(values, tuple) or len(values) != 2:
                raise ValueError("Redemption requires binary chain evidence")
            if any(type(value) is not int or not 0 <= value < 2**256 for value in values):
                raise ValueError("Invalid redemption chain units")
        denominator = self.payout_denominator
        if type(denominator) is not int or not 0 <= denominator < 2**256:
            raise ValueError("Invalid redemption payout denominator")
        if sum(self.payout_numerators) != denominator:
            raise ValueError("Inconsistent redemption payout evidence")
        if type(self.approval_required) is not bool:
            raise ValueError("Invalid redemption approval evidence")

    @property
    def eligible(self) -> bool:
        """Return whether resolution and positive held shares permit redemption."""
        return self.payout_denominator > 0 and any(self.balances)

    @property
    def shares(self) -> tuple[Decimal, Decimal]:
        """Return each outcome's six-decimal shares without binary floats."""
        return (
            SettlementUnits.decimal(self.balances[0]),
            SettlementUnits.decimal(self.balances[1]),
        )

    @property
    def expected_payout(self) -> Decimal:
        """Return expected collateral, flooring each outcome in integer micro-units."""
        if not self.payout_denominator:
            return Decimal(0)
        units = sum(
            size * payout // self.payout_denominator
            for size, payout in zip(self.balances, self.payout_numerators, strict=True)
        )
        return SettlementUnits.decimal(units)


@dataclass(frozen=True, slots=True)
class RedemptionAuthorization:
    """Non-secret identity of one expiring, nonce-bound wallet authorization."""

    nonce: int
    deadline: int
    digest: str

    def __post_init__(self) -> None:
        """Reject unusable authorization identities before durable submission."""
        if type(self.nonce) is not int or not 0 <= self.nonce < 2**256:
            raise ValueError("Invalid redemption nonce")
        if type(self.deadline) is not int or self.deadline <= 0:
            raise ValueError("Invalid redemption deadline")
        if re.fullmatch(r"0x[0-9a-f]{64}", self.digest) is None:
            raise ValueError("Invalid redemption authorization digest")


class RedemptionState(StrEnum):
    """Separate planning, reconciliation ownership, and terminal evidence."""

    PLANNED = "planned"
    SKIPPED = "skipped"
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    EXPIRED = "expired"
    CLEARED = "cleared"


@dataclass(frozen=True, slots=True)
class RedemptionAttempt:
    """Durable, secret-free ownership of a single settlement submission."""

    wallet: str
    snapshot: RedemptionSnapshot
    authorization: RedemptionAuthorization
    state: RedemptionState = RedemptionState.PENDING
    transaction_id: str = ""
    transaction_hash: str = ""
    received_payout_units: int | None = None


@dataclass(frozen=True, slots=True)
class RedemptionConfirmation:
    """Receipt-backed confirmation, terminal rejection, or incomplete evidence."""

    state: RedemptionState
    transaction_hash: str = ""
    received_payout_units: int | None = None

    def __post_init__(self) -> None:
        """Require successful receipt identity and exact payout for confirmation."""
        if self.state not in (
            RedemptionState.PENDING,
            RedemptionState.FAILED,
            RedemptionState.CONFIRMED,
        ):
            raise ValueError("Invalid redemption confirmation state")
        if self.state is RedemptionState.CONFIRMED and (
            re.fullmatch(r"0x[0-9a-fA-F]{64}", self.transaction_hash) is None
            or type(self.received_payout_units) is not int
            or self.received_payout_units < 0
        ):
            raise ValueError("Incomplete redemption receipt evidence")


@dataclass(frozen=True, slots=True)
class RedemptionResult:
    """One operator-visible condition result without signing or provider secrets."""

    market: RedemptionMarket
    state: RedemptionState
    snapshot: RedemptionSnapshot | None = None
    reason: str = ""
    transaction_id: str = ""
    transaction_hash: str = ""
    received_payout_units: int | None = None

    @property
    def received_payout(self) -> Decimal | None:
        """Return only receipt-supported collateral, never a projected payout."""
        units = self.received_payout_units
        return None if units is None else SettlementUnits.decimal(units)


@dataclass(frozen=True, slots=True)
class RedemptionReport:
    """Complete or partial results of a single explicit operator command."""

    wallet: str
    execute: bool
    results: tuple[RedemptionResult, ...] = ()
    error: str = ""
    discovered_condition_ids: tuple[str, ...] | None = None
    inspected_condition_ids: tuple[str, ...] = ()

    @property
    def uninspected_condition_ids(self) -> tuple[str, ...]:
        """Return known conditions whose inspection was never attempted this run."""
        inspected = set(self.inspected_condition_ids)
        return tuple(key for key in self.discovered_condition_ids or () if key not in inspected)

    @property
    def scan_complete(self) -> bool:
        """Distinguish a fully visited inventory from an early or pre-discovery stop."""
        return self.discovered_condition_ids is not None and not self.uninspected_condition_ids

    @property
    def summary(self) -> dict[str, int | None]:
        """Count the latest outcome per condition, including recovered attempts.

        Inspected includes failed inspection attempts. Unknown inventory totals
        remain None when discovery could not finish or recovery blocked discovery.
        """
        latest = {result.market.condition_id: result.state for result in self.results}
        counts: dict[str, int | None] = {
            state.value: sum(value is state for value in latest.values())
            for state in RedemptionState
        }
        counts.update(
            discovered=(
                None
                if self.discovered_condition_ids is None
                else len(set(self.discovered_condition_ids))
            ),
            inspected=len(set(self.inspected_condition_ids)),
            uninspected=(
                None
                if self.discovered_condition_ids is None
                else len(self.uninspected_condition_ids)
            ),
        )
        return counts

    @property
    def success(self) -> bool:
        """Return false whenever a requested operation failed or remains pending."""
        return (
            self.scan_complete
            and not self.error
            and all(
                result.state not in (RedemptionState.PENDING, RedemptionState.FAILED)
                for result in self.results
            )
        )


class SettlementError(RuntimeError):
    """Carry a stable safe error code; adapters must never pass provider messages."""


class SettlementAuthError(SettlementError):
    """Stop the wallet on authentication or ownership failure."""


class SettlementUnavailable(SettlementError):
    """Represent incomplete network or chain evidence without implying absence."""


class SettlementMarketError(SettlementError):
    """Reject one market or a definitively rejected submission."""


class SettlementStorageError(SettlementError):
    """Stop submission if durable state or exclusive ownership is unavailable."""


class SettlementAmbiguous(SettlementError):
    """Keep ownership after a POST whose acceptance cannot be established."""
