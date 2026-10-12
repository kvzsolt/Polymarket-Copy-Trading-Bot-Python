"""Bounded recovery for ambiguous submissions that have no venue order id.

The service is deliberately reconciliation-only.  Its collaborators expose
read-side evidence and guarded persistence transitions; there is no order
submission or retry method in this module.  A lost POST response therefore
remains duplicate-blocking until authoritative evidence recovers its venue
order id or repeated complete absence scans satisfy every configured check.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol, Sequence


_MAX_RECOVERY_BATCH_SIZE = 1_000
_MAX_REQUIRED_ABSENCE_SCANS = 100
_MAX_EVIDENCE_HISTORY = 100


class SubmissionSide(StrEnum):
    """Venue-neutral side retained from the ambiguous submission request."""

    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


class SubmissionAmountUnit(StrEnum):
    """Unit of the persisted ``amount`` on an ambiguous submission.

    Polymarket marketable sizing is unit-asymmetric: a BUY ``amount`` is
    quote (USDC) spend while a SELL ``amount`` is outcome shares.  Recovery
    matching is unit-strict — the unit recorded at submission time decides
    which aggregate the venue evidence must reproduce.  ``None`` (legacy
    rows that predate the field) can never be verified and fails closed.
    """

    QUOTE = "quote"
    SHARES = "shares"


class AccountCheckResult(StrEnum):
    """Result of an authoritative position or collateral balance check."""

    CONSISTENT_WITH_ABSENCE = "consistent_with_absence"
    EVIDENCE_OF_EXECUTION = "evidence_of_execution"
    INCOMPLETE = "incomplete"
    NOT_CHECKED = "not_checked"


class RecoveryMatchSource(StrEnum):
    """Complete private venue collection that identified the submission."""

    PRIVATE_ORDERS = "private_orders"
    PRIVATE_FILLS = "private_fills"


class EvidenceCollectionOutcome(StrEnum):
    """Outcome of one authoritative private-history collection read.

    Audit A03: ``complete``/``incomplete`` booleans alone could not distinguish
    a genuinely exhausted collection from a misread or truncated one.  The
    five outcomes are total and mutually exclusive:

    * ``complete_empty`` — the venue answered a recognized, exhausted
      collection with zero rows.
    * ``complete_nonempty`` — exhausted with rows; supports identity
      matching and, when the submission's candidate set is empty, absence
      proof (the rows are provably unrelated to this submission).
    * ``partial`` — rows may exist but pagination could not prove
      exhaustion (cursor loop or page-budget cutoff).
    * ``unavailable`` — the read could not be performed (transport,
      timeout, auth, rate limit).
    * ``invalid`` — the venue answered a payload whose shape cannot be
      trusted as history (schema drift, error body in a 200).

    Only the two ``complete`` outcomes may support any proof; absence
    additionally requires the candidate set for the submission to be empty.
    """

    COMPLETE_EMPTY = "complete_empty"
    COMPLETE_NONEMPTY = "complete_nonempty"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    INVALID = "invalid"

    @property
    def is_complete(self) -> bool:
        """Whether the collection was proven exhausted (either emptiness)."""

        return self in (
            EvidenceCollectionOutcome.COMPLETE_EMPTY,
            EvidenceCollectionOutcome.COMPLETE_NONEMPTY,
        )


class RecoveryDisposition(StrEnum):
    """One bounded recovery attempt's durable or fail-closed outcome."""

    WAITING_FOR_GRACE = "waiting_for_grace"
    RECOVERED_ORDER_ID = "recovered_order_id"
    EVIDENCE_RECORDED = "evidence_recorded"
    INCONCLUSIVE = "inconclusive"
    CONFIRMED_ABSENT = "confirmed_absent"
    STALE_RECORD = "stale_record"
    # The submission belongs to a different account than this service, or the
    # evidence gateway cannot prove it serves the submission's account.  The
    # row is a complete no-op: no private read, no attach, no proof mutation.
    ACCOUNT_MISMATCH = "account_mismatch"


@dataclass(frozen=True, slots=True)
class AmbiguousSubmission:
    """Immutable persisted request identity used only for evidence matching.

    Optional request fields remain optional so legacy rows with incomplete
    sanitized payloads fail closed in the evidence adapter instead of being
    dropped from recovery.  The idempotency key and user identity are always
    required because repository mutations are scoped by the former and venue
    account evidence by the latter.
    """

    idempotency_key: str
    user_id: str
    leader_id: str | None
    token_id: str | None
    condition_id: str | None
    side: SubmissionSide
    amount: Decimal | None
    price_limit: Decimal | None
    submitted_at: datetime
    amount_unit: SubmissionAmountUnit | None = None

    def __post_init__(self) -> None:
        key = str(self.idempotency_key).strip()
        user_id = str(self.user_id).strip()
        if not key:
            raise ValueError("idempotency_key is required")
        if not user_id:
            raise ValueError("user_id is required")
        if self.submitted_at.tzinfo is None:
            raise ValueError("submitted_at must be timezone-aware")
        if not isinstance(self.side, SubmissionSide):
            raise TypeError("side must be a SubmissionSide")
        if self.amount_unit is not None and not isinstance(
            self.amount_unit, SubmissionAmountUnit
        ):
            raise TypeError("amount_unit must be a SubmissionAmountUnit")

        object.__setattr__(self, "idempotency_key", key)
        object.__setattr__(self, "user_id", user_id)
        object.__setattr__(self, "leader_id", _optional_text(self.leader_id))
        object.__setattr__(self, "token_id", _optional_text(self.token_id))
        object.__setattr__(self, "condition_id", _optional_text(self.condition_id))
        object.__setattr__(self, "submitted_at", self.submitted_at.astimezone(timezone.utc))
        object.__setattr__(self, "amount", _optional_positive_decimal(self.amount, "amount"))
        object.__setattr__(
            self,
            "price_limit",
            _optional_positive_decimal(self.price_limit, "price_limit"),
        )


@dataclass(frozen=True, slots=True)
class RecoveryMatch:
    """Authoritative venue order identity recovered from private history."""

    venue_order_id: str
    source: RecoveryMatchSource

    def __post_init__(self) -> None:
        order_id = str(self.venue_order_id).strip()
        if not order_id:
            raise ValueError("venue_order_id is required")
        if not isinstance(self.source, RecoveryMatchSource):
            raise TypeError("source must be a RecoveryMatchSource")
        object.__setattr__(self, "venue_order_id", order_id)


@dataclass(frozen=True, slots=True)
class AuthoritativeRecoveryScan:
    """One complete-or-incomplete private-history and account-state scan.

    ``private_*_complete`` means the adapter exhausted the authenticated
    collection, including pagination.  A recovered match is valid only when
    its corresponding collection is complete.  Absence (audit A03, fixup 1)
    is qualified over the submission's CANDIDATE SET, not raw collection
    emptiness: the market-scoped collections carry the account's entire
    history on the condition, so unrelated rows must not block the proof.
    Qualification requires BOTH collections complete (``complete_empty`` or
    ``complete_nonempty`` — never ``partial``/``unavailable``/``invalid``),
    BOTH candidate sets empty (no row survives the token-exact matcher for
    THIS submission), no recovered match, and both account checks consistent.
    A scan constructed without declared outcomes or candidate verdicts can
    never qualify.
    """

    private_orders_complete: bool
    private_fills_complete: bool
    position_check: AccountCheckResult
    balance_check: AccountCheckResult
    match: RecoveryMatch | None = None
    private_orders_outcome: EvidenceCollectionOutcome | None = None
    private_fills_outcome: EvidenceCollectionOutcome | None = None
    private_orders_candidates_empty: bool | None = None
    private_fills_candidates_empty: bool | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.position_check, AccountCheckResult):
            raise TypeError("position_check must be an AccountCheckResult")
        if not isinstance(self.balance_check, AccountCheckResult):
            raise TypeError("balance_check must be an AccountCheckResult")
        for outcome, complete, name in (
            (self.private_orders_outcome, self.private_orders_complete, "private_orders"),
            (self.private_fills_outcome, self.private_fills_complete, "private_fills"),
        ):
            if outcome is None:
                continue
            if not isinstance(outcome, EvidenceCollectionOutcome):
                raise TypeError(f"{name}_outcome must be an EvidenceCollectionOutcome")
            if outcome.is_complete != bool(complete):
                raise ValueError(f"{name}_outcome contradicts {name}_complete")
        for candidates_empty, name in (
            (self.private_orders_candidates_empty, "private_orders_candidates_empty"),
            (self.private_fills_candidates_empty, "private_fills_candidates_empty"),
        ):
            if candidates_empty is not None and not isinstance(candidates_empty, bool):
                raise TypeError(f"{name} must be a bool or None")
        if self.match is None:
            return
        if (
            self.match.source is RecoveryMatchSource.PRIVATE_ORDERS
            and not self.private_orders_complete
        ):
            raise ValueError("a private-orders match requires complete private order history")
        if (
            self.match.source is RecoveryMatchSource.PRIVATE_FILLS
            and not self.private_fills_complete
        ):
            raise ValueError("a private-fills match requires complete private fill history")
        if (
            self.match.source is RecoveryMatchSource.PRIVATE_FILLS
            and self.private_fills_candidates_empty is True
        ):
            raise ValueError("a private-fills match requires fill candidates for the submission")
        if (
            self.match.source is RecoveryMatchSource.PRIVATE_ORDERS
            and self.private_orders_candidates_empty is True
        ):
            raise ValueError("a private-orders match requires order candidates for the submission")

    @property
    def qualifies_as_complete_absence(self) -> bool:
        """Whether this scan can advance the consecutive absence proof."""

        return (
            self.match is None
            and self.private_fills_outcome is not None
            and self.private_fills_outcome.is_complete
            and self.private_orders_outcome is not None
            and self.private_orders_outcome.is_complete
            and self.private_fills_candidates_empty is True
            and self.private_orders_candidates_empty is True
            and self.position_check is AccountCheckResult.CONSISTENT_WITH_ABSENCE
            and self.balance_check is AccountCheckResult.CONSISTENT_WITH_ABSENCE
        )


@dataclass(frozen=True, slots=True)
class AmbiguousRecoveryProgress:
    """Bounded evidence progress returned by the persistence port."""

    consecutive_complete_absence_scans: int
    retained_evidence_entries: int

    def __post_init__(self) -> None:
        if self.consecutive_complete_absence_scans < 0:
            raise ValueError("consecutive_complete_absence_scans cannot be negative")
        if self.retained_evidence_entries < 0:
            raise ValueError("retained_evidence_entries cannot be negative")


@dataclass(frozen=True, slots=True)
class AmbiguousRecoveryResult:
    """Immutable result for one candidate processed by a recovery run."""

    idempotency_key: str
    disposition: RecoveryDisposition
    consecutive_complete_absence_scans: int
    venue_order_id: str | None


class Clock(Protocol):
    """UTC wall-clock port used for grace and persisted evidence times."""

    def now(self) -> datetime:
        """Return a timezone-aware current instant."""

        ...


class SystemClock:
    """Production UTC clock."""

    def now(self) -> datetime:
        """Return the current UTC wall-clock instant."""

        return datetime.now(timezone.utc)


class AmbiguousSubmissionEvidenceSource(Protocol):
    """Read-only authoritative evidence provider.

    Implementations may enumerate private orders, private fills, positions,
    and balances.  They must never submit, cancel, or otherwise mutate a venue.
    """

    def wallet_for_user(self, user_id: str) -> str | None:
        """Return the wallet this source's gateway serves for ``user_id``.

        ``None`` means the submission's account is not served by this
        gateway, so any private history collected here would belong to a
        different account and can never be valid evidence.
        """

        ...

    async def scan(self, submission: AmbiguousSubmission) -> AuthoritativeRecoveryScan:
        """Return one bounded scan for ``submission``."""

        ...


class AmbiguousSubmissionRepository(Protocol):
    """Guarded persistence operations required by recovery.

    Every operation carries the caller's ``user_id`` and must be a no-op when
    it does not match the row's owner: batches are scoped by account before
    the bound is applied, and mutations compare-and-set on the account
    identity so one account's recovery can never touch another account's
    rows or absence proof.
    """

    async def list_no_id_ambiguous_submissions(
        self, *, user_id: str, limit: int
    ) -> Sequence[AmbiguousSubmission]:
        """List at most ``limit`` no-ID rows owned by ``user_id``."""

        ...

    async def record_ambiguous_scan_evidence(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        scan: AuthoritativeRecoveryScan,
        observed_at: datetime,
        required_scan_count: int,
        evidence_history_limit: int,
    ) -> AmbiguousRecoveryProgress | None:
        """Append bounded evidence while the row is still ambiguous."""

        ...

    async def attach_recovered_venue_order_id(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        venue_order_id: str,
        source: RecoveryMatchSource,
        recovered_at: datetime,
    ) -> bool:
        """Attach an authoritative venue identity to the still-ambiguous row."""

        ...

    async def mark_ambiguous_submission_confirmed_absent(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        required_scan_count: int,
        confirmed_at: datetime,
    ) -> bool:
        """Atomically resolve only a row whose persisted proof meets the threshold."""

        ...


class AmbiguousSubmissionRecoveryService:
    """Conservatively resolve bounded batches of no-ID ambiguous submissions.

    The service intentionally has no dependency capable of venue submission.
    A scan failure propagates and leaves the persisted row unchanged and
    duplicate-blocking.  Incomplete or contradictory scans reset consecutive
    absence progress and likewise keep the row blocking.

    The service owns exactly one account (``account_user_id``).  A submission
    from any other account — or one whose account wallet cannot be proven to
    be served by the injected evidence gateway — is rejected before any
    private read or persistence mutation, so with N follower accounts each
    recovery loop can only ever see, scan, and resolve its own rows.
    """

    def __init__(
        self,
        *,
        repository: AmbiguousSubmissionRepository,
        evidence_source: AmbiguousSubmissionEvidenceSource,
        account_user_id: str,
        account_wallet: str | None = None,
        clock: Clock | None = None,
        grace_period: timedelta = timedelta(minutes=2),
        required_absence_scans: int = 3,
        batch_size: int = 100,
        evidence_history_limit: int = 8,
    ) -> None:
        owner = str(account_user_id).strip()
        if not owner:
            raise ValueError("account_user_id is required")
        if account_wallet is not None and not str(account_wallet).strip():
            raise ValueError("account_wallet must be a non-empty address when provided")
        if grace_period < timedelta(0):
            raise ValueError("grace_period cannot be negative")
        if not 1 <= required_absence_scans <= _MAX_REQUIRED_ABSENCE_SCANS:
            raise ValueError(
                f"required_absence_scans must be between 1 and {_MAX_REQUIRED_ABSENCE_SCANS}"
            )
        if not 1 <= batch_size <= _MAX_RECOVERY_BATCH_SIZE:
            raise ValueError(f"batch_size must be between 1 and {_MAX_RECOVERY_BATCH_SIZE}")
        if not 1 <= evidence_history_limit <= _MAX_EVIDENCE_HISTORY:
            raise ValueError(
                f"evidence_history_limit must be between 1 and {_MAX_EVIDENCE_HISTORY}"
            )

        self._repository = repository
        self._evidence_source = evidence_source
        self._account_user_id = owner
        self._account_wallet = str(account_wallet).strip() if account_wallet is not None else None
        self._clock = clock if clock is not None else SystemClock()
        self._grace_period = grace_period
        self._required_absence_scans = int(required_absence_scans)
        self._batch_size = int(batch_size)
        self._evidence_history_limit = int(evidence_history_limit)

    async def recover_once(self) -> tuple[AmbiguousRecoveryResult, ...]:
        """Process one bounded batch without ever retrying a venue submission."""

        now = self._clock.now()
        if now.tzinfo is None:
            raise ValueError("Clock.now() must return a timezone-aware datetime")
        now = now.astimezone(timezone.utc)
        submissions = await self._repository.list_no_id_ambiguous_submissions(
            user_id=self._account_user_id,
            limit=self._batch_size,
        )
        results: list[AmbiguousRecoveryResult] = []
        for submission in submissions:
            results.append(await self._recover_submission(submission, now=now))
        return tuple(results)

    async def _recover_submission(
        self,
        submission: AmbiguousSubmission,
        *,
        now: datetime,
    ) -> AmbiguousRecoveryResult:
        if not self._evidence_serves_submission(submission):
            return self._result(submission, RecoveryDisposition.ACCOUNT_MISMATCH)

        if now - submission.submitted_at < self._grace_period:
            return self._result(submission, RecoveryDisposition.WAITING_FOR_GRACE)

        scan = await self._evidence_source.scan(submission)
        if scan.match is not None:
            attached = await self._repository.attach_recovered_venue_order_id(
                user_id=self._account_user_id,
                idempotency_key=submission.idempotency_key,
                venue_order_id=scan.match.venue_order_id,
                source=scan.match.source,
                recovered_at=now,
            )
            return self._result(
                submission,
                (
                    RecoveryDisposition.RECOVERED_ORDER_ID
                    if attached
                    else RecoveryDisposition.STALE_RECORD
                ),
                venue_order_id=scan.match.venue_order_id if attached else None,
            )

        progress = await self._repository.record_ambiguous_scan_evidence(
            user_id=self._account_user_id,
            idempotency_key=submission.idempotency_key,
            scan=scan,
            observed_at=now,
            required_scan_count=self._required_absence_scans,
            evidence_history_limit=self._evidence_history_limit,
        )
        if progress is None:
            return self._result(submission, RecoveryDisposition.STALE_RECORD)

        if not scan.qualifies_as_complete_absence:
            return self._result(
                submission,
                RecoveryDisposition.INCONCLUSIVE,
                consecutive=progress.consecutive_complete_absence_scans,
            )

        if progress.consecutive_complete_absence_scans < self._required_absence_scans:
            return self._result(
                submission,
                RecoveryDisposition.EVIDENCE_RECORDED,
                consecutive=progress.consecutive_complete_absence_scans,
            )

        resolved = await self._repository.mark_ambiguous_submission_confirmed_absent(
            user_id=self._account_user_id,
            idempotency_key=submission.idempotency_key,
            required_scan_count=self._required_absence_scans,
            confirmed_at=now,
        )
        return self._result(
            submission,
            (
                RecoveryDisposition.CONFIRMED_ABSENT
                if resolved
                else RecoveryDisposition.STALE_RECORD
            ),
            consecutive=progress.consecutive_complete_absence_scans,
        )

    def _evidence_serves_submission(self, submission: AmbiguousSubmission) -> bool:
        """Whether this account's gateway provably owns the submission.

        Two independent facts must hold before any private read: the
        submission belongs to this service's account, and the evidence
        gateway resolves the submission's account to the wallet this service
        was wired for.  An evidence source that cannot declare wallets at
        all fails closed (no wallet, no scan).
        """

        if submission.user_id != self._account_user_id:
            return False
        resolver = getattr(self._evidence_source, "wallet_for_user", None)
        if not callable(resolver):
            return False
        wallet = resolver(submission.user_id)
        if wallet is None:
            return False
        if self._account_wallet is not None and wallet != self._account_wallet:
            return False
        return True

    @staticmethod
    def _result(
        submission: AmbiguousSubmission,
        disposition: RecoveryDisposition,
        *,
        consecutive: int = 0,
        venue_order_id: str | None = None,
    ) -> AmbiguousRecoveryResult:
        return AmbiguousRecoveryResult(
            idempotency_key=submission.idempotency_key,
            disposition=disposition,
            consecutive_complete_absence_scans=consecutive,
            venue_order_id=venue_order_id,
        )


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_positive_decimal(value: Decimal | None, field: str) -> Decimal | None:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be decimal-like") from exc
    if not decimal_value.is_finite() or decimal_value <= 0:
        raise ValueError(f"{field} must be finite and positive")
    return decimal_value


__all__ = [
    "AccountCheckResult",
    "AmbiguousRecoveryProgress",
    "AmbiguousRecoveryResult",
    "AmbiguousSubmission",
    "AmbiguousSubmissionEvidenceSource",
    "AmbiguousSubmissionRecoveryService",
    "AmbiguousSubmissionRepository",
    "AuthoritativeRecoveryScan",
    "Clock",
    "EvidenceCollectionOutcome",
    "RecoveryDisposition",
    "RecoveryMatch",
    "RecoveryMatchSource",
    "SubmissionAmountUnit",
    "SubmissionSide",
    "SystemClock",
]
