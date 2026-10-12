"""Read-only authoritative evidence for no-order-id ambiguous submissions.

A lost POST response leaves an ``orders`` row at ``submitted_unconfirmed`` with
no venue identity.  The venue cannot be asked for "the order for this market"
(``get_order`` needs an id we never received), so recovery is bounded to three
authoritative, read-only observations:

* the paginated private **fill** history for the submission's market;
* the market's **open order** history; and
* the account's outcome-token **position** and collateral **balance**.

This adapter owns only collection and matching.  It never submits, cancels, or
otherwise mutates a venue, and it holds no collaborator capable of doing so.
Every incomplete or unreadable observation fails closed: the returned scan can
then support neither a recovered identity nor an absence proof, so the row
stays duplicate-blocking for the next cycle.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

from services.execution.ambiguous_recovery import (
    AccountCheckResult,
    AmbiguousSubmission,
    AuthoritativeRecoveryScan,
    Clock,
    EvidenceCollectionOutcome,
    RecoveryMatch,
    RecoveryMatchSource,
    SubmissionAmountUnit,
    SubmissionSide,
    SystemClock,
)
from integrations.polymarket.errors import (
    VenueHistoryIncompleteError,
    VenueSchemaError,
)
from utils.logger import get_logger

logger = get_logger("AmbiguousEvidence")

_DEFAULT_READ_TIMEOUT_SECONDS = 10.0
_DEFAULT_MATCH_WINDOW = timedelta(minutes=15)
_DEFAULT_CLOCK_SKEW = timedelta(seconds=60)
# Quantized order sizes/prices are compared with a dust epsilon only; the
# submission amount is persisted post-quantization, so a genuine match is
# exact to the venue tick while never relying on binary float equality.
_AMOUNT_EPSILON = Decimal("0.000001")


@runtime_checkable
class PrivateHistoryGateway(Protocol):
    """Bounded authenticated reads for one market's private history."""

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> Sequence[Any]:
        """Return the complete paginated fill history for ``market``."""

        ...

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> Sequence[Any]:
        """Return the complete paginated open-order set for ``market``."""

        ...


@runtime_checkable
class CompleteSharesReader(Protocol):
    """Outcome-token holdings that return ``None`` for incomplete evidence."""

    async def shares(self, wallet: str, token_id: str) -> Decimal | None:
        """Return exact shares, or ``None`` when the read is not authoritative."""

        ...


class CcxtAmbiguousEvidenceSource:
    """Production ``AmbiguousSubmissionEvidenceSource``.

    All collaborators are injected so the source is testable without a venue
    and so the read-only boundary is structural rather than credential-based.
    """

    def __init__(
        self,
        *,
        private_history: PrivateHistoryGateway,
        shares_reader: CompleteSharesReader,
        balance_reader: Callable[[str], Awaitable[Decimal | None]],
        local_shares_reader: Callable[[str, str, str], Awaitable[Decimal | None]],
        reserved_quote_reader: Callable[[str], Awaitable[Decimal | None]],
        wallet_for_user: Callable[[str], str | None],
        clock: Clock | None = None,
        read_timeout_seconds: float = _DEFAULT_READ_TIMEOUT_SECONDS,
        match_window: timedelta = _DEFAULT_MATCH_WINDOW,
        clock_skew: timedelta = _DEFAULT_CLOCK_SKEW,
    ) -> None:
        if read_timeout_seconds <= 0:
            raise ValueError("read_timeout_seconds must be positive")
        if match_window < timedelta(0):
            raise ValueError("match_window cannot be negative")
        if clock_skew < timedelta(0):
            raise ValueError("clock_skew cannot be negative")

        self._private_history = private_history
        self._shares_reader = shares_reader
        self._balance_reader = balance_reader
        self._local_shares_reader = local_shares_reader
        self._reserved_quote_reader = reserved_quote_reader
        self._wallet_for_user = wallet_for_user
        self._clock = clock if clock is not None else SystemClock()
        self._read_timeout_seconds = float(read_timeout_seconds)
        self._match_window = match_window
        self._clock_skew = clock_skew

    # ------------------------------------------------------------------
    # Account ownership (audit A01)
    # ------------------------------------------------------------------
    def wallet_for_user(self, user_id: str) -> str | None:
        """Return the wallet this gateway serves for ``user_id``, if any.

        The recovery service calls this before any private read to prove the
        submission's account is served by this gateway's credentials.  A
        foreign account resolves to ``None`` and is never scanned here.
        """

        return _text(self._wallet_for_user(user_id))

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------
    async def scan(self, submission: AmbiguousSubmission) -> AuthoritativeRecoveryScan:
        """Return one bounded, read-only evidence scan for ``submission``."""

        # Ownership is verified before any private read: the gateway is
        # authenticated for exactly the configured wallets, and enumerating
        # private history for a submission owned by a different account would
        # read the wrong account's fills/orders.  The token/side/time/price/
        # size match keying cannot distinguish accounts, so a foreign scan
        # could otherwise attach a foreign venue order id.  Fail closed.
        if self.wallet_for_user(submission.user_id) is None:
            logger.warning(
                "ambiguous_evidence_account_not_served | key=%s user=%s",
                submission.idempotency_key,
                submission.user_id,
            )
            return _fail_closed_scan()

        token_id = _text(submission.token_id)
        condition_id = _text(submission.condition_id)
        if token_id is None or condition_id is None:
            # Without a market identity there is nothing authoritative to read;
            # legacy rows with incomplete payloads stay duplicate-blocking.
            logger.warning("ambiguous_evidence_missing_identity key=%s", submission.idempotency_key)
            return _fail_closed_scan()
        if not self._unit_identity_verified(submission):
            # A row that persisted an amount but not its unit (or with a unit
            # contradicting the side's quote/share semantics) can never be
            # verified unit-strictly.  Legacy identity is never fabricated:
            # the scan is a complete fail-closed no-op, so the row keeps
            # blocking duplicates until an operator resolves it.
            logger.warning(
                "ambiguous_evidence_missing_unit_identity key=%s side=%s",
                submission.idempotency_key,
                submission.side.value,
            )
            return _fail_closed_scan()

        fills, fills_outcome = await self._collect(
            self._private_history.get_fills(market=condition_id)
        )
        orders, orders_outcome = await self._collect(
            self._private_history.list_open_orders(market=condition_id)
        )

        match = self._recover_match(
            submission,
            fills,
            orders,
            fills_complete=fills_outcome.is_complete,
            orders_complete=orders_outcome.is_complete,
        )
        position_check = await self._position_check(submission, token_id, condition_id)
        balance_check = await self._balance_check(submission)

        return AuthoritativeRecoveryScan(
            private_orders_complete=orders_outcome.is_complete,
            private_fills_complete=fills_outcome.is_complete,
            private_orders_outcome=orders_outcome,
            private_fills_outcome=fills_outcome,
            position_check=position_check,
            balance_check=balance_check,
            match=match,
        )

    # ------------------------------------------------------------------
    # Bounded reads
    # ------------------------------------------------------------------
    async def _collect(
        self, awaitable: Awaitable[Sequence[Any]]
    ) -> tuple[tuple[Any, ...], EvidenceCollectionOutcome]:
        """Await one private collection and classify how the read ended.

        Audit A03 mapping (kept deliberately narrow):

        * venue :class:`VenueSchemaError` — the payload shape cannot be
          trusted → ``invalid``;
        * venue :class:`VenueHistoryIncompleteError` — pagination could not
          prove exhaustion → ``partial``;
        * any other exception (transport, timeout, auth) → ``unavailable``;
        * a returned non-collection (``None`` included) → ``invalid``;
        * a recognized collection → ``complete_empty``/``complete_nonempty``.

        Cancellation is never caught: ``asyncio.CancelledError`` is a
        ``BaseException`` and must propagate, never become an outcome.
        """

        try:
            async with asyncio.timeout(self._read_timeout_seconds):
                values = await awaitable
        except VenueSchemaError as exc:
            logger.warning(
                "ambiguous_evidence_collection_invalid error_type=%s",
                type(exc).__name__,
            )
            return (), EvidenceCollectionOutcome.INVALID
        except VenueHistoryIncompleteError as exc:
            logger.warning(
                "ambiguous_evidence_collection_partial error_type=%s",
                type(exc).__name__,
            )
            return (), EvidenceCollectionOutcome.PARTIAL
        except Exception as exc:  # noqa: BLE001 - any failure is incomplete evidence
            logger.warning(
                "ambiguous_evidence_collection_unavailable error_type=%s",
                type(exc).__name__,
            )
            return (), EvidenceCollectionOutcome.UNAVAILABLE
        if not isinstance(values, (list, tuple)):
            logger.warning(
                "ambiguous_evidence_collection_invalid value_type=%s",
                type(values).__name__,
            )
            return (), EvidenceCollectionOutcome.INVALID
        if not values:
            return (), EvidenceCollectionOutcome.COMPLETE_EMPTY
        return tuple(values), EvidenceCollectionOutcome.COMPLETE_NONEMPTY

    async def _bounded(self, awaitable: Awaitable[Any]) -> Any:
        """Await one account read; any failure is unknown, never absence."""

        try:
            async with asyncio.timeout(self._read_timeout_seconds):
                return await awaitable
        except Exception as exc:  # noqa: BLE001 - any failure is unknown evidence
            logger.warning(
                "ambiguous_evidence_account_read_unavailable error_type=%s",
                type(exc).__name__,
            )
            return None

    # ------------------------------------------------------------------
    # Identity recovery
    # ------------------------------------------------------------------
    def _recover_match(
        self,
        submission: AmbiguousSubmission,
        fills: Sequence[Any],
        orders: Sequence[Any],
        *,
        fills_complete: bool,
        orders_complete: bool,
    ) -> RecoveryMatch | None:
        fill_match = self._match_fills(submission, fills) if fills_complete else None
        order_match = self._match_orders(submission, orders) if orders_complete else None
        if fill_match is not None and order_match is not None:
            # Two complete collections disagreeing about the venue identity is
            # contradictory evidence: attach nothing.
            if fill_match.venue_order_id != order_match.venue_order_id:
                logger.warning(
                    "ambiguous_evidence_conflicting_match key=%s", submission.idempotency_key
                )
                return None
            return fill_match
        if fill_match is not None:
            return fill_match
        return order_match

    def _match_fills(
        self, submission: AmbiguousSubmission, fills: Sequence[Any]
    ) -> RecoveryMatch | None:
        """Recover an identity only from one consistent, unambiguous order id."""

        aggregate: dict[str, list[Any]] = {}
        for fill in fills:
            if not self._fill_is_candidate(submission, fill):
                continue
            order_id = _text(getattr(fill, "order_id", None))
            if order_id is None:
                continue
            aggregate.setdefault(order_id, []).append(fill)

        consistent = [
            order_id
            for order_id, legs in aggregate.items()
            if self._fills_consistent_with_submission(submission, legs)
        ]
        if len(consistent) != 1:
            return None
        return RecoveryMatch(
            venue_order_id=consistent[0],
            source=RecoveryMatchSource.PRIVATE_FILLS,
        )

    def _match_orders(
        self, submission: AmbiguousSubmission, orders: Sequence[Any]
    ) -> RecoveryMatch | None:
        """Recover an identity from a single matching resting limit order."""

        candidates = {
            order_id
            for order_id in (
                _text(getattr(order, "order_id", None))
                if self._order_is_candidate(submission, order)
                else None
                for order in orders
            )
            if order_id is not None
        }
        if len(candidates) != 1:
            return None
        return RecoveryMatch(
            venue_order_id=next(iter(candidates)),
            source=RecoveryMatchSource.PRIVATE_ORDERS,
        )

    def _fill_is_candidate(self, submission: AmbiguousSubmission, fill: Any) -> bool:
        if _text(getattr(fill, "token_id", None)) != submission.token_id:
            return False
        if str(getattr(fill, "side", "") or "").upper() != submission.side.value:
            return False
        occurred_at = _venue_time(getattr(fill, "timestamp", None))
        if occurred_at is None or not self._within_window(submission, occurred_at):
            return False
        return self._price_respects_limit(submission, getattr(fill, "price", None))

    def _order_is_candidate(self, submission: AmbiguousSubmission, order: Any) -> bool:
        # Outcome-token-exact: two outcomes of one condition share price,
        # size, and side, so only the token can separate a YES submission
        # from a NO venue order.  Missing token identity on either side is
        # a non-match (fail closed), never a guess.
        if _text(getattr(order, "token_id", None)) != submission.token_id:
            return False
        if str(getattr(order, "side", "") or "").upper() != submission.side.value:
            return False
        occurred_at = _as_utc(getattr(order, "created_at", None))
        if occurred_at is None or not self._within_window(submission, occurred_at):
            return False

        venue_price = _decimal(getattr(order, "price", None))
        if venue_price is None or not self._price_respects_limit(submission, venue_price):
            return False
        if submission.price_limit is not None and not _close(venue_price, submission.price_limit):
            return False

        venue_size = _decimal(getattr(order, "size", None))
        if submission.amount is not None:
            if venue_size is None or not _close(venue_size, submission.amount):
                return False
        return True

    def _fills_consistent_with_submission(
        self, submission: AmbiguousSubmission, legs: Sequence[Any]
    ) -> bool:
        """Accept an aggregate that matches the submission in ITS unit.

        Unit-strict: a BUY submission's ``amount`` is quote (USDC) spend,
        so the aggregate quote ``sum(size * price)`` must account for it
        exactly; a SELL submission's ``amount`` is outcome shares, so the
        aggregate share ``sum(size)`` must account for it exactly.  The two
        aggregates are never interchangeable — a 1-share SELL must not
        match a 2-share aggregate whose quote happens to equal 1.
        """

        amount = submission.amount
        if amount is None:
            return True
        total_size = Decimal("0")
        total_quote = Decimal("0")
        for leg in legs:
            size = _decimal(getattr(leg, "size", None))
            price = _decimal(getattr(leg, "price", None))
            if size is None or size <= 0 or price is None or price <= 0:
                return False
            total_size += size
            total_quote += size * price
        if submission.amount_unit is SubmissionAmountUnit.QUOTE:
            return _close(total_quote, amount)
        return _close(total_size, amount)

    def _unit_identity_verified(self, submission: AmbiguousSubmission) -> bool:
        """Whether the submission's amount unit is present and side-coherent.

        A row with no ``amount`` has nothing to verify, so its (absent) unit
        cannot block the scan.  Otherwise the recorded unit must exist and
        agree with the side's quote/share semantics: BUY spends quote,
        SELL sells shares.  Anything else is a corrupt or legacy record and
        fails closed.
        """

        if submission.amount is None:
            return True
        if submission.side is SubmissionSide.BUY:
            return submission.amount_unit is SubmissionAmountUnit.QUOTE
        if submission.side is SubmissionSide.SELL:
            return submission.amount_unit is SubmissionAmountUnit.SHARES
        return False

    def _price_respects_limit(self, submission: AmbiguousSubmission, price: Any) -> bool:
        value = _decimal(price)
        limit = submission.price_limit
        if limit is None:
            return True
        if value is None or value <= 0:
            return False
        if submission.side is SubmissionSide.BUY:
            return value <= limit
        if submission.side is SubmissionSide.SELL:
            return value >= limit
        return False

    def _within_window(self, submission: AmbiguousSubmission, moment: datetime) -> bool:
        return (
            submission.submitted_at - self._clock_skew
            <= moment
            <= submission.submitted_at + self._match_window
        )

    # ------------------------------------------------------------------
    # Account checks
    # ------------------------------------------------------------------
    async def _position_check(
        self,
        submission: AmbiguousSubmission,
        token_id: str,
        condition_id: str,
    ) -> AccountCheckResult:
        """Compare authoritative venue shares against persisted local shares.

        Local positions only ever reflect *confirmed* fills, so a local row is
        the expected pre-submission baseline.  An unexplained or
        direction-mismatched delta is neither absence nor attributable
        execution and therefore fails closed.
        """

        if submission.side not in (SubmissionSide.BUY, SubmissionSide.SELL):
            return AccountCheckResult.INCOMPLETE
        wallet = _text(self._wallet_for_user(submission.user_id))
        if wallet is None:
            return AccountCheckResult.INCOMPLETE

        venue_shares = await self._bounded(self._shares_reader.shares(wallet, token_id))
        if not _is_non_negative_decimal(venue_shares):
            return AccountCheckResult.INCOMPLETE
        local_shares = await self._bounded(
            self._local_shares_reader(submission.user_id, condition_id, token_id)
        )
        if not _is_non_negative_decimal(local_shares):
            return AccountCheckResult.INCOMPLETE

        delta = venue_shares - local_shares
        if delta == 0:
            return AccountCheckResult.CONSISTENT_WITH_ABSENCE
        if submission.side is SubmissionSide.BUY and delta > 0:
            return AccountCheckResult.EVIDENCE_OF_EXECUTION
        if submission.side is SubmissionSide.SELL and delta < 0:
            return AccountCheckResult.EVIDENCE_OF_EXECUTION
        return AccountCheckResult.INCOMPLETE

    async def _balance_check(self, submission: AmbiguousSubmission) -> AccountCheckResult:
        """Bounded collateral-integrity corroboration, never a proof of absence.

        Absence is proven by the two complete private collections plus the
        position check.  This check adds one more fail-closed requirement: the
        account's collateral must be readable and still cover every active BUY
        reservation, so a submission that silently consumed reserved funds
        cannot be declared absent.
        """

        wallet = _text(self._wallet_for_user(submission.user_id))
        if wallet is None:
            return AccountCheckResult.INCOMPLETE

        balance = await self._bounded(self._balance_reader(wallet))
        if not _is_non_negative_decimal(balance):
            return AccountCheckResult.INCOMPLETE
        reserved_quote = await self._bounded(self._reserved_quote_reader(submission.user_id))
        if not _is_non_negative_decimal(reserved_quote):
            return AccountCheckResult.INCOMPLETE

        if balance >= reserved_quote:
            return AccountCheckResult.CONSISTENT_WITH_ABSENCE
        return AccountCheckResult.INCOMPLETE


def _fail_closed_scan() -> AuthoritativeRecoveryScan:
    return AuthoritativeRecoveryScan(
        private_orders_complete=False,
        private_fills_complete=False,
        position_check=AccountCheckResult.INCOMPLETE,
        balance_check=AccountCheckResult.INCOMPLETE,
    )


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _is_non_negative_decimal(value: Any) -> bool:
    parsed = _decimal(value)
    return parsed is not None and parsed >= 0


def _close(left: Decimal, right: Decimal) -> bool:
    return abs(left - right) <= _AMOUNT_EPSILON


def _as_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _venue_time(value: Any) -> datetime | None:
    """Interpret a venue timestamp (epoch seconds) as a UTC instant."""

    if value is None or isinstance(value, bool):
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


__all__ = [
    "CompleteSharesReader",
    "CcxtAmbiguousEvidenceSource",
    "PrivateHistoryGateway",
]
