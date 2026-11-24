"""Reserve collateral or shares atomically before venue submission."""

import logging
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol, Awaitable, Callable, Any

from pmcopy.domain.order_lifecycle import OrderIntent, IntentSide, Reservation
from pmcopy.interfaces.repositories import (
    AbstractPositionRepository,
    AbstractReservationRepository,
    DuplicateActiveReservationError,
)
from pmcopy.services.execution.reservation_expiry import ReservationExpiryDecision

logger = logging.getLogger(__name__)

# Release reasons written by the expiry sweep, per outcome class. ``None``
# means the decision denies release (the reservation stays active).
_EXPIRY_RELEASE_REASONS: dict[ReservationExpiryDecision, str] = {
    ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE: "expired_terminal",
    ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN: "expired_pre_submit_orphan",
}


def _expiry_release_reason(callback_result: Any) -> str | None:
    """Map a reconciler-callback result to a release reason (None = keep).

    The callback returns a :class:`ReservationExpiryDecision` under the typed
    contract. Legacy boolean callbacks stay valid: ``True`` releases with the
    historical ``expired_terminal`` reason, ``False``/``None`` keep the
    reservation active (``None`` marks asynchronous ownership).
    """

    if isinstance(callback_result, ReservationExpiryDecision):
        return _EXPIRY_RELEASE_REASONS.get(callback_result)
    if callback_result is True:
        return "expired_terminal"
    return None


class ReservationConflict(Exception):
    """Signal a conflicting active reservation for the same intent."""

    pass


class InsufficientFreeQuote(Exception):
    """Signal insufficient unreserved quote collateral."""

    pass


class CopierOpenExposureCapExceeded(Exception):
    """A BUY would push the copier past its explicitly configured open-exposure cap.

    Raised by the authoritative checked-reservation transaction (and the
    fail-closed evidence path before it) when the account's confirmed open
    exposure plus active reserved quote plus this reservation would exceed
    the schema-v2 ``limits.max_open_exposure_quote`` cap, or when the
    confirmed-exposure evidence is unavailable under a configured cap.
    """


class InsufficientFreeShares(Exception):
    """Signal insufficient unreserved outcome shares."""

    pass


class Clock(Protocol):
    """Define the Clock port used by application services."""

    def utcnow_timestamp(self) -> int:
        """Return the current UTC POSIX timestamp."""
        ...


class DefaultClock(Clock):
    """Provide production UTC timestamps to reservation logic."""

    def utcnow_timestamp(self) -> int:
        """Return the current UTC POSIX timestamp."""
        return int(time.time())


class ReconcilerCallback(Protocol):
    """Define the ReconcilerCallback port used by application services.

    The callback classifies one expired reservation into a
    :class:`~services.execution.reservation_expiry.ReservationExpiryDecision`.
    Only ``RELEASE_TERMINAL_EVIDENCE`` and ``RELEASE_PRE_SUBMIT_ORPHAN``
    authorize release; unresolved submissions (``KEEP_PENDING_RECOVERY``) and
    persistence outages (``PERSISTENCE_UNAVAILABLE``) keep the reservation
    active. Legacy boolean returns (``True`` releases, ``False`` keeps) and
    ``None`` (asynchronous ownership, keep) remain accepted.
    """

    async def __call__(self, reservation_id: str) -> ReservationExpiryDecision | bool | None: ...


@dataclass
class ReservationScopeTotals:
    """Describe exact active reservation totals for one scope."""

    reserved_usdc: Decimal
    reserved_size: Decimal


class ReservationService:
    """Reserve and release trading capacity through an injected repository.

    Reservation release is proof-gated. The legacy
    ``force_release_grace_seconds`` constructor argument remains accepted for
    compatibility, but elapsed time alone never authorizes share or quote
    capacity to be unlocked after an ambiguous submission.
    """

    def __init__(
        self,
        reservations_repo: AbstractReservationRepository,
        positions_repo: AbstractPositionRepository,
        get_balance_func: Callable[[str], Awaitable[Decimal | None]],
        funder_address: str,
        clock: Clock | None = None,
        ttl_seconds: int = 120,
        reconciler_callback: ReconcilerCallback | None = None,
        enabled: bool = True,
        force_release_grace_seconds: float | None = None,
        task_market_fraction: Decimal | None = None,
        task_max_open_markets: int = 0,
        open_exposure_cap: Decimal | None = None,
        confirmed_exposure_reader: Callable[[str], Awaitable[Decimal | None]] | None = None,
    ):
        self.reservations_repo = reservations_repo
        self.positions_repo = positions_repo
        self.get_balance_func = get_balance_func
        self.funder_address = funder_address
        # Mutable default DefaultClock() was a Python anti-pattern: every
        # caller relying on the default shared one instance, which would
        # mask state-sharing bugs if Clock ever gained state. Build per
        # instance.
        self.clock = clock if clock is not None else DefaultClock()
        self.ttl_seconds = ttl_seconds
        self.reconciler_callback = reconciler_callback
        self.enabled = enabled
        self.force_release_grace_seconds = force_release_grace_seconds
        if task_market_fraction is not None and (
            not isinstance(task_market_fraction, Decimal)
            or not task_market_fraction.is_finite()
            or not Decimal("0") < task_market_fraction <= Decimal("1")
        ):
            raise ValueError("Task market capacity fraction must be in (0, 1]")
        if (
            isinstance(task_max_open_markets, bool)
            or not isinstance(task_max_open_markets, int)
            or task_max_open_markets < 0
        ):
            raise ValueError("Task open-market limit must be a non-negative integer")
        self._task_market_fraction = task_market_fraction
        self._task_max_open_markets = task_max_open_markets
        # Task 24 fix round 2 — an explicitly configured copier-level open
        # exposure cap (schema v2 ``limits.max_open_exposure_quote``) plus
        # the reader for its CONFIRMED open-exposure basis. Both default to
        # None (legacy accounts: zero new work on the reservation path).
        if open_exposure_cap is not None and (
            not isinstance(open_exposure_cap, Decimal)
            or not open_exposure_cap.is_finite()
            or open_exposure_cap <= 0
        ):
            raise ValueError("open_exposure_cap must be a positive finite Decimal")
        self.open_exposure_cap = open_exposure_cap
        self.confirmed_exposure_reader = confirmed_exposure_reader

    async def reserve_for_intent(self, intent: OrderIntent) -> Reservation:
        """Atomically reserve the capacity required by an execution intent."""
        stable_key = intent.key.stable_key()
        if not self.enabled:
            logger.info(
                "reservation.acquire.disabled key=%s side=%s",
                stable_key,
                intent.key.side.value,
            )
            return self._disabled_reservation(intent, stable_key)

        # Fast-path lookup: most callers hit this once with no prior
        # reservation. The auto-committed read here is intentionally
        # racy — it short-circuits the common no-contention path before
        # acquiring an advisory lock. The BUY branch below closes the
        # race window by re-reading inside ``create_buy_reservation_
        # checked_by_intent``'s transaction; the SELL branch closes its
        # equivalent window via the per-(user, condition, token) lock in
        # ``create_sell_reservation_checked``. ``ReservationConflict``
        # from ``UniqueViolationError`` covers the residual.
        existing = await self.reservations_repo.get_active_by_intent_key(stable_key)

        if existing is None:
            # Transitional dual-read for the ``source_event_id`` key-format
            # change. A reservation created by a build that predates that
            # component is stored under the 6-field legacy key and is
            # invisible to the 7-field lookup above, which would let a second
            # reservation be taken on the same scope until the orphan expires.
            # Re-check under the legacy key so an orphaned reservation still
            # blocks (active) or is expire-reconciled (stale). New
            # reservations are always stored under the 7-field ``stable_key``,
            # so this only ever matches pre-deployment rows and is a no-op
            # once they drain (see OrderIntentKey.stable_key DEPLOYMENT NOTE).
            legacy_key = intent.key.legacy_stable_key()
            if legacy_key != stable_key:
                existing = await self.reservations_repo.get_active_by_intent_key(legacy_key)

        if existing:
            if existing.expires_at.timestamp() < self.clock.utcnow_timestamp():
                logger.info(
                    "reservation.acquire.expired_existing key=%s id=%s — "
                    "triggering reconciliation before release",
                    stable_key,
                    existing.id,
                )
                released = await self._expire_with_reconciliation(existing)
                if not released:
                    raise ReservationConflict(
                        f"expired reservation still pending reconciliation for intent={stable_key}"
                    )
            else:
                logger.warning(
                    "reservation.acquire.blocked key=%s id=%s — active reservation already exists",
                    stable_key,
                    existing.id,
                )
                raise ReservationConflict(f"active reservation exists for intent={stable_key}")

        expires_at = self.clock.utcnow_timestamp() + self.ttl_seconds

        if intent.key.side == IntentSide.BUY:
            required_usdc = intent.size * intent.price
            balance = await self._get_available_balance(intent.key.user_id, required_usdc)
            if intent.task_id is not None and self._task_market_fraction is not None:
                intent = replace(
                    intent,
                    max_market_notional=balance * self._task_market_fraction,
                    max_open_markets=self._task_max_open_markets,
                )
            logger.info(
                "reservation.acquire.buy key=%s size=%s price=%s usdc=%s expires=%d",
                stable_key,
                intent.size,
                intent.price,
                required_usdc,
                expires_at,
            )
            # Task 24 fix round 2 — resolve the copier open-exposure cap's
            # CONFIRMED basis before the transaction. The transaction itself
            # remains authoritative for the cap; this read only supplies the
            # basis value. Unknown/unreadable evidence fails closed (never
            # reserve under a configured cap without proof).
            _open_exposure_cap = self.open_exposure_cap
            _confirmed_open_exposure = None
            if _open_exposure_cap is not None:
                reader = self.confirmed_exposure_reader
                if reader is None:
                    raise CopierOpenExposureCapExceeded(
                        "copier open-exposure cap configured without a "
                        "confirmed-exposure reader — failing closed"
                    )
                try:
                    _confirmed_open_exposure = await reader(intent.key.user_id)
                except Exception as exc:
                    logger.warning(
                        "reservation.cap_confirmed_exposure_unavailable | user=%s error_type=%s",
                        intent.key.user_id,
                        type(exc).__name__,
                    )
                    _confirmed_open_exposure = None
                if _confirmed_open_exposure is None:
                    raise CopierOpenExposureCapExceeded(
                        "confirmed open exposure unavailable under the "
                        "configured copier cap — failing closed"
                    )
            try:
                # Atomic intent-key check + capacity check + insert inside
                # one advisory-lock transaction. Closes the race window the
                # outer ``get_active_by_intent_key`` fast-path cannot cover
                # (Greptile finding in PR #108) — and, when a copier
                # open-exposure cap is configured, the cap race across
                # DIFFERENT markets (Task 24 fix round 2).
                (
                    existing_under_lock,
                    result,
                    failure_reason,
                ) = await self.reservations_repo.create_buy_reservation_checked_by_intent(
                    intent=intent,
                    reserved_usdc=required_usdc,
                    expires_at=expires_at,
                    available_balance=balance,
                    open_exposure_cap=_open_exposure_cap,
                    confirmed_open_exposure=_confirmed_open_exposure,
                )
            except DuplicateActiveReservationError as exc:
                raise ReservationConflict(
                    f"active reservation exists for intent={stable_key}"
                ) from exc
            except Exception as exc:
                if type(exc).__name__ == "UniqueViolationError":
                    raise ReservationConflict(
                        f"active reservation exists for intent={stable_key}"
                    ) from exc
                raise
            if existing_under_lock is not None:
                # A concurrent caller raced past the fast-path lookup and
                # committed an active row before we acquired the lock.
                logger.warning(
                    "reservation.acquire.buy.race_lost key=%s existing_id=%s",
                    stable_key,
                    existing_under_lock.id,
                )
                raise ReservationConflict(f"active reservation exists for intent={stable_key}")
            if result is None:
                if failure_reason in {
                    "copier_open_exposure_cap",
                    "copier_open_exposure_unavailable",
                }:
                    raise CopierOpenExposureCapExceeded(
                        f"copier open-exposure cap exceeded after atomic check: "
                        f"need {required_usdc} ({failure_reason})"
                    )
                raise InsufficientFreeQuote(
                    f"insufficient free USDC after atomic check: need {required_usdc}"
                )
            logger.info(
                "reservation.acquire.buy.ok id=%s key=%s usdc=%s",
                result.id,
                stable_key,
                required_usdc,
            )
            return result

        if intent.key.side == IntentSide.SELL:
            if intent.sell_fraction is None:
                held_size = await self._get_held_size(
                    intent.key.user_id,
                    intent.key.condition_id,
                    intent.key.token_id,
                    intent.size,
                )
            else:
                held_size = await self._get_position_size(
                    intent.key.user_id,
                    intent.key.condition_id,
                    intent.key.token_id,
                    intent.size,
                )
            logger.info(
                "reservation.acquire.sell key=%s size=%s expires=%d",
                stable_key,
                intent.size,
                expires_at,
            )
            try:
                # Atomic intent-key check + capacity check + insert under
                # the per-(user, condition, token) advisory lock, closing
                # the fast-path lookup race that the BUY branch already
                # covers via ``create_buy_reservation_checked_by_intent``.
                (
                    existing_under_lock,
                    result,
                ) = await self.reservations_repo.create_sell_reservation_checked_by_intent(
                    intent=intent,
                    reserved_size=intent.size,
                    expires_at=expires_at,
                    held_size=held_size,
                    sell_fraction=intent.sell_fraction,
                    live_position_size=intent.live_position_size,
                    min_order_size=intent.min_order_size,
                    round_up_to_minimum=intent.round_up_to_minimum,
                )
            except DuplicateActiveReservationError as exc:
                raise ReservationConflict(
                    f"active reservation exists for intent={stable_key}"
                ) from exc
            except Exception as exc:
                if type(exc).__name__ == "UniqueViolationError":
                    raise ReservationConflict(
                        f"active reservation exists for intent={stable_key}"
                    ) from exc
                raise
            if existing_under_lock is not None:
                logger.warning(
                    "reservation.acquire.sell.race_lost key=%s existing_id=%s",
                    stable_key,
                    existing_under_lock.id,
                )
                raise ReservationConflict(f"active reservation exists for intent={stable_key}")
            if result is None:
                raise InsufficientFreeShares(
                    f"insufficient free shares after atomic check: need {intent.size}"
                )
            logger.info(
                "reservation.acquire.sell.ok id=%s key=%s size=%s",
                result.id,
                stable_key,
                result.reserved_size,
            )
            return result

        raise ValueError(f"unsupported side={intent.key.side}")

    async def _get_available_balance(
        self,
        user_id: str,
        required_usdc: Decimal,
    ) -> Decimal:
        raw_balance = await self.get_balance_func(self.funder_address)
        if raw_balance is None:
            raise ValueError("failed to fetch available USDC balance")

        # Contract is Decimal | None, but defensively coerce a legacy
        # float caller via Decimal(str(x)) so a stale float-returning
        # adapter cannot leak IEEE-754 representations into the gate.
        balance = raw_balance if isinstance(raw_balance, Decimal) else Decimal(str(raw_balance))
        # Quote balance is account/funder-wide. Do not scope BUY reserved
        # USDC by condition/token, or two different markets can collectively
        # over-reserve the same wallet balance.
        already_reserved = await self.reservations_repo.sum_active_quote_for_user(user_id)
        free_balance = balance - already_reserved

        logger.info(
            "reservation.check.buy user=%s balance=%s already_reserved=%s free=%s required=%s",
            user_id,
            balance,
            already_reserved,
            free_balance,
            required_usdc,
        )

        if free_balance < required_usdc:
            already_reserved = await self._reap_expired_quote_before_reject(
                user_id=user_id,
                already_reserved=already_reserved,
            )
            free_balance = balance - already_reserved

        if free_balance < required_usdc:
            raise InsufficientFreeQuote(
                f"insufficient free USDC: need {required_usdc}, "
                f"free={free_balance} (balance={balance}, "
                f"reserved={already_reserved})"
            )
        return balance

    async def _reap_expired_quote_before_reject(
        self,
        *,
        user_id: str,
        already_reserved: Decimal,
    ) -> Decimal:
        if already_reserved <= 0:
            return already_reserved
        try:
            released_ids = await self.release_expired_active()
        except Exception as exc:
            logger.warning(
                "reservation.check.buy.expiry_reap_failed user=%s error=%r",
                user_id,
                exc,
            )
            return already_reserved
        refreshed = await self.reservations_repo.sum_active_quote_for_user(user_id)
        if refreshed != already_reserved:
            logger.info(
                "reservation.check.buy.expiry_reap_applied user=%s "
                "released_or_enqueued=%d reserved_before=%s reserved_after=%s",
                user_id,
                len(released_ids or []),
                already_reserved,
                refreshed,
            )
        return refreshed

    async def _get_held_size(
        self,
        user_id: str,
        condition_id: str,
        token_id: str,
        required_size: Decimal,
    ) -> Decimal:
        matching_pos = await self._find_position(user_id, condition_id, token_id)

        if not matching_pos:
            raise InsufficientFreeShares(
                f"no position found for token {token_id} to sell {required_size}"
            )

        held = Decimal(matching_pos.size_tokens)
        already_reserved = await self._get_reserved_size(user_id, condition_id, token_id)
        free_shares = held - already_reserved

        logger.info(
            "reservation.check.sell user=%s held=%s already_reserved=%s free=%s required=%s",
            user_id,
            held,
            already_reserved,
            free_shares,
            required_size,
        )

        if free_shares < required_size:
            raise InsufficientFreeShares(
                f"insufficient free shares: need {required_size}, "
                f"free={free_shares} (held={held}, "
                f"reserved={already_reserved})"
            )
        return held

    async def _get_position_size(
        self,
        user_id: str,
        condition_id: str,
        token_id: str,
        requested_size: Decimal,
    ) -> Decimal:
        """Return DB-held shares without a stale pre-lock capacity decision."""

        matching_pos = await self._find_position(user_id, condition_id, token_id)
        if not matching_pos:
            raise InsufficientFreeShares(
                f"no position found for token {token_id} to sell {requested_size}"
            )
        held = Decimal(matching_pos.size_tokens)
        if held <= 0:
            raise InsufficientFreeShares(f"position for token {token_id} has no held shares")
        return held

    async def _get_reserved_usdc(self, user_id: str, condition_id: str, token_id: str) -> Decimal:
        total_usdc, _ = await self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.BUY
        )
        return total_usdc

    async def _get_reserved_size(self, user_id: str, condition_id: str, token_id: str) -> Decimal:
        _, total_size = await self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.SELL
        )
        return total_size

    async def get_scope_totals(
        self, user_id: str, condition_id: str, token_id: str
    ) -> ReservationScopeTotals:
        """Return exact active reservation totals for a scope."""
        buy_usdc, _ = await self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.BUY
        )
        _, sell_size = await self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.SELL
        )
        return ReservationScopeTotals(
            reserved_usdc=buy_usdc,
            reserved_size=sell_size,
        )

    async def _find_position(self, user_id: str, condition_id: str, token_id: str) -> Any:
        positions = await self.positions_repo.list_open_positions(user_id)
        for pos in positions:
            if pos.condition_id == condition_id and pos.token_id == token_id:
                return pos
        return None

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        """Release terminal."""
        if self._is_disabled_reservation_id(reservation_id):
            logger.info(
                "reservation.release.disabled_noop id=%s reason=%s",
                reservation_id,
                reason,
            )
            return
        logger.info(
            "reservation.release id=%s reason=%s",
            reservation_id,
            reason,
        )
        await self.reservations_repo.mark_released(
            reservation_id=reservation_id,
            reason=reason,
            released_at=self.clock.utcnow_timestamp(),
        )

    def _disabled_reservation(self, intent: OrderIntent, stable_key: str) -> Reservation:
        now_ts = self.clock.utcnow_timestamp()
        # Each disabled reservation gets a unique ID so callers that track
        # reservation IDs in a dict/set don't silently deduplicate concurrent
        # or retried acquisitions for the same intent.
        unique_id = f"disabled:{stable_key}:{uuid.uuid4().hex}"
        return Reservation(
            id=unique_id,
            user_id=intent.key.user_id,
            intent_key=stable_key,
            condition_id=intent.key.condition_id,
            token_id=intent.key.token_id,
            side=intent.key.side,
            reason=intent.key.reason,
            reserved_usdc=Decimal("0"),
            reserved_size=Decimal("0"),
            status="disabled",
            created_at=datetime.fromtimestamp(now_ts, tz=timezone.utc),
            expires_at=datetime.fromtimestamp(now_ts, tz=timezone.utc),
            order_id=None,
            venue_order_id=None,
        )

    @staticmethod
    def _is_disabled_reservation_id(reservation_id: str) -> bool:
        return str(reservation_id).startswith("disabled:")

    async def release_expired_active(self) -> list[str]:
        """Reconcile expired active reservations per their typed decision.

        Expiry alone is not a terminal venue outcome. The reconciler callback
        classifies each reservation; only the release-allowing outcomes
        (terminal venue evidence, or positively proven pre-submit absence)
        free the reserved capacity. Unresolved no-ID submissions stay active
        under the ambiguous-recovery machinery's ownership, and persistence
        outages keep the reservation for a later retry.
        """
        now = self.clock.utcnow_timestamp()
        expired = await self.reservations_repo.list_expired_active(now)
        enqueued_ids: list[str] = []
        for res in expired:
            logger.info(
                "reservation.expiry_enqueue id=%s key=%s side=%s usdc=%s size=%s",
                res.id,
                res.intent_key,
                res.side.value,
                res.reserved_usdc,
                res.reserved_size,
            )
            if self.reconciler_callback is not None:
                try:
                    result = await self.reconciler_callback(res.id)
                    reason = _expiry_release_reason(result)
                    if reason is not None:
                        await self.reservations_repo.mark_released(res.id, reason, now)
                        enqueued_ids.append(res.id)
                        continue
                    if result is None:
                        # Legacy asynchronous-ownership handshake: the
                        # callback took charge of the reservation's fate.
                        enqueued_ids.append(res.id)
                        continue
                    if result is ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE:
                        logger.warning(
                            "reservation.expiry_persistence_unavailable id=%s "
                            "— keeping reservation active for retry",
                            res.id,
                        )
                        continue
                    if result is ReservationExpiryDecision.KEEP_PENDING_RECOVERY:
                        logger.info(
                            "reservation.expiry_keep_pending_recovery id=%s "
                            "— unresolved submission owned by recovery",
                            res.id,
                        )
                        continue
                    logger.info(
                        "reservation.expiry_callback_deferred id=%s — keeping active",
                        res.id,
                    )
                    continue
                except Exception as exc:
                    logger.warning(
                        "reservation.expiry_reconciliation_failed id=%s "
                        "error=%s — keeping reservation active",
                        res.id,
                        exc,
                    )
                    continue
            else:
                logger.info(
                    "reservation.expiry_no_reconciler id=%s — keeping reservation active",
                    res.id,
                )
        return enqueued_ids

    async def _expire_with_reconciliation(self, res: Reservation) -> bool:
        now = self.clock.utcnow_timestamp()
        if self.reconciler_callback is not None:
            try:
                result = await self.reconciler_callback(res.id)
                reason = _expiry_release_reason(result)
                if reason is not None:
                    await self.reservations_repo.mark_released(res.id, reason, now)
                    return True
                if result is ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE:
                    logger.warning(
                        "reservation.expiry_persistence_unavailable id=%s "
                        "— keeping reservation active for retry",
                        res.id,
                    )
                return False
            except Exception as exc:
                logger.warning(
                    "reservation.expiry_reconciliation_failed id=%s "
                    "error=%s — keeping reservation active",
                    res.id,
                    exc,
                )
                return False
        return False
