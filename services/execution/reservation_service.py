import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, Awaitable, Callable, Any

import asyncpg

from domain.order_lifecycle import OrderIntent, IntentSide, Reservation
from storage.postgres.reservations_repo import ReservationsRepo
from storage.postgres.positions_repo import PositionsRepo

logger = logging.getLogger(__name__)


class ReservationConflict(Exception):
    pass


class InsufficientFreeQuote(Exception):
    pass


class InsufficientFreeShares(Exception):
    pass


class Clock(Protocol):
    def utcnow_timestamp(self) -> int: ...


class DefaultClock(Clock):
    def utcnow_timestamp(self) -> int:
        return int(time.time())


class ReconcilerCallback(Protocol):
    async def __call__(self, reservation_id: str) -> bool | None: ...


@dataclass
class ReservationScopeTotals:
    reserved_usdc: Decimal
    reserved_size: Decimal


class ReservationService:
    def __init__(
        self,
        reservations_repo: ReservationsRepo,
        positions_repo: PositionsRepo,
        get_balance_func: Callable[[str], Awaitable[Decimal | None]],
        funder_address: str,
        clock: Clock | None = None,
        ttl_seconds: int = 120,
        reconciler_callback: ReconcilerCallback | None = None,
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

    async def reserve_for_intent(self, intent: OrderIntent) -> Reservation:
        stable_key = intent.key.stable_key()
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
                existing = await self.reservations_repo.get_active_by_intent_key(
                    legacy_key
                )

        if existing:
            if existing.expires_at.timestamp() < self.clock.utcnow_timestamp():
                logger.info(
                    "reservation.acquire.expired_existing key=%s id=%s — "
                    "triggering reconciliation before release",
                    stable_key, existing.id,
                )
                released = await self._expire_with_reconciliation(existing)
                if not released:
                    raise ReservationConflict(
                        f"expired reservation still pending reconciliation for intent={stable_key}"
                    )
            else:
                logger.warning(
                    "reservation.acquire.blocked key=%s id=%s — "
                    "active reservation already exists",
                    stable_key, existing.id,
                )
                raise ReservationConflict(
                    f"active reservation exists for intent={stable_key}"
                )

        expires_at = self.clock.utcnow_timestamp() + self.ttl_seconds

        if intent.key.side == IntentSide.BUY:
            required_usdc = intent.size * intent.price
            balance = await self._get_available_balance(intent.key.user_id, required_usdc)
            logger.info(
                "reservation.acquire.buy key=%s size=%s price=%s usdc=%s "
                "expires=%d",
                stable_key, intent.size, intent.price, required_usdc, expires_at,
            )
            try:
                # Atomic intent-key check + capacity check + insert inside
                # one advisory-lock transaction. Closes the race window the
                # outer ``get_active_by_intent_key`` fast-path cannot cover
                # (Greptile finding in PR #108).
                existing_under_lock, result = (
                    await self.reservations_repo
                    .create_buy_reservation_checked_by_intent(
                        intent=intent,
                        reserved_usdc=required_usdc,
                        expires_at=expires_at,
                        available_balance=balance,
                    )
                )
            except asyncpg.exceptions.UniqueViolationError as exc:
                raise ReservationConflict(
                    f"active reservation exists for intent={stable_key}"
                ) from exc
            if existing_under_lock is not None:
                # A concurrent caller raced past the fast-path lookup and
                # committed an active row before we acquired the lock.
                logger.warning(
                    "reservation.acquire.buy.race_lost key=%s existing_id=%s",
                    stable_key, existing_under_lock.id,
                )
                raise ReservationConflict(
                    f"active reservation exists for intent={stable_key}"
                )
            if result is None:
                raise InsufficientFreeQuote(
                    f"insufficient free USDC after atomic check: need {required_usdc}"
                )
            logger.info(
                "reservation.acquire.buy.ok id=%s key=%s usdc=%s",
                result.id, stable_key, required_usdc,
            )
            return result

        if intent.key.side == IntentSide.SELL:
            held_size = await self._get_held_size(
                intent.key.user_id,
                intent.key.condition_id,
                intent.key.token_id,
                intent.size,
            )
            logger.info(
                "reservation.acquire.sell key=%s size=%s expires=%d",
                stable_key, intent.size, expires_at,
            )
            try:
                # Atomic intent-key check + capacity check + insert under
                # the per-(user, condition, token) advisory lock, closing
                # the fast-path lookup race that the BUY branch already
                # covers via ``create_buy_reservation_checked_by_intent``.
                existing_under_lock, result = (
                    await self.reservations_repo
                    .create_sell_reservation_checked_by_intent(
                        intent=intent,
                        reserved_size=intent.size,
                        expires_at=expires_at,
                        held_size=held_size,
                    )
                )
            except asyncpg.exceptions.UniqueViolationError as exc:
                raise ReservationConflict(
                    f"active reservation exists for intent={stable_key}"
                ) from exc
            if existing_under_lock is not None:
                logger.warning(
                    "reservation.acquire.sell.race_lost key=%s existing_id=%s",
                    stable_key, existing_under_lock.id,
                )
                raise ReservationConflict(
                    f"active reservation exists for intent={stable_key}"
                )
            if result is None:
                raise InsufficientFreeShares(
                    f"insufficient free shares after atomic check: need {intent.size}"
                )
            logger.info(
                "reservation.acquire.sell.ok id=%s key=%s size=%s",
                result.id, stable_key, intent.size,
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
        balance = (
            raw_balance if isinstance(raw_balance, Decimal) else Decimal(str(raw_balance))
        )
        # Quote balance is account/funder-wide. Do not scope BUY reserved
        # USDC by condition/token, or two different markets can collectively
        # over-reserve the same wallet balance.
        already_reserved = await self.reservations_repo.sum_active_quote_for_user(user_id)
        free_balance = balance - already_reserved

        logger.info(
            "reservation.check.buy user=%s balance=%s already_reserved=%s "
            "free=%s required=%s",
            user_id, balance, already_reserved, free_balance, required_usdc,
        )

        if free_balance < required_usdc:
            raise InsufficientFreeQuote(
                f"insufficient free USDC: need {required_usdc}, "
                f"free={free_balance} (balance={balance}, "
                f"reserved={already_reserved})"
            )
        return balance

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
                f"no position found for token {token_id} to sell "
                f"{required_size}"
            )

        held = Decimal(matching_pos.size_tokens)
        already_reserved = await self._get_reserved_size(
            user_id, condition_id, token_id
        )
        free_shares = held - already_reserved

        logger.info(
            "reservation.check.sell user=%s held=%s already_reserved=%s "
            "free=%s required=%s",
            user_id, held, already_reserved, free_shares, required_size,
        )

        if free_shares < required_size:
            raise InsufficientFreeShares(
                f"insufficient free shares: need {required_size}, "
                f"free={free_shares} (held={held}, "
                f"reserved={already_reserved})"
            )
        return held

    async def _get_reserved_usdc(
        self, user_id: str, condition_id: str, token_id: str
    ) -> Decimal:
        total_usdc, _ = await self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.BUY
        )
        return total_usdc

    async def _get_reserved_size(
        self, user_id: str, condition_id: str, token_id: str
    ) -> Decimal:
        _, total_size = await self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.SELL
        )
        return total_size

    async def get_scope_totals(
        self, user_id: str, condition_id: str, token_id: str
    ) -> ReservationScopeTotals:
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

    async def _find_position(
        self, user_id: str, condition_id: str, token_id: str
    ) -> Any:
        positions = await self.positions_repo.list_open_positions(user_id)
        for pos in positions:
            if pos.condition_id == condition_id and pos.token_id == token_id:
                return pos
        return None

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        logger.info(
            "reservation.release id=%s reason=%s",
            reservation_id, reason,
        )
        await self.reservations_repo.mark_released(
            reservation_id=reservation_id,
            reason=reason,
            released_at=self.clock.utcnow_timestamp(),
        )

    async def release_expired_active(self) -> list[str]:
        """Enqueue reconciliation for expired active reservations.

        Expiry alone is not a terminal venue outcome. Reservations stay active
        until reconciliation confirms the linked order is terminal and calls
        ``release_terminal``.
        """
        now = self.clock.utcnow_timestamp()
        expired = await self.reservations_repo.list_expired_active(now)
        enqueued_ids: list[str] = []
        for res in expired:
            logger.info(
                "reservation.expiry_enqueue id=%s key=%s side=%s "
                "usdc=%s size=%s",
                res.id, res.intent_key, res.side.value,
                res.reserved_usdc, res.reserved_size,
            )
            if self.reconciler_callback is not None:
                try:
                    result = await self.reconciler_callback(res.id)
                    if result is True:
                        await self.reservations_repo.mark_released(
                            res.id, "expired_terminal", now
                        )
                        enqueued_ids.append(res.id)
                        continue
                    if result is False:
                        logger.info(
                            "reservation.expiry_callback_deferred id=%s — keeping active",
                            res.id,
                        )
                        continue
                    enqueued_ids.append(res.id)
                    continue
                except Exception as exc:
                    logger.warning(
                        "reservation.expiry_reconciliation_failed id=%s "
                        "error=%s — keeping reservation active",
                        res.id, exc,
                    )
                    continue
            else:
                logger.info(
                    "reservation.expiry_no_reconciler id=%s — keeping reservation active",
                    res.id,
                )
        return enqueued_ids

    async def _expire_with_reconciliation(self, res: Reservation) -> bool:
        if self.reconciler_callback is not None:
            try:
                result = await self.reconciler_callback(res.id)
                if result is True:
                    await self.reservations_repo.mark_released(
                        res.id, "expired_terminal", self.clock.utcnow_timestamp()
                    )
                    return True
                return False
            except Exception as exc:
                logger.warning(
                    "reservation.expiry_reconciliation_failed id=%s "
                    "error=%s — keeping reservation active",
                    res.id, exc,
                )
                return False
        return False
