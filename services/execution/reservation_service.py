import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, Awaitable, Callable, Any

from domain.order_lifecycle import OrderIntent, IntentSide, Reservation
from storage.sqlite.reservations_repo import ReservationsRepo
from storage.sqlite.positions_repo import PositionsRepo

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
    async def __call__(self, reservation_id: str) -> None: ...


@dataclass
class ReservationScopeTotals:
    reserved_usdc: Decimal
    reserved_size: Decimal


class ReservationService:
    def __init__(
        self,
        reservations_repo: ReservationsRepo,
        positions_repo: PositionsRepo,
        get_balance_func: Callable[[str], Awaitable[float | None]],
        funder_address: str,
        clock: Clock = DefaultClock(),
        ttl_seconds: int = 120,
        reconciler_callback: ReconcilerCallback | None = None,
    ):
        self.reservations_repo = reservations_repo
        self.positions_repo = positions_repo
        self.get_balance_func = get_balance_func
        self.funder_address = funder_address
        self.clock = clock
        self.ttl_seconds = ttl_seconds
        self.reconciler_callback = reconciler_callback

    async def reserve_for_intent(self, intent: OrderIntent) -> Reservation:
        stable_key = intent.key.stable_key()
        existing = self.reservations_repo.get_active_by_intent_key(stable_key)

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
            await self._assert_available_usdc(
                intent.key.user_id,
                intent.key.condition_id,
                intent.key.token_id,
                required_usdc,
            )
            logger.info(
                "reservation.acquire.buy key=%s size=%s price=%s usdc=%s "
                "expires=%d",
                stable_key, intent.size, intent.price, required_usdc, expires_at,
            )
            result = self.reservations_repo.create_buy_reservation(
                intent=intent,
                reserved_usdc=required_usdc,
                expires_at=expires_at,
            )
            logger.info(
                "reservation.acquire.buy.ok id=%s key=%s usdc=%s",
                result.id, stable_key, required_usdc,
            )
            return result

        if intent.key.side == IntentSide.SELL:
            self._assert_available_shares(
                intent.key.user_id,
                intent.key.condition_id,
                intent.key.token_id,
                intent.size,
            )
            logger.info(
                "reservation.acquire.sell key=%s size=%s expires=%d",
                stable_key, intent.size, expires_at,
            )
            result = self.reservations_repo.create_sell_reservation(
                intent=intent,
                reserved_size=intent.size,
                expires_at=expires_at,
            )
            logger.info(
                "reservation.acquire.sell.ok id=%s key=%s size=%s",
                result.id, stable_key, intent.size,
            )
            return result

        raise ValueError(f"unsupported side={intent.key.side}")

    async def _assert_available_usdc(
        self,
        user_id: str,
        condition_id: str,
        token_id: str,
        required_usdc: Decimal,
    ) -> None:
        balance_float = await self.get_balance_func(self.funder_address)
        if balance_float is None:
            raise ValueError("failed to fetch available USDC balance")

        balance = Decimal(str(balance_float))
        # Quote balance is account/funder-wide. Do not scope BUY reserved
        # USDC by condition/token, or two different markets can collectively
        # over-reserve the same wallet balance.
        already_reserved = self.reservations_repo.sum_active_quote_for_user(user_id)
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

    def _assert_available_shares(
        self,
        user_id: str,
        condition_id: str,
        token_id: str,
        required_size: Decimal,
    ) -> None:
        matching_pos = self._find_position(user_id, condition_id, token_id)

        if not matching_pos:
            raise InsufficientFreeShares(
                f"no position found for token {token_id} to sell "
                f"{required_size}"
            )

        held = Decimal(matching_pos.size_tokens)
        already_reserved = self._get_reserved_size(
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

    def _get_reserved_usdc(
        self, user_id: str, condition_id: str, token_id: str
    ) -> Decimal:
        total_usdc, _ = self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.BUY
        )
        return total_usdc

    def _get_reserved_size(
        self, user_id: str, condition_id: str, token_id: str
    ) -> Decimal:
        _, total_size = self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.SELL
        )
        return total_size

    def get_scope_totals(
        self, user_id: str, condition_id: str, token_id: str
    ) -> ReservationScopeTotals:
        buy_usdc, _ = self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.BUY
        )
        _, sell_size = self.reservations_repo.sum_active_for_scope(
            user_id, condition_id, token_id, IntentSide.SELL
        )
        return ReservationScopeTotals(
            reserved_usdc=buy_usdc,
            reserved_size=sell_size,
        )

    def _find_position(
        self, user_id: str, condition_id: str, token_id: str
    ) -> Any:
        positions = self.positions_repo.list_open_positions(user_id)
        for pos in positions:
            if pos.condition_id == condition_id and pos.token_id == token_id:
                return pos
        return None

    def release_terminal(self, reservation_id: str, reason: str) -> None:
        logger.info(
            "reservation.release id=%s reason=%s",
            reservation_id, reason,
        )
        self.reservations_repo.mark_released(
            reservation_id=reservation_id,
            reason=reason,
            released_at=self.clock.utcnow_timestamp(),
        )

    async def release_expired_active(self) -> list[str]:
        """Release all expired active reservations (with reconciliation)."""
        now = self.clock.utcnow_timestamp()
        expired = self.reservations_repo.list_expired_active(now)
        released_ids: list[str] = []
        for res in expired:
            logger.info(
                "reservation.expiry_releasing id=%s key=%s side=%s "
                "usdc=%s size=%s",
                res.id, res.intent_key, res.side.value,
                res.reserved_usdc, res.reserved_size,
            )
            if self.reconciler_callback is not None:
                try:
                    await self.reconciler_callback(res.id)
                except Exception as exc:
                    logger.warning(
                        "reservation.expiry_reconciliation_failed id=%s "
                        "error=%s — keeping reservation active",
                        res.id, exc,
                    )
                    continue
            self.reservations_repo.mark_released(
                res.id, "expired", now
            )
            released_ids.append(res.id)
        return released_ids

    async def _expire_with_reconciliation(self, res: Reservation) -> bool:
        if self.reconciler_callback is not None:
            try:
                await self.reconciler_callback(res.id)
            except Exception as exc:
                logger.warning(
                    "reservation.expiry_reconciliation_failed id=%s "
                    "error=%s — keeping reservation active",
                    res.id, exc,
                )
                return False
        self.reservations_repo.mark_released(
            res.id, "expired", self.clock.utcnow_timestamp()
        )
        return True
