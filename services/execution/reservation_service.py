import asyncio
import time
from decimal import Decimal
from typing import Protocol, Awaitable, Callable

from domain.order_lifecycle import OrderIntent, IntentSide, Reservation
from storage.sqlite.reservations_repo import ReservationsRepo
from storage.sqlite.positions_repo import PositionsRepo

class ReservationConflict(Exception):
    pass


class Clock(Protocol):
    def utcnow_timestamp(self) -> int: ...


class DefaultClock(Clock):
    def utcnow_timestamp(self) -> int:
        return int(time.time())


class ReservationService:
    def __init__(
        self,
        reservations_repo: ReservationsRepo,
        positions_repo: PositionsRepo,
        get_balance_func: Callable[[str], Awaitable[float | None]],
        funder_address: str,
        clock: Clock = DefaultClock(),
        ttl_seconds: int = 120,
    ):
        self.reservations_repo = reservations_repo
        self.positions_repo = positions_repo
        self.get_balance_func = get_balance_func
        self.funder_address = funder_address
        self.clock = clock
        self.ttl_seconds = ttl_seconds

    async def reserve_for_intent(self, intent: OrderIntent) -> Reservation:
        # Prevent duplicates
        stable_key = intent.key.stable_key()
        existing = self.reservations_repo.get_active_by_intent_key(stable_key)
        
        if existing:
            # Check if existing has expired
            if existing.expires_at.timestamp() < self.clock.utcnow_timestamp():
                self.reservations_repo.mark_released(existing.id, "expired", self.clock.utcnow_timestamp())
            else:
                raise ReservationConflict(f"active reservation exists for intent={stable_key}")

        expires_at = self.clock.utcnow_timestamp() + self.ttl_seconds

        if intent.key.side == IntentSide.BUY:
            required_usdc = intent.size * intent.price
            await self._assert_available_usdc(required_usdc)
            return self.reservations_repo.create_buy_reservation(
                intent=intent,
                reserved_usdc=required_usdc,
                expires_at=expires_at,
            )

        if intent.key.side == IntentSide.SELL:
            self._assert_available_position(intent.key.user_id, intent.key.condition_id, intent.key.token_id, intent.size)
            return self.reservations_repo.create_sell_reservation(
                intent=intent,
                reserved_size=intent.size,
                expires_at=expires_at,
            )

        raise ValueError(f"unsupported side={intent.key.side}")

    async def _assert_available_usdc(self, required_usdc: Decimal) -> None:
        balance_float = await self.get_balance_func(self.funder_address)
        if balance_float is None:
            raise ValueError("failed to fetch available USDC balance")
            
        balance = Decimal(str(balance_float))
        if balance < required_usdc:
            raise ValueError(f"insufficient USDC: need {required_usdc}, have {balance}")

    def _assert_available_position(self, user_id: str, condition_id: str, token_id: str, required_size: Decimal) -> None:
        positions = self.positions_repo.list_open_positions(user_id)
        # Find matching position
        matching_pos = None
        for pos in positions:
            if pos.condition_id == condition_id and pos.token_id == token_id:
                matching_pos = pos
                break
                
        if not matching_pos:
            raise ValueError(f"no position found for token {token_id} to sell {required_size}")
            
        available_size = Decimal(matching_pos.size_tokens)
        if available_size < required_size:
            raise ValueError(f"insufficient position size: need {required_size}, have {available_size}")

    def release_terminal(self, reservation_id: str, reason: str) -> None:
        self.reservations_repo.mark_released(
            reservation_id=reservation_id,
            reason=reason,
            released_at=self.clock.utcnow_timestamp(),
        )
