"""Cached pUSD balance authority for V5 sizing.

The follower balance should be wired to the authenticated Polymarket wallet
balance path. Leader balances may come from the existing public/on-chain wallet
balance adapter. Both are cached so the hot copy path does not add an RPC/REST
round trip for every signal.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from decimal import Decimal, InvalidOperation

FollowerBalanceCallable = Callable[[], Awaitable[object]]
LeaderBalanceCallable = Callable[[str], Awaitable[object]]


def _positive_balance(value: object, *, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return result


class CachedPusdBalanceProvider:
    def __init__(
        self,
        *,
        follower_balance: FollowerBalanceCallable,
        leader_balance: LeaderBalanceCallable,
        follower_ttl_seconds: float = 2.0,
        leader_ttl_seconds: float = 30.0,
    ) -> None:
        if follower_ttl_seconds < 0 or leader_ttl_seconds < 0:
            raise ValueError("balance cache TTLs must be non-negative")
        self._follower_getter = follower_balance
        self._leader_getter = leader_balance
        self._follower_ttl = float(follower_ttl_seconds)
        self._leader_ttl = float(leader_ttl_seconds)
        self._follower_cache: tuple[float, Decimal] | None = None
        self._leader_cache: dict[str, tuple[float, Decimal]] = {}
        self._follower_lock = asyncio.Lock()
        self._leader_locks: dict[str, asyncio.Lock] = {}

    async def follower_pusd_balance(self) -> Decimal:
        now = time.monotonic()
        cached = self._follower_cache
        if cached is not None and cached[0] > now:
            return cached[1]
        async with self._follower_lock:
            now = time.monotonic()
            cached = self._follower_cache
            if cached is not None and cached[0] > now:
                return cached[1]
            value = _positive_balance(
                await self._follower_getter(),
                name="follower pUSD balance",
            )
            self._follower_cache = (now + self._follower_ttl, value)
            return value

    async def leader_pusd_balance(self, wallet: str) -> Decimal:
        key = str(wallet).strip().lower()
        if not key:
            raise ValueError("leader wallet must not be empty")
        now = time.monotonic()
        cached = self._leader_cache.get(key)
        if cached is not None and cached[0] > now:
            return cached[1]
        lock = self._leader_locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            cached = self._leader_cache.get(key)
            if cached is not None and cached[0] > now:
                return cached[1]
            value = _positive_balance(
                await self._leader_getter(key),
                name="leader pUSD balance",
            )
            self._leader_cache[key] = (now + self._leader_ttl, value)
            return value

    async def balances_for(self, wallet: str) -> tuple[Decimal, Decimal]:
        follower, leader = await asyncio.gather(
            self.follower_pusd_balance(),
            self.leader_pusd_balance(wallet),
        )
        return follower, leader

    async def invalidate(self, wallet: str | None = None) -> None:
        if wallet is None:
            self._follower_cache = None
            self._leader_cache.clear()
            return
        self._leader_cache.pop(str(wallet).strip().lower(), None)


__all__ = [
    "FollowerBalanceCallable",
    "LeaderBalanceCallable",
    "CachedPusdBalanceProvider",
]
