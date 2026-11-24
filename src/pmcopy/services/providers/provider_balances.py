"""Exact read-only account balances through the shared provider gateway."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal
import re
import time
from typing import Protocol


class BalanceRpc(Protocol):
    """Narrow admitted-read port for account balance evidence."""

    async def call(
        self, provider: str, method: str, params: list[object], *, category: str = "essential"
    ) -> object:
        """Return one RPC result or an availability failure."""
        ...


class CollateralBalanceReader(Protocol):
    """Narrow async port for reading exact six-decimal collateral balances."""

    async def collateral(self, address: str) -> Decimal | None:
        """Return exact pUSD/USDC.e balance for ``address`` or ``None`` if unavailable."""
        ...


class ProviderBalanceReader:
    """Preserve unknown balance evidence and exact six-decimal token units."""

    def __init__(self, gateway: BalanceRpc, provider: str, contracts: tuple[str, ...]) -> None:
        """Inject configured contract candidates and the account-wide admitted gateway."""
        self._gateway = gateway
        self._provider = provider
        self._contracts = tuple(dict.fromkeys(c for c in contracts if c))
        # Contract bytecode at a verified collateral address is immutable on Polygon;
        # caching verified contracts removes a redundant eth_getCode round-trip from
        # every hot-path balance lookup.
        self._verified_contracts: set[str] = set()

    @staticmethod
    def _address(value: str) -> bool:
        return isinstance(value, str) and re.fullmatch(r"0x[0-9a-fA-F]{40}", value) is not None

    @staticmethod
    def _uint(value: object) -> int | None:
        if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{1,64}", value):
            return None
        return int(value, 16)

    async def collateral(self, address: str) -> Decimal | None:
        """Read the first configured deployed collateral contract; failure stays unknown."""
        if not self._address(address):
            return None
        for contract in self._contracts:
            if not self._address(contract):
                continue
            contract_key = contract.lower()
            try:
                if contract_key not in self._verified_contracts:
                    code = await self._gateway.call(
                        self._provider, "eth_getCode", [contract, "latest"]
                    )
                    if not isinstance(code, str) or code in {"0x", "0x0"}:
                        continue
                    self._verified_contracts.add(contract_key)
                raw = await self._gateway.call(
                    self._provider,
                    "eth_call",
                    [
                        {"to": contract, "data": "0x70a08231" + address[2:].lower().zfill(64)},
                        "latest",
                    ],
                )
                value = self._uint(raw)
                if value is not None:
                    return Decimal(f"{value // 1_000_000}.{value % 1_000_000:06d}")
            except Exception:
                return None
        return None

    async def native(self, address: str) -> int | None:
        """Read integer native units without constructing an unmetered Web3 provider."""
        if not self._address(address):
            return None
        try:
            return self._uint(
                await self._gateway.call(self._provider, "eth_getBalance", [address, "latest"])
            )
        except Exception:
            return None


class CachedCollateralBalanceReader:
    """Bounded TTL + singleflight RPC balance cache for low-latency leader sizing.

    Burst fills from the same leader funder wallet (for example, 6 OrderFilled
    logs within a few seconds) coalesce onto a single async HTTP/2 ``eth_call``
    and reuse the exact ``Decimal`` balance within ``ttl_seconds``. Failures
    always remain fail-closed (``None``) and are never cached as positive state.
    """

    def __init__(
        self,
        reader: CollateralBalanceReader,
        *,
        ttl_seconds: float = 5.0,
        max_entries: int = 1024,
        proxy_fallbacks: Mapping[str, str] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative")
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._reader = reader
        self._ttl_seconds = float(ttl_seconds)
        self._max_entries = int(max_entries)
        self._proxy_fallbacks: dict[str, str] = {
            str(k).strip().lower(): str(v).strip().lower()
            for k, v in (proxy_fallbacks or {}).items()
            if str(k).strip() and str(v).strip()
        }
        self._monotonic = monotonic
        self._cache: OrderedDict[str, tuple[Decimal, float]] = OrderedDict()
        self._inflight: dict[str, asyncio.Future[Decimal | None]] = {}

    async def read(self, wallet: str) -> Decimal | None:
        """Return cached or freshly queried exact pUSD collateral for ``wallet``."""
        key = str(wallet or "").strip().lower()
        if not key:
            return None
        now = self._monotonic()
        if self._ttl_seconds > 0:
            cached = self._cache.get(key)
            if cached is not None:
                cached_balance, expires_at = cached
                if now < expires_at:
                    self._cache.move_to_end(key)
                    return cached_balance
                self._cache.pop(key, None)

        existing = self._inflight.get(key)
        if existing is not None:
            return await asyncio.shield(existing)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Decimal | None] = loop.create_future()
        self._inflight[key] = fut
        try:
            balance = await self._fetch_with_fallback(key)
            if balance is not None and self._ttl_seconds > 0:
                self._cache[key] = (balance, self._monotonic() + self._ttl_seconds)
                self._cache.move_to_end(key)
                while len(self._cache) > self._max_entries:
                    self._cache.popitem(last=False)
            if not fut.done():
                fut.set_result(balance)
            return balance
        except BaseException as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            self._inflight.pop(key, None)

    async def _fetch_with_fallback(self, normalized_wallet: str) -> Decimal | None:
        balance = await self._reader.collateral(normalized_wallet)
        if balance is not None and balance > 0:
            return balance
        fallback = self._proxy_fallbacks.get(normalized_wallet)
        if fallback and fallback != normalized_wallet:
            proxy_balance = await self._reader.collateral(fallback)
            if proxy_balance is not None and (balance is None or proxy_balance > balance):
                return proxy_balance
        return balance

    async def warm(self, wallets: Iterable[str]) -> None:
        """Best-effort concurrent pre-warm of tracked funder wallets at startup."""
        targets = tuple(
            dict.fromkeys(str(addr or "").strip().lower() for addr in wallets if str(addr or "").strip())
        )
        if not targets:
            return
        await asyncio.gather(*(self.read(addr) for addr in targets), return_exceptions=True)

    def invalidate(self, wallet: str | None = None) -> None:
        """Invalidate one wallet or the entire cache."""
        if wallet is None:
            self._cache.clear()
            return
        self._cache.pop(str(wallet).strip().lower(), None)


__all__ = [
    "BalanceRpc",
    "CachedCollateralBalanceReader",
    "CollateralBalanceReader",
    "ProviderBalanceReader",
]
