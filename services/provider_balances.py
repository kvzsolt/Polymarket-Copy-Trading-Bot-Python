"""Exact read-only account balances through the shared provider gateway."""

from __future__ import annotations

from decimal import Decimal
import re
from typing import Protocol


class BalanceRpc(Protocol):
    """Narrow admitted-read port for account balance evidence."""

    async def call(
        self, provider: str, method: str, params: list[object], *, category: str = "essential"
    ) -> object:
        """Return one RPC result or an availability failure."""
        ...


class ProviderBalanceReader:
    """Preserve unknown balance evidence and exact six-decimal token units."""

    def __init__(self, gateway: BalanceRpc, provider: str, contracts: tuple[str, ...]) -> None:
        """Inject configured contract candidates and the account-wide admitted gateway."""
        self._gateway = gateway
        self._provider = provider
        self._contracts = tuple(dict.fromkeys(c for c in contracts if c))

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
            try:
                code = await self._gateway.call(self._provider, "eth_getCode", [contract, "latest"])
                if not isinstance(code, str) or code in {"0x", "0x0"}:
                    continue
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
