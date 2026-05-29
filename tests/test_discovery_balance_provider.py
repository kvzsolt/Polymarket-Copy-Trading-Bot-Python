from __future__ import annotations

from decimal import Decimal

import pytest

from services.discovery.balance_provider import WalletBalanceProvider


class _BalanceClient:
    def __init__(self, balance):
        self.balance = balance
        self.calls = 0

    async def get_usdc_balance(self, address: str):
        self.calls += 1
        assert address == "0x" + "a" * 40
        return self.balance


@pytest.mark.asyncio
async def test_wallet_balance_provider_fetches_and_caches_usdc_balance():
    client = _BalanceClient(Decimal("42.5"))
    provider = WalletBalanceProvider(
        balance_client=client,
        wallet_address="0x" + "a" * 40,
    )

    assert await provider.get_available_balance_usdc() == Decimal("42.5")
    assert await provider.get_available_balance_usdc() == Decimal("42.5")
    assert client.calls == 1


@pytest.mark.asyncio
async def test_wallet_balance_provider_raises_without_balance_or_fallback():
    provider = WalletBalanceProvider(
        balance_client=_BalanceClient(None),
        wallet_address="0x" + "a" * 40,
    )

    with pytest.raises(RuntimeError, match="Unable to fetch USDC balance"):
        await provider.get_available_balance_usdc()

