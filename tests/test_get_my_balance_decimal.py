"""Regression: get_my_balance returns Decimal, not float.

Pre-fix: ``bal = float(raw_balance / 10**6)`` did Python int / int → float
division, then wrapped in float(). For balances above ~2^53 microUSDC this
silently lost cent precision. More fundamentally, returning float pushes
IEEE-754 representations into the downstream risk-cap math.

Test verifies:
  * ``_get_balance_sync`` returns Decimal computed via Decimal(raw) / Decimal(10**6)
  * Large raw values (above 2^53) round-trip without precision loss
  * ``get_my_balance`` async wrapper preserves the Decimal
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from utils.getMyBalance import _get_balance_sync, get_my_balance


@pytest.fixture()
def web3_returns(monkeypatch):
    """Patch Web3 + ENV + bytecode so _get_balance_sync hits balanceOf cleanly."""

    def _go(raw_balance: int):
        # Module-level config singletons (legacy-kill slice #77 replaced
        # the previous ENV.get reads with these explicit module vars).
        monkeypatch.setattr("utils.getMyBalance._RPC_URL", "https://rpc.example")
        monkeypatch.setattr(
            "utils.getMyBalance._COLLATERAL_CONTRACT_ADDRESS",
            "0x1111111111111111111111111111111111111111",
        )
        monkeypatch.setattr("utils.getMyBalance._PUSD_CONTRACT_ADDRESS", "")
        monkeypatch.setattr("utils.getMyBalance._USDC_CONTRACT_ADDRESS", "")
        w3_mock = MagicMock()
        w3_mock.eth.get_code.return_value = b"\x01"  # truthy bytecode = "contract exists"
        contract_mock = MagicMock()
        contract_mock.functions.balanceOf.return_value.call.return_value = raw_balance
        w3_mock.eth.contract.return_value = contract_mock
        web3_cls = MagicMock()
        web3_cls.return_value = w3_mock
        web3_cls.HTTPProvider = MagicMock()
        web3_cls.to_checksum_address = lambda a: a
        monkeypatch.setattr("utils.getMyBalance.Web3", web3_cls)

        class FakeFallbackWeb3Provider:
            def __init__(self, rpc_url, fallback_rpc_url=""):
                self.rpc_url = rpc_url
                self.fallback_rpc_url = fallback_rpc_url

            def get_web3(self):
                return w3_mock

        monkeypatch.setattr(
            "utils.getMyBalance.FallbackWeb3Provider",
            FakeFallbackWeb3Provider,
        )
        return w3_mock

    return _go


def test_get_balance_sync_returns_decimal(web3_returns):
    web3_returns(raw_balance=100_500_000)  # 100.50 USDC
    bal = _get_balance_sync("0x2222222222222222222222222222222222222222")
    assert isinstance(bal, Decimal), f"expected Decimal, got {type(bal).__name__}"
    assert bal == Decimal("100.5")


def test_huge_balance_keeps_cent_precision(web3_returns):
    """Above ~2^53 microUSDC, float loses cent precision; Decimal must not."""
    # 9_007_199_254_740_993 microUSDC = 9_007_199_254.740993 USDC
    # In float, this would round to 9_007_199_254.740992.
    raw = 9_007_199_254_740_993
    web3_returns(raw_balance=raw)
    bal = _get_balance_sync("0x2222222222222222222222222222222222222222")
    assert bal == Decimal(raw) / Decimal(10**6)
    # Sanity: float coercion would lose precision here, Decimal preserves it.
    expected_str = "9007199254.740993"
    assert str(bal) == expected_str


@pytest.mark.asyncio
async def test_get_my_balance_async_returns_decimal():
    with patch(
        "utils.getMyBalance._get_balance_sync",
        return_value=Decimal("42.50"),
    ):
        bal = await get_my_balance("0x3333333333333333333333333333333333333333")
    assert isinstance(bal, Decimal)
    assert bal == Decimal("42.50")
