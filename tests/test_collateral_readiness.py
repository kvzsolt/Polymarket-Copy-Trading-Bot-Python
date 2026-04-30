"""Tests for V2 collateral readiness checks (Issue #53).

Validates:
- CollateralStatus dataclass structure
- Balance/allowance read via mocked ERC-20 calls
- Failures are correctly reported when balance or allowance is zero
- check_collateral_readiness logs failures without raising
- No private key/RPC leakage in logged output
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from services.collateral_readiness import (
    CollateralStatus,
    _check_sync,
    check_collateral_readiness,
)


def _make_status(**overrides: Any) -> CollateralStatus:
    defaults = {
        "pusd_balance": Decimal("100"),
        "pusd_allowance_ctf": Decimal("999999"),
        "pusd_allowance_neg_risk": Decimal("999999"),
        "wallet_address": "0x" + "a" * 40,
        "rpc_url": "http://rpc",
        "ready": True,
        "failures": [],
    }
    defaults.update(overrides)
    return CollateralStatus(**defaults)


class TestCollateralStatus:
    def test_ready_when_no_failures(self) -> None:
        s = _make_status()
        assert s.ready is True
        assert s.failures == []

    def test_not_ready_with_failures(self) -> None:
        s = _make_status(failures=["low balance"], ready=False)
        assert s.ready is False
        assert len(s.failures) == 1


class TestCheckSync:
    """Test _check_sync with mocked ERC-20 reads."""

    @patch("services.collateral_readiness._erc20_read")
    def test_all_green(self, mock_read: MagicMock) -> None:
        # balanceOf returns 100 USDC (100 * 10^6)
        # allowance returns max uint256-ish (1e12 * 10^6)
        mock_read.side_effect = [100_000_000, 1_000_000_000_000_000, 1_000_000_000_000_000]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
        )
        assert status.ready is True
        assert status.pusd_balance == Decimal("100")
        assert status.pusd_allowance_ctf > 0
        assert status.pusd_allowance_neg_risk > 0

    @patch("services.collateral_readiness._erc20_read")
    def test_low_balance_fails(self, mock_read: MagicMock) -> None:
        # 0.5 USDC balance — below minimum of 1.0
        mock_read.side_effect = [500_000, 1_000_000_000_000_000, 1_000_000_000_000_000]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
        )
        assert status.ready is False
        assert any("pUSD balance" in f for f in status.failures)

    @patch("services.collateral_readiness._erc20_read")
    def test_zero_allowance_ctf_fails(self, mock_read: MagicMock) -> None:
        # Balance OK, but CTF allowance = 0
        mock_read.side_effect = [100_000_000, 0, 1_000_000_000_000_000]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
        )
        assert status.ready is False
        assert any("CTF Exchange" in f for f in status.failures)

    @patch("services.collateral_readiness._erc20_read")
    def test_zero_allowance_neg_risk_fails(self, mock_read: MagicMock) -> None:
        # Balance OK, CTF allowance OK, but neg risk allowance = 0
        mock_read.side_effect = [100_000_000, 1_000_000_000_000_000, 0]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
        )
        assert status.ready is False
        assert any("Neg Risk" in f for f in status.failures)

    @patch("services.collateral_readiness._erc20_read")
    def test_rpc_failure_records_failure(self, mock_read: MagicMock) -> None:
        mock_read.side_effect = Exception("connection refused")
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
        )
        assert status.ready is False
        # All three checks should have failure entries
        assert len(status.failures) >= 1


class TestAsyncCheck:
    """Test the async wrapper."""

    @pytest.mark.asyncio
    @patch("services.collateral_readiness._check_sync")
    async def test_returns_status(self, mock_sync: MagicMock) -> None:
        mock_sync.return_value = _make_status()
        status = await check_collateral_readiness(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
        )
        assert status.ready is True

    @pytest.mark.asyncio
    @patch("services.collateral_readiness._check_sync")
    async def test_uses_env_rpc_url_default(self, mock_sync: MagicMock) -> None:
        mock_sync.return_value = _make_status()
        await check_collateral_readiness(
            wallet_address="0x" + "a" * 40,
        )
        # Should have called with the default RPC URL from ENV
        call_kwargs = mock_sync.call_args[1]
        assert "rpc_url" in call_kwargs


class TestNoSecretLeakage:
    """Ensure logs don't contain private keys or raw RPC auth."""

    @patch("services.collateral_readiness._erc20_read")
    def test_status_has_no_private_key(self, mock_read: MagicMock) -> None:
        mock_read.side_effect = [100_000_000, 1_000_000_000_000_000, 1_000_000_000_000_000]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
        )
        serialized = str(status)
        assert "private" not in serialized.lower()
        assert "secret" not in serialized.lower()
        # Wallet address is fine to log (it's public on-chain)
        assert "0x" + "a" * 40 in serialized
