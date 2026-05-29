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
from unittest.mock import AsyncMock, MagicMock, patch

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
        "wallet_address": "0x" + "a" * 40,
        "rpc_url": "http://rpc",
        "ready": True,
        "failures": (),
    }
    defaults.update(overrides)
    return CollateralStatus(**defaults)


class TestCollateralStatus:
    def test_ready_when_no_failures(self) -> None:
        s = _make_status()
        assert s.ready is True
        assert s.failures == ()

    def test_not_ready_with_failures(self) -> None:
        s = _make_status(failures=("low balance",), ready=False)
        assert s.ready is False
        assert len(s.failures) == 1


class TestCheckSync:
    """Test _check_sync with mocked ERC-20 reads."""

    @patch("services.collateral_readiness._erc20_read")
    def test_all_green(self, mock_read: MagicMock) -> None:
        mock_read.side_effect = [100_000_000, 1_000_000_000_000_000]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
            pusd_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ctf_exchange_address="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
            min_pusd_balance=Decimal("1.0"),
        )
        assert status.ready is True
        assert status.pusd_balance == Decimal("100")
        assert status.pusd_allowance_ctf > 0

    @patch("services.collateral_readiness._erc20_read")
    def test_low_balance_fails(self, mock_read: MagicMock) -> None:
        mock_read.side_effect = [500_000, 1_000_000_000_000_000]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
            pusd_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ctf_exchange_address="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
            min_pusd_balance=Decimal("1.0"),
        )
        assert status.ready is False
        assert any("pUSD balance" in f for f in status.failures)

    @patch("services.collateral_readiness._erc20_read")
    def test_zero_allowance_ctf_fails(self, mock_read: MagicMock) -> None:
        mock_read.side_effect = [100_000_000, 0]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
            pusd_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ctf_exchange_address="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
            min_pusd_balance=Decimal("1.0"),
        )
        assert status.ready is False
        assert any("CTF Exchange" in f for f in status.failures)

    @patch("services.collateral_readiness._erc20_read")
    def test_rpc_failure_records_failure(self, mock_read: MagicMock) -> None:
        mock_read.side_effect = Exception("connection refused")
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
            pusd_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ctf_exchange_address="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
            min_pusd_balance=Decimal("1.0"),
        )
        assert status.ready is False
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
            pusd_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ctf_exchange_address="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
        )
        assert status.ready is True

    @pytest.mark.asyncio
    @patch("services.collateral_readiness._check_sync")
    async def test_uses_explicit_rpc_url(self, mock_sync: MagicMock) -> None:
        mock_sync.return_value = _make_status()
        await check_collateral_readiness(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://explicit-rpc",
            pusd_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ctf_exchange_address="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
        )
        call_kwargs = mock_sync.call_args[1]
        assert call_kwargs["rpc_url"] == "http://explicit-rpc"


class TestNoSecretLeakage:
    """Ensure logs don't contain private keys or raw RPC auth."""

    @patch("services.collateral_readiness._erc20_read")
    def test_status_has_no_private_key(self, mock_read: MagicMock) -> None:
        mock_read.side_effect = [100_000_000, 1_000_000_000_000_000]
        status = _check_sync(
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
            pusd_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ctf_exchange_address="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
            min_pusd_balance=Decimal("1.0"),
        )
        serialized = str(status)
        assert "private" not in serialized.lower()
        assert "secret" not in serialized.lower()
        assert "0x" + "a" * 40 in serialized


class TestFailClosedStartup:
    """Verify create_clob_client() raises on collateral readiness failure."""

    @pytest.mark.asyncio
    @patch("services.collateral_readiness.check_collateral_readiness")
    async def test_startup_raises_on_readiness_failure(self, mock_check: AsyncMock) -> None:
        from services.collateral_readiness import CollateralStatus
        from utils.createClobClient import create_clob_client
        from unittest.mock import patch as _p

        mock_check.return_value = CollateralStatus(
            pusd_balance=Decimal("0"),
            pusd_allowance_ctf=Decimal("0"),
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
            ready=False,
            failures=("pUSD balance 0.00 < minimum 1", "CTF Exchange pUSD allowance is 0"),
        )

        mock_client = MagicMock()
        mock_client.get_tick_size.return_value = "0.01"
        mock_client.get_neg_risk.return_value = False
        mock_client.get_order_book.return_value = MagicMock(
            bids=[], asks=[], tick_size="0.01", neg_risk=False, min_order_size="5"
        )

        # Build a minimal AppConfig stub with the CLOB V2 gate flipped.
        # The legacy-kill slice (#77) moved the gate from ENV.get into
        # ``config.clob.clob_v2_ready`` (typed AppConfig field).
        from types import SimpleNamespace
        mock_cfg = SimpleNamespace(
            clob=SimpleNamespace(clob_v2_ready=True),
            network=SimpleNamespace(
                pusd_contract_address="",
                collateral_contract_address="",
                ctf_exchange_v2_address="",
            ),
        )
        with _p("utils.createClobClient.build_wallet_config") as mock_bwc, \
             _p("utils.createClobClient.validate_wallet_config"), \
             _p("utils.createClobClient.ClobClient", return_value=mock_client), \
             _p("utils.createClobClient.detect_wallet_signature_type", return_value=(1, "EOA", "0x" + "a" * 40)):
            from config.env import SignatureType, WalletConfig
            mock_bwc.return_value = WalletConfig(
                private_key="0x" + "a" * 64,
                chain_id=137,
                signature_type=SignatureType.EOA,
                funder_address="0x" + "a" * 40,
                polygon_rpc_url="http://localhost",
            )
            with pytest.raises(RuntimeError, match="V2 collateral readiness FAILED"):
                await create_clob_client(private_key="0x" + "a" * 64, config=mock_cfg)

    @pytest.mark.asyncio
    @patch("services.collateral_readiness.check_collateral_readiness")
    async def test_startup_derives_l2_creds_without_create_attempt(self, mock_check: AsyncMock) -> None:
        from config.env import SignatureType, WalletConfig
        from utils.createClobClient import create_clob_client
        from types import SimpleNamespace
        from unittest.mock import patch as _p

        mock_check.return_value = CollateralStatus(
            pusd_balance=Decimal("100"),
            pusd_allowance_ctf=Decimal("1000"),
            wallet_address="0x" + "a" * 40,
            rpc_url="http://rpc",
            ready=True,
            failures=(),
        )
        mock_client = MagicMock()
        mock_creds = MagicMock()
        mock_client.derive_api_key.return_value = mock_creds
        mock_cfg = SimpleNamespace(
            clob=SimpleNamespace(
                clob_v2_ready=True,
            ),
            network=SimpleNamespace(
                pusd_contract_address="",
                collateral_contract_address="",
                ctf_exchange_v2_address="",
            ),
        )

        with _p("utils.createClobClient.build_wallet_config") as mock_bwc, \
             _p("utils.createClobClient.validate_wallet_config"), \
             _p("utils.createClobClient.ClobClient", return_value=mock_client), \
             _p("utils.createClobClient.detect_wallet_signature_type", return_value=(1, "EOA", "0x" + "a" * 40)):
            mock_bwc.return_value = WalletConfig(
                private_key="0x" + "a" * 64,
                chain_id=137,
                signature_type=SignatureType.EOA,
                funder_address="0x" + "a" * 40,
                polygon_rpc_url="http://localhost",
            )

            wrapper = await create_clob_client(private_key="0x" + "a" * 64, config=mock_cfg)

        mock_client.create_api_key.assert_not_called()
        mock_client.create_or_derive_api_key.assert_not_called()
        mock_client.derive_api_key.assert_called_once()
        mock_client.set_api_creds.assert_called_once_with(mock_creds)
        assert wrapper.api_creds is mock_creds
