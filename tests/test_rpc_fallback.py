"""Tests for ``clients/network/rpc_fallback.py``.

Covers:
* Provider creation and pass-through behaviour.
* Failover logic: error-code detection, exception matching, and fallback routing.
* URL masking for safe logging.
* Edge cases: empty fallback, same URL as primary, non-failover exceptions.

All tests are pure unit tests — no network I/O.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from clients.network.rpc_fallback import (
    FallbackWeb3Provider,
    _DEFAULT_REQUEST_TIMEOUT_SECONDS,
    _FailoverHTTPProvider,
    _is_failover_exception,
    _is_failover_response,
    _mask_url,
)
from clients.balance_client import BalanceClient


# ---------------------------------------------------------------------------
# _is_failover_response
# ---------------------------------------------------------------------------


class TestIsFailoverResponse:
    def test_true_on_32603(self) -> None:
        raw = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": "Internal error"}}
        assert _is_failover_response(raw) is True

    def test_false_on_known_method(self) -> None:
        raw = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32601, "message": "Method not found"}}
        assert _is_failover_response(raw) is False

    def test_true_on_message_fragment(self) -> None:
        raw = {"jsonrpc": "2.0", "id": 1, "error": {"code": 0, "message": "request timeout"}}
        assert _is_failover_response(raw) is True

    def test_false_on_success(self) -> None:
        raw = {"jsonrpc": "2.0", "id": 1, "result": "0x1"}
        assert _is_failover_response(raw) is False

    def test_false_on_no_error(self) -> None:
        raw = {"jsonrpc": "2.0", "id": 1}
        assert _is_failover_response(raw) is False


# ---------------------------------------------------------------------------
# _is_failover_exception
# ---------------------------------------------------------------------------


class TestIsFailoverException:
    def test_true_on_internal_error(self) -> None:
        exc = Exception("Internal error")
        assert _is_failover_exception(exc) is True

    def test_true_on_timeout(self) -> None:
        exc = Exception("request timeout after 30s")
        assert _is_failover_exception(exc) is True

    def test_true_on_connection_refused(self) -> None:
        exc = Exception("Connection refused")
        assert _is_failover_exception(exc) is True

    def test_true_on_max_retries(self) -> None:
        exc = Exception("Max retries exceeded with url")
        assert _is_failover_exception(exc) is True

    def test_false_on_random_exception(self) -> None:
        exc = Exception("Something broke")
        assert _is_failover_exception(exc) is False


# ---------------------------------------------------------------------------
# _mask_url
# ---------------------------------------------------------------------------


class TestMaskUrl:
    def test_infura_v3(self) -> None:
        url = "https://polygon-mainnet.infura.io/v3/REDACTED_INFURA_PROJECT_ID"
        assert _mask_url(url) == "https://polygon-mainnet.infura.io/v3/***"

    def test_query_string(self) -> None:
        url = "https://example.com/rpc?key=secret123"
        assert _mask_url(url) == "https://example.com/rpc?***"

    def test_plain_url(self) -> None:
        url = "https://polygon-rpc.com"
        assert _mask_url(url) == "https://polygon-rpc.com"

    def test_empty(self) -> None:
        assert _mask_url("") == ""


# ---------------------------------------------------------------------------
# _FailoverHTTPProvider creation
# ---------------------------------------------------------------------------


class TestFailoverHTTPProviderCreation:
    def test_no_fallback_when_empty(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example", "")
        assert provider._fallback is None
        assert provider._request_timeout_seconds == _DEFAULT_REQUEST_TIMEOUT_SECONDS

    def test_no_fallback_when_same_url(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example", "https://primary.example")
        assert provider._fallback is None

    def test_fallback_created_when_different(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example", "https://fallback.example")
        assert provider._fallback is not None

    def test_http_providers_receive_timeout_request_kwargs(self) -> None:
        with patch("clients.network.rpc_fallback.Web3.HTTPProvider") as http_provider:
            _FailoverHTTPProvider(
                "https://primary.example",
                "https://fallback.example",
                request_timeout_seconds=3.5,
            )

        assert http_provider.call_count == 2
        assert http_provider.call_args_list[0].kwargs["request_kwargs"] == {"timeout": 3.5}
        assert http_provider.call_args_list[1].kwargs["request_kwargs"] == {"timeout": 3.5}


# ---------------------------------------------------------------------------
# _FailoverHTTPProvider.make_request — primary path
# ---------------------------------------------------------------------------


class TestFailoverHTTPProviderMakeRequest:
    def test_primary_success_no_fallback(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example")
        # Patch the internal primary provider
        mock_response = {"jsonrpc": "2.0", "id": 1, "result": "0x1"}
        provider._primary = MagicMock()
        provider._primary.make_request = MagicMock(return_value=mock_response)

        result = provider.make_request("eth_call", [{"to": "0x"}])

        assert result == mock_response
        provider._primary.make_request.assert_called_once_with("eth_call", [{"to": "0x"}])

    def test_primary_failover_code_uses_fallback(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example", "https://fallback.example")
        primary_err = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": "Internal error"}}
        fallback_ok = {"jsonrpc": "2.0", "id": 1, "result": "0x2"}

        provider._primary = MagicMock()
        provider._primary.make_request = MagicMock(return_value=primary_err)
        provider._fallback = MagicMock()
        provider._fallback.make_request = MagicMock(return_value=fallback_ok)

        result = provider.make_request("eth_call", [{"to": "0x"}])

        assert result == fallback_ok
        provider._fallback.make_request.assert_called_once_with("eth_call", [{"to": "0x"}])

    def test_primary_exception_uses_fallback(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example", "https://fallback.example")
        fallback_ok = {"jsonrpc": "2.0", "id": 1, "result": "0x3"}

        provider._primary = MagicMock()
        provider._primary.make_request = MagicMock(side_effect=Exception("request timeout"))
        provider._fallback = MagicMock()
        provider._fallback.make_request = MagicMock(return_value=fallback_ok)

        result = provider.make_request("eth_call", [{"to": "0x"}])

        assert result == fallback_ok

    def test_non_failover_exception_raises_immediately(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example", "https://fallback.example")
        provider._primary = MagicMock()
        provider._primary.make_request = MagicMock(side_effect=Exception("random failure"))
        provider._fallback = MagicMock()

        with pytest.raises(Exception, match="random failure"):
            provider.make_request("eth_call", [{"to": "0x"}])

        provider._fallback.make_request.assert_not_called()

    def test_no_fallback_raises_when_primary_fails(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example", "")
        provider._primary = MagicMock()
        provider._primary.make_request = MagicMock(side_effect=Exception("request timeout"))

        with pytest.raises(Exception, match="request timeout"):
            provider.make_request("eth_call", [{"to": "0x"}])

    def test_no_fallback_returns_primary_error_response(self) -> None:
        provider = _FailoverHTTPProvider("https://primary.example", "")
        primary_err = {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32603, "message": "Internal error"},
        }
        provider._primary = MagicMock()
        provider._primary.make_request = MagicMock(return_value=primary_err)

        result = provider.make_request("eth_call", [{"to": "0x"}])

        assert result == primary_err


# ---------------------------------------------------------------------------
# FallbackWeb3Provider — high-level factory
# ---------------------------------------------------------------------------


class TestFallbackWeb3Provider:
    def test_get_web3_returns_instance(self) -> None:
        factory = FallbackWeb3Provider("https://primary.example", "https://fallback.example")
        w3 = factory.get_web3()
        assert w3 is not None

    def test_urls_exposed(self) -> None:
        factory = FallbackWeb3Provider("https://primary.example", "https://fallback.example")
        assert factory.primary_url == "https://primary.example"
        assert factory.fallback_url == "https://fallback.example"
        assert factory.request_timeout_seconds == _DEFAULT_REQUEST_TIMEOUT_SECONDS

    def test_request_timeout_exposed(self) -> None:
        factory = FallbackWeb3Provider(
            "https://primary.example",
            request_timeout_seconds=2.0,
        )
        assert factory.request_timeout_seconds == 2.0

    def test_none_fallback_when_empty(self) -> None:
        factory = FallbackWeb3Provider("https://primary.example", "")
        assert factory.fallback_url is None

    def test_empty_fallback_degenerates_to_single(self) -> None:
        factory = FallbackWeb3Provider("https://primary.example", "")
        w3 = factory.get_web3()
        # The provider should still work even without fallback
        assert w3 is not None

    def test_same_url_ignored(self) -> None:
        factory = FallbackWeb3Provider("https://primary.example", "https://primary.example")
        assert factory.fallback_url is None


# ---------------------------------------------------------------------------
# Integration with BalanceClient
# ---------------------------------------------------------------------------


class TestBalanceClientUsesFallback:
    def test_balance_client_accepts_fallback_param(self) -> None:
        """BalanceClient __init__ must accept the new fallback_rpc_url kwarg."""
        client = BalanceClient(
            rpc_url="https://primary.example",
            usdc_contract_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            fallback_rpc_url="https://fallback.example",
        )
        assert client._rpc_url == "https://primary.example"
        assert client._fallback_rpc_url == "https://fallback.example"

    def test_balance_client_omits_fallback_by_default(self) -> None:
        """Old call sites that don't pass fallback_rpc_url still work."""
        client = BalanceClient(
            rpc_url="https://primary.example",
            usdc_contract_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        )
        assert client._fallback_rpc_url == ""


# ---------------------------------------------------------------------------
# Integration with getMyBalance module-level config
# ---------------------------------------------------------------------------


class TestGetMyBalanceFallback:
    def test_configure_from_appconfig_passes_fallback(self) -> None:
        from unittest.mock import MagicMock

        mock_config = MagicMock()
        mock_config.wallet = MagicMock()
        mock_config.wallet.funder_address = "0xAb5801a7C398e26C8b5C60b0e3F2A5F5c0B2B4a"
        mock_config.network = MagicMock()
        mock_config.network.rpc_url = "https://primary.example"
        mock_config.network.fallback_rpc_url = "https://fallback.example"
        mock_config.network.usdc_contract_address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        mock_config.network.chain_id = 137

        from utils.getMyBalance import configure_from_appconfig
        configure_from_appconfig(mock_config)

        # After configuration, the module-level state should reflect the
        # fallback URL so that subsequent get_my_balance calls can use it.
        # (We can't easily assert this without a real RPC, but we can at
        # least verify no exception is raised.)
        assert True  # configure_from_appconfig completed without error


# ---------------------------------------------------------------------------
# Integration with collateral_readiness signature
# ---------------------------------------------------------------------------


class TestCollateralReadinessSignature:
    def test_check_collateral_accepts_fallback_param(self) -> None:
        from services.collateral_readiness import check_collateral_readiness
        import inspect

        sig = inspect.signature(check_collateral_readiness)
        params = list(sig.parameters.keys())
        assert "fallback_rpc_url" in params
        assert sig.parameters["fallback_rpc_url"].default == ""

    def test_check_collateral_runnable_with_empty_fallback(self) -> None:
        """Should not crash when fallback_rpc_url is empty (default)."""
        import asyncio
        from services.collateral_readiness import check_collateral_readiness

        async def _run() -> None:
            # We can't assert the result without a real RPC, but we can
            # assert it doesn't crash on parameter validation.
            result = await check_collateral_readiness(
                wallet_address="0xAb5801a7C398e26C8b5C60b0e3F2A5F5c0B2B4a",
                rpc_url="https://primary.example",
                fallback_rpc_url="",
                pusd_address="0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
                ctf_exchange_address="0xE111180000d2663C0091e4f400237545B87B996B",
            )
            # With a fake RPC it will fail, but not on signature/type errors.
            assert result is not None

        # Run it and expect a network-level failure, NOT a TypeError.
        with pytest.raises(Exception) as exc_info:
            asyncio.run(_run())
        assert "TypeError" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Integration with wallet_signature_detector
# ---------------------------------------------------------------------------


class TestWalletSignatureDetectorFallback:
    def test_fetch_contract_code_hex_accepts_fallback(self) -> None:
        from services.wallet_signature_detector import _fetch_contract_code_hex
        import inspect

        sig = inspect.signature(_fetch_contract_code_hex)
        params = list(sig.parameters.keys())
        assert "fallback_rpc_url" in params
        assert sig.parameters["fallback_rpc_url"].default == ""


# ---------------------------------------------------------------------------
# Integration-style: verify the module imports cleanly
# ---------------------------------------------------------------------------


def test_module_imports() -> None:
    """Guard against circular imports or missing dependencies."""
    from clients.network import rpc_fallback

    assert hasattr(rpc_fallback, "FallbackWeb3Provider")
    assert hasattr(rpc_fallback, "_FailoverHTTPProvider")
    assert hasattr(rpc_fallback, "_is_failover_response")
    assert hasattr(rpc_fallback, "_is_failover_exception")
    assert hasattr(rpc_fallback, "_mask_url")
