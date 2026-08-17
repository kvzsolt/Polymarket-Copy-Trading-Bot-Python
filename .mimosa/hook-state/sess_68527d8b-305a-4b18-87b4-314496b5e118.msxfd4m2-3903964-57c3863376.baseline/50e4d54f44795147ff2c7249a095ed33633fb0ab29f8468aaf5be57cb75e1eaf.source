"""Composition-root contract for the optional hybrid Polymarket backend.

``build_user_state`` must keep constructing the official SDK gateway by
default. The venue-backend selector is wired at the same boundary so a single
composition root decides which ``ClobGateway`` the runtime uses, and so the
optional CCXT transport has an owner that can close it at shutdown.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import config.app_config as app_config_mod
import orchestrators.runtime_bootstrap as runtime_bootstrap
from integrations.polymarket.backend_factory import VenueBackend
from integrations.polymarket.clob_v2_gateway import PolymarketGateway
from integrations.polymarket.hybrid_gateway import HybridClobGateway
from tests.config_helpers import load_test_config

_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


def _config(**overrides):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "USER_ADDRESS": _VALID_USER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "PERCENTAGE": "7",
        "BUY_MIN_AMOUNT": "3",
        "BUY_MAX_AMOUNT": "42",
        "MAX_MARKET_EXPOSURE_USDC": "123",
        "SLIPPAGE_TOLERANCE": "1.5",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


def _runtime_user():
    return SimpleNamespace(
        user_id="user-1",
        wallet=SimpleNamespace(
            funder_address=_VALID_FUNDER,
            private_key=_VALID_PRIVATE_KEY,
        ),
    )


@pytest.fixture(autouse=True)
def _no_backend_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default must come from the code, not from an operator's shell."""
    monkeypatch.delenv("PMCOPY_VENUE_BACKEND", raising=False)
    monkeypatch.delenv("PMCOPY_ALLOW_EXPERIMENTAL_CCXT_LIVE", raising=False)


class _Exchange:
    def __init__(self, settings):
        self.settings = settings
        self.options = settings["options"]
        self.close = AsyncMock()
        self.load_api_credentials = AsyncMock(return_value={"apiKey": "derived"})

    def polymarket_order_raw_amounts(self, *_args):
        return {}


def _build_state(config, *, clob_gateway=None):
    with (
        patch("orchestrators.runtime_bootstrap.PositionManager"),
        patch("orchestrators.runtime_bootstrap.ExecutionService"),
    ):
        return runtime_bootstrap.build_user_state(
            _runtime_user(),
            client=object(),
            pool=MagicMock(),
            config=config,
            clob_gateway=clob_gateway,
        )


def test_build_user_state_still_defaults_to_the_official_sdk_gateway() -> None:
    state = _build_state(_config())

    assert isinstance(state.clob_gateway, PolymarketGateway)


def test_build_user_state_uses_an_injected_gateway_for_every_venue_consumer() -> None:
    injected = MagicMock()

    state = _build_state(_config(), clob_gateway=injected)

    assert state.clob_gateway is injected
    assert state.market_data_service._gateway is injected


def test_build_user_state_exposes_one_cached_trading_permission_owner() -> None:
    """Startup and periodic refreshes update the same hot-path permission."""

    with (
        patch("orchestrators.runtime_bootstrap.PositionManager"),
        patch("orchestrators.runtime_bootstrap.ExecutionService") as execution_type,
    ):
        state = runtime_bootstrap.build_user_state(
            _runtime_user(),
            client=object(),
            pool=MagicMock(),
            config=_config(),
            clob_gateway=MagicMock(),
        )

    assert execution_type.call_args.kwargs["trading_permission"] is state.trading_permission


def test_venue_backend_defaults_to_sdk_and_never_constructs_ccxt() -> None:
    constructed = False

    def factory(settings):
        nonlocal constructed
        constructed = True
        return _Exchange(settings)

    runtime_user = _runtime_user()
    bundle = asyncio.run(
        runtime_bootstrap.build_user_venue_backend(
            runtime_user,
            client=object(),
            config=_config(TRADING_MODE="live"),
            exchange_factory=factory,
            ccxt_version="4.5.70",
        )
    )

    assert bundle.backend is VenueBackend.SDK
    assert isinstance(bundle.gateway, PolymarketGateway)
    assert bundle.ccxt_client is None
    assert constructed is False


def test_shadow_backend_keeps_the_sdk_primary_and_owns_only_the_ccxt_transport() -> None:
    runtime_user = _runtime_user()
    bundle = asyncio.run(
        runtime_bootstrap.build_user_venue_backend(
            runtime_user,
            client=object(),
            config=_config(TRADING_MODE="live"),
            backend=VenueBackend.SDK_SHADOW_CCXT,
            exchange_factory=_Exchange,
            ccxt_version="4.5.70",
        )
    )

    assert isinstance(bundle.gateway, HybridClobGateway)
    assert isinstance(bundle.gateway.primary, PolymarketGateway)
    assert bundle.ccxt_client is not None

    # Shutdown must close the optional transport this bundle created and must
    # not reach through to the SDK client the runtime already owns.
    asyncio.run(bundle.close_optional_backend())
    bundle.ccxt_client.exchange.close.assert_awaited_once()


def test_live_ccxt_primary_is_refused_without_the_explicit_experimental_gate() -> None:
    factory = MagicMock()

    with pytest.raises(RuntimeError, match="explicit"):
        asyncio.run(
            runtime_bootstrap.build_user_venue_backend(
                _runtime_user(),
                client=object(),
                config=_config(TRADING_MODE="live"),
                backend=VenueBackend.CCXT,
                exchange_factory=factory,
                ccxt_version="4.5.70",
            )
        )

    factory.assert_not_called()
