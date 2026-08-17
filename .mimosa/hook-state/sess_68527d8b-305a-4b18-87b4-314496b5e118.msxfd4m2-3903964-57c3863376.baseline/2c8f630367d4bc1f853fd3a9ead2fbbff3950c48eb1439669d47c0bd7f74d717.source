"""Fail-closed contracts for an unavailable mandatory CCXT runtime."""

from __future__ import annotations

import builtins
import importlib
from types import SimpleNamespace

import pytest

import integrations.polymarket.ccxt_prediction_client as client_mod
from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionUnavailable,
    create_ccxt_prediction_client,
)
from integrations.polymarket.backend_factory import VenueBackend
from integrations.polymarket.runtime_gateway import build_runtime_polymarket_gateway


class SdkGateway:
    """Minimal official-gateway sentinel for backend composition."""


@pytest.fixture
def ccxt_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make all dynamic CCXT imports fail deterministically."""

    real_import_module = importlib.import_module
    real_import = builtins.__import__

    def fake_import_module(name: str, *args: object, **kwargs: object) -> object:
        """Reject CCXT while delegating every unrelated import."""

        if name == "ccxt" or name.startswith("ccxt."):
            raise ImportError(f"No module named {name!r}")
        return real_import_module(name, *args, **kwargs)

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        """Reject direct CCXT imports while preserving Python import behavior."""

        if name == "ccxt" or name.startswith("ccxt."):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(client_mod.importlib, "import_module", fake_import_module)
    monkeypatch.setattr(builtins, "__import__", fake_import)


@pytest.mark.asyncio
async def test_missing_ccxt_raises_actionable_error(ccxt_absent: None) -> None:
    """Identify a broken runtime dependency without an opaque import error."""

    with pytest.raises(CcxtPredictionUnavailable, match="runtime requirements"):
        await create_ccxt_prediction_client(authenticated=False)


@pytest.mark.asyncio
async def test_runtime_factory_fails_closed_when_ccxt_is_unavailable(
    ccxt_absent: None,
) -> None:
    """Refuse an explicit CCXT selection without affecting the SDK default."""

    config = SimpleNamespace(
        execution=SimpleNamespace(
            trading_mode="paper",
            allow_experimental_ccxt_live=False,
        ),
        wallet=SimpleNamespace(private_key="", funder_address=""),
    )
    with pytest.raises(CcxtPredictionUnavailable):
        await build_runtime_polymarket_gateway(
            sdk_gateway=SdkGateway(),
            config=config,
            wallet_config=config.wallet,
            backend=VenueBackend.CCXT,
        )


@pytest.mark.asyncio
async def test_sdk_default_does_not_import_missing_ccxt(ccxt_absent: None) -> None:
    """An unavailable optional adapter cannot break default SDK startup."""

    sdk = SdkGateway()
    bundle = await build_runtime_polymarket_gateway(
        sdk_gateway=sdk,
        config=SimpleNamespace(execution=SimpleNamespace(trading_mode="paper")),
    )

    assert bundle.gateway is sdk
    assert bundle.ccxt_client is None
